mod args;
mod aws;
mod context;
mod error;
mod feedback;
mod glue;
mod highlight;
mod kafka;
mod msk_iam_context;
mod payload;
mod schema_registry;

use std::cmp::min;
use std::collections::HashSet;
use std::error::Error;
use std::fs::File;
use std::io;
use std::io::{BufWriter, Write};

pub type Result<T> = core::result::Result<T, Box<dyn Error + Send + Sync>>;
pub(crate) type CoreResult<T> = core::result::Result<T, Box<dyn Error>>;

use crate::args::{Args, Authentication, Password};
use crate::aws::create_credentials_provider;
use crate::context::KiekContext;
use crate::error::KiekError;
use crate::feedback::Feedback;
use crate::glue::GlueSchemaRegistryFacade;
use crate::highlight::Highlighting;
use crate::kafka::{
    assign_partition_for_key, assign_topic_or_partition, format_timestamp, list_topics,
    partition_for_plain_key, seek_start_offsets, select_topic_or_partition, Assigment,
    FormatBootstrapServers, TopicOrPartition, DEFAULT_BROKER_STRING, DEFAULT_PORT,
};
use crate::payload::{format_payload, parse_payload, Payload};
use crate::schema_registry::SchemaRegistryFacade;
use chrono::{DateTime, Local};
use clap::CommandFactory;
use clap_complete::{generate, Shell};
use futures::FutureExt;
use log::{debug, error, info, trace, LevelFilter};
use rdkafka::consumer::{Consumer, ConsumerContext, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::BorrowedMessage;
use rdkafka::{Message, TopicPartitionList};
use reachable::TcpTarget;
use simple_logger::SimpleLogger;
use std::net::{SocketAddr, TcpStream};
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;

pub async fn start() -> Result<()> {
    let args = Args::validated().await;

    configure_logging(args.verbose, args.colors());

    match run(args).await {
        Err(e) => Args::fail(e),
        Ok(_) => Ok(()),
    }
}

///
/// Set up the Kafka consumer, create schema registry facade and continue with connecting.
///
async fn run(args: Args) -> Result<()> {
    debug!("{:?}", args);

    let highlighting = args.highlighting();

    let feedback = args.feedback();

    let credentials = credentials(&args, &feedback)?;

    let authentication = args.authentication();

    info!("Authentication mechanism {authentication:?} with credentials {credentials:?}.");

    let bootstrap_servers = args.bootstrap_servers();

    feedback.info(
        "Connecting",
        format!(
            "to Kafka cluster at {}",
            FormatBootstrapServers(&bootstrap_servers)
        ),
    );

    verify_connection(&bootstrap_servers, highlighting).await?;

    feedback.info("Set up", "authentication");

    let (credentials_provider, profile, region) = create_credentials_provider(
        args.profile.clone(),
        args.region.clone(),
        args.role_arn.clone(),
    )
    .await;

    let glue_schema_registry_facade =
        GlueSchemaRegistryFacade::new(credentials_provider.clone(), region.clone(), &feedback);

    let schema_registry_facade = args
        .schema_registry_url()
        .map(|url| SchemaRegistryFacade::new(url, credentials.clone(), &feedback));

    match authentication {
        Authentication::MskIam => {
            let consumer = kafka::create_msk_consumer(
                &bootstrap_servers,
                credentials_provider.clone(),
                profile.clone(),
                region.clone(),
                args.no_ssl,
                &feedback,
            )
            .await?;

            if args.list {
                list_topics(&consumer, &feedback).await
            } else {
                connect(
                    args,
                    &feedback,
                    glue_schema_registry_facade,
                    schema_registry_facade,
                    consumer,
                )
                .await
            }
        }
        _ => {
            let consumer = kafka::create_consumer(
                &bootstrap_servers,
                authentication,
                credentials,
                args.no_ssl,
            )
            .await?;

            if args.list {
                list_topics(&consumer, &feedback).await
            } else {
                connect(
                    args,
                    &feedback,
                    glue_schema_registry_facade,
                    schema_registry_facade,
                    consumer,
                )
                .await
            }
        }
    }
}

///
/// Connect to the broker, assign partition(s) and delegate to consuming messages.
///
async fn connect<C>(
    args: Args,
    feedback: &Feedback,
    glue_schema_registry_facade: GlueSchemaRegistryFacade,
    schema_registry_facade: Option<SchemaRegistryFacade>,
    consumer: StreamConsumer<C>,
) -> Result<()>
where
    C: KiekContext + 'static,
{
    kafka::connect(&consumer, feedback.highlighting).await?;

    let topic_or_partition: TopicOrPartition = match &args.topic_or_partition {
        Some(topic_or_partition) => topic_or_partition.clone(),
        None => select_topic_or_partition(&consumer, feedback).await?,
    };

    let start_offset = args.start_offset();

    feedback.info("Assigning", "partitions");

    let assignment = match &args.key {
        Some(key) => {
            assign_partition_for_key(&consumer, &topic_or_partition, key, start_offset, feedback)
                .await?
        }
        None => {
            assign_topic_or_partition(&consumer, &topic_or_partition, start_offset, feedback)
                .await?
        }
    };

    let start_date = Local::now();

    if let Some(time_definition) = &args.from {
        feedback.info("Fetching", "start offsets");
        let start = time_definition.resolve_from(start_date);
        info!("Starting to read from {start}.");
        seek_start_offsets(&consumer, start).await?;
    }

    let max_date = args.to.clone().map(|time_definition| {
        let end = time_definition.resolve_to(start_date);
        info!("Reading until {end}.");
        end
    });

    consume(
        args,
        consumer,
        assignment,
        glue_schema_registry_facade,
        schema_registry_facade,
        feedback,
        start_date,
        max_date,
    )
    .await
}

///
/// Consume messages from the Kafka topic or partition and print them to stdout.
///
async fn consume<Ctx>(
    args: Args,
    consumer: StreamConsumer<Ctx>,
    assigment: Assigment,
    glue_schema_registry_facade: GlueSchemaRegistryFacade,
    schema_registry_facade: Option<SchemaRegistryFacade>,
    feedback: &Feedback,
    start_date: DateTime<Local>,
    max_date: Option<DateTime<Local>>,
) -> Result<()>
where
    Ctx: ConsumerContext + 'static,
{
    let mut received_messages: usize = 0;

    // collect the partitions that are paused because they reached the max. timestamp
    let mut paused = HashSet::<i32>::new();

    feedback.info("Consuming", "messages");

    let mut reconnects = 0;

    loop {
        let result = read_batch(
            &args,
            &consumer,
            &assigment,
            &glue_schema_registry_facade,
            schema_registry_facade.as_ref(),
            &feedback,
            &start_date,
            &max_date,
            &mut received_messages,
            &mut paused,
        )
        .await;
        match result {
            Ok(_) => {
                reconnects = 0;
            }
            Err(e) => {
                if e.to_string() == KiekError::BROKER_TRANSPORT_FAILURE {
                    reconnects += 1;
                    if reconnects > 25 {
                        return Err(KiekError::new("Too many reconnection attempts. Aborting."));
                    } else if reconnects >= 4 {
                        feedback.info(
                            "Reconnecting",
                            format!("to broker in {reconnects}th attempt"),
                        );
                    } else {
                        feedback.info("Reconnecting", "to broker");
                    }
                    let backoff = min(5000, 50 * 2u64.pow(reconnects));
                    let backoff = Duration::from_millis(backoff);
                    sleep(backoff).await;
                } else {
                    return Err(e);
                }
            }
        }

        if received_messages >= args.max() {
            break Ok(());
        }

        if consumer.assignment()?.count() == paused.len() {
            info!(
                "All {} partitions are paused. Reached the max. date for all of them.",
                paused.len()
            );
            // reached the max. timestamps for all partitions
            break Ok(());
        }
    }
}

///
/// Read a batch of messages from the Kafka topic or partition.
/// Keeps track of the number of messages received and the partitions that are paused.
/// A partition is paused if the consumer has reached the `to` timestamp for that partition.
///
async fn read_batch<Ctx>(
    args: &Args,
    consumer: &StreamConsumer<Ctx>,
    assigment: &Assigment,
    glue_schema_registry_facade: &GlueSchemaRegistryFacade,
    schema_registry_facade: Option<&SchemaRegistryFacade>,
    feedback: &&Feedback,
    start_date: &DateTime<Local>,
    max_date: &Option<DateTime<Local>>,
    received_messages: &mut usize,
    paused_partitions: &mut HashSet<i32>,
) -> Result<()>
where
    Ctx: ConsumerContext + 'static,
{
    // Await the next message which is most likely the beginning of a new batch
    let awaited_record = consumer.recv().await;

    let mut lock = io::stdout().lock();
    let mut out = BufWriter::new(&mut lock);

    *received_messages += process_record(
        args,
        consumer,
        &mut out,
        awaited_record,
        assigment,
        paused_partitions,
        glue_schema_registry_facade,
        schema_registry_facade,
        feedback,
        start_date,
        max_date,
    )
    .await?;

    // As long as there are messages in the batch, process them immediately without writing to the terminal
    while *received_messages < args.max() {
        match consumer.recv().now_or_never() {
            None => break,
            Some(record) => {
                *received_messages += process_record(
                    args,
                    &consumer,
                    &mut out,
                    record,
                    assigment,
                    paused_partitions,
                    glue_schema_registry_facade,
                    schema_registry_facade,
                    feedback,
                    start_date,
                    max_date,
                )
                .await?;
            }
        }
    }

    if *received_messages > 0 && args.filtering() {
        feedback.info("Consumed", format!("{received_messages} messages"));
    }

    io::stdout().flush()?;
    Ok(())
}

async fn process_record<'a, Ctx>(
    args: &Args,
    consumer: &StreamConsumer<Ctx>,
    out: &mut BufWriter<impl Write>,
    record: core::result::Result<BorrowedMessage<'a>, KafkaError>,
    assigment: &Assigment,
    paused: &mut HashSet<i32>,
    glue_schema_registry_facade: &GlueSchemaRegistryFacade,
    schema_registry_facade: Option<&SchemaRegistryFacade>,
    feedback: &&Feedback,
    start_date: &DateTime<Local>,
    max_date: &Option<DateTime<Local>>,
) -> Result<usize>
where
    Ctx: ConsumerContext + 'static,
{
    match record {
        Err(KafkaError::MessageConsumption(RDKafkaErrorCode::GroupAuthorizationFailed)) => {
            // This error is expected when the consumer group is not authorized to commit offsets which isn't supported anyway
            trace!("Consumer group is not authorized to commit offsets.");
            Ok(0)
        }
        Err(other) => {
            error!("Received error during polling: {:?}", other);
            Err(other.into())
        }

        Ok(message) => {
            // If max date is set, check if the message is younger than the max date.
            // If so, pause the partition from consuming more messages.
            if let Some(max_date) = max_date {
                if paused.contains(&message.partition()) {
                    debug!(
                        "Skipping message of paused partition {}.",
                        message.partition()
                    );
                    return Ok(1);
                } else if let Some(message_time) = message.timestamp().to_millis() {
                    if message_time > max_date.timestamp_millis() {
                        println!(
                            "{} {} {:?} vs. {} => {}",
                            message.partition(),
                            message.offset(),
                            message.timestamp(),
                            max_date.timestamp_millis(),
                            message_time - max_date.timestamp_millis()
                        );

                        info!(
                            "Message is younger than max date. Pausing partition {}.",
                            message.partition()
                        );
                        paused.insert(message.partition());
                        let mut list = TopicPartitionList::with_capacity(paused.len());
                        let topic = assigment.topic_or_partition.topic();
                        for partition in paused.iter() {
                            list.add_partition(topic, *partition);
                        }
                        consumer.pause(&list)?;
                        return Ok(1);
                    }
                }
            }

            let key = String::from_utf8_lossy(message.key().unwrap_or(&[])).to_string();

            // Skip messages that don't match the key if a key is scanned for
            match &args.key {
                Some(search_key) if !search_key.eq(&key) => {
                    // but check if the key is in the correct partition
                    if message.key().is_some() {
                        // otherwise there is a wrong assumption about the partitioning
                        let expected_partition = partition_for_plain_key(
                            message.key().unwrap(),
                            assigment.num_partitions,
                        );
                        if message.partition() != expected_partition {
                            return Err(KiekError::new(format!("The topic {bold}{topic}{bold:#} is not partitioned by the standard partitioner using murmur2 hashes.\nkiek cannot identify the correct partition for key {bold}{search_key}{bold:#}.\nYou can pass the specific partition as {bold}{topic}-p{bold:#} or use {bold}-f{bold:#}, {bold}--filter {search_key}{bold:#} instead.", topic = assigment.topic_or_partition.topic(), bold = feedback.highlighting.bold)));
                        }
                    }

                    debug!("Skipping message with key {key}.");
                    return Ok(1);
                }
                _ => {}
            }

            let key = parse_payload(
                message.key(),
                glue_schema_registry_facade,
                schema_registry_facade,
                feedback.highlighting,
            )
            .await?;

            let payload = parse_payload(
                message.payload(),
                glue_schema_registry_facade,
                schema_registry_facade,
                feedback.highlighting,
            )
            .await?;

            // Skip messages that don't contain the filter if configured
            match &args.filter {
                Some(filter)
                    if !(check_filter(message.key(), &key, args.indent, filter)
                        || check_filter(message.payload(), &payload, args.indent, filter)) =>
                {
                    debug!("Skipping message that does not match filter.");
                    return Ok(1);
                }
                _ => {}
            }

            let key = format_payload(&key, args.indent, feedback.highlighting);

            let payload = format_payload(&payload, args.indent, feedback.highlighting);

            let partition_style = feedback.highlighting.partition(message.partition());
            let partition_style_bold = feedback.highlighting.partition_bold(message.partition());
            let partition_style_dimmed =
                feedback.highlighting.partition_dimmed(message.partition());

            let topic = message.topic();
            let partition = message.partition();
            let offset = message.offset();

            let timestamp =
                format_timestamp(&message.timestamp(), start_date, feedback.highlighting)
                    .unwrap_or("".to_string());

            feedback.clear();

            writeln!(out, "{partition_style}{topic}{partition_style:#}{partition_style_dimmed}-{partition_style_dimmed:#}{partition_style}{partition}{partition_style:#} {timestamp} {partition_style_bold}{offset}{partition_style_bold:#} {key} {payload}")?;
            Ok(1)
        }
    }
}

/// In verbose mode, logs everything in the main module at the debug level, and everything else at the info level.
/// In non-verbose mode, logging is turned off.
fn configure_logging(verbose: bool, colors: bool) {
    if verbose {
        SimpleLogger::new()
            .with_colors(colors)
            .with_level(LevelFilter::Info)
            .with_module_level(module_path!(), LevelFilter::Debug)
            .with_local_timestamps()
            .init()
            .unwrap();
    } else {
        SimpleLogger::new()
            .with_level(LevelFilter::Off)
            .init()
            .unwrap();
    }
}

///
/// If username is provided, password is required.
/// If password is missing ask for it in interactive mode or fail.
///
fn credentials(args: &Args, feedback: &Feedback) -> Result<Option<(String, Password)>> {
    match (args.username(), args.password()) {
        (Some(username), Some(password)) =>
            Ok(Some((username, password))), // credentials are passed
        (Some(username), _) if feedback.interactive => {
            let password =
                dialoguer::Password::new()
                    .with_prompt(format!("Enter password for user {bold}{username}{bold:#}", bold = feedback.highlighting.bold))
                    .interact()?;
            Ok(Some((username, password.parse::<Password>()?)))
        }
        (Some(username), _) => {
            Err(KiekError::new(format!("Password is required for user {username}. Use {bold}--pw, --password{bold:#} or {bold}-u {username}:<PASSWORD>{bold:#}.", bold = feedback.highlighting.bold)))
        }
        _ => Ok(None), // No credentials required
    }
}

/// Timeout to connect to a broker
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

///
/// Verify that the broker is reachable.
///
async fn verify_connection(bootstrap_servers: &str, highlighting: &Highlighting) -> Result<()> {
    let first_server = bootstrap_servers.split(',').next().unwrap();
    // Add default port if not set
    let first_server = if first_server.contains(':') {
        first_server.to_string()
    } else {
        format!("{first_server}:{DEFAULT_PORT}")
    };
    match TcpTarget::from_str(&first_server) {
        Err(e) => {
            error!("Could not parse broker address {first_server}: {e:?}");
            Err(KiekError::from(e))
        }
        Ok(target) => {
            info!("Resolving {}", target.get_fqhn());
            match target.get_resolve_policy().resolve(target.get_fqhn()) {
                Err(e) => {
                    error!("Could not resolve {}: {e}", target.get_fqhn());
                    Err(KiekError::new(format!(
                        "Failed to resolve broker address {first_server}."
                    )))
                }
                Ok(addrs) => {
                    let mut attempt_addrs = addrs.clone();
                    attempt_addrs.sort(); // IpV4 first
                    info!("Checking {attempt_addrs:?} for reachability.");
                    let available = attempt_addrs
                        .into_iter()
                        .map(|addr| SocketAddr::from((addr, *target.get_portnumber())))
                        .take(2)
                        .find(|addr| TcpStream::connect_timeout(addr, CONNECT_TIMEOUT).is_ok());

                    match available {
                        Some(addr) => {
                            info!("Reached {addr}.");
                            Ok(())
                        }
                        None => {
                            error!("Could not reach {addrs:?}.");
                            if bootstrap_servers == DEFAULT_BROKER_STRING {
                                Err(KiekError::new(format!("Failed to connect to Kafka cluster at {DEFAULT_BROKER_STRING}. Use {bold}-b, --bootstrap-servers{bold:#} to configure.", bold = highlighting.bold)))
                            } else {
                                Err(KiekError::new(format!(
                                    "Failed to connect to Kafka cluster at {}.",
                                    FormatBootstrapServers(bootstrap_servers)
                                )))
                            }
                        }
                    }
                }
            }
        }
    }
}

fn check_filter(raw_value: Option<&[u8]>, value: &Payload, indent: bool, filter: &String) -> bool {
    let raw_check = raw_value
        .map(|raw_payload| String::from_utf8_lossy(raw_payload).contains(filter))
        .unwrap_or(false);
    raw_check
        || format!("{}", format_payload(value, indent, Highlighting::plain())).contains(filter)
}

///
/// Generate shell completions for Zsh, Bash and Fish.
/// This function is called when the program is invoked with the single argument "completions".
///
pub fn check_completions() {
    let mut args = std::env::args();
    if args.len() == 2 && args.nth(1).unwrap() == "completions" {
        const NAME: &str = env!("CARGO_PKG_NAME");
        // ensure the target directory exists
        std::fs::create_dir_all("completions").unwrap();
        generate(
            Shell::Zsh,
            &mut Args::command(),
            NAME,
            &mut File::create(format!("completions/_{NAME}")).unwrap(),
        );
        generate(
            Shell::Bash,
            &mut Args::command(),
            NAME,
            &mut File::create(format!("completions/{NAME}.bash")).unwrap(),
        );
        generate(
            Shell::Fish,
            &mut Args::command(),
            NAME,
            &mut File::create(format!("completions/{NAME}.fish")).unwrap(),
        );
        std::process::exit(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_verify_connection() {
        let h = Highlighting::plain();

        assert!(verify_connection("foo", h).await.is_err());
        assert!(verify_connection("foo:xs", h).await.is_err());
        assert!(verify_connection("foo:123456", h).await.is_err());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        assert!(verify_connection(&format!("127.0.0.1:{port}"), h)
            .await
            .is_ok());

        drop(listener); // disconnect

        assert!(verify_connection(&format!("127.0.0.1:{port}"), h)
            .await
            .is_err());

        assert!(verify_connection("www.google.de:443", h).await.is_ok());
    }

    #[test]
    fn test_check_filter() {
        assert!(check_filter(
            Some(b"\"foo\": \"foo\""),
            &Payload::Null,
            false,
            &"\": \"".to_string()
        ));

        assert!(check_filter(
            Some(&[]),
            &Payload::Null,
            false,
            &"null".to_string()
        ));
        assert!(!check_filter(
            Some(&[]),
            &Payload::Null,
            false,
            &"foo".to_string()
        ));

        assert!(check_filter(
            Some(&[]),
            &Payload::String("foo".to_string()),
            false,
            &"foo".to_string()
        ));
        assert!(check_filter(
            Some(&[]),
            &Payload::String("foo".to_string()),
            false,
            &"fo".to_string()
        ));
        assert!(check_filter(
            Some(&[]),
            &Payload::String("foo".to_string()),
            false,
            &"oo".to_string()
        ));
        assert!(!check_filter(
            Some(&[]),
            &Payload::String("foo".to_string()),
            false,
            &"\"foo\"".to_string()
        ));

        assert!(check_filter(
            Some(&[]),
            &Payload::Json(serde_json::Value::String("foo".to_string())),
            false,
            &"\"foo\"".to_string()
        ));
        assert!(check_filter(
            Some(&[]),
            &Payload::Json(serde_json::Value::String("foo".to_string())),
            false,
            &"\"f".to_string()
        ));
        assert!(check_filter(
            Some(&[]),
            &Payload::Json(serde_json::Value::String("foo".to_string())),
            false,
            &"oo\"".to_string()
        ));
        assert!(check_filter(
            Some(&[]),
            &Payload::Json(serde_json::Value::String("foo".to_string())),
            false,
            &"foo".to_string()
        ));

        assert!(check_filter(
            Some(&[]),
            &Payload::Avro(apache_avro::types::Value::String("foo".to_string())),
            false,
            &"\"foo\"".to_string()
        ));
        assert!(check_filter(
            Some(&[]),
            &Payload::Avro(apache_avro::types::Value::String("foo".to_string())),
            false,
            &"\"f".to_string()
        ));
        assert!(check_filter(
            Some(&[]),
            &Payload::Avro(apache_avro::types::Value::String("foo".to_string())),
            false,
            &"oo\"".to_string()
        ));
        assert!(check_filter(
            Some(&[]),
            &Payload::Avro(apache_avro::types::Value::String("foo".to_string())),
            false,
            &"foo".to_string()
        ));
    }
}

mod args;
mod aws;
mod context;
mod exception;
mod feedback;
mod glue;
mod highlight;
mod kafka;
mod msk_iam_context;
mod payload;

use std::error::Error;
use std::io;
use std::io::Write;

pub type Result<T> = core::result::Result<T, Box<dyn Error + Send + Sync>>;
pub(crate) type CoreResult<T> = core::result::Result<T, Box<dyn Error>>;

use crate::args::{Args, Authentication, Password};
use crate::aws::create_credentials_provider;
use crate::context::KiekContext;
use crate::exception::KiekException;
use crate::feedback::Feedback;
use crate::glue::GlueSchemaRegistryFacade;
use crate::highlight::Highlighting;
use crate::kafka::{
    assign_partition_for_key, assign_topic_or_partition, format_timestamp,
    select_topic_or_partition, FormatBootstrapServers, TopicOrPartition, DEFAULT_BROKER_STRING,
    DEFAULT_PORT,
};
use crate::payload::{format_payload, parse_payload};
use chrono::{DateTime, Local};
use futures::FutureExt;
use log::{debug, error, info, trace, LevelFilter};
use rdkafka::consumer::{ConsumerContext, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;
use reachable::TcpTarget;
use simple_logger::SimpleLogger;
use std::net::{SocketAddr, TcpStream};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

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

    verify_connection(&bootstrap_servers, &highlighting).await?;

    feedback.info("Set up", "authentication");

    let (credentials_provider, profile, region) = create_credentials_provider(
        args.profile.clone(),
        args.region.clone(),
        args.role_arn.clone(),
    )
    .await;

    let glue_schema_registry_facade =
        GlueSchemaRegistryFacade::new(credentials_provider.clone(), region.clone(), &feedback);

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
            connect(args, &feedback, glue_schema_registry_facade, consumer).await
        }
        _ => {
            let consumer = kafka::create_consumer(
                &bootstrap_servers,
                authentication,
                credentials,
                args.no_ssl,
            )
            .await?;
            connect(args, &feedback, glue_schema_registry_facade, consumer).await
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
    consumer: StreamConsumer<C>,
) -> Result<()>
where
    C: KiekContext + 'static,
{
    kafka::connect(&consumer, &feedback.highlighting).await?;

    let topic_or_partition: TopicOrPartition = match &args.topic_or_partition {
        Some(topic_or_partition) => topic_or_partition.clone(),
        None => select_topic_or_partition(&consumer, feedback).await?,
    };

    let start_offset = args.start_offset();

    feedback.info("Assigning", "partitions");

    match &args.key {
        Some(key) => {
            assign_partition_for_key(&consumer, &topic_or_partition, key, start_offset, feedback)
                .await?;
        }
        None => {
            assign_topic_or_partition(&consumer, &topic_or_partition, start_offset, feedback)
                .await?;
        }
    }

    consume(args, consumer, glue_schema_registry_facade, feedback).await
}

///
/// Consume messages from the Kafka topic or partition and print them to stdout.
///
async fn consume<Ctx>(
    args: Args,
    consumer: StreamConsumer<Ctx>,
    glue_schema_registry_facade: GlueSchemaRegistryFacade,
    feedback: &Feedback,
) -> Result<()>
where
    Ctx: ConsumerContext + 'static,
{
    let start_date = chrono::Local::now();
    let mut received_messages: usize = 0;

    feedback.info("Consuming", "messages");

    loop {
        // Await the next message which is most likely the beginning of a new batch
        let awaited_record = consumer.recv().await;

        // Buffer the output of the batch
        let buffer = Arc::new(Mutex::new(Vec::<u8>::with_capacity(128 * 1024)));

        received_messages += process_record(
            &args,
            buffer.clone(),
            awaited_record,
            &glue_schema_registry_facade,
            &feedback,
            &start_date,
        )
        .await?;

        // As long as there are messages in the batch, process them immediately without writing to the terminal
        loop {
            match consumer.recv().now_or_never() {
                None => break,
                Some(record) => {
                    received_messages += process_record(
                        &args,
                        buffer.clone(),
                        record,
                        &glue_schema_registry_facade,
                        &feedback,
                        &start_date,
                    )
                    .await?;
                }
            }
        }

        io::stdout().write_all(buffer.lock().unwrap().as_slice())?;

        // If scanning for a key print the no. of consumed messages to indicate consumption
        if received_messages > 0 && args.key.is_some() {
            feedback.info("Consumed", format!("{received_messages} messages"));
        }

        io::stdout().flush()?;
    }
}

async fn process_record<'a>(
    args: &Args,
    buffer: Arc<Mutex<Vec<u8>>>,
    record: core::result::Result<BorrowedMessage<'a>, KafkaError>,
    glue_schema_registry_facade: &GlueSchemaRegistryFacade,
    feedback: &&Feedback,
    start_date: &DateTime<Local>,
) -> Result<usize> {
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
            let key = String::from_utf8_lossy(message.key().unwrap_or(&[])).to_string();

            // Skip messages that don't match the key if a key is scanned for
            match &args.key {
                Some(search_key) if !search_key.eq(&key) => {
                    return Ok(1);
                }
                _ => {}
            }

            let value = parse_payload(message.payload(), glue_schema_registry_facade).await?;
            let value = format_payload(&value, &feedback.highlighting);

            let partition_style = feedback.highlighting.partition(message.partition());
            let partition_style_bold = partition_style.bold();
            let separator_style = partition_style.dimmed();

            let topic = message.topic();
            let partition = message.partition();
            let offset = message.offset();

            let timestamp =
                format_timestamp(&message.timestamp(), start_date, &feedback.highlighting)
                    .unwrap_or("".to_string());

            feedback.clear();

            writeln!(buffer.lock().unwrap(), "{partition_style}{topic}{partition_style:#}{separator_style}-{separator_style:#}{partition_style}{partition}{partition_style:#} {timestamp} {partition_style_bold}{offset}{partition_style_bold:#} {key} {value}")?;
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
            Err(KiekException::new(format!("Password is required for user {username}. Use {bold}--pw, --password{bold:#} or {bold}-u {username}:<PASSWORD>{bold:#}.", bold = feedback.highlighting.bold)))
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
            Err(KiekException::from(e))
        }
        Ok(target) => {
            info!("Resolving {}", target.get_fqhn());
            match target.get_resolve_policy().resolve(target.get_fqhn()) {
                Err(e) => {
                    error!("Could not resolve {}: {e}", target.get_fqhn());
                    Err(KiekException::new(format!(
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
                                Err(KiekException::new(format!("Failed to connect to Kafka cluster at {DEFAULT_BROKER_STRING}. Use {bold}-b, --bootstrap-servers{bold:#} to configure.", bold = highlighting.bold)))
                            } else {
                                Err(KiekException::new(format!(
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn test_verify_connection() {
        let h = Highlighting::plain();

        assert!(verify_connection("foo", &h).await.is_err());
        assert!(verify_connection("foo:xs", &h).await.is_err());
        assert!(verify_connection("foo:123456", &h).await.is_err());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        assert!(verify_connection(&format!("127.0.0.1:{port}"), &h)
            .await
            .is_ok());

        drop(listener); // disconnect

        assert!(verify_connection(&format!("127.0.0.1:{port}"), &h)
            .await
            .is_err());

        assert!(verify_connection("www.google.de:443", &h).await.is_ok());
    }
}

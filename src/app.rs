use crate::aws::create_credentials_provider;
use crate::exception::KiekException;
use crate::feedback::Feedback;
use crate::glue::GlueSchemaRegistryFacade;
use crate::highlight::Highlighting;
use crate::kafka::{assign_partition_for_key, assign_topic_or_partition, connect, format_timestamp, select_topic_or_partition, FormatBootstrapServers, TopicOrPartition, DEFAULT_BROKER_STRING, DEFAULT_PORT};
use crate::payload::{format_payload, parse_payload};
use crate::{kafka, Result};
use log::{debug, error, info, trace, LevelFilter};
use rdkafka::consumer::{ConsumerContext, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::Message;
use reachable::TcpTarget;
use simple_logger::SimpleLogger;
use std::net::{SocketAddr, TcpStream};
use std::str::FromStr;
use std::time::Duration;
use crate::args::{Args, Password};

pub async fn run() -> Result<()> {
    let args = Args::validated().await;

    configure_logging(args.verbose, args.colors());

    match setup(args).await {
        Err(e) => {
            Args::fail(e)
        }
        Ok(_) => {
            Ok(())
        }
    }
}

///
/// Set up the Kafka consumer, connect, create schema registry facade, assign partitions and
/// delegate to consuming messages.
///
async fn setup(args: Args) -> Result<()> {
    debug!("{:?}", args);

    let highlighting = args.highlighting();

    let feedback = args.feedback();

    let credentials = credentials(&args, &feedback)?;

    let authentication = args.authentication(credentials);

    info!("Authentication mechanism: {:?}", authentication);

    let bootstrap_servers = args.bootstrap_servers();

    feedback.info("Connecting", format!("to Kafka cluster at {}", FormatBootstrapServers(&bootstrap_servers)));

    verify_connection(&bootstrap_servers, &highlighting).await?;

    feedback.info("Set up", "authentication");

    let (credentials_provider, profile, region) = create_credentials_provider(args.profile.clone(), args.region.clone(), args.role_arn.clone()).await;

    let glue_schema_registry_facade = GlueSchemaRegistryFacade::new(credentials_provider.clone(), region.clone(), &feedback);

    let consumer = kafka::create_msk_consumer(&bootstrap_servers, credentials_provider.clone(), profile.clone(), region.clone(), &feedback).await?;

    connect(&consumer, &highlighting).await?;

    let topic_or_partition: TopicOrPartition =
        match &args.topic_or_partition {
            Some(topic_or_partition) => topic_or_partition.clone(),
            None => select_topic_or_partition(&consumer, &feedback).await?
        };

    let start_offset = args.start_offset();

    feedback.info("Assigning", "partitions");

    match &args.key {
        Some(key) => {
            assign_partition_for_key(&consumer, &topic_or_partition, key, start_offset, &feedback).await?;
        }
        None => {
            assign_topic_or_partition(&consumer, &topic_or_partition, start_offset, &feedback).await?;
        }
    }

    consume(args, consumer, glue_schema_registry_facade, &feedback).await
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
                    .allow_empty_password(true)
                    .interact()?;
            Ok(Some((username, password.parse::<Password>()?)))
        }
        (Some(username), _) => {
            Err(KiekException::new(format!("Password is required for user {username}. Use {bold}--pw, --password{bold:#} or {bold}-u {username}:<PASSWORD>{bold:#}.", bold = feedback.highlighting.bold)))
        }
        _ => Ok(None), // No credentials required
    }
}

///
/// Consume messages from the Kafka topic or partition and print them to stdout.
///
async fn consume<Ctx>(args: Args, consumer: StreamConsumer<Ctx>, glue_schema_registry_facade: GlueSchemaRegistryFacade, feedback: &Feedback) -> Result<()>
where
    Ctx: ConsumerContext + 'static,
{
    let start_date = chrono::Local::now();
    let mut received_messages: usize = 0;

    feedback.info("Consuming", "messages");

    loop {
        match consumer.recv().await {
            Err(KafkaError::MessageConsumption(RDKafkaErrorCode::GroupAuthorizationFailed)) => {
                // This error is expected when the consumer group is not authorized to commit offsets which isn't supported anyway
                trace!("Consumer group is not authorized to commit offsets.");
            }
            Err(other) => {
                error!("Received error during polling: {:?}", other);
                return Err(other.into());
            }

            Ok(message) => {
                received_messages += 1;
                let key = String::from_utf8_lossy(message.key().unwrap_or(&[])).to_string();

                // Skip messages that don't match the key if a key is scanned for
                match &args.key {
                    Some(search_key) if !search_key.eq(&key) => {
                        feedback.info("Consumed", format!("{received_messages} messages"));
                        continue;
                    }
                    _ => {}
                }

                let value = parse_payload(message.payload(), &glue_schema_registry_facade).await;
                let value = format_payload(&value, &feedback.highlighting);

                let partition_style = feedback.highlighting.partition(message.partition());
                let partition_style_bold = partition_style.bold();
                let separator_style = partition_style.dimmed();

                let topic = message.topic();
                let partition = message.partition();
                let offset = message.offset();

                let timestamp = format_timestamp(&message.timestamp(), &start_date, &feedback.highlighting).unwrap_or("".to_string());

                feedback.clear();
                println!("{partition_style}{topic}{partition_style:#}{separator_style}-{separator_style:#}{partition_style}{partition}{partition_style:#} {timestamp} {partition_style_bold}{offset}{partition_style_bold:#} {key} {value}");
            }
        }
    }
}


/// In verbose mode, logs everything in the main module at the debug level, and everything else at the info level.
/// In non-verbose mode, logging is turned off.
fn configure_logging(verbose: bool, colors: bool) {
    if verbose {
        SimpleLogger::new().with_colors(colors).with_level(LevelFilter::Info).with_module_level(module_path!(), LevelFilter::Debug).init().unwrap();
    } else {
        SimpleLogger::new().with_level(LevelFilter::Off).init().unwrap();
    }
}

/// Timeout to connect to a broker
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

///
/// Verify that the broker is reachable.
///
async fn verify_connection(bootstrap_servers: &String, highlighting: &Highlighting) -> Result<()> {
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
                    Err(KiekException::new(format!("Failed to resolve broker address {first_server}.")))
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
                                Err(KiekException::new(format!("Failed to connect to Kafka cluster at {}.", FormatBootstrapServers(bootstrap_servers))))
                            }
                        }
                    }
                }
            }
        }
    }
}

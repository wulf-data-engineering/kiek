use crate::aws::create_credentials_provider;
use crate::feedback::Feedback;
use crate::glue::GlueSchemaRegistryFacade;
use crate::highlight::Highlighting;
use crate::kafka::{assign_partition_for_key, assign_topic_or_partition, connect, format_timestamp, select_topic_or_partition, FormatBootstrapServers, StartOffset, TopicOrPartition, DEFAULT_BROKER_STRING, DEFAULT_PORT};
use crate::payload::{format_payload, parse_payload};
use crate::{kafka, Result};
use clap::error::ErrorKind;
use clap::{command, CommandFactory, Parser};
use lazy_static::lazy_static;
use log::{debug, error, info, trace, LevelFilter};
use rdkafka::consumer::{ConsumerContext, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::Message;
use reachable::TcpTarget;
use regex::Regex;
use simple_logger::SimpleLogger;
use std::error::Error;
use std::io::IsTerminal;
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::str::FromStr;
use std::time::Duration;

#[derive(Parser, Debug)]
/// kiek (/ˈkiːk/ - Nothern German for Look!) helps you to look into Kafka topics, especially, if
/// they are in AWS MSK behind IAM authentication or even a role to assume and contain AVRO encoded
/// messages with schemas in AWS Glue Schema Registry.
/// kiek analyzes the message payloads in a topic, if necessary looks up corresponding AVRO schemas
/// and prints the payloads to stdout in a human-readable format.
#[command(version, about)]
struct Args {
    /// Kafka topic/partition name
    ///
    /// If a topic name is provided, kiek will consume from all partitions.
    /// If a topic/partition (e.g. "topic-0") is provided, kiek will consume from the specific partition.
    #[arg(value_name = "TOPIC/PARTITION")]
    topic_or_partition: Option<TopicOrPartition>,

    /// Kafka cluster broker string
    ///
    /// The Kafka cluster to connect to. Default is 127.0.0.1:9092.
    #[arg(short, long, value_name = "127.0.0.1:9092", aliases = ["brokers", "broker-string"])]
    bootstrap_servers: Option<String>,

    /// Optional start offset for the consumer: earliest, latest or a negative integer.
    ///
    /// If not set, the consumer will start from the earliest offset on localhost brokers and
    /// latest on remote brokers.
    ///
    /// Please note: in compacted topics the n latest offsets may not exist!
    #[arg(
        group = "start-offset",
        short,
        long,
        value_parser = clap::value_parser!(StartOffset),
        value_name = "earliest|latest|-n"
    )]
    offset: Option<StartOffset>,

    /// Optional key to search scanning just the partition with the key
    ///
    /// If set, only messages with the given key are printed.
    /// kiek will just scan the partition that contains the key.
    /// Please note: this works only for the default partitioner.
    #[arg(short, long)]
    key: Option<String>,

    /// Optional specific AWS profile to use
    ///
    /// If not set, $AWS_PROFILE or otherwise "default" is used.
    #[arg(short, long, value_name = "default")]
    profile: Option<String>,

    /// Optional specific AWS region to use
    ///
    /// If not set, $AWS_REGION or otherwise "eu-central-1" is used.
    #[arg(short, long, value_name = "eu-central-1")]
    region: Option<String>,

    /// Optional AWS role to assume to connect the Kafka cluster and Glue Schema Registry.
    ///
    /// If set, the AWS credentials provider will assume the role.
    #[arg(long)]
    role_arn: Option<String>,

    /// Short option for --offset=earliest
    ///
    /// Start from the beginning of the topic
    #[arg(group = "start-offset", short, long, action, aliases = ["beginning", "from-beginning"])]
    earliest: bool,

    /// Short option for --offset=latest
    ///
    /// Start from the end of the topic and wait for just new messages
    #[arg(group = "start-offset", short, long, action)]
    latest: bool,

    /// Deactivates syntax highlighting
    ///
    /// As a default kiek uses syntax highlighting for better readability when run in terminals.
    /// This option deactivates it, e.g. for piping the output to another program or file or running in CI.
    #[arg(long, action, aliases = ["plain"])]
    no_colors: bool,

    /// Omit everything but the Kafka messages
    ///
    /// If set, kiek omits all progress indicators, warnings and just prints the Kafka messages.
    /// Dialogs like asking for a topic name or schema registry URL are replaced with fatal errors.
    #[arg(group = "verbosity", short, long, action)]
    silent: bool,

    /// Activates logging on stdout
    #[arg(group = "verbosity", short, long, action)]
    verbose: bool,
}

pub async fn run() -> Result<()> {
    let args = Args::parse();

    configure_logging(args.verbose, args.no_colors);

    let highlighting =
        if args.no_colors || !std::io::stdout().is_terminal() {
            Highlighting::plain()
        } else {
            Highlighting::colors()
        };

    match setup(args, &highlighting).await {
        Err(e) => {
            let mut cmd = Args::command();
            cmd.error(ErrorKind::Io, e.to_string()).exit();
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
async fn setup(args: Args, highlighting: &Highlighting) -> Result<()> {
    debug!("{:?}", args);

    let feedback = Feedback::prepare(highlighting, args.silent);

    let bootstrap_servers = args.bootstrap_servers.clone().unwrap_or(DEFAULT_BROKER_STRING.to_string());

    feedback.info("Connecting", format!("to Kafka cluster at {}", FormatBootstrapServers(&bootstrap_servers)));

    verify_connection(&bootstrap_servers, &highlighting).await?;

    feedback.info("Set up", "authentication");

    let (credentials_provider, profile, region) = create_credentials_provider(args.profile.clone(), args.region.clone(), args.role_arn.clone()).await;

    let glue_schema_registry_facade = GlueSchemaRegistryFacade::new(credentials_provider.clone(), region.clone(), &feedback);

    let consumer = kafka::create_msk_consumer(&bootstrap_servers, credentials_provider.clone(), profile.clone(), region.clone(), &feedback).await?;

    connect(&consumer, &bootstrap_servers, &highlighting).await?;

    let topic_or_partition: TopicOrPartition =
        match &args.topic_or_partition {
            Some(topic_or_partition) => topic_or_partition.clone(),
            None => select_topic_or_partition(&consumer, &feedback).await?
        };

    let start_offset = calc_start_offset(&args, &bootstrap_servers);

    feedback.info("Assigning", "partitions");

    match &args.key {
        Some(key) => {
            assign_partition_for_key(&consumer, &topic_or_partition, key, start_offset, &feedback).await?;
        }
        None => {
            assign_topic_or_partition(&consumer, &topic_or_partition, start_offset, &feedback).await?;
        }
    }

    consume(args, consumer, glue_schema_registry_facade, &highlighting, &feedback).await
}

///
/// Consume messages from the Kafka topic or partition and print them to stdout.
///
async fn consume<Ctx>(args: Args, consumer: StreamConsumer<Ctx>, glue_schema_registry_facade: GlueSchemaRegistryFacade, highlighting: &Highlighting, feedback: &Feedback) -> Result<()>
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
                let value = format_payload(&value, highlighting);

                let partition_style = highlighting.partition(message.partition());
                let partition_style_bold = partition_style.bold();
                let separator_style = partition_style.dimmed();

                let topic = message.topic();
                let partition = message.partition();
                let offset = message.offset();

                let timestamp = format_timestamp(&message.timestamp(), &start_date, highlighting).unwrap_or("".to_string());

                feedback.clear();
                println!("{partition_style}{topic}{partition_style:#}{separator_style}-{separator_style:#}{partition_style}{partition}{partition_style:#} {timestamp} {partition_style_bold}{offset}{partition_style_bold:#} {key} {value}");
            }
        }
    }
}


/// In verbose mode, logs everything in the main module at the debug level, and everything else at the info level.
/// In non-verbose mode, logging is turned off.
fn configure_logging(verbose: bool, no_colors: bool) {
    if verbose {
        let colors = !no_colors;
        SimpleLogger::new().with_colors(colors).with_level(LevelFilter::Info).with_module_level(module_path!(), LevelFilter::Debug).init().unwrap();
    } else {
        SimpleLogger::new().with_level(LevelFilter::Off).init().unwrap();
    }
}

lazy_static! {
    static ref TOPIC_PARTITION_REGEX: Regex = Regex::new(r"^(.+)-([0-9]+)$").unwrap();
}

///
/// clap parser for topic names and topic-partition names
///
impl FromStr for TopicOrPartition {
    type Err = String;

    fn from_str(string: &str) -> std::result::Result<Self, Self::Err> {
        match TOPIC_PARTITION_REGEX.captures(string) {
            Some(captures) => {
                let topic = captures.get(1).unwrap().as_str().to_string();
                let partition = captures.get(2).unwrap().as_str().parse().unwrap();
                Ok(TopicOrPartition::TopicPartition(topic, partition))
            }
            None => Ok(TopicOrPartition::Topic(string.to_string()))
        }
    }
}

const INVALID_OFFSET: &str = "Offsets must be 'earliest', 'latest' or a negative integer.";

///
/// clap parser for Kafka consumer start offsets
/// * earliest/beginning: start from the beginning of the topic
/// * latest/end: start from current offset, that is, waits for future records
/// * negative integer: start from the latest n messages for each partition
///
impl FromStr for StartOffset {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "earliest" | "beginning" => Ok(StartOffset::Earliest),
            "latest" | "end" => Ok(StartOffset::Latest(0)),
            s => {
                let offset: i64 = s.parse().map_err(|_| INVALID_OFFSET)?;
                if offset >= 0 {
                    Err(INVALID_OFFSET.to_string())
                } else {
                    Ok(StartOffset::Latest(-1 * offset))
                }
            }
        }
    }
}

///
/// Calculates the start offset for the consumer based on configuration:
///
/// - `--earliest` beats `--latest` beats `--offset`
/// - If no offset is set, `--earliest` is used for local brokers, `--latest` for remote brokers
///
fn calc_start_offset(args: &Args, bootstrap_servers: &String) -> StartOffset {
    if args.earliest {
        StartOffset::Earliest
    } else if args.latest {
        StartOffset::Latest(0)
    } else {
        match &args.offset {
            Some(offset) => offset.clone(),
            None if is_local(bootstrap_servers) => StartOffset::Earliest,
            None => StartOffset::Latest(0)
        }
    }
}

///
/// Check if the given bootstrap servers are local
///
fn is_local(bootstrap_servers: &String) -> bool {
    match bootstrap_servers.split(':').next() {
        None => false,
        Some(host) => {
            if host == "localhost" {
                true
            } else {
                match IpAddr::from_str(host) {
                    Ok(ip) => ip.is_loopback(),
                    Err(_) => false
                }
            }
        }
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
            Err(KiekException::boxed(e.to_string()))
        }
        Ok(target) => {
            info!("Resolving {}", target.get_fqhn());
            match target.get_resolve_policy().resolve(target.get_fqhn()) {
                Err(e) => {
                    error!("Could not resolve {}: {e}", target.get_fqhn());
                    Err(KiekException::boxed(format!("Failed to resolve broker address {first_server}.")))
                }
                Ok(addrs) => {
                    let mut attempt_addrs = addrs.clone();
                    attempt_addrs.sort(); // IpV4 first
                    info!("Checking {attempt_addrs:?} for reachability.");
                    let available = attempt_addrs
                        .into_iter()
                        .map(|addr| SocketAddr::from((addr, *target.get_portnumber())))
                        .take(2)
                        .find(|addr| TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT).is_ok());

                    match available {
                        Some(addr) => {
                            info!("Reached {addr}.");
                            Ok(())
                        }
                        None => {
                            error!("Could not reach {addrs:?}.");
                            if bootstrap_servers == DEFAULT_BROKER_STRING {
                                Err(KiekException::boxed(format!("Failed to connect to Kafka cluster at {DEFAULT_BROKER_STRING}. Use {bold}-b, --bootstrap-servers{bold:#} to configure.", bold = highlighting.bold)))
                            } else {
                                Err(KiekException::boxed(format!("Failed to connect to Kafka cluster at {}.", FormatBootstrapServers(bootstrap_servers))))
                            }
                        }
                    }
                }
            }
        }
    }
}


///
/// `KiekException` is a custom error type that can be used to wrap any error message
///
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct KiekException {
    pub message: String,
}

impl KiekException {
    pub fn boxed<S: Into<String>>(message: S) -> Box<dyn Error + Send + Sync> {
        Box::new(Self { message: message.into() })
    }
}

impl std::fmt::Display for KiekException {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for KiekException {}

// Test the arg parser
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_args_parser() {
        let args = Args::parse_from(&["kiek", "test-topic", "--bootstrap-servers", "localhost:9092"]);
        assert_eq!(args.topic_or_partition, Some(TopicOrPartition::Topic("test-topic".to_string())));
        assert_eq!(args.bootstrap_servers, Some("localhost:9092".to_string()));
        assert_eq!(args.key, None);
        assert_eq!(args.profile, None);
        assert_eq!(args.region, None);
        assert_eq!(args.role_arn, None);
        assert_eq!(args.verbose, false);
        assert_eq!(args.offset, None);

        let args = Args::parse_from(&["kiek", "test-topic", "--bootstrap-servers", "localhost:9092", "--profile", "test-profile", "--region", "us-west-1", "--verbose", "--offset", "earliest", "--key", "test-key"]);
        assert_eq!(args.topic_or_partition, Some(TopicOrPartition::Topic("test-topic".to_string())));
        assert_eq!(args.bootstrap_servers, Some("localhost:9092".to_string()));
        assert_eq!(args.key, Some("test-key".to_string()));
        assert_eq!(args.profile, Some("test-profile".to_string()));
        assert_eq!(args.region, Some("us-west-1".to_string()));
        assert_eq!(args.role_arn, None);
        assert_eq!(args.verbose, true);
        assert_eq!(args.offset, Some(StartOffset::Earliest));

        let args = Args::parse_from(&["kiek", "test-topic-1", "--bootstrap-servers", "localhost:9092", "--profile", "test-profile", "--region", "us-west-1", "--role-arn", "arn:aws:iam::123456789012:role/test-role", "--offset=-3", "-k", "test-key"]);
        assert_eq!(args.topic_or_partition, Some(TopicOrPartition::TopicPartition("test-topic".to_string(), 1)));
        assert_eq!(args.bootstrap_servers, Some("localhost:9092".to_string()));
        assert_eq!(args.key, Some("test-key".to_string()));
        assert_eq!(args.profile, Some("test-profile".to_string()));
        assert_eq!(args.region, Some("us-west-1".to_string()));
        assert_eq!(args.role_arn, Some("arn:aws:iam::123456789012:role/test-role".to_string()));
        assert_eq!(args.verbose, false);
        assert_eq!(args.offset, Some(StartOffset::Latest(3)));
    }

    #[test]
    fn test_max_one_offset_configuration() {
        assert!(Args::try_parse_from(&["kiek", "test-topic", "--offset=latest", "--offset=earliest"]).is_err());
        assert!(Args::try_parse_from(&["kiek", "test-topic", "--latest", "--earliest"]).is_err());
        assert!(Args::try_parse_from(&["kiek", "test-topic", "--offset=earliest", "--earliest"]).is_err());
        assert!(Args::try_parse_from(&["kiek", "test-topic", "--offset=-1", "--latest"]).is_err());
    }

    #[test]
    fn test_topic_partition_parser() {
        assert_eq!(TopicOrPartition::from_str("test-topic"), Ok(TopicOrPartition::Topic("test-topic".to_string())));
        assert_eq!(TopicOrPartition::from_str("test-topic-0"), Ok(TopicOrPartition::TopicPartition("test-topic".to_string(), 0)));
        assert_eq!(TopicOrPartition::from_str("test-topic-1"), Ok(TopicOrPartition::TopicPartition("test-topic".to_string(), 1)));
        assert_eq!(TopicOrPartition::from_str("test-topic-10"), Ok(TopicOrPartition::TopicPartition("test-topic".to_string(), 10)));
        assert_eq!(TopicOrPartition::from_str("test-topic-"), Ok(TopicOrPartition::Topic("test-topic-".to_string())));
        assert_eq!(TopicOrPartition::from_str("test-topic-abc"), Ok(TopicOrPartition::Topic("test-topic-abc".to_string())));
    }

    #[test]
    fn test_offsets_parser() {
        assert_eq!(StartOffset::from_str("earliest"), Ok(StartOffset::Earliest));
        assert_eq!(StartOffset::from_str("beginning"), Ok(StartOffset::Earliest));
        assert_eq!(StartOffset::from_str("latest"), Ok(StartOffset::Latest(0)));
        assert_eq!(StartOffset::from_str("end"), Ok(StartOffset::Latest(0)));
        assert_eq!(StartOffset::from_str("-1"), Ok(StartOffset::Latest(1)));
        assert_eq!(StartOffset::from_str("-10"), Ok(StartOffset::Latest(10)));
        assert_eq!(StartOffset::from_str("0"), Err(INVALID_OFFSET.to_string()));
        assert_eq!(StartOffset::from_str("1"), Err(INVALID_OFFSET.to_string()));
        assert_eq!(StartOffset::from_str("latest1"), Err(INVALID_OFFSET.to_string()));
    }

    #[test]
    fn test_local_check() {
        assert_eq!(is_local(&"localhost:9092".to_string()), true);
        assert_eq!(is_local(&"localhost:9092,localhost:19092".to_string()), true);
        assert_eq!(is_local(&"127.0.0.1:9092".to_string()), true);
        assert_eq!(is_local(&"127.0.0.1:9092,127.0.0.1:19092".to_string()), true);

        assert_eq!(is_local(&"123.542.123.123:9092,123.542.123.124:9092".to_string()), false);
        assert_eq!(is_local(&"b-1-public.backendintegrationv.ww63wt.c1.kafka.eu-central-1.amazonaws.com:9198".to_string()), false);
    }
}
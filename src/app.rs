use std::net::IpAddr;
use crate::aws::create_credentials_provider;
use crate::glue::GlueSchemaRegistryFacade;
use crate::highlight::{format, partition_color, partition_color_bold, SEPARATOR};
use crate::kafka::{assign_partition_for_key, assign_topic_or_partition, format_timestamp, StartOffset, TopicOrPartition};
use crate::payload::{parse_payload, render_payload};
use crate::{kafka, Result};
use clap::Parser;
use log::{debug, error, info, trace, LevelFilter};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::Message;
use simple_logger::SimpleLogger;
use std::str::FromStr;
use lazy_static::lazy_static;
use regex::Regex;

#[derive(Parser, Debug)]
/// kiek (/ˈkiːk/ - Nothern German for Look!) helps you to look into Kafka topics, especially, if
/// they are in AWS MSK behind IAM authentication or even a role to assume and contain AVRO encoded
/// messages with schemas in AWS Glue Schema Registry.
/// kiek analyzes the message payloads in a topic, if necessary looks up corresponding AVRO schemas
/// and prints the payloads to stdout in a human-readable format.
#[command(version, about, long_about = None)]
struct Args {
    /// Kafka topic/partition name
    ///
    /// If a topic name is provided, kiek will consume from all partitions.
    /// If a topic/partition (e.g. "topic-0") is provided, kiek will consume from the specific partition.
    #[arg(value_name = "TOPIC/PARTITION")]
    topic_or_partition: TopicOrPartition,

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
    /// As a default kiek uses syntax highlighting for better readability.
    /// This option deactivates it, e.g. for piping the output to another program or file.
    #[arg(long, action)]
    plain: bool,

    /// Activates logging on stdout
    #[arg(short, long, action)]
    verbose: bool,
}

pub async fn run() -> Result<()> {
    let args = Args::parse();

    configure_logging(args.verbose);

    debug!("{:?}", args);

    let (credentials_provider, region) = create_credentials_provider(args.profile, args.region, args.role_arn).await;

    let glue_schema_registry_facade = GlueSchemaRegistryFacade::new(credentials_provider.clone(), region.clone());

    let bootstrap_servers = args.bootstrap_servers.unwrap_or("127.0.0.1:9092".to_string());

    let consumer = kafka::create_msk_consumer(&bootstrap_servers, credentials_provider.clone(), region.clone()).await?;

    let start_offset =
        if args.earliest {
            StartOffset::Earliest
        } else if args.latest {
            StartOffset::Latest(0)
        } else {
            match args.offset {
                Some(offset) => offset,
                None if is_local(&bootstrap_servers) => StartOffset::Latest(0),
                None => StartOffset::Earliest
            }
        };

    info!("Starting consumer with offset: {:?}", start_offset);

    match &args.key {
        Some(key) => {
            assign_partition_for_key(&consumer, &args.topic_or_partition, key, start_offset).await?;
        }
        None => {
            assign_topic_or_partition(&consumer, &args.topic_or_partition, start_offset).await?;
        }
    }

    let highlight = !args.plain;

    debug!("Use syntax highlighting: {}", highlight);

    let start_date = chrono::Local::now();

    debug!("Starting at {}", start_date);

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
                let key = String::from_utf8_lossy(message.key().unwrap_or(&[])).to_string();

                // Skip messages that don't match the key if a key is scanned for
                match &args.key {
                    Some(search_key) if !search_key.eq(&key) => continue,
                    _ => {}
                }

                let value = parse_payload(message.payload(), &glue_schema_registry_facade).await;

                let rendered = render_payload(&value, highlight);

                let partition_color = partition_color(message.partition());
                let partition_color_bold = partition_color_bold(message.partition());

                let timestamp = format_timestamp(&message.timestamp(), &start_date, highlight).unwrap_or("".to_string());

                let topic = format(message.topic(), partition_color, highlight);
                let separator = format("-", SEPARATOR, highlight);
                let partition = format(message.partition().to_string(), partition_color, highlight);
                let offset = format(message.offset().to_string(), partition_color_bold, highlight);

                println!("{topic}{separator}{partition} {timestamp} {offset} {key} {rendered}");
            }
        }
    }
}

/// In verbose mode, logs everything in the main module at the debug level, and everything else at the info level.
/// In non-verbose mode, logging is turned off.
fn configure_logging(verbose: bool) {
    if verbose {
        SimpleLogger::new().with_colors(true).with_level(LevelFilter::Info).with_module_level(module_path!(), LevelFilter::Debug).init().unwrap();
    } else {
        SimpleLogger::new().with_level(LevelFilter::Off).init().unwrap();
    }
}

lazy_static! {
    static ref TOPIC_PARTITION_REGEX: Regex = Regex::new(r"^(.+)-([0-9]+)$").unwrap();
}

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

// Test the arg parser
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_args_parser() {
        let args = Args::parse_from(&["kiek", "test-topic", "--bootstrap-servers", "localhost:9092"]);
        assert_eq!(args.topic_or_partition, TopicOrPartition::Topic("test-topic".to_string()));
        assert_eq!(args.bootstrap_servers, Some("localhost:9092".to_string()));
        assert_eq!(args.key, None);
        assert_eq!(args.profile, None);
        assert_eq!(args.region, None);
        assert_eq!(args.role_arn, None);
        assert_eq!(args.verbose, false);
        assert_eq!(args.offset, None);

        let args = Args::parse_from(&["kiek", "test-topic", "--bootstrap-servers", "localhost:9092", "--profile", "test-profile", "--region", "us-west-1", "--verbose", "--offset", "earliest", "--key", "test-key"]);
        assert_eq!(args.topic_or_partition, TopicOrPartition::Topic("test-topic".to_string()));
        assert_eq!(args.bootstrap_servers, Some("localhost:9092".to_string()));
        assert_eq!(args.key, Some("test-key".to_string()));
        assert_eq!(args.profile, Some("test-profile".to_string()));
        assert_eq!(args.region, Some("us-west-1".to_string()));
        assert_eq!(args.role_arn, None);
        assert_eq!(args.verbose, true);
        assert_eq!(args.offset, Some(StartOffset::Earliest));

        let args = Args::parse_from(&["kiek", "test-topic-1", "--bootstrap-servers", "localhost:9092", "--profile", "test-profile", "--region", "us-west-1", "--role-arn", "arn:aws:iam::123456789012:role/test-role", "--offset=-3", "-k", "test-key"]);
        assert_eq!(args.topic_or_partition, TopicOrPartition::TopicPartition("test-topic".to_string(), 1));
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
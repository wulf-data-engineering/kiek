use crate::aws::create_credentials_provider;
use crate::glue::GlueSchemaRegistryFacade;
use crate::highlight::{format, partition_color, partition_color_bold, RESET, SEPARATOR};
use crate::kafka::{assign_all_partitions, assign_partition_for_key, format_timestamp, StartOffset};
use crate::payload::{parse_payload, render_payload};
use crate::{kafka, Result};
use clap::Parser;
use log::{debug, error, trace, LevelFilter};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::Message;
use simple_logger::SimpleLogger;
use std::str::FromStr;

#[derive(Parser, Debug)]
/// kiek (/ˈkiːk/ - Nothern German for Look!) helps you to look into Kafka topics, especially, if
/// they are in AWS MSK behind IAM authentication or even a role to assume and contain AVRO encoded
/// messages with schemas in AWS Glue Schema Registry.
/// kiek analyzes the message payloads in a topic, if necessary looks up corresponding AVRO schemas
/// and prints the payloads to stdout in a human-readable format.
#[command(version, about, long_about = None)]
struct Args {
    /// Kafka cluster broker string
    #[arg(short, long)]
    bootstrap_servers: String,

    /// Kafka topic name
    #[arg(short, long)]
    topic: String,

    /// Optional start offset for the consumer: earliest, latest or a negative integer;
    /// Please note: in compacted topics the n latest messages may not exist.
    #[arg(
        short,
        long,
        value_parser = clap::value_parser!(StartOffset),
        value_name = "earliest|latest|-n"
    )]
    offset: Option<StartOffset>,

    /// Optional key to search (scans just the partition with the key)
    #[arg(short, long)]
    key: Option<String>,

    /// Optional specific AWS profile to use
    #[arg(short, long, value_name = "default")]
    profile: Option<String>,

    /// Optional specific AWS region to use
    #[arg(short, long, value_name = "eu-central-1")]
    region: Option<String>,

    /// Optional AWS role to assume to connect the Kafka cluster and Glue Schema Registry
    #[arg(long)]
    role_arn: Option<String>,

    /// Short option for --offset=earliest
    #[arg(short, long, action)]
    earliest: bool,

    /// Short option for --offset=latest
    #[arg(short, long, action)]
    latest: bool,

    /// Deactivates syntax highlighting
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

    let consumer = kafka::create_msk_consumer(args.bootstrap_servers, credentials_provider.clone(), region.clone()).await?;

    let start_offset =
        if args.earliest {
            StartOffset::Earliest
        } else if args.latest {
            StartOffset::Latest(0)
        } else {
            args.offset.unwrap_or(StartOffset::Latest(0))
        };

    match &args.key {
        Some(key) => {
            assign_partition_for_key(&consumer, &args.topic, key, start_offset).await?;
        }
        None => {
            assign_all_partitions(&consumer, &args.topic, start_offset).await?;
        }
    }

    let highlight = !args.plain;

    let start_date = chrono::Local::now();

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

                println!("{topic}{separator}{partition} {timestamp} {offset} {rendered}");
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


// Test the arg parser
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_args_parser() {
        let args = Args::parse_from(&["kiek", "--bootstrap-servers", "localhost:9092", "--topic", "test-topic"]);
        assert_eq!(args.bootstrap_servers, "localhost:9092");
        assert_eq!(args.topic, "test-topic");
        assert_eq!(args.key, None);
        assert_eq!(args.profile, None);
        assert_eq!(args.region, None);
        assert_eq!(args.role_arn, None);
        assert_eq!(args.verbose, false);
        assert_eq!(args.offset, None);

        let args = Args::parse_from(&["kiek", "--bootstrap-servers", "localhost:9092", "--topic", "test-topic", "--profile", "test-profile", "--region", "us-west-1", "--verbose", "--offset", "earliest", "--key", "test-key"]);
        assert_eq!(args.bootstrap_servers, "localhost:9092");
        assert_eq!(args.topic, "test-topic");
        assert_eq!(args.key, Some("test-key".to_string()));
        assert_eq!(args.profile, Some("test-profile".to_string()));
        assert_eq!(args.region, Some("us-west-1".to_string()));
        assert_eq!(args.role_arn, None);
        assert_eq!(args.verbose, true);
        assert_eq!(args.offset, Some(StartOffset::Earliest));

        let args = Args::parse_from(&["kiek", "--bootstrap-servers", "localhost:9092", "--topic", "test-topic", "--profile", "test-profile", "--region", "us-west-1", "--role-arn", "arn:aws:iam::123456789012:role/test-role", "--offset=-3", "-k", "test-key"]);
        assert_eq!(args.bootstrap_servers, "localhost:9092");
        assert_eq!(args.topic, "test-topic");
        assert_eq!(args.key, Some("test-key".to_string()));
        assert_eq!(args.profile, Some("test-profile".to_string()));
        assert_eq!(args.region, Some("us-west-1".to_string()));
        assert_eq!(args.role_arn, Some("arn:aws:iam::123456789012:role/test-role".to_string()));
        assert_eq!(args.verbose, false);
        assert_eq!(args.offset, Some(StartOffset::Latest(3)));
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
}
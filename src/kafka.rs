use crate::args::{Authentication, Password};
use crate::context::{DefaultKiekContext, KiekContext};
use crate::error::KiekError;
use crate::feedback::Feedback;
use crate::highlight::Highlighting;
use crate::msk_iam_context::IamContext;
use crate::Result;
use aws_sdk_sts::config::SharedCredentialsProvider;
use aws_types::region::Region;
use chrono::{DateTime, Local};
use dialoguer::theme::Theme;
use dialoguer::Select;
use futures::FutureExt;
use levenshtein::levenshtein;
use log::{debug, info};
use murmur2::{murmur2, KAFKA_SEED};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::metadata::Metadata;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::{Offset, Timestamp};
use std::collections::HashMap;
use std::time::Duration;
use termion::clear;

pub(crate) const DEFAULT_PORT: i32 = 9092;
pub(crate) const DEFAULT_BROKER_STRING: &str = "127.0.0.1:9092";

/// Timeout for the Kafka metadata operations
const TIMEOUT: Duration = Duration::from_secs(2);

///
/// Create basic client configuration with the provided bootstrap servers
///
pub fn create_config<S: Into<String>>(bootstrap_servers: S) -> ClientConfig {
    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers", bootstrap_servers);
    client_config.set("group.id", "kieker"); // required but irrelevant for simple consumer
    client_config.set("enable.auto.commit", "false"); // never commit offsets
    client_config.set_log_level(rdkafka::config::RDKafkaLogLevel::Debug); // required for capturing log messages
    client_config
}

///
/// Create a Kafka consumer to connect to a Kafka cluster without or with username/password SASL
/// authentication.
///
pub async fn create_consumer(
    bootstrap_servers: &String,
    authentication: Authentication,
    credentials: Option<(String, Password)>,
    no_ssl: bool,
) -> Result<StreamConsumer<DefaultKiekContext>> {
    let mut client_config = create_config(bootstrap_servers);

    if authentication != Authentication::None {
        let mechanism = match authentication {
            Authentication::Plain => "PLAIN",
            Authentication::Sha256 => "SCRAM-SHA-256",
            Authentication::Sha512 => "SCRAM-SHA-512",
            _ => unreachable!(),
        };

        if !no_ssl {
            client_config.set("security.protocol", "SASL_SSL");
            info!("Using SASL_SSL as security protocol.");
        }

        client_config.set("sasl.mechanism", mechanism);
        info!("Using {mechanism} as SASL mechanism.");

        let (username, password) = credentials.ok_or(KiekError::new(
            "No credentials provided for SASL authentication.",
        ))?;
        client_config.set("sasl.username", &username);
        client_config.set("sasl.password", password.plain());
    } else {
        info!("Using default (PLAINTEXT) security protocol and no credentials.");
    }

    let context = DefaultKiekContext::new();

    Ok(client_config.create_with_context(context)?)
}

///
/// Create a Kafka consumer to connect to an MSK cluster with IAM authentication.
///
pub async fn create_msk_consumer(
    bootstrap_servers: &String,
    credentials_provider: SharedCredentialsProvider,
    profile: String,
    region: Region,
    no_ssl: bool,
    feedback: &Feedback,
) -> Result<StreamConsumer<IamContext>> {
    let mut client_config = create_config(bootstrap_servers);
    if !no_ssl {
        client_config.set("security.protocol", "SASL_SSL");
        info!("Using SASL_SSL as security protocol.");
    }
    client_config.set("sasl.mechanism", "OAUTHBEARER");

    let context = IamContext::new(credentials_provider, profile, region, feedback);

    Ok(client_config.create_with_context(context)?)
}

///
/// Prepare the Kafka consumer context for authentication.
///
pub async fn connect<Ctx>(consumer: &StreamConsumer<Ctx>, highlighting: &Highlighting) -> Result<()>
where
    Ctx: KiekContext + 'static,
{
    consumer.context().verify(highlighting).await?;

    // Make sure there is an OAuth token before obtaining metadata
    // See https://github.com/yuhao-su/aws-msk-iam-sasl-signer-rs/blob/a6efa88801333d634c8370cf85128bce6b513c11/examples/consumer.rs
    match consumer.recv().now_or_never() {
        None => {
            info!("Connected.");
            Ok(())
        }
        Some(e) => match e {
            Err(e) => Err(KiekError::from(e)),
            Ok(_) => unreachable!("Consumer is not assigned yet."),
        },
    }
}

///
/// If result is Kafka broker transport failure, the error message is captured from the Kafka log
/// message.
///
async fn map_errors<Ctx, R>(
    consumer: &StreamConsumer<Ctx>,
    result: std::result::Result<R, KafkaError>,
) -> Result<R>
where
    Ctx: KiekContext + 'static,
{
    match result {
        Ok(r) => Ok(r),
        Err(error) => Err(match error.rdkafka_error_code() {
            Some(RDKafkaErrorCode::BrokerTransportFailure) => {
                // The FAIL message just appears in the log after the next receive (or consumer drop).
                // Therfore we need to trigger a receive to get the log message.
                // In case the consumer decides to retry the position before receive is captured and restored.
                let position = consumer.position();
                consumer.recv().now_or_never();
                position.into_iter().for_each(|p| {
                    let _ = consumer.seek_partitions(p, TIMEOUT);
                });
                let error = consumer.context().last_fail().lock().unwrap().take();
                match error {
                    Some(error) => KiekError::new(error),
                    None => KiekError::new(KiekError::BROKER_TRANSPORT_FAILURE),
                }
            }
            _ => Box::new(error),
        }),
    }
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum TopicOrPartition {
    Topic(String),
    TopicPartition(String, i32),
}

impl TopicOrPartition {
    pub fn topic(&self) -> &str {
        match self {
            TopicOrPartition::Topic(topic) => topic,
            TopicOrPartition::TopicPartition(topic, _) => topic,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct Assigment {
    pub(crate) topic_or_partition: TopicOrPartition,
    pub(crate) num_partitions: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StartOffset {
    Earliest,
    Latest,
    Relative(i64),
}

///
/// Looks up the number of partitions for given topic in the Kafka cluster.
/// Calculates the partition for the given key and assigns it to the consumer with given offset configuration.
///
pub async fn assign_partition_for_key<Ctx>(
    consumer: &StreamConsumer<Ctx>,
    topic_or_partition: &TopicOrPartition,
    key: &str,
    start_offset: StartOffset,
    feedback: &Feedback,
) -> Result<Assigment>
where
    Ctx: KiekContext + 'static,
{
    let (topic_or_partition, num_partitions) =
        verify_topic_or_partition(consumer, topic_or_partition, feedback).await?;

    let topic = topic_or_partition.topic();

    let partition = partition_for_key(key, num_partitions);

    let partition = match topic_or_partition {
        TopicOrPartition::Topic(_) => partition,
        TopicOrPartition::TopicPartition(_, configured_partition) => {
            if partition != configured_partition {
                feedback.warning(format!("Key {bold}{key}{bold:#} would be expected in partition {success}{partition}{success:#} with default partitioning, not in configured partition {error}{configured_partition}{error:#}.",
                                         bold = feedback.highlighting.bold,
                                         success = feedback.highlighting.success,
                                         error = feedback.highlighting.error))
            }
            configured_partition
        }
    };

    let partition_offsets: HashMap<(String, i32), Offset> = HashMap::from([(
        (topic.to_string(), partition as i32),
        offset_for(start_offset),
    )]);

    info!("Assigning partition {partition} for {topic} to search for key {key}.");

    let topic_partition_list = TopicPartitionList::from_topic_map(&partition_offsets)?;
    consumer.assign(&topic_partition_list)?;

    let assignment = Assigment {
        topic_or_partition: topic_or_partition.clone(),
        num_partitions,
    };

    Ok(assignment)
}

///
/// Lists all topics from the Kafka cluster
///
pub async fn list_topics<Ctx>(
    consumer: &StreamConsumer<Ctx>,
    feedback: &Feedback,
) -> Result<()>
where
    Ctx: KiekContext + 'static,
{
    feedback.info("Fetching", "topics from Kafka cluster");

    let metadata = map_errors(consumer, consumer.fetch_metadata(None, TIMEOUT)).await?;

    let topics: Vec<_> = metadata
        .topics()
        .iter()
        .map(|t| (t.name().to_string(), t.partitions().len()))
        .collect();

    if topics.is_empty() {
        feedback.info("No topics", "available in the Kafka cluster");
    } else {
        feedback.clear();
        for (topic_name, partition_count) in topics {
            println!("{} ({})", topic_name, partition_count);
        }
    }

    Ok(())
}

///
/// Loads the topics from the Kafka cluster and maybe prompt the user to select one.
///
/// - single topic => use that one
/// - multiple topics => choose topic and then partition
///
/// Fail if there is no topic at all or if topic choice is ambiguous in non-interactive mode.
///
pub async fn select_topic_or_partition<Ctx>(
    consumer: &StreamConsumer<Ctx>,
    feedback: &Feedback,
) -> Result<TopicOrPartition>
where
    Ctx: KiekContext + 'static,
{
    feedback.info("Fetching", "topics from Kafka cluster");

    let metadata = map_errors(consumer, consumer.fetch_metadata(None, TIMEOUT)).await?;

    let topic_names: Vec<String> = metadata
        .topics()
        .iter()
        .map(|t| t.name().to_string())
        .collect();

    if topic_names.is_empty() {
        Err(KiekError::new("No topics available in the Kafka cluster"))
    } else if topic_names.len() == 1 {
        feedback.info("Using", format!("topic {topic}", topic = &topic_names[0]));
        Ok(TopicOrPartition::Topic(topic_names[0].clone()))
    } else if feedback.interactive {
        prompt_topic_or_partition(&metadata, None, feedback).map(|(topic, _)| topic)
    } else {
        Err(KiekError::new(
            "Multiple topics available in the Kafka cluster: Please specify a topic.",
        ))
    }
}

///
/// Prompt the user to select a topic or partition from the Kafka cluster.
///
/// If a topic had been given and does not exist, sorts the topics by Levensthein distance to the
/// given topic name.
///
fn prompt_topic_or_partition(
    metadata: &Metadata,
    given: Option<&TopicOrPartition>,
    feedback: &Feedback,
) -> Result<(TopicOrPartition, i32)> {
    assert!(feedback.interactive);
    let topic_names: Vec<String> = metadata
        .topics()
        .iter()
        .map(|t| t.name().to_string())
        .collect();

    let given_topic = given.map(|t| t.topic());

    // Sort topics by Levenshtein distance to the given topic name
    let topic_names = if let Some(given_topic) = given_topic {
        let mut similar_topics: Vec<(String, usize)> = topic_names
            .iter()
            .map(|t| (t.clone(), levenshtein(given_topic, t)))
            .collect();
        similar_topics.sort_by(|a, b| a.1.cmp(&b.1));
        similar_topics.iter().map(|(t, _)| t.clone()).collect()
    } else {
        topic_names
    };

    let prompt = if let Some(given) = given {
        format!(
            "{topic} does not exist. Please select a topic.",
            topic = given.topic()
        )
    } else {
        "Select a topic".to_string()
    };

    println!("{}", clear::CurrentLine);
    let theme = feedback.highlighting.dialoguer_theme();
    let select = Select::with_theme(&*theme)
        .with_prompt(prompt)
        .items(&topic_names)
        .report(true)
        .max_length(15);

    // preselect the topic with the best Levenshtein distance
    let select = if given.is_some() {
        select.default(0)
    } else {
        select
    };

    let selection = select.interact().unwrap();

    let topic = &topic_names[selection];

    let num_partitions = metadata
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .unwrap()
        .partitions()
        .len() as i32;

    match given {
        Some(TopicOrPartition::TopicPartition(_, partition)) => Ok((
            TopicOrPartition::TopicPartition(topic.clone(), *partition),
            num_partitions,
        )),
        Some(TopicOrPartition::Topic(_)) => {
            Ok((TopicOrPartition::Topic(topic.clone()), num_partitions))
        }
        None => prompt_partition(feedback, theme, topic, num_partitions),
    }
}

fn prompt_partition(
    feedback: &Feedback,
    theme: Box<dyn Theme>,
    topic: &String,
    num_partitions: i32,
) -> Result<(TopicOrPartition, i32)> {
    let partitions: Vec<String> = (-1..num_partitions)
        .map(|p| {
            if p == -1 {
                format!("{topic} (all partitions)")
            } else {
                format!(
                    "{color}{topic}{color:#}{dimmed}-{dimmed:#}{color}{p}{color:#}",
                    color = feedback.highlighting.partition(p),
                    dimmed = feedback.highlighting.partition(p).dimmed()
                )
            }
        })
        .collect();

    let partition = Select::with_theme(&*theme)
        .with_prompt("Select a partition")
        .items(&partitions)
        .default(0)
        .max_length(1 + 12) // "all partitions" and first 12 partitions
        .interact()
        .unwrap() as i32
        - 1;

    if partition == -1 {
        Ok((TopicOrPartition::Topic(topic.clone()), num_partitions))
    } else {
        Ok((
            TopicOrPartition::TopicPartition(topic.clone(), partition),
            num_partitions,
        ))
    }
}

///
/// Looks up the number of partitions for given topic in the Kafka cluster.
/// If topic does not exist, it prompts the user to select a topic.
///
pub async fn verify_topic_or_partition<Ctx>(
    consumer: &StreamConsumer<Ctx>,
    topic_or_partition: &TopicOrPartition,
    feedback: &Feedback,
) -> Result<(TopicOrPartition, i32)>
where
    Ctx: KiekContext + 'static,
{
    feedback.info(
        "Fetching",
        format!("number of partitions of {}", topic_or_partition.topic()),
    );

    let num_partitions = fetch_number_of_partitions(consumer, topic_or_partition.topic()).await?;

    match (num_partitions, topic_or_partition) {
        (Some(num_partitions), _) => Ok((topic_or_partition.clone(), num_partitions)),
        (None, _) => {
            if feedback.interactive {
                feedback.info("Fetching", "topics from Kafka cluster");
                let metadata = consumer.fetch_metadata(None, TIMEOUT)?;
                prompt_topic_or_partition(&metadata, Some(topic_or_partition), feedback)
            } else {
                Err(KiekError::new("Topic does not exist."))
            }
        }
    }
}

///
/// Looks up the number of partitions for given topic/partition in the Kafka cluster.
/// If a partition is given and valid, it assigns it to the consumer with given offset configuration.
/// If a topic is given, it assigns all partitions to the consumer with given offset configuration.
///
pub async fn assign_topic_or_partition<Ctx>(
    consumer: &StreamConsumer<Ctx>,
    topic_or_partition: &TopicOrPartition,
    start_offset: StartOffset,
    feedback: &Feedback,
) -> Result<Assigment>
where
    Ctx: KiekContext + 'static,
{
    let (topic_or_partition, num_partitions) =
        verify_topic_or_partition(consumer, topic_or_partition, feedback).await?;

    let topic = topic_or_partition.topic();

    let offset = offset_for(start_offset);

    let topic_partition_list: TopicPartitionList = match topic_or_partition {
        TopicOrPartition::Topic(_) => {
            let partition_offsets: HashMap<(String, i32), Offset> = (0..num_partitions)
                .map(|partition| ((topic.to_string(), partition), offset))
                .collect();

            info!("Assigning all {num_partitions} partitions for {topic}.");

            TopicPartitionList::from_topic_map(&partition_offsets)?
        }
        TopicOrPartition::TopicPartition(_, partition) => {
            if partition > num_partitions {
                return Err(KiekError::new(format!("Partition {partition} is out of range for {topic} with {num_partitions} partitions.")));
            }

            let partition_offsets: HashMap<(String, i32), Offset> =
                HashMap::from([((topic.to_string(), partition), offset)]);

            info!("Assigning partition {partition} for {topic}.");

            TopicPartitionList::from_topic_map(&partition_offsets)?
        }
    };

    consumer.assign(&topic_partition_list)?;

    let assignment = Assigment {
        topic_or_partition: topic_or_partition.clone(),
        num_partitions,
    };

    Ok(assignment)
}

///
/// Fetches the number of partitions for topic with given name.
///
async fn fetch_number_of_partitions<Ctx>(
    consumer: &StreamConsumer<Ctx>,
    topic: &str,
) -> Result<Option<i32>>
where
    Ctx: KiekContext + 'static,
{
    let metadata = map_errors(consumer, consumer.fetch_metadata(Some(topic), TIMEOUT)).await?;

    match metadata.topics().first() {
        Some(topic_metadata) if !topic_metadata.partitions().is_empty() => {
            Ok(Some(topic_metadata.partitions().len() as i32))
        }
        _ => Ok(None),
    }
}

fn offset_for(start_offset: StartOffset) -> Offset {
    match start_offset {
        StartOffset::Earliest => Offset::Beginning,
        StartOffset::Latest => Offset::End,
        StartOffset::Relative(offset) => Offset::OffsetTail(offset),
    }
}

///
/// Seeks to the offsets for the given start time.
///
pub async fn seek_start_offsets<Ctx>(
    consumer: &StreamConsumer<Ctx>,
    start: DateTime<Local>,
) -> Result<()>
where
    Ctx: KiekContext + 'static,
{
    let timestamp = start.timestamp_millis();
    debug!("Obtaining offsets for {timestamp} ({})", start.to_rfc3339());
    let offsets = consumer.offsets_for_timestamp(timestamp, TIMEOUT)?;
    debug!("Seeking to {offsets:?}");
    consumer.seek_partitions(offsets, TIMEOUT)?;
    Ok(())
}

///
/// Implementation of the partitioner algorithm used by Kafka's default partitioner
///
pub fn partition_for_key(key: &str, partitions: i32) -> i32 {
    partition_for_plain_key(key.as_bytes(), partitions)
}

///
/// Implementation of the partitioner algorithm used by Kafka's default partitioner
///
pub fn partition_for_plain_key(key: &[u8], partitions: i32) -> i32 {
    let hash = murmur2(key, KAFKA_SEED);
    let hash = hash & 0x7fffffff; // "to positive" from Kafka's partitioner
    (hash % partitions as u32) as i32
}

///
/// Format a Kafka record timestamp for display
///
pub fn format_timestamp(
    timestamp: &Timestamp,
    start_date: &DateTime<Local>,
    highlighting: &Highlighting,
) -> Option<String> {
    match timestamp {
        Timestamp::NotAvailable => None,
        Timestamp::CreateTime(ms) => format_timestamp_millis(*ms, start_date, highlighting),
        Timestamp::LogAppendTime(ms) => format_timestamp_millis(*ms, start_date, highlighting),
    }
}

fn format_timestamp_millis(
    ms: i64,
    start_date: &DateTime<Local>,
    highlighting: &Highlighting,
) -> Option<String> {
    chrono::DateTime::from_timestamp_millis(ms).map(|dt| {
        let local_time = dt.with_timezone(&Local);
        let formatted = local_time.format("%Y-%m-%d %H:%M:%S%.3f");

        let after_start = start_date.signed_duration_since(dt).num_milliseconds() < 0;
        let same_day = start_date.date_naive() == dt.date_naive();
        let style = if after_start {
            highlighting.bold
        } else if same_day {
            highlighting.plain
        } else {
            highlighting.dimmed
        };
        format!("{style}{formatted}{style:#}")
    })
}

/// Formats the bootstrap servers for the log output with ellipsis for more than one broker
pub(crate) struct FormatBootstrapServers<'a>(pub(crate) &'a str);

impl std::fmt::Display for FormatBootstrapServers<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut split = self.0.split(',');
        match split.next() {
            Some(first) => write!(f, "{}", first)?,
            None => return Ok(()),
        }
        if split.next().is_some() {
            write!(f, ", ...")?
        }
        Ok(())
    }
}

// Test the partition_for_key function
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_for_key() {
        assert_eq!(partition_for_key("control+e2e-zgddu3j-delete", 6), 5);
        assert_eq!(partition_for_key("control+e2e-6n304jn-delete", 6), 1);

        assert_eq!(
            partition_for_key("834408c2-6061-7057-bdc1-320cc24c8873", 6),
            3
        );
        assert_eq!(partition_for_key("control+e2e-bgrl0v-delete", 6), 0);
        assert_eq!(partition_for_key("control+e2e-5ijyt78-delete", 6), 5);
        assert_eq!(
            partition_for_key("03b40832-b061-703d-4d68-895dee9e1f90", 6),
            4
        );
        assert_eq!(partition_for_key("control+e2e-86d8ra-delete", 6), 2);
        assert_eq!(partition_for_key("control+e2e-lhwcl2-delete", 6), 1);
    }

    #[test]
    fn test_format_bootstrap_servers() {
        assert_eq!(
            format!("{}", FormatBootstrapServers("broker1:9092")),
            "broker1:9092"
        );
        assert_eq!(
            format!("{}", FormatBootstrapServers("broker1:9092,broker2:9092")),
            "broker1:9092, ..."
        );
    }
}

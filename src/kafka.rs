use crate::app::KiekException;
use crate::aws::list_profiles;
use crate::feedback::Feedback;
use crate::highlight::Highlighting;
use crate::{aws, CoreResult, Result};
use aws_credential_types::provider::error::CredentialsError::ProviderError;
use aws_credential_types::provider::ProvideCredentials;
use aws_msk_iam_sasl_signer::generate_auth_token_from_credentials_provider;
use aws_sdk_sts::config::SharedCredentialsProvider;
use aws_types::region::Region;
use chrono::{DateTime, Local};
use dialoguer::theme::Theme;
use dialoguer::Select;
use futures::FutureExt;
use levenshtein::levenshtein;
use log::{error, info};
use murmur2::{murmur2, KAFKA_SEED};
use rdkafka::client::{ClientContext, OAuthToken};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::metadata::Metadata;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::{Offset, Timestamp};
use std::collections::HashMap;
use std::thread;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::time::timeout;

pub(crate) const DEFAULT_PORT: i32 = 9092;
pub(crate) const DEFAULT_BROKER_STRING: &str = "127.0.0.1:9092";

/// Timeout for the Kafka operations
const TIMEOUT: Duration = Duration::from_secs(10);

///
/// Create basic client configuration with the provided bootstrap servers
///
pub fn create_config<S: Into<String>>(bootstrap_servers: S) -> ClientConfig {
    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers", bootstrap_servers);
    client_config.set("group.id", "kieker");
    client_config.set("enable.auto.commit", "false");
    client_config
}

///
/// Create a Kafka consumer to connect to an MSK cluster with IAM authentication.
///
pub async fn create_msk_consumer(bootstrap_servers: &String, credentials_provider: SharedCredentialsProvider, profile: String, region: Region, feedback: &Feedback) -> Result<StreamConsumer<IamContext>> {
    let mut client_config = create_config(bootstrap_servers);
    client_config.set("security.protocol", "SASL_SSL");
    client_config.set("sasl.mechanism", "OAUTHBEARER");

    let client_context = IamContext::new(credentials_provider, profile, region, feedback, Handle::current());

    let consumer: StreamConsumer<IamContext> = client_config.create_with_context(client_context)?;

    Ok(consumer)
}

pub async fn connect<Ctx>(consumer: &StreamConsumer<Ctx>, highlighting: &Highlighting) -> Result<()>
where
    Ctx: KiekContext + 'static,
{
    // Make sure there is an OAuth token before obtaining metadata
    // See https://github.com/yuhao-su/aws-msk-iam-sasl-signer-rs/blob/a6efa88801333d634c8370cf85128bce6b513c11/examples/consumer.rs
    match consumer.recv().now_or_never() {
        None => {
            info!("Connected.");
            Ok(())
        }
        Some(e) => {
            match e {
                Err(error) => translate_error(consumer, error, highlighting).await,
                Ok(_) => unreachable!("Consumer is not assigned yet.")
            }
        }
    }
}

///
/// Translate Kafka error to a more user-friendly message.
///
/// An authentication error can have many reasons:
/// - MSK/IAM: AWS profile does not exist
/// - MSK/IAM: AWS credentials cannot be provided
/// - MSK/IAM: AWS profile & credentials are valid but the SSO session has expired
///
async fn translate_error<Ctx>(consumer: &StreamConsumer<Ctx>, error: KafkaError, highlighting: &Highlighting) -> Result<()>
where
    Ctx: KiekContext + 'static,
{
    Err(match error.rdkafka_error_code() {
        Some(RDKafkaErrorCode::Authentication) => {
            let ctx = consumer.client().context();
            match ctx.aws_credentials_provider() {
                Some(credentials_provider) => {
                    // On MSK/IAM analyze if credentials could be the problem
                    let profile = ctx.iam_profile().cloned().unwrap_or(aws::profile());
                    match credentials_provider.provide_credentials().await {
                        Err(ProviderError(e)) if format!("{e:?}").contains("Session token not found or invalid") => {
                            KiekException::boxed(format!("Session token not found or invalid. Run {bold}aws sso login --profile {profile}{bold:#} to refresh your session.", bold = highlighting.bold))
                        }
                        Err(ProviderError(e)) if format!("{e:?}").contains("is not authorized to perform: sts:AssumeRole") => {
                            KiekException::boxed(format!("Assuming the passed role failed. Check if profile {profile} has the rights."))
                        }
                        Err(_) if !list_profiles().await.unwrap_or(vec![profile.clone()]).contains(&profile) => {
                            KiekException::boxed(format!("AWS profile {profile} does not exist."))
                        }
                        Err(e) => {
                            error!("Failed to provide AWS credentials: {e:?}");
                            KiekException::boxed(e.to_string())
                        }
                        Ok(_) => {
                            // IAM credentials are valid => most likely the profile has no access to the MSK cluster
                            KiekException::boxed(format!("Authentication with IAM credentials failed. Check if profile {profile} has access rights."))
                        }
                    }
                }
                None => {
                    // not MSK/IAM
                    KiekException::boxed("Authentication failed. Check your credentials.")
                }
            }
        }
        // other trouble
        _ => Box::new(error)
    })
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum TopicOrPartition {
    Topic(String),
    TopicPartition(String, usize),
}

impl TopicOrPartition {
    pub fn topic(&self) -> &str {
        match self {
            TopicOrPartition::Topic(topic) => topic,
            TopicOrPartition::TopicPartition(topic, _) => topic,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum StartOffset {
    Earliest,
    Latest(i64),
}

///
/// Looks up the number of partitions for given topic in the Kafka cluster.
/// Calculates the partition for the given key and assigns it to the consumer with given offset configuration.
///
pub async fn assign_partition_for_key<Ctx>(consumer: &StreamConsumer<Ctx>, topic_or_partition: &TopicOrPartition, key: &str, start_offset: StartOffset, feedback: &Feedback) -> Result<()>
where
    Ctx: KiekContext + 'static,
{
    let (topic_or_partition, num_partitions) = verify_topic_or_partition(consumer, topic_or_partition, feedback).await?;

    let topic = topic_or_partition.topic();

    let partition = partition_for_key(key, num_partitions);

    let partition =
        match topic_or_partition {
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

    let partition_offsets: HashMap<(String, i32), Offset> =
        HashMap::from([((topic.to_string(), partition as i32), offset_for(start_offset))]);

    info!("Assigning partition {partition} for {topic} to search for key {key}.");

    let topic_partition_list = TopicPartitionList::from_topic_map(&partition_offsets)?;
    consumer.assign(&topic_partition_list)?;

    Ok(())
}

///
/// Loads the topics from the Kafka cluster and maybe prompt the user to select one.
///
/// - single topic => use that one
/// - multiple topics => choose topic and then partition
///
/// Fail if there is no topic at all or in silent mode if topic choice is ambiguous.
///
pub async fn select_topic_or_partition<Ctx>(consumer: &StreamConsumer<Ctx>, feedback: &Feedback) -> Result<TopicOrPartition>
where
    Ctx: KiekContext + 'static,
{
    feedback.info("Fetching", "topics from Kafka cluster");

    let metadata = consumer.fetch_metadata(None, TIMEOUT)?;

    let topic_names: Vec<String> = metadata.topics().iter().map(|t| t.name().to_string()).collect();

    if topic_names.is_empty() {
        Err(KiekException::boxed("No topics available in the Kafka cluster"))
    } else if topic_names.len() == 1 {
        feedback.info("Using", format!("topic {topic}", topic = &topic_names[0]));
        return Ok(TopicOrPartition::Topic(topic_names[0].clone()));
    } else if feedback.silent {
        return Err(KiekException::boxed("Multiple topics available in the Kafka cluster: Please specify a topic."));
    } else {
        prompt_topic_or_partition(&metadata, None, feedback).map(|(topic, _)| topic)
    }
}

///
/// Prompt the user to select a topic or partition from the Kafka cluster.
///
/// If a topic had been given and does not exist, sorts the topics by Levensthein distance to the
/// given topic name.
///
fn prompt_topic_or_partition(metadata: &Metadata, given: Option<&TopicOrPartition>, feedback: &Feedback) -> Result<(TopicOrPartition, usize)> {
    assert!(!feedback.silent);
    let topic_names: Vec<String> = metadata.topics().iter().map(|t| t.name().to_string()).collect();

    let given_topic = given.map(|t| t.topic());

    // Sort topics by Levenshtein distance to the given topic name
    let topic_names =
        if let Some(given_topic) = given_topic {
            let mut similar_topics: Vec<(String, usize)> = topic_names.iter()
                .map(|t| (t.clone(), levenshtein(given_topic, t)))
                .collect();
            similar_topics.sort_by(|a, b| a.1.cmp(&b.1));
            similar_topics.iter().map(|(t, _)| t.clone()).collect()
        } else {
            topic_names
        };

    let prompt = if let Some(given) = given {
        format!("{topic} does not exist. Please select a topic.", topic = given.topic())
    } else {
        "Select a topic".to_string()
    };

    feedback.clear();
    let theme = feedback.highlighting.dialoguer_theme();
    let select = Select::with_theme(&*theme)
        .with_prompt(prompt)
        .items(&topic_names)
        .max_length(15);

    // preselect the topic with the best Levenshtein distance
    let select =
        if given.is_some() {
            select.default(0)
        } else {
            select
        };

    let selection = select.interact().unwrap();

    let topic = &topic_names[selection];

    let num_partitions = metadata.topics().iter().find(|t| t.name() == topic).unwrap().partitions().len();

    match given {
        Some(TopicOrPartition::TopicPartition(_, partition)) => {
            Ok((TopicOrPartition::TopicPartition(topic.clone(), *partition), num_partitions))
        }
        Some(TopicOrPartition::Topic(_)) => {
            Ok((TopicOrPartition::Topic(topic.clone()), num_partitions))
        }
        None => {
            prompt_partition(feedback, theme, topic, num_partitions)
        }
    }
}

fn prompt_partition(feedback: &Feedback, theme: Box<dyn Theme>, topic: &String, num_partitions: usize) -> Result<(TopicOrPartition, usize)> {
    let partitions: Vec<String> =
        (-1..num_partitions as i32).map(|p|
            if p == -1 {
                format!("{topic} (all partitions)")
            } else {
                format!("{color}{topic}{color:#}{dimmed}-{dimmed:#}{color}{p}{color:#}",
                        color = feedback.highlighting.partition(p),
                        dimmed = feedback.highlighting.partition(p).dimmed())
            }
        ).collect();

    let partition = Select::with_theme(&*theme)
        .with_prompt("Select a partition")
        .items(&partitions)
        .default(0)
        .max_length(13) // "all partitions" and first 12 partitions
        .interact()
        .unwrap() as i32 - 1;

    if partition == -1 {
        Ok((TopicOrPartition::Topic(topic.clone()), num_partitions))
    } else {
        Ok((TopicOrPartition::TopicPartition(topic.clone(), partition as usize), num_partitions))
    }
}

///
/// Looks up the number of partitions for given topic in the Kafka cluster.
/// If topic does not exist, it prompts the user to select a topic.
///
pub async fn verify_topic_or_partition<Ctx>(consumer: &StreamConsumer<Ctx>, topic_or_partition: &TopicOrPartition, feedback: &Feedback) -> Result<(TopicOrPartition, usize)>
where
    Ctx: KiekContext + 'static,
{
    feedback.info("Fetching", format!("number of partitions of {}", topic_or_partition.topic()));

    let num_partitions = fetch_number_of_partitions(consumer, topic_or_partition.topic()).await?;

    match (num_partitions, topic_or_partition) {
        (Some(num_partitions), _) => Ok((topic_or_partition.clone(), num_partitions)),
        (None, _) => {
            if feedback.silent {
                Err(KiekException::boxed("Topic does not exist."))
            } else {
                feedback.info("Fetching", "topics from Kafka cluster");

                let metadata = consumer.fetch_metadata(None, TIMEOUT)?;
                prompt_topic_or_partition(&metadata, Some(topic_or_partition), feedback)
            }
        }
    }
}

///
/// Looks up the number of partitions for given topic/partition in the Kafka cluster.
/// If a partition is given and valid, it assigns it to the consumer with given offset configuration.
/// If a topic is given, it assigns all partitions to the consumer with given offset configuration.
///
pub async fn assign_topic_or_partition<Ctx>(consumer: &StreamConsumer<Ctx>, topic_or_partition: &TopicOrPartition, start_offset: StartOffset, feedback: &Feedback) -> Result<()>
where
    Ctx: KiekContext + 'static,
{
    let (topic_or_partition, num_partitions) = verify_topic_or_partition(consumer, topic_or_partition, feedback).await?;

    let topic = topic_or_partition.topic();

    let offset = offset_for(start_offset);

    let topic_partition_list: TopicPartitionList =
        match topic_or_partition {
            TopicOrPartition::Topic(_) => {
                let partition_offsets: HashMap<(String, i32), Offset> =
                    (0..num_partitions)
                        .map(|partition| { ((topic.to_string(), partition as i32), offset) })
                        .collect();

                info!("Assigning all {num_partitions} partitions for {topic}.");

                TopicPartitionList::from_topic_map(&partition_offsets)?
            }
            TopicOrPartition::TopicPartition(_, partition) => {
                if partition > num_partitions {
                    return Err(KiekException::boxed(format!("Partition {partition} is out of range for {topic} with {num_partitions} partitions.")));
                }

                let partition_offsets: HashMap<(String, i32), Offset> =
                    HashMap::from([((topic.to_string(), partition as i32), offset)]);

                info!("Assigning partition {partition} for {topic}.");

                TopicPartitionList::from_topic_map(&partition_offsets)?
            }
        };

    consumer.assign(&topic_partition_list)?;

    Ok(())
}

///
/// Fetches the number of partitions for topic with given name.
///
async fn fetch_number_of_partitions<Ctx>(consumer: &StreamConsumer<Ctx>, topic: &str) -> Result<Option<usize>>
where
    Ctx: 'static + ConsumerContext,
{
    let metadata = consumer.fetch_metadata(Some(topic), TIMEOUT)?;

    match metadata.topics().first() {
        Some(topic_metadata) if !topic_metadata.partitions().is_empty() =>
            Ok(Some(topic_metadata.partitions().len())),
        _ => {
            Ok(None)
        }
    }
}

fn offset_for(start_offset: StartOffset) -> Offset {
    match start_offset {
        StartOffset::Earliest => Offset::Beginning,
        StartOffset::Latest(0) => Offset::End,
        StartOffset::Latest(offset) => Offset::OffsetTail(offset),
    }
}

pub(crate) trait KiekContext: ConsumerContext {
    fn iam_profile(&self) -> Option<&String>;
    fn aws_credentials_provider(&self) -> Option<&SharedCredentialsProvider>;
}

///
/// This client & consumer context generates OAuth tokens for the Kafka cluster based on an AWS
/// credentials provider.
///
#[derive(Clone)]
pub(crate) struct IamContext {
    credentials_provider: SharedCredentialsProvider,
    profile: String,
    region: Region,
    feedback: Feedback,
    rt: Handle,
}

impl IamContext {
    fn new(credentials_provider: SharedCredentialsProvider, profile: String, region: Region, feedback: &Feedback, rt: Handle) -> Self {
        let feedback = feedback.clone();
        Self { credentials_provider, profile, region, feedback, rt }
    }
}

impl ConsumerContext for IamContext {}

impl ClientContext for IamContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    ///
    /// Use generate_auth_token_from_credentials_provider from the aws_msk_iam_sasl_signer crate to
    /// generate an OAuth token from the AWS credentials provider.
    ///
    fn generate_oauth_token(&self, _oauthbearer_config: Option<&str>) -> CoreResult<OAuthToken> {
        let region = self.region.clone();
        let credentials_provider = self.credentials_provider.clone();
        let handle = self.rt.clone();
        self.feedback.info("Authorizing", "using AWS IAM auth token");
        let (token, expiration_time_ms) = {
            let handle = thread::spawn(move || {
                handle.block_on(async {
                    timeout(TIMEOUT, generate_auth_token_from_credentials_provider(region, credentials_provider).map(|r| {
                        match r {
                            Ok(token) => {
                                info!("Generated OAuth token: {} {}", token.0, token.1);
                                Ok(token)
                            }
                            Err(err) => {
                                error!("Failed to generate OAuth token: {}", err);
                                Err(err)
                            }
                        }
                    }
                    )).await
                })
            });
            handle.join().unwrap()??
        };
        Ok(OAuthToken {
            token,
            principal_name: "".to_string(),
            lifetime_ms: expiration_time_ms,
        })
    }
}

impl KiekContext for IamContext {
    fn iam_profile(&self) -> Option<&String> {
        Some(&self.profile)
    }

    fn aws_credentials_provider(&self) -> Option<&SharedCredentialsProvider> {
        Some(&self.credentials_provider)
    }
}

///
/// Implementation of the partitioner algorithm used by Kafka's default partitioner
///
pub fn partition_for_key(key: &str, partitions: usize) -> usize {
    let hash = murmur2(key.as_bytes(), KAFKA_SEED);
    let hash = hash & 0x7fffffff; // "to positive" from Kafka's partitioner
    (hash % partitions as u32) as usize
}

///
/// Format a Kafka record timestamp for display
///
pub fn format_timestamp(timestamp: &Timestamp, start_date: &DateTime<Local>, highlighting: &Highlighting) -> Option<String> {
    match timestamp {
        Timestamp::NotAvailable => None,
        Timestamp::CreateTime(ms) => format_timestamp_millis(*ms, start_date, highlighting),
        Timestamp::LogAppendTime(ms) => format_timestamp_millis(*ms, start_date, highlighting),
    }
}

fn format_timestamp_millis(ms: i64, start_date: &DateTime<Local>, highlighting: &Highlighting) -> Option<String> {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|dt| {
            let local_time = dt.with_timezone(&Local);
            let formatted = local_time.format("%Y-%m-%d %H:%M:%S%.3f");

            let after_start = start_date.signed_duration_since(dt).num_milliseconds() < 0;
            let same_day = start_date.date_naive() == dt.date_naive();
            let style = if after_start { highlighting.bold } else if same_day { highlighting.plain } else { highlighting.dimmed };
            format!("{style}{formatted}{style:#}")
        })
}

/// Formats the bootstrap servers for the log output with ellipsis for more than one broker
pub(crate) struct FormatBootstrapServers<'a>(pub(crate) &'a String);

impl<'a> std::fmt::Display for FormatBootstrapServers<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut split = self.0.split(',');
        match split.next() {
            Some(first) => write!(f, "{}", first)?,
            None => return Ok(())
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

        assert_eq!(partition_for_key("834408c2-6061-7057-bdc1-320cc24c8873", 6), 3);
        assert_eq!(partition_for_key("control+e2e-bgrl0v-delete", 6), 0);
        assert_eq!(partition_for_key("control+e2e-5ijyt78-delete", 6), 5);
        assert_eq!(partition_for_key("03b40832-b061-703d-4d68-895dee9e1f90", 6), 4);
        assert_eq!(partition_for_key("control+e2e-86d8ra-delete", 6), 2);
        assert_eq!(partition_for_key("control+e2e-lhwcl2-delete", 6), 1);
    }

    #[test]
    fn test_format_bootstrap_servers() {
        assert_eq!(format!("{}", FormatBootstrapServers(&"broker1:9092".to_string())), "broker1:9092");
        assert_eq!(format!("{}", FormatBootstrapServers(&"broker1:9092,broker2:9092".to_string())), "broker1:9092, ...");
    }
}
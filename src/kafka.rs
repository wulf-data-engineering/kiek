use crate::{CoreResult, Result};
use aws_msk_iam_sasl_signer::generate_auth_token_from_credentials_provider;
use aws_sdk_sts::config::SharedCredentialsProvider;
use aws_types::region::Region;
use futures::{FutureExt};
use log::{debug, error, info};
use rdkafka::client::{ClientContext, OAuthToken};
use rdkafka::config::{ClientConfig};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::{Offset, Timestamp};
use std::collections::HashMap;
use std::thread;
use std::time::Duration;
use chrono::{DateTime, Local};
use murmur2::{murmur2, KAFKA_SEED};
use rdkafka::consumer::{Consumer, ConsumerContext};
use tokio::runtime::Handle;
use tokio::time::timeout;
use crate::highlight::{format, format_null, BOLD, GREY, RESET};

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
pub async fn create_msk_consumer(bootstrap_servers: String, credentials_provider: SharedCredentialsProvider, region: Region) -> Result<StreamConsumer<IamContext>> {
    let mut client_config = create_config(bootstrap_servers);
    client_config.set("security.protocol", "SASL_SSL");
    client_config.set("sasl.mechanism", "OAUTHBEARER");

    let client_context = IamContext::new(region, credentials_provider, Handle::current());

    let consumer: StreamConsumer<IamContext> = client_config.create_with_context(client_context)?;

    // Make sure there is an OAuth token before obtaining metadata
    // See https://github.com/yuhao-su/aws-msk-iam-sasl-signer-rs/blob/a6efa88801333d634c8370cf85128bce6b513c11/examples/consumer.rs
    assert!(consumer.recv().now_or_never().is_none());

    Ok(consumer)
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

pub async fn assign_partition_for_key<Ctx>(consumer: &StreamConsumer<Ctx>, topic: &str, key: &str, start_offset: StartOffset) -> Result<()>
where
    Ctx: ConsumerContext + 'static,
{
    let metadata = consumer.fetch_metadata(Some(topic), TIMEOUT)?;

    let topic_metadata =
        metadata.topics().into_iter().find(|t| t.name() == topic)
            .ok_or(format!("Topic {topic} was not found."))?;

    let partition = partition_for_key(key, topic_metadata.partitions().len());

    let partition_offsets: HashMap<(String, i32), Offset> =
        HashMap::from([((topic.to_string(), partition as i32), offset_for(start_offset))]);

    info!("Assigning partition {partition} for {topic}.");

    let topic_partition_list = TopicPartitionList::from_topic_map(&partition_offsets)?;
    consumer.assign(&topic_partition_list)?;

    Ok(())
}

///
/// Looks up the number of partitions for given topic in the Kafka cluster.
/// Assigns all partitions to the consumer with given offset configuration.
///
pub async fn assign_all_partitions<Ctx>(consumer: &StreamConsumer<Ctx>, topic: &str, start_offset: StartOffset) -> Result<()>
where
    Ctx: ConsumerContext + 'static,
{
    let metadata = consumer.fetch_metadata(Some(topic), TIMEOUT)?;

    let topic_metadata =
        metadata.topics().into_iter().find(|t| t.name() == topic)
            .ok_or(format!("Topic {topic} was not found."))?;

    let offset = offset_for(start_offset);
    let partition_offsets: HashMap<(String, i32), Offset> =
        topic_metadata
            .partitions().iter()
            .map(|p| { ((topic.to_string(), p.id()), offset) })
            .collect();

    let topic_partition_list = TopicPartitionList::from_topic_map(&partition_offsets)?;

    info!("Assigning all {} partitions for {topic}.", topic_partition_list.count());

    consumer.assign(&topic_partition_list)?;

    Ok(())
}

fn offset_for(start_offset: StartOffset) -> Offset {
    match start_offset {
        StartOffset::Earliest => Offset::Beginning,
        StartOffset::Latest(0) => Offset::End,
        StartOffset::Latest(offset) => Offset::OffsetTail(offset),
    }
}

///
/// This client & consumer context generates OAuth tokens for the Kafka cluster based on an AWS
/// credentials provider.
///
#[derive(Clone)]
pub(crate) struct IamContext {
    region: Region,
    credentials_provider: SharedCredentialsProvider,
    rt: Handle,
}

impl IamContext {
    fn new(region: Region, credentials_provider: SharedCredentialsProvider, rt: Handle) -> Self {
        Self { region, credentials_provider, rt }
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
        let (token, expiration_time_ms) = {
            let handle = thread::spawn(move || {
                handle.block_on(async {
                    timeout(TIMEOUT, generate_auth_token_from_credentials_provider(region, credentials_provider).map(|r| {
                        match r {
                            Ok(token) => {
                                debug!("Generated OAuth token: {} {}", token.0, token.1);
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
pub fn format_timestamp(timestamp: &Timestamp, start_date: &DateTime<Local>, highlight: bool) -> Option<String> {
    match timestamp {
        Timestamp::NotAvailable => None,
        Timestamp::CreateTime(ms) => format_timestamp_millis(*ms, start_date, highlight),
        Timestamp::LogAppendTime(ms) => format_timestamp_millis(*ms, start_date, highlight),
    }
}

fn format_timestamp_millis(ms: i64, start_date: &DateTime<Local>, highlight: bool) -> Option<String> {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|dt| {
            let local_time = dt.with_timezone(&Local);
            let formatted = local_time.format("%Y-%m-%d %H:%M:%S%.3f");

            let after_start = start_date.signed_duration_since(dt).num_milliseconds() < 0;
            let same_day = start_date.date_naive() == dt.date_naive();
            let highlighting = if after_start { BOLD } else if same_day { RESET } else { GREY };

            format(formatted.to_string(), highlighting, highlight)
        })
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
}
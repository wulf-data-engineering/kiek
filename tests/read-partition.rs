use assert_cmd::prelude::*;
use assertables::{
    assert_contains, assert_ends_with, assert_is_empty, assert_not_contains, assert_starts_with,
};
use chrono::Local;
use futures::future::join_all;
use murmur2::{murmur2, KAFKA_SEED};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::producer::FutureProducer;
use rdkafka::ClientConfig;
use std::process::{Command, Stdio};
use tokio::task::block_in_place;
use tokio::time::sleep;

#[tokio::test]
async fn read_nothing() -> Result<(), Box<dyn std::error::Error>> {
    let topic_name = "read_nothing";

    empty_topic(topic_name, 1).await?;

    let mut cmd = Command::cargo_bin("kiek")?;

    cmd.arg(topic_name);
    cmd.arg("--no-colors");
    cmd.arg("--timeout=1");

    let output = cmd.output()?;
    let output = String::from_utf8(output.stdout)?;
    let lines: Vec<&str> = output.lines().collect();

    assert_is_empty!(lines);

    Ok(())
}

#[tokio::test]
async fn read_from_beginning() -> Result<(), Box<dyn std::error::Error>> {
    let topic_name = "read_from_beginning";

    empty_topic(topic_name, 1).await?;

    produce_messages(
        topic_name,
        None,
        None,
        vec![("key0", "value0"), ("key1", "value1")],
    )
    .await?;

    let mut cmd = Command::cargo_bin("kiek")?;

    cmd.arg(topic_name);
    cmd.arg("--no-colors");
    cmd.arg("--max=2");
    cmd.arg("--timeout=10");

    let output = cmd.output()?;
    let output = String::from_utf8(output.stdout)?;
    let lines: Vec<&str> = output.lines().collect();

    assert_starts_with!(lines[0], format!("{topic_name}-0"));
    assert_ends_with!(lines[0], "0 key0 value0");
    assert_starts_with!(lines[1], format!("{topic_name}-0"));
    assert_ends_with!(lines[1], "1 key1 value1");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn read_from_end() -> Result<(), Box<dyn std::error::Error>> {
    let topic_name = "read_from_end";

    empty_topic(topic_name, 1).await?;

    produce_messages(topic_name, None, None, vec![("key0", "value0")]).await?;

    let mut cmd = Command::cargo_bin("kiek")?;

    cmd.arg(topic_name);
    cmd.arg("--no-colors");
    cmd.arg("--latest");
    cmd.arg("--max=1");
    cmd.arg("--timeout=5");

    let child = cmd.stdout(Stdio::piped()).spawn()?;

    let handle = tokio::spawn(async move {
        loop {
            produce_messages(topic_name, None, None, vec![("key1", "value1")])
                .await
                .unwrap();
            sleep(std::time::Duration::from_millis(10)).await;
        }
    });

    let output = block_in_place(|| child.wait_with_output())?;

    handle.abort();

    let output = String::from_utf8(output.stdout)?;

    let lines: Vec<&str> = output.lines().collect();

    assert_eq!(lines.len(), 1);
    assert_starts_with!(lines[0], format!("{topic_name}-0"));
    assert_ends_with!(lines[0], "key1 value1");

    Ok(())
}

#[tokio::test]
async fn filter_key_and_value() -> Result<(), Box<dyn std::error::Error>> {
    let topic_name = "filter_key_and_value";

    empty_topic(topic_name, 1).await?;

    produce_messages(
        topic_name,
        None,
        None,
        vec![
            ("foo", "bar"),
            ("bar", "baz"),
            ("baz", "qux"),
            ("qux", "quux"),
        ],
    )
    .await?;

    let mut cmd = Command::cargo_bin("kiek")?;

    cmd.arg(topic_name);
    cmd.arg("--no-colors");
    cmd.arg("--max=4");
    cmd.arg("--filter=baz");
    cmd.arg("--timeout=5");

    let output = cmd.output()?;
    let output = String::from_utf8(output.stdout)?;

    println!("{}", output);

    let lines: Vec<&str> = output.lines().collect();

    assert_eq!(lines.len(), 2);
    assert_starts_with!(lines[0], format!("{topic_name}-0"));
    assert_ends_with!(lines[0], "1 bar baz");
    assert_starts_with!(lines[1], format!("{topic_name}-0"));
    assert_ends_with!(lines[1], "2 baz qux");

    Ok(())
}

#[tokio::test]
async fn scan_for_key() -> Result<(), Box<dyn std::error::Error>> {
    let topic_name = "scan_for_key";

    fn partition_for_key(key: &str, partitions: usize) -> usize {
        let hash = murmur2(key.as_bytes(), KAFKA_SEED);
        let hash = hash & 0x7fffffff; // "to positive" from Kafka's partitioner
        (hash % partitions as u32) as usize
    }

    let scanned_key = "a";
    let scanned_key_partition = partition_for_key(scanned_key, 2);
    let similar_partition_key = "b";
    let other_partition_key = "d";

    // assume
    assert_eq!(
        scanned_key_partition,
        partition_for_key(similar_partition_key, 2)
    );
    assert_ne!(
        scanned_key_partition,
        partition_for_key(other_partition_key, 2)
    );

    empty_topic(topic_name, 2).await?;

    produce_messages(
        topic_name,
        None,
        None,
        vec![
            (similar_partition_key, "value"),
            (other_partition_key, "value"),
            (scanned_key, "value"),
        ],
    )
    .await?;

    let mut cmd = Command::cargo_bin("kiek")?;

    cmd.arg(topic_name);
    cmd.arg("--no-colors");
    cmd.arg("--max=2");
    cmd.arg(format!("--key={scanned_key}"));
    cmd.arg("--timeout=5");

    let output = cmd.output()?;
    let output = String::from_utf8(output.stdout)?;

    let lines: Vec<&str> = output.lines().collect();

    assert_eq!(lines.len(), 1);
    assert_starts_with!(lines[0], format!("{topic_name}-{scanned_key_partition}"));
    assert_ends_with!(lines[0], format!(" {scanned_key} value"));

    Ok(())
}

#[tokio::test]
async fn scan_for_key_with_wrong_partitioning() -> Result<(), Box<dyn std::error::Error>> {
    let topic_name = "scan_for_key_with_wrong_partitioning";

    empty_topic(topic_name, 2).await?;

    // Given a topic that does not use standard partitioning (2 partitions with the same keys)
    produce_messages(
        topic_name,
        Some(0),
        None,
        vec![
            ("0", "value"),
            ("1", "value"),
            ("2", "value"),
            ("3", "value"),
        ],
    )
    .await?;

    produce_messages(
        topic_name,
        Some(1),
        None,
        vec![
            ("0", "value"),
            ("1", "value"),
            ("2", "value"),
            ("3", "value"),
        ],
    )
    .await?;

    // When we scan for a key
    let scanned_key = "4";

    let mut cmd = Command::cargo_bin("kiek")?;

    cmd.arg(topic_name);
    cmd.arg("--no-colors");
    cmd.arg("-v");
    cmd.arg(format!("--key={scanned_key}"));
    cmd.arg("--timeout=5");

    // Then kiek should fail indicating that the wrong partitioning

    cmd.assert().failure().stderr(predicates::str::contains(
        format!("You can pass the specific partition as {topic_name}-p or use -f, --filter {scanned_key} instead."),
    ));

    Ok(())
}

#[tokio::test]
async fn read_with_from_relative_date() -> Result<(), Box<dyn std::error::Error>> {
    let topic_name = "read-with-from-relative-date";

    empty_topic(topic_name, 2).await?;

    produce_messages(
        topic_name,
        None,
        None,
        vec![
            ("0", "before"),
            ("1", "before"),
            ("2", "before"),
            ("3", "before"),
        ],
    )
    .await?;

    sleep(std::time::Duration::from_millis(1500)).await;

    produce_messages(
        topic_name,
        None,
        None,
        vec![
            ("0", "after"),
            ("1", "after"),
            ("2", "after"),
            ("3", "after"),
        ],
    )
    .await?;

    let mut cmd = Command::cargo_bin("kiek")?;

    cmd.arg(topic_name);
    cmd.arg("--no-colors");
    cmd.arg("-v");
    cmd.arg("--max=4");
    cmd.arg("--from=-1s");
    cmd.arg("--timeout=5");

    let output = cmd.output()?;
    let output = String::from_utf8(output.stdout)?;

    assert_contains!(output, "0 after");
    assert_contains!(output, "1 after");
    assert_contains!(output, "2 after");
    assert_contains!(output, "3 after");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn read_from_now() -> Result<(), Box<dyn std::error::Error>> {
    let topic_name = "read_from_now";

    empty_topic(topic_name, 1).await?;

    produce_messages(topic_name, None, None, vec![("0", "before")]).await?;

    sleep(std::time::Duration::from_millis(1000)).await;

    let mut cmd = Command::cargo_bin("kiek")?;

    cmd.arg(topic_name);
    cmd.arg("--no-colors");
    cmd.arg("--from=now");
    cmd.arg("--max=1");
    cmd.arg("--timeout=5");

    let child = cmd.stdout(Stdio::piped()).spawn()?;

    sleep(std::time::Duration::from_millis(500)).await;

    let handle = tokio::spawn(async move {
        loop {
            produce_messages(topic_name, None, None, vec![("0", "after")])
                .await
                .unwrap();
            sleep(std::time::Duration::from_millis(10)).await;
        }
    });

    let output = block_in_place(|| child.wait_with_output())?;

    handle.abort();

    let output = String::from_utf8(output.stdout)?;

    let lines: Vec<&str> = output.lines().collect();

    assert_eq!(lines.len(), 1);
    assert_starts_with!(lines[0], format!("{topic_name}-0"));
    assert_contains!(lines[0], "0 after");

    Ok(())
}

#[tokio::test]
async fn read_from_to_relative_dates() -> Result<(), Box<dyn std::error::Error>> {
    let topic_name = "read-from-to-relative-dates";

    empty_topic(topic_name, 2).await?;

    let wait = 2;

    produce_messages(
        topic_name,
        None,
        None,
        vec![
            ("0", "before"),
            ("1", "before"),
            ("2", "before"),
            ("3", "before"),
        ],
    )
    .await?;

    sleep(std::time::Duration::from_millis(wait * 1000)).await;

    produce_messages(
        topic_name,
        None,
        None,
        vec![
            ("0", "actual"),
            ("1", "actual"),
            ("2", "actual"),
            ("3", "actual"),
        ],
    )
    .await?;

    sleep(std::time::Duration::from_millis(wait * 1000)).await;

    produce_messages(
        topic_name,
        None,
        None,
        vec![
            ("0", "after"),
            ("1", "after"),
            ("2", "after"),
            ("3", "after"),
        ],
    )
    .await?;

    let mut cmd = Command::cargo_bin("kiek")?;

    cmd.arg(topic_name);
    cmd.arg("--no-colors");
    cmd.arg("-v");
    cmd.arg(format!("--from=-{}s", 2 * wait));
    cmd.arg(format!("--to=-{}s", wait));
    cmd.arg("--timeout=5");

    let output = cmd.output()?;
    let error = String::from_utf8(output.stderr)?;
    let output = String::from_utf8(output.stdout)?;

    assert_is_empty!(error);

    assert_contains!(output, "0 actual");
    assert_contains!(output, "1 actual");
    assert_contains!(output, "2 actual");
    assert_contains!(output, "3 actual");

    assert_not_contains!(output, "0 before");
    assert_not_contains!(output, "1 before");
    assert_not_contains!(output, "2 before");
    assert_not_contains!(output, "3 before");

    assert_not_contains!(output, "0 after");
    assert_not_contains!(output, "1 after");
    assert_not_contains!(output, "2 after");
    assert_not_contains!(output, "3 after");
    Ok(())
}

#[tokio::test]
async fn read_from_to_absolute_dates() -> Result<(), Box<dyn std::error::Error>> {
    let topic_name = "read-from-to-absolute-dates";

    empty_topic(topic_name, 2).await?;

    produce_messages(
        topic_name,
        None,
        None,
        vec![
            ("0", "before"),
            ("1", "before"),
            ("2", "before"),
            ("3", "before"),
        ],
    )
    .await?;

    sleep(std::time::Duration::from_millis(1000)).await;

    let from = chrono::Utc::now().with_timezone(&Local);

    produce_messages(
        topic_name,
        None,
        None,
        vec![
            ("0", "actual"),
            ("1", "actual"),
            ("2", "actual"),
            ("3", "actual"),
        ],
    )
    .await?;

    let to = chrono::Utc::now().with_timezone(&Local);

    sleep(std::time::Duration::from_millis(1000)).await;

    produce_messages(
        topic_name,
        None,
        None,
        vec![
            ("0", "after"),
            ("1", "after"),
            ("2", "after"),
            ("3", "after"),
        ],
    )
    .await?;

    let mut cmd = Command::cargo_bin("kiek")?;

    cmd.arg(topic_name);
    cmd.arg("--no-colors");
    cmd.arg("-v");
    cmd.arg(format!("--from={}", from.format("%Y-%m-%d %H:%M:%S")));
    cmd.arg(format!("--to={}", to.format("%Y-%m-%d %H:%M:%S")));
    cmd.arg("--timeout=5");

    let output = cmd.output()?;
    let error = String::from_utf8(output.stderr)?;
    let output = String::from_utf8(output.stdout)?;

    assert_is_empty!(error);

    assert_contains!(output, "0 actual");
    assert_contains!(output, "1 actual");
    assert_contains!(output, "2 actual");
    assert_contains!(output, "3 actual");

    assert_not_contains!(output, "0 before");
    assert_not_contains!(output, "1 before");
    assert_not_contains!(output, "2 before");
    assert_not_contains!(output, "3 before");

    assert_not_contains!(output, "0 after");
    assert_not_contains!(output, "1 after");
    assert_not_contains!(output, "2 after");
    assert_not_contains!(output, "3 after");
    Ok(())
}

#[tokio::test]
async fn read_compressed() -> Result<(), Box<dyn std::error::Error>> {
    let topic_name = "read_compressed";

    empty_topic(topic_name, 1).await?;

    let compressions = vec!["gzip", "snappy", "lz4", "zstd"];

    for compression in compressions.iter() {
        produce_messages(
            topic_name,
            None,
            Some(compression),
            vec![(*compression, *compression)],
        )
        .await?;
    }

    let mut cmd = Command::cargo_bin("kiek")?;

    cmd.arg(topic_name);
    cmd.arg("--no-colors");
    cmd.arg(format!("--max={}", compressions.len()));
    cmd.arg("--timeout=5");

    let output = cmd.output()?;
    let output = String::from_utf8(output.stdout)?;
    let lines: Vec<&str> = output.lines().collect();

    for (i, compression) in compressions.iter().enumerate() {
        assert_starts_with!(lines[i], format!("{topic_name}-0"));
        assert_ends_with!(lines[i], format!("{} {}", compression, compression));
    }

    Ok(())
}

// Helpers

async fn empty_topic(topic_name: &str, partitions: i32) -> Result<(), Box<dyn std::error::Error>> {
    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers", "127.0.0.1:9092");

    let opts = AdminOptions::default().request_timeout(Some(std::time::Duration::from_secs(10)));

    let admin_client: AdminClient<_> = client_config
        .create()
        .expect("Failed to create admin client");

    let _ = admin_client.delete_topics(&[topic_name], &opts).await;

    let topics = vec![NewTopic::new(
        topic_name,
        partitions,
        TopicReplication::Fixed(1),
    )];
    admin_client.create_topics(&topics, &opts).await?;

    Ok(())
}

async fn produce_messages<I>(
    topic_name: &str,
    partition: Option<i32>,
    compression: Option<&str>,
    messages: I,
) -> Result<(), Box<dyn std::error::Error>>
where
    I: IntoIterator<Item = (&'static str, &'static str)>,
{
    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers", "127.0.0.1:9092");
    client_config.set("partitioner", "murmur2_random"); // the Java client partitioner
    client_config.set("compression.type", compression.unwrap_or("none"));

    let producer: FutureProducer = client_config.create()?;

    join_all(messages.into_iter().map(|(key, message)| {
        let record = rdkafka::producer::FutureRecord::to(topic_name)
            .key(key)
            .payload(message);

        let record = match partition {
            Some(partition) => record.partition(partition),
            None => record,
        };

        producer.send(record, std::time::Duration::from_secs(1))
    }))
    .await;

    Ok(())
}

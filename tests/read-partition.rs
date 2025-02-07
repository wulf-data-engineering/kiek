use assert_cmd::prelude::*;
use assertables::{assert_ends_with, assert_starts_with};
use futures::future::join_all;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::producer::FutureProducer;
use rdkafka::ClientConfig;
use std::process::{Command, Stdio};
use tokio::task::block_in_place;
use tokio::time::sleep;

#[tokio::test]
async fn read_from_beginning() -> Result<(), Box<dyn std::error::Error>> {
    let topic_name = "read_from_beginning";

    empty_topic(topic_name, 1).await?;

    produce_messages(topic_name, vec![("key0", "value0"), ("key1", "value1")]).await?;

    let mut cmd = Command::cargo_bin("kiek")?;

    cmd.arg(topic_name);
    cmd.arg("--no-colors");
    cmd.arg("--max=2");

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

    produce_messages(topic_name, vec![("key0", "value0")]).await?;

    let mut cmd = Command::cargo_bin("kiek")?;

    cmd.arg(topic_name);
    cmd.arg("--no-colors");
    cmd.arg("--latest");
    cmd.arg("--max=1");

    let child = cmd.stdout(Stdio::piped()).spawn()?;

    let handle = tokio::spawn(async move {
        loop {
            produce_messages(topic_name, vec![("key1", "value1")])
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

async fn empty_topic(topic_name: &str, partitions: i32) -> Result<(), Box<dyn std::error::Error>> {
    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers", "127.0.0.1:9092");

    let opts = AdminOptions::default().request_timeout(Some(std::time::Duration::from_secs(2)));

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
    messages: I,
) -> Result<(), Box<dyn std::error::Error>>
where
    I: IntoIterator<Item = (&'static str, &'static str)>,
{
    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers", "127.0.0.1:9092");

    let producer: FutureProducer = client_config.create()?;

    join_all(messages.into_iter().map(|(key, message)| {
        producer.send(
            rdkafka::producer::FutureRecord::to(topic_name)
                .key(key)
                .payload(message),
            std::time::Duration::from_secs(1),
        )
    }))
    .await;

    Ok(())
}

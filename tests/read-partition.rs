use assert_cmd::prelude::*;
use futures::future::join_all;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::producer::FutureProducer;
use rdkafka::ClientConfig;
use std::process::Command;

#[tokio::test]
async fn read_from_beginning() -> Result<(), Box<dyn std::error::Error>> {
    let topic_name = "read";

    empty_topic(topic_name).await?;

    produce_messages(topic_name, vec![("key", "value1"), ("key", "value2")]).await?;

    let mut cmd = Command::cargo_bin("kiek")?;

    cmd.arg(topic_name);
    cmd.arg("--max=2");

    let output = cmd.output()?;
    let string = String::from_utf8(output.stdout)?;

    assert!(string.contains("key value1"));
    assert!(string.contains("key value2"));

    Ok(())
}

async fn empty_topic(topic_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers", "127.0.0.1:9092");

    let opts = AdminOptions::default().request_timeout(Some(std::time::Duration::from_secs(2)));

    let admin_client: AdminClient<_> = client_config
        .create()
        .expect("Failed to create admin client");

    let _ = admin_client.delete_topics(&[topic_name], &opts).await;

    let topics = vec![NewTopic::new(topic_name, 1, TopicReplication::Fixed(1))];
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

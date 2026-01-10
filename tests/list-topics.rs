use assert_cmd::prelude::*;
use assertables::assert_contains;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::ClientConfig;
use std::process::Command;

#[tokio::test]
async fn list_topics_with_list_flag() -> Result<(), Box<dyn std::error::Error>> {
    // Create some test topics
    empty_topic("list-test-topic-1", 1).await?;
    empty_topic("list-test-topic-2", 2).await?;

    let mut cmd = Command::cargo_bin("kiek")?;
    cmd.arg("--list");
    cmd.arg("--no-colors");
    if let Ok(bootstrap_servers) = std::env::var("KIEK_TEST_BOOTSTRAP_SERVERS") {
        cmd.arg("--bootstrap-servers");
        cmd.arg(bootstrap_servers);
    }

    let output = cmd.output()?;
    let output = String::from_utf8(output.stdout)?;

    // Should contain both test topics
    assert_contains!(output, "list-test-topic-1 (1)");
    assert_contains!(output, "list-test-topic-2 (2)");

    Ok(())
}

#[tokio::test]
async fn list_topics_with_list_topics_flag() -> Result<(), Box<dyn std::error::Error>> {
    // Create a test topic
    let topic_name = "list-topics-test";
    empty_topic(topic_name, 5).await?;

    let mut cmd = Command::cargo_bin("kiek")?;
    cmd.arg("--list-topics");
    cmd.arg("--no-colors");
    if let Ok(bootstrap_servers) = std::env::var("KIEK_TEST_BOOTSTRAP_SERVERS") {
        cmd.arg("--bootstrap-servers");
        cmd.arg(bootstrap_servers);
    }

    let output = cmd.output()?;
    let output = String::from_utf8(output.stdout)?;

    // Should contain the test topic with partition count
    assert_contains!(output, "list-topics-test (5)");

    Ok(())
}

#[tokio::test]
async fn list_topics_with_custom_broker() -> Result<(), Box<dyn std::error::Error>> {
    // Create a test topic
    let topic_name = "list-custom-broker-test";
    empty_topic(topic_name, 2).await?;

    let mut cmd = Command::cargo_bin("kiek")?;
    cmd.arg("--list");
    cmd.arg("--bootstrap-servers");
    cmd.arg(std::env::var("KIEK_TEST_BOOTSTRAP_SERVERS").unwrap_or("127.0.0.1:9092".into()));
    cmd.arg("--no-colors");

    let output = cmd.output()?;
    let output = String::from_utf8(output.stdout)?;

    // Should contain the test topic with partition count
    assert_contains!(output, "list-custom-broker-test (2)");

    Ok(())
}

#[tokio::test]
async fn list_topics_no_topics_available() -> Result<(), Box<dyn std::error::Error>> {
    // Delete all topics first
    let topic_names = vec![
        "list-test-topic-1",
        "list-test-topic-2",
        "list-topics-test",
        "list-custom-broker-test",
    ];

    let mut client_config = ClientConfig::new();
    client_config.set(
        "bootstrap.servers",
        std::env::var("KIEK_TEST_BOOTSTRAP_SERVERS").unwrap_or("127.0.0.1:9092".into()),
    );

    let opts = AdminOptions::default().request_timeout(Some(std::time::Duration::from_secs(10)));

    let admin_client: AdminClient<_> = client_config
        .create()
        .expect("Failed to create admin client");

    let _ = admin_client.delete_topics(&topic_names, &opts).await;

    // Wait a bit for topics to be deleted
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    let mut cmd = Command::cargo_bin("kiek")?;
    cmd.arg("--list");
    cmd.arg("--no-colors");
    if let Ok(bootstrap_servers) = std::env::var("KIEK_TEST_BOOTSTRAP_SERVERS") {
        cmd.arg("--bootstrap-servers");
        cmd.arg(bootstrap_servers);
    }

    let output = cmd.output()?;
    let output = String::from_utf8(output.stdout)?;

    // Should handle empty topic list gracefully
    // The output should be empty or contain a message about no topics
    println!("Output: {}", output);

    Ok(())
}

// Helper function
async fn empty_topic(topic_name: &str, partitions: i32) -> Result<(), Box<dyn std::error::Error>> {
    let mut client_config = ClientConfig::new();
    client_config.set(
        "bootstrap.servers",
        std::env::var("KIEK_TEST_BOOTSTRAP_SERVERS").unwrap_or("127.0.0.1:9092".into()),
    );

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

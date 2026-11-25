//! End-to-end tests of `SITE SEND`, `SITE TOPICS` and `SITE TOPIC` against a real Kafka broker, provided by
//! `testcontainers`. Requires a working Docker daemon, so it's excluded from
//! the default `cargo test` run - opt in explicitly with:
//!
//! ```sh
//! cargo test --test kafka_integration -- --ignored
//! ```
//!
//! Docker maps the container's Kafka listener to a random host port, which
//! the test passes to the server through `config.toml`.

mod common;

use std::time::Duration;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::ClientConfig;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{Kafka, KAFKA_PORT};

use common::{TestServer, PASSWORD, USERNAME};

const TOPIC: &str = "rust-topic";
const OTHER_TOPIC: &str = "other-topic";

#[tokio::test]
#[ignore = "requires a running Docker daemon"]
async fn site_send_publishes_the_uploaded_file_to_kafka() {
    let kafka_container = Kafka::default()
        .start()
        .await
        .expect("failed to start the Kafka test container");
    let kafka_port = kafka_container
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("failed to read the Kafka container's host port");
    let brokers = format!("127.0.0.1:{kafka_port}");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "flatpack-integration-test")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("failed to create Kafka consumer");
    consumer
        .subscribe(&[TOPIC])
        .expect("failed to subscribe to topic");

    let server = TestServer::start_with_config(&format!(
        r#"
            [kafka]
            brokers = "{brokers}"
            default_topic = "{TOPIC}"
        "#
    ));
    let mut client = server.connect();
    client.login(USERNAME, PASSWORD);

    let store_reply = client.store("hello.txt", b"hello kafka");
    assert!(store_reply.starts_with('2'), "STOR failed: {store_reply}");

    let send_reply = client.command("SITE SEND hello.txt");
    assert!(
        send_reply.starts_with('2'),
        "SITE SEND failed: {send_reply}"
    );

    // `kafka.rs` relies on Kafka's auto-create-topics behaviour rather than
    // pre-creating the topic, so the consumer may see `UnknownTopicOrPartition`
    // for a brief moment while the topic is created and its metadata
    // propagates. Retry until a message arrives or the overall timeout fires.
    let message = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match consumer.recv().await {
                Ok(message) => return message,
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .expect("timed out waiting for the Kafka message");

    assert_eq!(message.key(), Some("hello.txt".as_bytes()));
    assert_eq!(message.payload(), Some("hello kafka".as_bytes()));
}

#[tokio::test]
#[ignore = "requires a running Docker daemon"]
async fn site_topics_and_site_topic_work_against_a_real_broker() {
    let kafka_container = Kafka::default()
        .start()
        .await
        .expect("failed to start the Kafka test container");
    let kafka_port = kafka_container
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("failed to read the Kafka container's host port");
    let brokers = format!("127.0.0.1:{kafka_port}");

    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("failed to create the Kafka admin client");
    admin
        .create_topics(
            &[
                NewTopic::new(TOPIC, 1, TopicReplication::Fixed(1)),
                NewTopic::new(OTHER_TOPIC, 1, TopicReplication::Fixed(1)),
            ],
            &AdminOptions::new(),
        )
        .await
        .expect("failed to create the topic");

    let server = TestServer::start_with_config(&format!(
        r#"
            [kafka]
            brokers = "{brokers}"
            default_topic = "{TOPIC}"
        "#
    ));
    let mut client = server.connect();
    client.login(USERNAME, PASSWORD);

    let listed = client.command_expecting("213", "SITE TOPICS");
    assert!(listed.contains(TOPIC), "{listed}");
    assert!(listed.contains(OTHER_TOPIC), "{listed}");
    assert!(!listed.contains("__consumer_offsets"), "{listed}");

    let before = client.command_expecting("200", "SITE TOPIC");
    assert!(before.contains(TOPIC), "{before}");
    client.command_expecting("200", &format!("SITE TOPIC {OTHER_TOPIC}"));
    let after = client.command_expecting("200", "SITE TOPIC");
    assert!(after.contains(OTHER_TOPIC), "{after}");

    let refused = client.command_expecting("5", "SITE TOPIC no-such-topic");
    assert!(refused.contains("no-such-topic"), "{refused}");
    let unchanged = client.command_expecting("200", "SITE TOPIC");
    assert!(unchanged.contains(OTHER_TOPIC), "{unchanged}");
}

#[tokio::test]
#[ignore = "requires a running Docker daemon"]
async fn site_topic_create_makes_the_topic_and_sets_it_as_the_session_topic() {
    let kafka_container = Kafka::default()
        .start()
        .await
        .expect("failed to start the Kafka test container");
    let kafka_port = kafka_container
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("failed to read the Kafka container's host port");
    let server = TestServer::start_with_config(&format!(
        r#"
            [kafka]
            brokers = "127.0.0.1:{kafka_port}"
            default_topic = "{TOPIC}"
        "#
    ));
    let mut client = server.connect();
    client.login(USERNAME, PASSWORD);
    client.store("hello.txt", b"hello kafka");

    client.command_expecting("200", "SITE TOPIC -c created-topic");
    let current = client.command_expecting("200", "SITE TOPIC");
    assert!(current.contains("created-topic"), "{current}");
    let listed = client.command_expecting("213", "SITE TOPICS 'created-*'");
    assert!(listed.contains("created-topic"), "{listed}");
    let sent = client.command_expecting("200", "SITE SEND hello.txt");
    assert!(sent.contains("created-topic"), "{sent}");

    let existing = client.command_expecting("550", "SITE TOPIC -c created-topic");
    assert!(existing.contains("already exists"), "{existing}");
    client.command_expecting("501", "SITE TOPIC -c bad/name");
    client.command_expecting("501", "SITE TOPIC -c");
    let unchanged = client.command_expecting("200", "SITE TOPIC");
    assert!(unchanged.contains("created-topic"), "{unchanged}");
}

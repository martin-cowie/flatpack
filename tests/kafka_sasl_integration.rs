//! FTP logins checked by a real Kafka broker that requires SASL/SCRAM.
//! Requires a working Docker daemon, so it's excluded from the default
//! `cargo test` run - opt in explicitly with:
//!
//! ```sh
//! cargo test --test kafka_sasl_integration -- --ignored
//! ```

mod common;

use std::time::{Duration, Instant};

use common::sasl_kafka::{SaslKafka, SASL_PORT};
use common::TestServer;

const KAFKA_USER: &str = "alice";
const KAFKA_PASSWORD: &str = "correct-horse";

fn server_config(brokers: &str, mechanism: &str) -> String {
    format!(
        r#"
            [kafka]
            brokers = "{brokers}"
            default_topic = "rust-topic"
            security_protocol = "SASL_PLAINTEXT"
            sasl_mechanism = "{mechanism}"
            auth_timeout_ms = 30000
        "#
    )
}

#[tokio::test]
#[ignore = "requires a running Docker daemon"]
async fn scram_logins_are_accepted_only_with_the_kafka_password() {
    let broker = SaslKafka::with_user(KAFKA_USER, KAFKA_PASSWORD)
        .start()
        .await;
    let port = broker.get_host_port_ipv4(SASL_PORT).await.unwrap();
    let brokers = format!("127.0.0.1:{port}");

    for mechanism in ["SCRAM-SHA-256", "SCRAM-SHA-512"] {
        let server = TestServer::start_with_config(&server_config(&brokers, mechanism));

        let accepted = server.connect().login(KAFKA_USER, KAFKA_PASSWORD);
        assert!(
            accepted.starts_with('2'),
            "{mechanism}: correct password rejected: {accepted}"
        );

        let started = Instant::now();
        let rejected = server.connect().login(KAFKA_USER, "wrong-password");
        assert!(
            rejected.starts_with('5'),
            "{mechanism}: wrong password accepted: {rejected}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{mechanism}: wrong password took {:?} to reject; expected a fast failure",
            started.elapsed()
        );

        let unknown = server.connect().login("mallory", KAFKA_PASSWORD);
        assert!(
            unknown.starts_with('5'),
            "{mechanism}: unknown user accepted: {unknown}"
        );
    }
}

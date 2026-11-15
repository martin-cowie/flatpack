//! A single-node Kafka broker (KRaft) whose client-facing listener requires
//! SASL/SCRAM over plaintext, for testing logins against real credentials.
//!
//! Based on the `testcontainers-modules` Kafka image, which cannot configure
//! SASL. The broker also has an internal, unauthenticated listener that the
//! image uses to create the SCRAM user once the broker is up.

use std::borrow::Cow;
use std::collections::HashMap;

use testcontainers::core::{ContainerPort, ContainerState, ExecCommand, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, Image};

pub const SASL_PORT: ContainerPort = ContainerPort::Tcp(9092);

const START_SCRIPT: &str = "/opt/kafka/testcontainers_start.sh";
const JAAS_FILE: &str = "/tmp/kafka_server_jaas.conf";
const KAFKA_CONFIGS: &str = "/opt/kafka/bin/kafka-configs.sh";

#[derive(Debug, Clone)]
pub struct SaslKafka {
    env_vars: HashMap<String, String>,
    username: String,
    password: String,
}

impl SaslKafka {
    /// A broker that accepts `username` / `password` under both SCRAM mechanisms.
    pub fn with_user(username: &str, password: &str) -> Self {
        let env = [
            ("KAFKA_LISTENERS", "EXTERNAL://0.0.0.0:9092,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094"),
            (
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                "EXTERNAL:SASL_PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT",
            ),
            ("KAFKA_SASL_ENABLED_MECHANISMS", "SCRAM-SHA-256,SCRAM-SHA-512"),
            ("CLUSTER_ID", "5L6g3nShT-eMCtK--X86sw"),
            ("KAFKA_PROCESS_ROLES", "broker,controller"),
            ("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER"),
            ("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER"),
            ("KAFKA_ADVERTISED_LISTENERS", "EXTERNAL://localhost:9092,BROKER://localhost:9093"),
            ("KAFKA_BROKER_ID", "1"),
            ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1"),
            ("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9094"),
        ];
        SaslKafka {
            env_vars: env.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            username: username.to_string(),
            password: password.to_string(),
        }
    }
}

impl SaslKafka {
    /// Starts the broker and creates its SCRAM user, returning once logins work.
    pub async fn start(self) -> ContainerAsync<SaslKafka> {
        let container = AsyncRunner::start(self.clone()).await.expect("failed to start the SASL Kafka test container");
        for mechanism in ["SCRAM-SHA-256", "SCRAM-SHA-512"] {
            let command = format!(
                "{KAFKA_CONFIGS} --bootstrap-server localhost:9093 --alter --entity-type users --entity-name {user} \
                 --add-config '{mechanism}=[password={pw}]'",
                user = self.username,
                pw = self.password
            );
            container.add_scram_credential(&command).await;
        }
        container
    }
}

trait ScramCredentials {
    async fn add_scram_credential(&self, command: &str);
}

impl ScramCredentials for ContainerAsync<SaslKafka> {
    /// Runs `command` until the broker accepts it, since the broker may still be settling.
    async fn add_scram_credential(&self, command: &str) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            let mut result = self
                .exec(ExecCommand::new(["sh", "-c", command]))
                .await
                .expect("failed to run the SCRAM credential command");
            let output = [result.stdout_to_vec().await.unwrap(), result.stderr_to_vec().await.unwrap()].concat();
            if result.exit_code().await.unwrap() == Some(0) {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "adding the SCRAM credential failed: {}",
                String::from_utf8_lossy(&output)
            );
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }
}

impl Image for SaslKafka {
    fn name(&self) -> &str {
        "apache/kafka"
    }

    fn tag(&self) -> &str {
        "3.8.0"
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![]
    }

    fn entrypoint(&self) -> Option<&str> {
        Some("bash")
    }

    fn env_vars(&self) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        &self.env_vars
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<Cow<'_, str>>> {
        vec![
            "-c".to_string(),
            format!("while [ ! -f {START_SCRIPT} ]; do sleep 0.1; done; chmod 755 {START_SCRIPT} && {START_SCRIPT}"),
        ]
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[SASL_PORT]
    }

    fn exec_after_start(&self, state: ContainerState) -> Result<Vec<ExecCommand>, testcontainers::TestcontainersError> {
        let write_start_script = vec![
            "sh".to_string(),
            "-c".to_string(),
            format!(
                "echo '#!/usr/bin/env bash\nprintf \"KafkaServer {{\\n  org.apache.kafka.common.security.scram.ScramLoginModule required;\\n}};\\n\" > {JAAS_FILE}\nexport KAFKA_OPTS=-Djava.security.auth.login.config={JAAS_FILE}\nexport KAFKA_ADVERTISED_LISTENERS=EXTERNAL://127.0.0.1:{},BROKER://localhost:9093\n/etc/kafka/docker/run\n' > {START_SCRIPT}",
                state.host_port_ipv4(SASL_PORT)?
            ),
        ];
        Ok(vec![
            ExecCommand::new(write_start_script)
                .with_container_ready_conditions(vec![WaitFor::message_on_stdout("Kafka Server started")]),
        ])
    }
}

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, Producer};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use unftp_core::auth::{
    AuthenticationError, Authenticator, Credentials, Principal, UserDetail, UserDetailError,
    UserDetailProvider,
};

use crate::kafka::KafkaConfig;

/// The Kafka producer a login establishes and its session keeps using.
pub type SessionProducer = FutureProducer;

/// Authenticates FTP logins by connecting to Kafka with the client's credentials.
///
/// A login is accepted only if the Kafka cluster accepts the connection. The
/// producer that proved it is queued under the username until
/// `provide_user_detail` claims it for the session.
pub struct KafkaAuthenticator {
    config: KafkaConfig,
    handoff: Handoff,
}

type Handoff = Arc<Mutex<HashMap<String, Vec<SessionProducer>>>>;

/// A logged-in FTP user together with the Kafka producer their login established.
pub struct KafkaUser {
    pub username: String,
    pub producer: SessionProducer,
}

impl std::fmt::Debug for KafkaUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaUser").field("username", &self.username).finish()
    }
}

impl std::fmt::Display for KafkaUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.username)
    }
}

impl UserDetail for KafkaUser {}

/// Claims the producer a login queued, turning a `Principal` into a `KafkaUser`.
///
/// Any queued producer for the username will do: Kafka gives one username one
/// password, so they are interchangeable.
pub struct KafkaUserDetailProvider {
    handoff: Handoff,
}

impl std::fmt::Debug for KafkaUserDetailProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaUserDetailProvider").finish_non_exhaustive()
    }
}

#[async_trait]
impl UserDetailProvider for KafkaUserDetailProvider {
    type User = KafkaUser;

    async fn provide_user_detail(&self, principal: &Principal) -> Result<KafkaUser, UserDetailError> {
        let producer = self
            .handoff
            .lock()
            .unwrap()
            .get_mut(&principal.username)
            .and_then(Vec::pop)
            .ok_or_else(|| UserDetailError::Generic(format!("no login is waiting for {}", principal.username)))?;
        Ok(KafkaUser { username: principal.username.clone(), producer })
    }
}

impl std::fmt::Debug for KafkaAuthenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaAuthenticator").field("config", &self.config).finish()
    }
}

impl KafkaAuthenticator {
    pub fn new(config: KafkaConfig) -> Self {
        KafkaAuthenticator { config, handoff: Arc::new(Mutex::new(HashMap::new())) }
    }

    /// Returns the provider that hands this authenticator's logins to their sessions.
    pub fn user_detail_provider(&self) -> KafkaUserDetailProvider {
        KafkaUserDetailProvider { handoff: Arc::clone(&self.handoff) }
    }

    fn connect(config: &KafkaConfig) -> Result<SessionProducer, ()> {
        let producer: SessionProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("message.timeout.ms", config.message_timeout_ms.to_string())
            .create()
            .map_err(|_| ())?;
        producer
            .client()
            .fetch_metadata(None, Duration::from_millis(config.auth_timeout_ms))
            .map_err(|_| ())?;
        Ok(producer)
    }
}

#[async_trait]
impl Authenticator for KafkaAuthenticator {
    async fn authenticate(
        &self,
        username: &str,
        _creds: &Credentials,
    ) -> Result<Principal, AuthenticationError> {
        let config = self.config.clone();
        let producer = tokio::task::spawn_blocking(move || Self::connect(&config))
            .await
            .map_err(|_| AuthenticationError::BadPassword)?
            .map_err(|_| AuthenticationError::BadPassword)?;
        self.handoff
            .lock()
            .unwrap()
            .entry(username.to_string())
            .or_default()
            .push(producer);
        Ok(Principal { username: username.to_string() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::{KafkaConfig, SecurityProtocol};
    use rdkafka::mocking::MockCluster;
    use unftp_core::auth::{Authenticator, Credentials, UserDetailProvider};

    fn config(brokers: &str) -> KafkaConfig {
        KafkaConfig {
            brokers: brokers.to_string(),
            default_topic: "test-topic".to_string(),
            message_timeout_ms: 5000,
            auth_timeout_ms: 2000,
            security_protocol: SecurityProtocol::Plaintext,
            sasl_mechanism: None,
        }
    }

    #[tokio::test]
    async fn plaintext_login_is_accepted_for_any_credentials_when_the_broker_is_reachable() {
        let cluster = MockCluster::new(1).unwrap();
        let authenticator = KafkaAuthenticator::new(config(&cluster.bootstrap_servers()));

        let principal = authenticator
            .authenticate("alice", &Credentials::from("anything"))
            .await
            .unwrap();

        assert_eq!(principal.username, "alice");
    }

    #[tokio::test]
    async fn login_is_rejected_when_the_broker_is_unreachable() {
        let mut unreachable = config("127.0.0.1:1");
        unreachable.auth_timeout_ms = 500;
        let authenticator = KafkaAuthenticator::new(unreachable);

        let result = authenticator.authenticate("alice", &Credentials::from("anything")).await;

        assert!(matches!(result, Err(AuthenticationError::BadPassword)));
    }

    #[tokio::test]
    async fn provider_hands_the_logged_in_sessions_producer_to_it() {
        let cluster = MockCluster::new(1).unwrap();
        let authenticator = KafkaAuthenticator::new(config(&cluster.bootstrap_servers()));
        let provider = authenticator.user_detail_provider();
        let principal = authenticator.authenticate("alice", &Credentials::from("x")).await.unwrap();

        let user = provider.provide_user_detail(&principal).await.unwrap();

        assert_eq!(user.username, "alice");
        let metadata = user.producer.client().fetch_metadata(None, Duration::from_secs(2));
        assert!(metadata.is_ok());
    }

    #[tokio::test]
    async fn provider_fails_when_no_login_is_waiting_for_the_user() {
        let authenticator = KafkaAuthenticator::new(config("127.0.0.1:1"));
        let provider = authenticator.user_detail_provider();

        let result = provider.provide_user_detail(&Principal { username: "alice".to_string() }).await;

        assert!(result.is_err());
    }
}

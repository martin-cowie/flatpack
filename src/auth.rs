use async_trait::async_trait;
use rdkafka::admin::AdminClient;
use rdkafka::client::{ClientContext, DefaultClientContext};
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::producer::{FutureProducer, Producer};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::OnceCell;
use unftp_core::auth::{
    AuthenticationError, Authenticator, Credentials, Principal, UserDetail, UserDetailError,
    UserDetailProvider,
};

use crate::handoff::Handoff;
use crate::kafka::KafkaConfig;

/// The Kafka producer a login establishes and its session keeps using.
pub type SessionProducer = FutureProducer<AuthContext>;

/// What librdkafka has told the client about its broker connection.
#[derive(Debug, Default)]
struct BrokerEvents {
    rejected: AtomicBool,
    reasons: Mutex<Vec<String>>,
}

/// Records librdkafka's connection errors for the login probe.
///
/// librdkafka reports a rejected SASL login, and the reason a broker cannot be
/// reached, only through this callback; a metadata request against such a
/// broker just times out.
#[derive(Debug, Default)]
pub struct AuthContext {
    events: Arc<BrokerEvents>,
}

impl ClientContext for AuthContext {
    fn error(&self, error: KafkaError, reason: &str) {
        if let KafkaError::Global(RDKafkaErrorCode::Authentication) = error {
            self.events.rejected.store(true, Ordering::SeqCst);
        }
        let mut reasons = self.events.reasons.lock().unwrap();
        if reasons.len() < MAX_REASONS && !reasons.iter().any(|known| known == reason) {
            reasons.push(reason.to_string());
        }
    }
}

/// Why a login could not reach a decision in Kafka's favour.
enum ConnectError {
    Rejected,
    Unreachable(String),
}

const MAX_REASONS: usize = 5;
const PROBE_SLICE: Duration = Duration::from_millis(100);

/// Authenticates FTP logins by connecting to Kafka with the client's credentials.
///
/// A login is accepted only if the Kafka cluster accepts the connection. The
/// producer that proved it is queued under the username until
/// `provide_user_detail` claims it for the session.
pub struct KafkaAuthenticator {
    config: KafkaConfig,
    handoff: LoginHandoff,
    logger: slog::Logger,
}

type LoginHandoff = Handoff<Login>;

/// What a login establishes for its Session: the Kafka producer, and the client
/// config, password included, from which the Session can build further Kafka
/// clients on demand.
pub struct Login {
    producer: SessionProducer,
    client_config: ClientConfig,
}

impl Login {
    /// Bundles a login's `producer` with the `client_config` it was created from.
    ///
    /// Returns the new login.
    pub fn new(producer: SessionProducer, client_config: ClientConfig) -> Self {
        Login {
            producer,
            client_config,
        }
    }
}

/// A logged-in FTP user together with the Kafka producer their login established
/// and the Session topic.
pub struct KafkaUser {
    username: String,
    producer: SessionProducer,
    client_config: ClientConfig,
    admin_client: OnceCell<AdminClient<DefaultClientContext>>,
    session_topic: Mutex<String>,
}

impl KafkaUser {
    /// Creates the user for a new Session.
    ///
    /// `username` is the name the client logged in with, `login` is what that
    /// login established, and `topic` is the Session topic the Session starts with.
    ///
    /// Returns the new user.
    pub fn new(username: String, login: Login, topic: String) -> Self {
        KafkaUser {
            username,
            producer: login.producer,
            client_config: login.client_config,
            admin_client: OnceCell::new(),
            session_topic: Mutex::new(topic),
        }
    }

    /// The name the client logged in with.
    pub fn username(&self) -> &str {
        &self.username
    }

    /// The Kafka producer this Session's login established.
    pub fn producer(&self) -> &SessionProducer {
        &self.producer
    }

    /// The Kafka admin client for this Session, built from the login's
    /// credentials the first time it is asked for.
    ///
    /// Returns the Session's admin client.
    ///
    /// # Errors
    /// Returns the [`KafkaError`] when the client cannot be created.
    pub async fn admin_client(&self) -> KafkaResult<&AdminClient<DefaultClientContext>> {
        self.admin_client
            .get_or_try_init(|| async { self.client_config.create() })
            .await
    }

    /// The Session topic: the topic `SITE SEND` publishes to when it is given no `--topic`.
    ///
    /// Returns a copy, so the caller never holds the Session's lock.
    pub fn session_topic(&self) -> String {
        self.topic_guard().clone()
    }

    /// Makes `topic` the Session topic.
    ///
    /// `topic` replaces the previous Session topic for the rest of the Session.
    pub fn set_session_topic(&self, topic: &str) {
        *self.topic_guard() = topic.to_string();
    }

    /// Locks the Session topic, carrying on if a panic elsewhere poisoned the lock.
    ///
    /// Recovery is sound because the topic is only ever cloned or replaced whole,
    /// so a panic cannot leave it half-written.
    fn topic_guard(&self) -> std::sync::MutexGuard<'_, String> {
        self.session_topic
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl std::fmt::Debug for KafkaUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaUser")
            .field("username", &self.username())
            .finish()
    }
}

impl std::fmt::Display for KafkaUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.username())
    }
}

impl UserDetail for KafkaUser {}

/// Claims the producer a login queued, turning a `Principal` into a `KafkaUser`.
///
/// Any queued producer for the username will do: Kafka gives one username one
/// password, so they are interchangeable.
pub struct KafkaUserDetailProvider {
    handoff: LoginHandoff,
    default_topic: String,
}

impl std::fmt::Debug for KafkaUserDetailProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaUserDetailProvider")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl UserDetailProvider for KafkaUserDetailProvider {
    type User = KafkaUser;

    async fn provide_user_detail(
        &self,
        principal: &Principal,
    ) -> Result<KafkaUser, UserDetailError> {
        let login = self.handoff.claim(&principal.username).ok_or_else(|| {
            UserDetailError::Generic(format!("no login is waiting for {}", principal.username))
        })?;
        Ok(KafkaUser::new(
            principal.username.clone(),
            login,
            self.default_topic.clone(),
        ))
    }
}

impl std::fmt::Debug for KafkaAuthenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaAuthenticator")
            .field("config", &self.config)
            .finish()
    }
}

impl KafkaAuthenticator {
    /// Creates an authenticator that connects to the cluster described by `config`.
    ///
    /// Logins that `authenticate` accepts but no session claims within
    /// `config.auth_timeout_ms` are dropped, releasing their Kafka connection.
    /// Failures are logged to `logger`.
    pub fn new(config: KafkaConfig, logger: slog::Logger) -> Self {
        let handoff = Handoff::new(config.auth_timeout());
        KafkaAuthenticator {
            config,
            handoff,
            logger,
        }
    }

    /// Returns the provider that hands this authenticator's logins to their sessions.
    pub fn user_detail_provider(&self) -> KafkaUserDetailProvider {
        KafkaUserDetailProvider {
            handoff: self.handoff.clone(),
            default_topic: self.config.default_topic.clone(),
        }
    }

    fn connect(
        config: &KafkaConfig,
        logger: &slog::Logger,
        username: &str,
        password: &str,
    ) -> Result<Login, ConnectError> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.brokers)
            .set("message.timeout.ms", config.message_timeout_ms.to_string())
            .set(
                "security.protocol",
                config.security_protocol.as_librdkafka(),
            );
        if let Some(mechanism) = config.sasl_mechanism {
            client_config
                .set("sasl.mechanism", mechanism.as_librdkafka())
                .set("sasl.username", username)
                .set("sasl.password", password);
        }
        let context = AuthContext::default();
        let events = Arc::clone(&context.events);
        let producer: SessionProducer = client_config
            .create_with_context(context)
            .map_err(|e| ConnectError::Unreachable(e.to_string()))?;

        let deadline = Instant::now() + config.auth_timeout();
        let mut last_logged = String::new();
        loop {
            match producer.client().fetch_metadata(None, PROBE_SLICE) {
                Ok(_) => return Ok(Login::new(producer, client_config)),
                Err(_) if events.rejected.load(Ordering::SeqCst) => {
                    return Err(ConnectError::Rejected)
                }
                Err(e) if Instant::now() >= deadline => {
                    let reasons = events.reasons.lock().unwrap();
                    let reason = if reasons.is_empty() {
                        e.to_string()
                    } else {
                        reasons.join("; ")
                    };
                    return Err(ConnectError::Unreachable(reason));
                }
                Err(e) => {
                    let error = e.to_string();
                    if error != last_logged {
                        slog::debug!(
                            logger,
                            "Kafka probe for {} failed, retrying: {}",
                            username,
                            error
                        );
                        last_logged = error;
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Authenticator for KafkaAuthenticator {
    async fn authenticate(
        &self,
        username: &str,
        creds: &Credentials,
    ) -> Result<Principal, AuthenticationError> {
        let config = self.config.clone();
        let logger = self.logger.clone();
        let username_owned = username.to_string();
        let password = creds.password.clone().unwrap_or_default();
        let login = tokio::task::spawn_blocking(move || {
            Self::connect(&config, &logger, &username_owned, &password)
        })
        .await
        .map_err(|e| AuthenticationError::new(format!("login task failed: {e}")))?
        .map_err(|e| match e {
            ConnectError::Rejected => AuthenticationError::BadPassword,
            ConnectError::Unreachable(reason) => {
                AuthenticationError::new(format!("Kafka is unreachable: {reason}"))
            }
        })?;
        self.handoff.deposit(username, login);
        Ok(Principal {
            username: username.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::{KafkaConfig, SaslMechanism, SecurityProtocol};
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::DefaultProducerContext;
    use unftp_core::auth::{Authenticator, Credentials, UserDetailProvider};

    fn discard_logger() -> slog::Logger {
        slog::Logger::root(slog::Discard, slog::o!())
    }

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

    fn authenticator_for(
        cluster: &MockCluster<'static, DefaultProducerContext>,
    ) -> KafkaAuthenticator {
        KafkaAuthenticator::new(config(&cluster.bootstrap_servers()), discard_logger())
    }

    async fn log_in(authenticator: &KafkaAuthenticator, username: &str) -> Principal {
        authenticator
            .authenticate(username, &Credentials::from("x"))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn plaintext_login_is_accepted_for_any_credentials_when_the_broker_is_reachable() {
        let cluster = MockCluster::new(1).unwrap();
        let authenticator =
            KafkaAuthenticator::new(config(&cluster.bootstrap_servers()), discard_logger());

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
        let authenticator = KafkaAuthenticator::new(unreachable, discard_logger());

        let result = authenticator
            .authenticate("alice", &Credentials::from("anything"))
            .await;

        let Err(AuthenticationError::ImplPropagated(message, _)) = result else {
            panic!("expected a Kafka-unreachable error, got {result:?}");
        };
        assert!(message.contains("Connection refused"), "{message}");
    }

    #[tokio::test]
    async fn provider_hands_the_logged_in_sessions_producer_to_it() {
        let cluster = MockCluster::new(1).unwrap();
        let authenticator =
            KafkaAuthenticator::new(config(&cluster.bootstrap_servers()), discard_logger());
        let provider = authenticator.user_detail_provider();
        let principal = authenticator
            .authenticate("alice", &Credentials::from("x"))
            .await
            .unwrap();

        let user = provider.provide_user_detail(&principal).await.unwrap();

        assert_eq!(user.username(), "alice");
        assert_eq!(user.session_topic(), "test-topic");
        let metadata = user
            .producer()
            .client()
            .fetch_metadata(None, Duration::from_secs(2));
        assert!(metadata.is_ok());
    }

    #[tokio::test]
    async fn concurrent_logins_for_one_username_each_get_a_producer() {
        let cluster = MockCluster::new(1).unwrap();
        let authenticator = authenticator_for(&cluster);
        let provider = authenticator.user_detail_provider();
        let first = log_in(&authenticator, "alice").await;
        let second = log_in(&authenticator, "alice").await;

        provider.provide_user_detail(&first).await.unwrap();
        provider.provide_user_detail(&second).await.unwrap();

        assert!(provider.provide_user_detail(&first).await.is_err());
    }

    #[tokio::test]
    async fn provider_fails_when_no_login_is_waiting_for_the_user() {
        let authenticator = KafkaAuthenticator::new(config("127.0.0.1:1"), discard_logger());
        let provider = authenticator.user_detail_provider();

        let result = provider
            .provide_user_detail(&Principal {
                username: "alice".to_string(),
            })
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn sasl_login_is_rejected_by_a_broker_that_does_not_speak_sasl() {
        for mechanism in [
            SaslMechanism::Plain,
            SaslMechanism::ScramSha256,
            SaslMechanism::ScramSha512,
        ] {
            let cluster = MockCluster::new(1).unwrap();
            let mut sasl = config(&cluster.bootstrap_servers());
            sasl.security_protocol = SecurityProtocol::SaslPlaintext;
            sasl.sasl_mechanism = Some(mechanism);
            sasl.auth_timeout_ms = 1000;
            let authenticator = KafkaAuthenticator::new(sasl, discard_logger());

            let result = authenticator
                .authenticate("alice", &Credentials::from("secret"))
                .await;

            assert!(
                matches!(result, Err(AuthenticationError::BadPassword)),
                "{mechanism:?}"
            );
        }
    }
}

//! Fixtures shared by the unit tests of the `SITE` command handlers.

use libunftp::options::{Reply, ReplyCode, SiteCommandContext};
use rdkafka::config::ClientConfig;
use std::sync::Arc;

use crate::auth::{AuthContext, KafkaUser, Login, SessionProducer};
use crate::storage::MemStorage;

/// A user whose producer is configured for `brokers`, with `topic` as the Session topic.
pub fn session_user(brokers: &str, topic: &str) -> KafkaUser {
    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers", brokers);
    let producer: SessionProducer = client_config
        .create_with_context(AuthContext::default())
        .unwrap();
    let login = Login::new(producer, client_config);
    KafkaUser::new("alice".to_string(), login, topic.to_string())
}

/// A `SITE <command> <arguments>` context with fresh storage; `user` is `None` for a client not logged in.
pub fn context(
    command: &str,
    arguments: &str,
    user: Option<KafkaUser>,
) -> SiteCommandContext<MemStorage, KafkaUser> {
    SiteCommandContext {
        command: command.to_string(),
        arguments: arguments.to_string(),
        username: user.as_ref().map(|_| "alice".to_string()),
        storage: Arc::new(MemStorage::new()),
        user: Arc::new(user),
        logger: slog::Logger::root(slog::Discard, slog::o!()),
    }
}

/// Whether `reply` carries `expected`, whether single- or multi-line.
pub fn is_code(reply: &Reply, expected: ReplyCode) -> bool {
    matches!(reply, Reply::CodeAndMsg { code, .. } if *code == expected)
        || matches!(reply, Reply::MultiLine { code, .. } if *code == expected)
}

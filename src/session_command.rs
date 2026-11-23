use async_trait::async_trait;
use clap::Parser;
use libunftp::options::{Reply, ReplyCode, SiteCommandContext, SiteCommandHandler};

use crate::auth::KafkaUser;
use crate::storage::MemStorage;

/// A `SITE` command that acts for the user logged in to the Session.
///
/// Register it with [`LoggedIn`], which refuses clients that are not logged in
/// so that implementations never see that case.
#[async_trait]
pub trait SessionCommand: Send + Sync + std::fmt::Debug {
    /// Runs the command.
    ///
    /// `user` is the Session's logged-in user and `context` carries the
    /// command's arguments and the Session's storage.
    ///
    /// Returns the reply to send to the client.
    async fn run(
        &self,
        user: &KafkaUser,
        context: &SiteCommandContext<MemStorage, KafkaUser>,
    ) -> Reply;
}

/// Parses a `SITE` command's argument string, split as a shell would, into `T`.
///
/// Returns the parsed arguments.
///
/// # Errors
/// Returns a `501` reply when the quotes are unbalanced or clap rejects the arguments.
/// clap's message spans several lines, so it is sent as a multi-line reply to keep
/// the control connection in step.
pub fn parse_arguments<T: Parser>(raw: &str) -> Result<T, Reply> {
    let Some(tokens) = shlex::split(raw) else {
        return Err(Reply::new(
            ReplyCode::ParameterSyntaxError,
            "Unbalanced quotes in arguments",
        ));
    };
    T::try_parse_from(tokens).map_err(|e| {
        let lines: Vec<String> = e.to_string().lines().map(str::to_string).collect();
        Reply::new_multiline(ReplyCode::ParameterSyntaxError, lines)
    })
}

/// Adapts a [`SessionCommand`] into a [`SiteCommandHandler`] that refuses
/// clients that are not logged in.
#[derive(Debug)]
pub struct LoggedIn<C>(pub C);

#[async_trait]
impl<C: SessionCommand> SiteCommandHandler<MemStorage, KafkaUser> for LoggedIn<C> {
    async fn handle(&self, context: &SiteCommandContext<MemStorage, KafkaUser>) -> Reply {
        match &*context.user {
            Some(user) => self.0.run(user, context).await,
            None => Reply::new(ReplyCode::NotLoggedIn, "Not logged in"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{context, is_code, session_user};

    #[derive(Debug)]
    struct Echo;

    #[async_trait]
    impl SessionCommand for Echo {
        async fn run(
            &self,
            user: &KafkaUser,
            _context: &SiteCommandContext<MemStorage, KafkaUser>,
        ) -> Reply {
            Reply::new(ReplyCode::CommandOkay, user.username())
        }
    }

    #[tokio::test]
    async fn a_client_that_is_not_logged_in_is_refused() {
        let ctx = context("ECHO", "", None);
        let reply = LoggedIn(Echo).handle(&ctx).await;
        assert!(is_code(&reply, ReplyCode::NotLoggedIn));
    }

    #[tokio::test]
    async fn a_logged_in_client_reaches_the_command() {
        let ctx = context("ECHO", "", Some(session_user("127.0.0.1:1", "topic")));
        let reply = LoggedIn(Echo).handle(&ctx).await;
        assert!(is_code(&reply, ReplyCode::CommandOkay));
    }
}

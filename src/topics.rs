use async_trait::async_trait;
use clap::Parser;
use libunftp::options::{Reply, ReplyCode, SiteCommandContext};
use rdkafka::admin::{AdminOptions, NewTopic, TopicReplication};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::Producer;
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::task::JoinError;

use crate::auth::{KafkaUser, SessionProducer};
use crate::session_command::{parse_arguments, SessionCommand};
use crate::storage::MemStorage;

const INTERNAL_TOPIC_PREFIX: &str = "_";
const MAX_TOPIC_NAME_LENGTH: usize = 249;

/// Why the cluster's topics could not be listed.
#[derive(Debug)]
enum TopicListError {
    Kafka(KafkaError),
    TaskFailed(JoinError),
}

impl std::fmt::Display for TopicListError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TopicListError::Kafka(e) => e.fmt(f),
            TopicListError::TaskFailed(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for TopicListError {}

impl From<KafkaError> for TopicListError {
    fn from(error: KafkaError) -> Self {
        TopicListError::Kafka(error)
    }
}

impl From<JoinError> for TopicListError {
    fn from(error: JoinError) -> Self {
        TopicListError::TaskFailed(error)
    }
}

/// Lists the topics on the cluster, sorted by name, internal ones included.
///
/// Queries the cluster with the Session's own Kafka credentials, waiting at most
/// `timeout`.
///
/// # Errors
/// Returns [`TopicListError`] when the cluster cannot be queried.
async fn cluster_topics(
    producer: &SessionProducer,
    timeout: Duration,
) -> Result<Vec<String>, TopicListError> {
    let producer = producer.clone();
    let metadata =
        tokio::task::spawn_blocking(move || producer.client().fetch_metadata(None, timeout))
            .await??;

    let result = metadata
        .topics()
        .iter()
        .map(|topic| topic.name().to_string())
        .collect::<BTreeSet<String>>()
        .into_iter()
        .collect();
    Ok(result)
}

/// Whether `name` matches `pattern`, where `*` matches any run of characters,
/// including none, and `?` matches exactly one.
///
/// The whole name must match. Matching takes time proportional to the product of
/// the two lengths, however many `*` the pattern holds.
fn glob_matches(pattern: &str, name: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let name: Vec<char> = name.chars().collect();
    let mut pattern_at = 0;
    let mut name_at = 0;
    let mut last_star: Option<(usize, usize)> = None;
    while name_at < name.len() {
        match pattern.get(pattern_at) {
            Some('*') => {
                last_star = Some((pattern_at, name_at));
                pattern_at += 1;
            }
            Some(&c) if c == '?' || c == name[name_at] => {
                pattern_at += 1;
                name_at += 1;
            }
            _ => match last_star {
                Some((star_pattern_at, star_name_at)) => {
                    last_star = Some((star_pattern_at, star_name_at + 1));
                    pattern_at = star_pattern_at + 1;
                    name_at = star_name_at + 1;
                }
                None => return false,
            },
        }
    }
    pattern[pattern_at..].iter().all(|&c| c == '*')
}

/// The topics `SITE TOPICS` shows for `pattern`, in the order given.
///
/// Internal topics, whose names start with `_`, are shown only when the pattern
/// itself starts with `_`.
fn listed_topics(topics: Vec<String>, pattern: Option<&str>) -> Vec<String> {
    let shows_internal = pattern.is_some_and(|p| p.starts_with(INTERNAL_TOPIC_PREFIX));
    topics
        .into_iter()
        .filter(|name| shows_internal || !name.starts_with(INTERNAL_TOPIC_PREFIX))
        .filter(|name| pattern.is_none_or(|p| glob_matches(p, name)))
        .collect()
}

/// The lines of a `SITE TOPICS` reply: a header saying what is listed, the
/// `topics` themselves, and a footer counting them.
fn topics_listing(topics: &[String], pattern: Option<&str>) -> Vec<String> {
    let header = match pattern {
        Some(pattern) => format!("topics matching {pattern}"),
        None => "all topics".to_string(),
    };
    let noun = if topics.len() == 1 { "topic" } else { "topics" };
    let footer = format!("listed {} {noun}", with_thousands_separators(topics.len()));
    let mut result = vec![header];
    result.extend(topics.iter().cloned());
    result.push(footer);
    result
}

/// `number` written with a comma between each group of three digits: `1,234,567`.
fn with_thousands_separators(number: usize) -> String {
    let digits = number.to_string();
    let first_group = digits.len() % 3;
    let groups = digits.as_bytes()[first_group..]
        .chunks(3)
        .map(|group| std::str::from_utf8(group).unwrap());
    std::iter::once(&digits[..first_group])
        .filter(|group| !group.is_empty())
        .chain(groups)
        .collect::<Vec<_>>()
        .join(",")
}

/// Checks `name` against Kafka's rules for topic names.
///
/// # Errors
/// Returns the reason when `name` is empty, `.` or `..`, longer than 249
/// characters, or holds anything but letters, digits, `.`, `_` and `-`.
fn validate_topic_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name == "." || name == ".." {
        return Err(format!("\"{name}\" is not a valid topic name"));
    }
    if name.len() > MAX_TOPIC_NAME_LENGTH {
        return Err(format!(
            "Topic names are at most {MAX_TOPIC_NAME_LENGTH} characters"
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        return Err("Topic names hold only letters, digits, '.', '_' and '-'".to_string());
    }
    Ok(())
}

/// Arguments of `SITE TOPICS`.
#[derive(Parser, Debug)]
#[command(no_binary_name = true)]
struct TopicsArgs {
    /// Only list topics matching this glob: `*` for any run of characters, `?` for one
    pattern: Option<String>,
}

/// Lists the cluster's topics: `SITE TOPICS [<glob>]`.
#[derive(Debug)]
pub struct TopicsHandler {
    timeout: Duration,
}

impl TopicsHandler {
    /// Creates the handler; `timeout` bounds each query of the cluster.
    ///
    /// Returns the new handler.
    pub fn new(timeout: Duration) -> Self {
        TopicsHandler { timeout }
    }
}

#[async_trait]
impl SessionCommand for TopicsHandler {
    async fn run(
        &self,
        user: &KafkaUser,
        context: &SiteCommandContext<MemStorage, KafkaUser>,
    ) -> Reply {
        let args: TopicsArgs = match parse_arguments(&context.arguments) {
            Ok(args) => args,
            Err(reply) => return reply,
        };
        match cluster_topics(user.producer(), self.timeout).await {
            Ok(topics) => {
                let topics = listed_topics(topics, args.pattern.as_deref());
                Reply::new_multiline(
                    ReplyCode::FileStatus,
                    topics_listing(&topics, args.pattern.as_deref()),
                )
            }
            Err(e) => Reply::new(ReplyCode::LocalError, &format!("Cannot list topics: {e}")),
        }
    }
}

/// Arguments of `SITE TOPIC`.
#[derive(Parser, Debug)]
#[command(no_binary_name = true)]
struct TopicArgs {
    /// Create the topic, then make it the Session topic
    #[arg(short = 'c', requires = "name")]
    create: bool,

    /// The topic to make the Session topic; omit it to report the Session topic
    name: Option<String>,
}

/// Reports, sets or creates the Session topic: `SITE TOPIC [-c] [<topic>]`.
#[derive(Debug)]
pub struct TopicHandler {
    timeout: Duration,
}

impl TopicHandler {
    /// Creates the handler; `timeout` bounds each request to the cluster.
    ///
    /// Returns the new handler.
    pub fn new(timeout: Duration) -> Self {
        TopicHandler { timeout }
    }
}

#[async_trait]
impl SessionCommand for TopicHandler {
    async fn run(
        &self,
        user: &KafkaUser,
        context: &SiteCommandContext<MemStorage, KafkaUser>,
    ) -> Reply {
        let args: TopicArgs = match parse_arguments(&context.arguments) {
            Ok(args) => args,
            Err(reply) => return reply,
        };
        match (args.create, args.name) {
            (_, None) => Reply::new(
                ReplyCode::CommandOkay,
                &format!("Current topic: {}", user.session_topic()),
            ),
            (false, Some(name)) => set_topic_if_it_exists(user, &name, self.timeout).await,
            (true, Some(name)) => create_topic(user, &name, self.timeout).await,
        }
    }
}

/// Makes `topic` the Session topic if it exists on the cluster.
///
/// The cluster's topic list is checked, rather than asking for `topic`'s
/// metadata, because a metadata request can make a broker auto-create the topic.
async fn set_topic_if_it_exists(user: &KafkaUser, topic: &str, timeout: Duration) -> Reply {
    match cluster_topics(user.producer(), timeout).await {
        Ok(topics) if topics.iter().any(|known| known == topic) => {
            user.set_session_topic(topic);
            Reply::new(ReplyCode::CommandOkay, &format!("Topic set to {topic}"))
        }
        Ok(_) => Reply::new(
            ReplyCode::FileError,
            &format!("Topic \"{topic}\" does not exist"),
        ),
        Err(e) => Reply::new(ReplyCode::LocalError, &format!("Cannot check topic: {e}")),
    }
}

/// Creates `topic` on the cluster with the broker's default partitions and
/// replication, then makes it the Session topic.
///
/// The broker's own message is relayed when it refuses.
async fn create_topic(user: &KafkaUser, topic: &str, timeout: Duration) -> Reply {
    if let Err(reason) = validate_topic_name(topic) {
        return Reply::new(ReplyCode::ParameterSyntaxError, &reason);
    }
    let admin = match user.admin_client().await {
        Ok(admin) => admin,
        Err(e) => return Reply::new(ReplyCode::LocalError, &format!("Cannot create topic: {e}")),
    };
    let options = AdminOptions::new()
        .request_timeout(Some(timeout))
        .operation_timeout(Some(timeout));
    let new_topic = NewTopic::new(topic, -1, TopicReplication::Fixed(-1));
    match admin.create_topics([&new_topic], &options).await {
        Ok(results) => match results.into_iter().next() {
            Some(Ok(_)) => {
                user.set_session_topic(topic);
                Reply::new(
                    ReplyCode::CommandOkay,
                    &format!("Topic {topic} created and set"),
                )
            }
            Some(Err((_, RDKafkaErrorCode::TopicAlreadyExists))) => Reply::new(
                ReplyCode::FileError,
                &format!("Topic \"{topic}\" already exists"),
            ),
            Some(Err((_, code))) => Reply::new(
                ReplyCode::FileError,
                &format!("Cannot create topic \"{topic}\": {code}"),
            ),
            None => Reply::new(ReplyCode::LocalError, "The broker did not answer"),
        },
        Err(e) => Reply::new(ReplyCode::LocalError, &format!("Cannot create topic: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_command::LoggedIn;
    use crate::test_support::{context, is_code, session_user};
    use libunftp::options::SiteCommandHandler;
    use rdkafka::mocking::MockCluster;

    const TIMEOUT: Duration = Duration::from_secs(2);

    fn topic_context(brokers: &str, arguments: &str) -> SiteCommandContext<MemStorage, KafkaUser> {
        context(
            "TOPIC",
            arguments,
            Some(session_user(brokers, "default-topic")),
        )
    }

    fn session_topic_of(ctx: &SiteCommandContext<MemStorage, KafkaUser>) -> String {
        (*ctx.user).as_ref().unwrap().session_topic()
    }

    #[tokio::test]
    async fn topic_without_an_argument_reports_the_session_topic() {
        let ctx = topic_context("127.0.0.1:1", "");
        let reply = LoggedIn(TopicHandler::new(TIMEOUT)).handle(&ctx).await;
        assert!(is_code(&reply, ReplyCode::CommandOkay));
        assert!(
            matches!(&reply, Reply::CodeAndMsg { msg, .. } if msg.contains("default-topic")),
            "{reply:?}"
        );
    }

    #[tokio::test]
    async fn topic_rejects_more_than_one_argument() {
        let ctx = topic_context("127.0.0.1:1", "a b");
        let reply = LoggedIn(TopicHandler::new(TIMEOUT)).handle(&ctx).await;
        assert!(is_code(&reply, ReplyCode::ParameterSyntaxError));
    }

    #[tokio::test]
    async fn topic_is_refused_and_unchanged_when_it_is_not_on_the_cluster() {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("known", 1, 1).unwrap();
        let ctx = topic_context(&cluster.bootstrap_servers(), "unknown");

        let reply = LoggedIn(TopicHandler::new(TIMEOUT)).handle(&ctx).await;

        assert!(is_code(&reply, ReplyCode::FileError));
        assert_eq!(session_topic_of(&ctx), "default-topic");
    }

    #[tokio::test]
    async fn topic_is_set_when_it_is_on_the_cluster() {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("known", 1, 1).unwrap();
        let ctx = topic_context(&cluster.bootstrap_servers(), "known");

        let reply = LoggedIn(TopicHandler::new(TIMEOUT)).handle(&ctx).await;

        assert!(is_code(&reply, ReplyCode::CommandOkay));
        assert_eq!(session_topic_of(&ctx), "known");
    }

    #[tokio::test]
    async fn topics_lists_the_clusters_topics_sorted_without_internal_ones() {
        let cluster = MockCluster::new(1).unwrap();
        for name in ["beta", "alpha", "__consumer_offsets"] {
            cluster.create_topic(name, 1, 1).unwrap();
        }
        let ctx = topic_context(&cluster.bootstrap_servers(), "");

        let reply = LoggedIn(TopicsHandler::new(TIMEOUT)).handle(&ctx).await;

        let Reply::MultiLine { code, lines } = reply else {
            panic!("expected a multi-line reply, got {reply:?}");
        };
        assert_eq!(code, ReplyCode::FileStatus);
        assert_eq!(
            lines,
            vec!["all topics", "alpha", "beta", "listed 2 topics"]
        );
    }

    #[tokio::test]
    async fn topics_lists_only_the_topics_matching_the_glob() {
        let cluster = MockCluster::new(1).unwrap();
        for name in ["orders-eu", "orders-us", "payments"] {
            cluster.create_topic(name, 1, 1).unwrap();
        }
        let ctx = context(
            "TOPICS",
            "'orders-*'",
            Some(session_user(&cluster.bootstrap_servers(), "default-topic")),
        );

        let reply = LoggedIn(TopicsHandler::new(TIMEOUT)).handle(&ctx).await;

        let Reply::MultiLine { lines, .. } = reply else {
            panic!("expected a multi-line reply, got {reply:?}");
        };
        assert_eq!(
            lines,
            vec![
                "topics matching orders-*",
                "orders-eu",
                "orders-us",
                "listed 2 topics"
            ]
        );
    }

    #[tokio::test]
    async fn topics_with_nothing_to_list_is_still_a_213_reply() {
        let cluster = MockCluster::new(1).unwrap();
        let ctx = context(
            "TOPICS",
            "nothing-matches",
            Some(session_user(&cluster.bootstrap_servers(), "default-topic")),
        );

        let reply = LoggedIn(TopicsHandler::new(TIMEOUT)).handle(&ctx).await;

        let Reply::MultiLine { code, lines } = reply else {
            panic!("expected a multi-line reply, got {reply:?}");
        };
        assert_eq!(code, ReplyCode::FileStatus);
        assert_eq!(
            lines,
            vec!["topics matching nothing-matches", "listed 0 topics"]
        );
    }

    #[tokio::test]
    async fn topics_rejects_more_than_one_argument() {
        let ctx = topic_context("127.0.0.1:1", "a b");
        let reply = LoggedIn(TopicsHandler::new(TIMEOUT)).handle(&ctx).await;
        assert!(is_code(&reply, ReplyCode::ParameterSyntaxError));
    }

    #[tokio::test]
    async fn topics_fails_when_the_cluster_is_unreachable() {
        let ctx = topic_context("127.0.0.1:1", "");
        let reply = LoggedIn(TopicsHandler::new(TIMEOUT)).handle(&ctx).await;
        assert!(is_code(&reply, ReplyCode::LocalError));
    }

    #[tokio::test]
    async fn create_refuses_an_invalid_name_without_asking_the_cluster() {
        let ctx = topic_context("127.0.0.1:1", "-c bad/name");
        let reply = LoggedIn(TopicHandler::new(TIMEOUT)).handle(&ctx).await;
        assert!(is_code(&reply, ReplyCode::ParameterSyntaxError));
        assert_eq!(session_topic_of(&ctx), "default-topic");
    }

    #[tokio::test]
    async fn create_without_a_name_is_a_syntax_error() {
        let ctx = topic_context("127.0.0.1:1", "-c");
        let reply = LoggedIn(TopicHandler::new(TIMEOUT)).handle(&ctx).await;
        assert!(is_code(&reply, ReplyCode::ParameterSyntaxError));
    }

    #[tokio::test]
    async fn create_fails_when_the_cluster_is_unreachable() {
        let ctx = topic_context("127.0.0.1:1", "-c fresh-topic");
        let reply = LoggedIn(TopicHandler::new(TIMEOUT)).handle(&ctx).await;
        assert!(is_code(&reply, ReplyCode::LocalError));
        assert_eq!(session_topic_of(&ctx), "default-topic");
    }

    #[test]
    fn the_listing_counts_one_topic_in_the_singular() {
        assert_eq!(
            topics_listing(&["alpha".to_string()], None),
            vec!["all topics", "alpha", "listed 1 topic"]
        );
    }

    #[test]
    fn counts_are_written_with_thousands_separators() {
        for (number, expected) in [
            (0, "0"),
            (999, "999"),
            (1_000, "1,000"),
            (12_345, "12,345"),
            (1_234_567, "1,234,567"),
        ] {
            assert_eq!(with_thousands_separators(number), expected);
        }
    }

    fn strings(items: &[&str]) -> Vec<String> {
        items.iter().map(|item| item.to_string()).collect()
    }

    #[test]
    fn glob_star_matches_any_run_of_characters_including_none() {
        assert!(glob_matches("orders-*", "orders-"));
        assert!(glob_matches("orders-*", "orders-eu"));
        assert!(glob_matches("*-eu", "orders-eu"));
        assert!(glob_matches("o*s*u", "orders-eu"));
        assert!(glob_matches("*", ""));
    }

    #[test]
    fn glob_question_mark_matches_exactly_one_character() {
        assert!(glob_matches("orders-??", "orders-eu"));
        assert!(!glob_matches("orders-??", "orders-e"));
        assert!(!glob_matches("orders-??", "orders-eur"));
    }

    #[test]
    fn glob_must_match_the_whole_name() {
        assert!(glob_matches("orders", "orders"));
        assert!(!glob_matches("orders", "orders-eu"));
        assert!(!glob_matches("rders", "orders"));
        assert!(!glob_matches("orders-*x", "orders-eu"));
    }

    #[test]
    fn glob_with_many_stars_does_not_blow_up() {
        let name = "a".repeat(200);
        assert!(!glob_matches("*a*a*a*a*a*a*a*a*a*a*b", &name));
    }

    #[test]
    fn internal_topics_are_listed_only_when_the_glob_starts_with_an_underscore() {
        let all = strings(&["__consumer_offsets", "_schemas", "orders"]);

        assert_eq!(listed_topics(all.clone(), None), strings(&["orders"]));
        assert_eq!(listed_topics(all.clone(), Some("*")), strings(&["orders"]));
        assert_eq!(
            listed_topics(all.clone(), Some("_*")),
            strings(&["__consumer_offsets", "_schemas"])
        );
        assert_eq!(listed_topics(all, Some("_s*")), strings(&["_schemas"]));
    }

    #[test]
    fn topic_names_follow_kafkas_rules() {
        assert!(validate_topic_name("orders.eu_1-b").is_ok());
        assert!(validate_topic_name(&"a".repeat(249)).is_ok());
        assert!(validate_topic_name(&"a".repeat(250)).is_err());
        for invalid in ["", ".", "..", "has space", "slash/name", "star*", "naïve"] {
            assert!(validate_topic_name(invalid).is_err(), "{invalid:?}");
        }
    }
}

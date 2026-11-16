use serde::Deserialize;
use std::path::Path;

use crate::kafka::KafkaConfig;
use crate::server_config::ServerConfig;

/// The whole of `config.toml`: an optional `[server]` table and a required `[kafka]` table.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    pub kafka: KafkaConfig,
}

impl Config {
    /// Reads and parses the TOML file at `path`.
    ///
    /// # Errors
    /// Returns an error if the file cannot be read, is not valid TOML, or has a missing,
    /// unknown or invalid setting.
    pub fn from_path(path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self::parse(&std::fs::read_to_string(path)?)?)
    }

    /// Parses the text of a configuration file.
    ///
    /// # Errors
    /// Returns an error if the text is not valid TOML, or has a missing, unknown or invalid setting.
    pub fn parse(text: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_kafka_table_from_a_path() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(
            &mut file,
            b"[kafka]\nbrokers = \"my-broker:9092\"\ndefault_topic = \"custom-topic\"\n",
        )
        .unwrap();

        let config = Config::from_path(file.path()).unwrap();

        assert_eq!(config.kafka.brokers, "my-broker:9092");
        assert_eq!(config.kafka.default_topic, "custom-topic");
    }

    #[test]
    fn fails_when_the_path_does_not_exist() {
        assert!(Config::from_path(Path::new("/nonexistent/path/config.toml")).is_err());
    }

    #[test]
    fn rejects_an_unknown_table() {
        let err = Config::parse(
            "[sever]\nport = 2200\n[kafka]\nbrokers = \"b:9092\"\ndefault_topic = \"t\"\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("sever"), "{err}");
    }

    #[test]
    fn rejects_an_unknown_server_key() {
        let err = Config::parse(
            "[server]\nlog_levl = \"debug\"\n[kafka]\nbrokers = \"b:9092\"\ndefault_topic = \"t\"\n",
        )
        .unwrap_err();
        assert!(err.to_string().contains("log_levl"), "{err}");
    }
}

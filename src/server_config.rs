use serde::Deserialize;

const DEFAULT_PORT: u16 = 2121;

/// Server settings, loaded from the optional `[server]` table in `config.toml`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    /// The TCP port the FTP server listens on; 2121 when left out.
    pub port: u16,
    /// The least severe messages to log; `info` when left out.
    pub log_level: LogLevel,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig { port: DEFAULT_PORT, log_level: LogLevel::default() }
    }
}

/// How verbose the server's log is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    #[default]
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub fn as_slog(self) -> slog::Level {
        match self {
            LogLevel::Trace => slog::Level::Trace,
            LogLevel::Debug => slog::Level::Debug,
            LogLevel::Info => slog::Level::Info,
            LogLevel::Warn => slog::Level::Warning,
            LogLevel::Error => slog::Level::Error,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    const KAFKA_TABLE: &str = "[kafka]\nbrokers = \"b:9092\"\ndefault_topic = \"t\"\n";

    fn parse(server_table: &str) -> Result<ServerConfig, toml::de::Error> {
        Config::parse(&format!("{server_table}\n{KAFKA_TABLE}")).map(|config| config.server)
    }

    #[test]
    fn server_table_is_optional_and_log_level_defaults_to_info() {
        let config = parse("").unwrap();
        assert_eq!(config.log_level, LogLevel::Info);
    }

    #[test]
    fn reads_every_log_level() {
        for (name, expected) in [
            ("trace", LogLevel::Trace),
            ("debug", LogLevel::Debug),
            ("info", LogLevel::Info),
            ("warn", LogLevel::Warn),
            ("error", LogLevel::Error),
        ] {
            let config = parse(&format!("[server]\nlog_level = \"{name}\"\n")).unwrap();
            assert_eq!(config.log_level, expected, "{name}");
        }
    }

    #[test]
    fn rejects_an_unknown_log_level() {
        let err = parse("[server]\nlog_level = \"loud\"\n").unwrap_err();
        assert!(err.to_string().contains("log_level") || err.to_string().contains("loud"), "{err}");
    }

    #[test]
    fn port_defaults_to_2121() {
        assert_eq!(parse("").unwrap().port, DEFAULT_PORT);
    }

    #[test]
    fn reads_the_port() {
        assert_eq!(parse("[server]\nport = 2200\n").unwrap().port, 2200);
    }

    #[test]
    fn rejects_an_invalid_port() {
        for value in ["70000", "-1", "\"ftp\""] {
            let err = parse(&format!("[server]\nport = {value}\n")).unwrap_err();
            assert!(err.to_string().contains("port") || err.to_string().contains(value), "{value}: {err}");
        }
    }

    #[test]
    fn maps_to_slog_levels() {
        assert_eq!(LogLevel::Warn.as_slog(), slog::Level::Warning);
        assert_eq!(LogLevel::Trace.as_slog(), slog::Level::Trace);
    }
}

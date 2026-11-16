use serde::Deserialize;

/// Server settings, loaded from the optional `[server]` table in `config.toml`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// The least severe messages to log; `info` when left out.
    pub log_level: LogLevel,
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

#[derive(Debug, Deserialize)]
struct ConfigFile {
    #[serde(default)]
    server: ServerConfig,
}

impl ServerConfig {
    /// Reads the `[server]` table from the TOML file at `path`.
    ///
    /// # Errors
    /// Returns an error if the file cannot be read or is not valid TOML, or the table has an unknown value.
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self::parse(&std::fs::read_to_string(path)?)?)
    }

    fn parse(text: &str) -> Result<Self, toml::de::Error> {
        Ok(toml::from_str::<ConfigFile>(text)?.server)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_table_is_optional_and_log_level_defaults_to_info() {
        let config = ServerConfig::parse("[kafka]\nbrokers = \"b:9092\"\ndefault_topic = \"t\"\n").unwrap();
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
            let config = ServerConfig::parse(&format!("[server]\nlog_level = \"{name}\"\n")).unwrap();
            assert_eq!(config.log_level, expected, "{name}");
        }
    }

    #[test]
    fn rejects_an_unknown_log_level() {
        let err = ServerConfig::parse("[server]\nlog_level = \"loud\"\n").unwrap_err();
        assert!(err.to_string().contains("log_level") || err.to_string().contains("loud"), "{err}");
    }

    #[test]
    fn maps_to_slog_levels() {
        assert_eq!(LogLevel::Warn.as_slog(), slog::Level::Warning);
        assert_eq!(LogLevel::Trace.as_slog(), slog::Level::Trace);
    }
}

//! Shared black-box test harness: spawns the real `ftp-server` binary in a
//! temp directory (so it picks up a throwaway `config.toml`) and drives it
//! as a plain FTP client would, over real TCP.
//!
//! Not every item here is used by every test binary that includes this
//! module, since each one only exercises the parts it needs.
#![allow(dead_code)]

pub mod sasl_kafka;

use rdkafka::mocking::MockCluster;
use rdkafka::producer::DefaultProducerContext;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

pub const USERNAME: &str = "alice";
pub const PASSWORD: &str = "password123";

/// Lets only one test run a server at a time.
///
/// On macOS, a socket the test process opens while another test spawns a
/// server can leak into that server before it is marked close-on-exec. A
/// leaked listener from `free_port` then accepts this test's connections and
/// resets them when the other server exits.
static ONE_SERVER_AT_A_TIME: Mutex<()> = Mutex::new(());

pub struct TestServer {
    _kafka: Option<MockCluster<'static, DefaultProducerContext>>,
    child: Child,
    port: u16,
    _dir: tempfile::TempDir,
    _turn: MutexGuard<'static, ()>,
}

impl TestServer {
    /// Spawns the server on a free port, backed by its own in-process mock Kafka
    /// cluster and the topic `rust-topic`.
    pub fn start() -> Self {
        let cluster = MockCluster::new(1).expect("failed to start the mock Kafka cluster");
        let mut server = Self::start_with_config(&format!(
            r#"
                [kafka]
                brokers = "{}"
                default_topic = "rust-topic"
            "#,
            cluster.bootstrap_servers()
        ));
        server._kafka = Some(cluster);
        server
    }

    pub fn start_with_config(config_toml: &str) -> Self {
        let turn = ONE_SERVER_AT_A_TIME
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let port = free_port();
        std::fs::write(dir.path().join("config.toml"), with_port(config_toml, port)).unwrap();

        let child = Command::new(env!("CARGO_BIN_EXE_ftp-server"))
            .current_dir(dir.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn ftp-server binary");

        let server = TestServer { _kafka: None, child, port, _dir: dir, _turn: turn };
        server.wait_until_accepting_connections();
        server
    }

    /// Spawns a second server in the same directory, reusing the same port -
    /// used to exercise the "address already in use" startup-failure path.
    pub fn start_conflicting(existing: &TestServer) -> Child {
        Command::new(env!("CARGO_BIN_EXE_ftp-server"))
            .current_dir(existing._dir.path())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn second ftp-server binary")
    }

    fn wait_until_accepting_connections(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                return;
            }
            if Instant::now() > deadline {
                panic!("ftp-server did not start listening on port {} in time", self.port);
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn connect(&self) -> FtpClient {
        FtpClient::connect(self.port)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Runs the server with `config_toml` and returns its output once it exits.
///
/// For testing startup failures: fails the test if the server is still
/// running after five seconds.
pub fn run_until_exit(config_toml: &str) -> std::process::Output {
    let _turn = ONE_SERVER_AT_A_TIME
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("config.toml"), with_port(config_toml, free_port())).unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_ftp-server"))
        .current_dir(dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn ftp-server binary");

    let deadline = Instant::now() + Duration::from_secs(5);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("ftp-server was expected to exit, but is still running");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    child.wait_with_output().unwrap()
}

/// Starts the server and returns everything it logged up to and including its
/// "listening" line, then stops it.
///
/// Startup messages are logged before that line, so reading up to it sees all
/// of them without sleeping. Fails the test if it never appears within ten seconds.
pub fn startup_log(config_toml: &str) -> String {
    log_until(config_toml, |_port| {}, "listening on")
}

/// Starts the server, waits until it is listening, runs `trigger` with its port,
/// and returns everything it logged up to the first line containing `until`.
///
/// Fails the test if no such line appears within ten seconds of starting.
pub fn log_until(config_toml: &str, trigger: impl FnOnce(u16), until: &str) -> String {
    let _turn = ONE_SERVER_AT_A_TIME
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    std::fs::write(dir.path().join("config.toml"), with_port(config_toml, port)).unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_ftp-server"))
        .current_dir(dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn ftp-server binary");

    let stderr = child.stderr.take().unwrap();
    let (lines, received) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            if lines.send(line).is_err() {
                break;
            }
        }
    });

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut log = String::new();
    let mut trigger = Some(trigger);
    let mut found = false;
    while !found {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else { break };
        let Ok(line) = received.recv_timeout(remaining) else { break };
        log.push_str(&line);
        log.push('\n');
        if line.contains("listening on") {
            if let Some(trigger) = trigger.take() {
                wait_until_accepting_connections(port);
                trigger(port);
            }
        }
        found = line.contains(until);
    }
    let _ = child.kill();
    let _ = child.wait();

    assert!(found, "ftp-server never logged {until:?}; it logged: {log}");
    log
}

/// The server logs that it is listening just before it binds, so a client that
/// connects on seeing the log line can be refused.
fn wait_until_accepting_connections(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "ftp-server did not start accepting connections on port {port}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Returns `config_toml` with `[server] port` set, keeping any other `[server]` settings.
///
/// Text that is not valid TOML is returned unchanged, so the server can report the error.
fn with_port(config_toml: &str, port: u16) -> String {
    let Ok(mut config) = config_toml.parse::<toml::Table>() else {
        return config_toml.to_string();
    };
    let server = config
        .entry("server")
        .or_insert_with(|| toml::Table::new().into())
        .as_table_mut()
        .expect("[server] is a table");
    server.insert("port".to_string(), i64::from(port).into());
    config.to_string()
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// A minimal, blocking FTP client, just enough to drive the commands this
/// server implements.
pub struct FtpClient {
    reader: BufReader<TcpStream>,
}

impl FtpClient {
    pub fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        let mut client = FtpClient { reader: BufReader::new(stream) };
        let greeting = client.read_response();
        assert!(greeting.starts_with("220"), "unexpected greeting: {greeting}");
        client
    }

    pub fn login(&mut self, username: &str, password: &str) -> String {
        self.command(&format!("USER {username}"));
        self.command(&format!("PASS {password}"))
    }

    /// Sends one command line and returns the (possibly multi-line) response.
    pub fn command(&mut self, line: &str) -> String {
        let stream = self.reader.get_mut();
        write!(stream, "{line}\r\n").unwrap();
        self.read_response()
    }

    fn read_response(&mut self) -> String {
        let mut first_line = String::new();
        self.reader.read_line(&mut first_line).unwrap();

        // "123-" (as opposed to "123 ") starts a multi-line reply that ends
        // with a line starting "123 ".
        if first_line.len() > 3 && first_line.as_bytes()[3] == b'-' {
            let code = &first_line[0..3];
            let terminator = format!("{code} ");
            let mut full = first_line.clone();
            loop {
                let mut line = String::new();
                self.reader.read_line(&mut line).unwrap();
                let done = line.starts_with(&terminator);
                full.push_str(&line);
                if done {
                    break;
                }
            }
            full
        } else {
            first_line
        }
    }

    /// Enters passive mode and returns a connected data-channel stream.
    pub fn passive_data_channel(&mut self) -> TcpStream {
        let response = self.command("PASV");
        assert!(response.starts_with("227"), "PASV failed: {response}");
        let (host, port) = parse_pasv(&response);
        TcpStream::connect((host, port)).unwrap()
    }

    pub fn store(&mut self, remote_name: &str, content: &[u8]) -> String {
        let mut data = self.passive_data_channel();
        let stream = self.reader.get_mut();
        write!(stream, "STOR {remote_name}\r\n").unwrap();
        let opening = self.read_response();
        assert!(opening.starts_with("150"), "STOR did not open a transfer: {opening}");

        data.write_all(content).unwrap();
        data.shutdown(std::net::Shutdown::Write).unwrap();
        drop(data);

        self.read_response()
    }

    pub fn retrieve(&mut self, remote_name: &str) -> (String, Vec<u8>) {
        let mut data = self.passive_data_channel();
        let stream = self.reader.get_mut();
        write!(stream, "RETR {remote_name}\r\n").unwrap();
        let opening = self.read_response();

        let mut content = Vec::new();
        if opening.starts_with("150") {
            data.read_to_end(&mut content).unwrap();
        }
        drop(data);

        (self.read_response(), content)
    }
}

/// Parses a PASV reply like "227 Entering Passive Mode (127,0,0,1,195,80)."
fn parse_pasv(response: &str) -> (String, u16) {
    let start = response.find('(').expect("no ( in PASV response");
    let end = response.find(')').expect("no ) in PASV response");
    let parts: Vec<u16> = response[start + 1..end]
        .split(',')
        .map(|p| p.parse().unwrap())
        .collect();
    let host = format!("{}.{}.{}.{}", parts[0], parts[1], parts[2], parts[3]);
    let port = parts[4] * 256 + parts[5];
    (host, port)
}

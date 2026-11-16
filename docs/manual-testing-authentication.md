# Manually testing authentication

ftp-kafka has four authentication configurations. Each section below shows how to configure ftp-kafka,
how to configure a matching [Redpanda](https://docs.redpanda.com/) broker, and how to check that it works.

| # | `security_protocol` | `sasl_mechanism` | Who gets in |
|---|---|---|---|
| 1 | `PLAINTEXT` | (none) | Anyone, if the broker is reachable |
| 2 | `SASL_PLAINTEXT` | `PLAIN` | Users known to the broker |
| 3 | `SASL_PLAINTEXT` | `SCRAM-SHA-256` | Users known to the broker |
| 4 | `SASL_PLAINTEXT` | `SCRAM-SHA-512` | Users known to the broker |

## Before you start

Start each section from a fresh broker and a fresh `config.toml`:

```sh
docker rm -f redpanda 2>/dev/null
```

Redpanda's Kafka API is published on `localhost:19092`. Run the server with `cargo run`, which listens on
port 2121, and test with `lftp`. Create a file to upload:

```sh
echo "hello kafka" > hello.txt
```

Sections 2 to 4 share a pair of shell helpers. `rpk` runs inside the container, and `admin_rpk` adds the
superuser's credentials:

```sh
rpk() { docker exec redpanda rpk "$@"; }
admin_rpk() { rpk "$@" -X brokers=localhost:19092 -X user=admin -X pass=adminpw -X sasl.mechanism=SCRAM-SHA-256; }
```

The commands come from Redpanda's documentation, so check them against the Redpanda version you run.

---

## 1. `PLAINTEXT`: no authentication

### ftp-kafka

```toml
[kafka]
brokers = "localhost:19092"
default_topic = "rust-topic"
security_protocol = "PLAINTEXT"
```

The server logs a warning at startup: `FTP logins are not authenticated`.

### Redpanda

No authentication is Redpanda's default:

```sh
docker run -d --name redpanda -p 19092:19092 -p 19644:9644 \
  redpandadata/redpanda:latest redpanda start \
  --mode dev-container --smp 1 \
  --kafka-addr external://0.0.0.0:19092 \
  --advertise-kafka-addr external://localhost:19092

docker exec redpanda rpk topic create rust-topic -X brokers=localhost:19092
```

### Test

```sh
lftp -u anyone,anything -e 'put hello.txt; quote SITE SEND hello.txt; bye' ftp://127.0.0.1:2121
```

- Login succeeds with any username and password, and `SITE SEND` returns `250` or `200`.
- The message arrived: `docker exec redpanda rpk topic consume rust-topic -n 1 -X brokers=localhost:19092`
- Stop Redpanda and try again: the login fails and the server logs `Kafka is unreachable: …`.

---

## 2. `SASL_PLAINTEXT` with `PLAIN`

### ftp-kafka

```toml
[kafka]
brokers = "localhost:19092"
default_topic = "rust-topic"
security_protocol = "SASL_PLAINTEXT"
sasl_mechanism = "PLAIN"
```

### Redpanda

Start the broker as in section 1, then make a superuser, enable SASL and allow `PLAIN`. `PLAIN` checks
the same accounts as SCRAM, so create the user with a SCRAM mechanism:

```sh
rpk cluster config set superusers '["admin"]'
rpk security user create admin -p adminpw --mechanism SCRAM-SHA-256
rpk cluster config set sasl_mechanisms '["SCRAM","PLAIN"]'
rpk cluster config set enable_sasl true

admin_rpk topic create rust-topic
rpk security user create alice -p alicepw --mechanism SCRAM-SHA-256
admin_rpk security acl create --allow-principal User:alice --operation write,describe --topic rust-topic
```

### Test

```sh
lftp -u alice,alicepw -e 'put hello.txt; quote SITE SEND hello.txt; bye' ftp://127.0.0.1:2121
lftp -u alice,wrong   -e 'ls; bye' ftp://127.0.0.1:2121
lftp -u mallory,alicepw -e 'ls; bye' ftp://127.0.0.1:2121
```

- First command: login succeeds and the message is published.
- Second and third: `530` login failure, returned within about a second.
- Check the message: `admin_rpk topic consume rust-topic -n 1`

---

## 3. `SASL_PLAINTEXT` with `SCRAM-SHA-256`

### ftp-kafka

```toml
[kafka]
brokers = "localhost:19092"
default_topic = "rust-topic"
security_protocol = "SASL_PLAINTEXT"
sasl_mechanism = "SCRAM-SHA-256"
```

### Redpanda

SCRAM is enabled by default, so `sasl_mechanisms` needs no change:

```sh
rpk cluster config set superusers '["admin"]'
rpk security user create admin -p adminpw --mechanism SCRAM-SHA-256
rpk cluster config set enable_sasl true

admin_rpk topic create rust-topic
rpk security user create alice -p alicepw --mechanism SCRAM-SHA-256
admin_rpk security acl create --allow-principal User:alice --operation write,describe --topic rust-topic
```

### Test

Run the same three `lftp` commands as in section 2 and expect the same results.

---

## 4. `SASL_PLAINTEXT` with `SCRAM-SHA-512`

### ftp-kafka

```toml
[kafka]
brokers = "localhost:19092"
default_topic = "rust-topic"
security_protocol = "SASL_PLAINTEXT"
sasl_mechanism = "SCRAM-SHA-512"
```

### Redpanda

Remove any existing broker, then start a fresh one as in section 1. Redpanda keeps its users until the
container is removed, so reusing a broker from an earlier section fails with `User already exists`.

```sh
docker rm -f redpanda
```

Then define the helpers from "Before you start" and run the following. The `admin` superuser stays on
`SCRAM-SHA-256`, because `admin_rpk` authenticates with it. Only `alice` uses `SCRAM-SHA-512`:

```sh
rpk cluster config set superusers '["admin"]'
rpk security user create admin -p adminpw --mechanism SCRAM-SHA-256
rpk cluster config set enable_sasl true

admin_rpk topic create rust-topic
rpk security user create alice -p alicepw --mechanism SCRAM-SHA-512
admin_rpk security acl create --allow-principal User:alice --operation write,describe --topic rust-topic
```

### Test

Run the same three `lftp` commands as in section 2.

#### Extra check: a user on the other mechanism

Redpanda stores each SCRAM user under one mechanism. A user created with `SCRAM-SHA-256` has no
`SCRAM-SHA-512` credential, so it must be refused while ftp-kafka asks for `SCRAM-SHA-512`.

1. Create `bob` with `SCRAM-SHA-256` only, and give them the same ACL as `alice`:

   ```sh
   rpk security user create bob -p bobpw --mechanism SCRAM-SHA-256
   admin_rpk security acl create --allow-principal User:bob --operation write,describe --topic rust-topic
   ```

2. With ftp-kafka still set to `SCRAM-SHA-512`, try to log in as `bob`:

   ```sh
   lftp -u bob,bobpw -e 'ls; bye' ftp://127.0.0.1:2121
   ```

   Expect `530` within about a second, even though the password is right. The server log reports a
   rejected login, not `Kafka is unreachable`.

3. Confirm the refusal is down to the mechanism. Change `sasl_mechanism` to `"SCRAM-SHA-256"`,
   restart `cargo run`, and repeat the `bob` login. It now succeeds. `alice` fails under `SCRAM-SHA-256`
   for the same reason.

4. Set `sasl_mechanism` back to `"SCRAM-SHA-512"` before moving on.

---

## What a failure looks like

| Situation | FTP reply | Server log |
|---|---|---|
| Wrong password or unknown user | `530` | Login rejected; returns quickly |
| Broker down or unreachable | `530` | `Kafka is unreachable: …` after `auth_timeout_ms` |
| Mechanism not enabled on the broker | `530` | `Kafka is unreachable: …` with librdkafka's reason |
| Logged in but no ACL for the topic | Login succeeds | `SITE SEND` fails |

Set `auth_timeout_ms` low (for example `1500`) when testing the unreachable case, or each attempt waits the
full default of 5 seconds.

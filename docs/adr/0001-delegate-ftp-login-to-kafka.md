# FTP login is delegated to Kafka; no local user store

The bridge keeps no user accounts. At login it connects to Kafka with the client's username and password, and the login succeeds only if Kafka accepts them; on a cluster without authentication, any login succeeds while the broker is reachable. This replaces the `[[users]]` table, which was a second credential system that bought little for a thin FTP-to-Kafka bridge, and it gives the Kafka connection real authentication for the first time.

## Considered Options

- **Keep the local user store and connect to Kafka with one shared service credential.** Rejected: two credential systems to provision and rotate, and Kafka could not tell users apart.

## Consequences

- Every login attempt, valid or not, costs a round trip to the Kafka broker. Rate limiting is not addressed and may be needed if the FTP port is exposed to untrusted networks.
- libunftp's `Principal` carries only a username and the `Authenticator` is shared across sessions, so the authenticated Kafka producer cannot travel with the login directly. It is handed from `authenticate()` to the user-detail provider through `Handoff` (`src/handoff.rs`), a map of username to queued logins. This is safe because identity is fully determined by username: any validated producer for "alice" is interchangeable with another.
- Only `PLAIN` and `SCRAM-SHA-256/512` are supported, over `PLAINTEXT` or `SASL_PLAINTEXT`; Kafka TLS is deferred. Whatever the broker allows is allowed here, so `PLAIN` stays, though `SCRAM` is preferable because it never sends the password to Kafka.
- SCRAM needs librdkafka built with OpenSSL (rdkafka's `ssl` feature), so building the project now needs OpenSSL development files.
- GSSAPI and OAUTHBEARER are out of scope: they need an external identity system, not just a forwarded password.

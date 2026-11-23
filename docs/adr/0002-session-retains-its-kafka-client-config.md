# A Session keeps its Kafka client config, password included, in memory

`SITE TOPIC -c` creates topics through Kafka's admin API. rdkafka can build an `AdminClient` only from a `ClientConfig`, which carries the SASL password, and the producer a login establishes cannot be reused for admin calls. ADR 0001 used the password only at login. A Session now keeps the `ClientConfig` its login was made from, on `KafkaUser`, for as long as the Session lasts, and builds the admin client from it on the first `-c`.

## Considered Options

- **Build the admin client at login, beside the producer.** Rejected: every login opens a second Kafka connection, though few Sessions create topics, and a login that is never claimed (#29) would leak two connections.
- **Build a short-lived admin client per `-c`.** Rejected: it needs the same retained password and pays a connection setup on every use.

## Consequences

- The password stays in the server's memory until the Session ends, where before it was dropped once the login was accepted. Under `PLAINTEXT` there is no password to keep.
- `KafkaUser`'s `Debug` and `Display` output show only the username. They must never print the client config.
- Kafka's authorisation still decides what a Session may do: a topic is created with the Session's own credentials, so a refusal comes back as a `550` carrying the broker's message.
- Anything else that later needs a Kafka client of its own can build it from the same config.

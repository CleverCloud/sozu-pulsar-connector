# Pulsar Connector

This little library provides a pulsar client that listen on a topic where
Sōzu commands will be written, and writes these commands on the UNIX socket
of the main process, for execution.

Example use:

```
cargo run -- \
      --pulsar-url  pulsar+ssl://c2-pulsar-clevercloud-customers.services.clever-cloud.com:2002 \
      --topic tenant/namespace/sozu-messages \
      --token <TOKEN>
      --config ../config.toml
```

More easily, copy the example env file into `.env`,

```
mv .env.example .env
```

Tune the values, source the file, launch the connector by doing:

```
source .env
cargo run -- --pulsar-url $PULSAR_URL --token $PULSAR_TOKEN --topic $PULSAR_TOPIC --config <PATH_TO_CONFIG>
```

## Test

1. Run Sōzu on your machine
2. source the env
3. run the pulsar-connector as explained above
4. run `cargo run example send_request`


# Pulsar Connector

This little library provides a pulsar connector for [Sōzu](https://github.com/sozu-proxy/sozu).

It contains a pulsar client that listens on a topic, to consume Sōzu 
[Request](https://docs.rs/sozu-command-lib/0.14.3/sozu_command_lib/proto/command/struct.Request.html)s and write them on the UNIX socket
of the Sōzu main process, for execution.

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
cp .env.example .env
```

Tune the values, source the file, launch the connector by doing:

```
source .env
cargo run -- --pulsar-url $PULSAR_URL --token $PULSAR_TOKEN --topic $PULSAR_TOPIC --config $SOZU_CONFIG
```

## Test

The `send_request` example produces a `Status` request on the same topic that is consumed by
the pulsar connector.

In three separate terminals:

1. run Sōzu
2. run the pulsar-connector as explained above
3. do `cargo run example send_request`

If all goes well, this happens:

1. the `send_request` example writes the request message on the topic
2. the pulsar connector consumes the request message on the topic
3. the pulsar connector writes the request to Sōzu via the unix socket
4. Sōzu executes the request


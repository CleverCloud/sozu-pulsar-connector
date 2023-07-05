# Pulsar Connector

This little library provides a pulsar connector for [Sōzu](https://github.com/sozu-proxy/sozu).
It forwards requests, from a pulsar topic, to Sōzu.

It contains a pulsar client that listens on a given topic, to consume Sōzu 
[Request](https://docs.rs/sozu-command-lib/0.14.3/sozu_command_lib/proto/command/struct.Request.html)s and write them on the UNIX socket
of the Sōzu main process, for execution.

## How to run

Copy the `example.config.toml` to be a `config.toml`:

```
cp example.config.toml config.toml
```


Tune the values to match the credentials of your pulsar broker, and to link to your Sōzu instance, and do:

```
cargo run -- --config ../config.toml
```

## Test

The `send_request` example produces a `Status` request on the same topic that is consumed by
the pulsar connector.

In three separate terminals:

1. run Sōzu
2. run the pulsar-connector as explained above
3. do `cargo run --example send_request -- --config config.toml`

If all goes well, this happens:

1. the `send_request` example writes the request message on the pulsar topic
2. the pulsar connector consumes the request message on the topic
3. the pulsar connector writes the request to Sōzu via the unix socket
4. Sōzu executes the request


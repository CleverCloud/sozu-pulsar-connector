# Pulsar Connector

This little library provides a connector between
[Pulsar](https://pulsar.apache.org/), a messaging platform,
and [Sōzu](https://github.com/sozu-proxy/sozu), a reverse proxy.

It subscribes to a pulsar topic, and consumes Sōzu
[Request](https://docs.rs/sozu-command-lib/0.14.3/sozu_command_lib/proto/command/struct.Request.html)s,
batches them in a file and send them to the Sōzu main process, for execution.

### Batching requests

In order to prevent clogging of the Sōzu channel,
the pulsar connector stores incoming requests in a temporary file, and sends a
[`LoadState`](https://docs.rs/sozu-command-lib/latest/sozu_command_lib/proto/command/request/enum.RequestType.html#variant.LoadState)
request to Sōzu with the path of this file.

They are several configurable caps that trigger the sending of a batch:

- a maximum number of requests per file
- a maximum file size (should be under Sōzu's buffer size)
- an maximum time to wait for new incoming requests before sending a batch

## Configure

Copy the `example.config.toml` to be a `config.toml`:

```
cp example.config.toml config.toml
```

Tune the values to match the credentials of your pulsar broker, and to link to your Sōzu instance.

The `check_request_redundancy` enables a feature of the Sōzu pulsar connector that is usefull when
receiving a lot of redundant requests. This feature recreates a
[Sōzu state](https://docs.rs/sozu-command-lib/latest/sozu_command_lib/state/struct.ConfigState.html)
by feeding it with the requests to forward. Thus, the pulsar connector keeps track of the proxy's state,
and can check wether incoming requests are redundant with it.

```toml
check_request_redundancy = true
```

## How to run

```
cargo run -- --config ../config.toml
```

## Test

The `send_requests` example produces regularly a group of cluster-creating requests on the same topic that is consumed by
the pulsar connector.

In three separate terminals:

1. run Sōzu
2. run the pulsar-connector as explained above
3. do `cargo run --example send_requests -- --config config.toml`

If all goes well, this happens:

1. the `send_requests` example writes the request messages on the pulsar topic
2. the pulsar connector consumes the request messages on the topic
3. the pulsar connector writes the requests to Sōzu via the unix socket
4. Sōzu executes the request

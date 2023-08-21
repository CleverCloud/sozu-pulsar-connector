# Sōzu Pulsar Connector

This little library provides a connector between
[Pulsar](https://pulsar.apache.org/), a messaging platform,
and [Sōzu](https://github.com/sozu-proxy/sozu), a reverse proxy.

It subscribes to a pulsar topic, consumes Sōzu
[Request](https://docs.rs/sozu-command-lib/0.14.3/sozu_command_lib/proto/command/struct.Request.html)s,
batches them in a file, and send them to the Sōzu main process for execution.

### Batching requests

In order to prevent clogging of the Sōzu channel,
the pulsar connector stores incoming requests in a temporary file, and sends a
[`LoadState`](https://docs.rs/sozu-command-lib/latest/sozu_command_lib/proto/command/request/enum.RequestType.html#variant.LoadState)
request to Sōzu with the path of this file.

They are several configurable caps that trigger the sending of a batch:

- a maximum number of requests per file
- a maximum file size (should be under Sōzu's buffer size)
- an maximum time to wait for new incoming requests before sending a batch

### Prometheus metrics

Accessible on the `/metrics` path, they offer insights on the requests transmitted to Sōzu,
for instance:

```text
# HELP proxy_manager_request_emitted Number of request emitted by the manager daemon
# TYPE proxy_manager_request_emitted counter
proxy_manager_request_emitted{kind="AddBackend"} 1340
proxy_manager_request_emitted{kind="AddCluster"} 670
proxy_manager_request_emitted{kind="AddHttpFrontend"} 670
proxy_manager_request_emitted{kind="AddHttpListener"} 1
proxy_manager_request_emitted{kind="LoadState"} 5
proxy_manager_request_emitted{kind="Status"} 14
```

Be sure to set the value of `metrics_address` in `config.toml`.

## Configure

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

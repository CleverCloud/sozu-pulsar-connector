use anyhow::Context;
use clap::Parser;
use pulsar::{Authentication, Pulsar, TokioExecutor};
use rand::{distributions::Alphanumeric, Rng};
use sozu_command_lib::{
    config::ListenerBuilder,
    proto::command::{request::RequestType, AddBackend, Cluster, Request, RequestHttpFrontend},
};
use tracing::{debug, info};

use sozu_pulsar_connector::{cfg::Configuration, cli::Args, message::RequestMessage};

struct RequestSender {
    config: Configuration,
    pulsar_client: Pulsar<pulsar::TokioExecutor>,
}

impl RequestSender {
    async fn new() -> anyhow::Result<Self> {
        let args = Args::parse();

        let config = Configuration::try_from(args.config)?;

        debug!(
            "The configuration, made from the environment: {:#?}",
            config
        );

        let auth = Authentication {
            name: String::from("token"),
            data: config.pulsar.token.clone().into_bytes(),
        };

        let pulsar_builder = Pulsar::builder(&config.pulsar.url, TokioExecutor).with_auth(auth);

        let pulsar_client: Pulsar<pulsar::TokioExecutor> = pulsar_builder
            .build()
            .await
            .with_context(|| "Can not create pulsar client")?;

        Ok(Self {
            config,
            pulsar_client,
        })
    }

    async fn send_requests(&self, requests: Vec<Request>) -> anyhow::Result<()> {
        info!(
            "Trying to send this group of requests: {:?} on topic {}",
            requests, self.config.pulsar.topic
        );
        let mut producer = self
            .pulsar_client
            .producer()
            .with_topic(self.config.pulsar.topic.clone())
            .with_name("request-sender")
            .build()
            .await
            .with_context(|| {
                format!(
                    "Could not create a pulsar producer on topic {}",
                    self.config.pulsar.topic
                )
            })?;
        info!("Created a producer");

        for request in requests {
            info!("Sending request {:?}", request);
            producer
                .send(RequestMessage(request))
                .await
                .with_context(|| "Could not send request message")?
                .await
                .with_context(|| "Did not receive a receipt from the pulsar server")?;
        }

        producer
            .close()
            .await
            .with_context(|| "Could not close producer used to respond")?;

        info!("The requests message was successfully sent");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let request_sender = RequestSender::new()
        .await
        .with_context(|| "Could not create request sender")?;

    for _ in 0..100 {
        let requests = generate_requests_for_a_random_cluster()?;
        info!("Creating request {:?}", requests);

        request_sender.send_requests(requests).await?;

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    Ok(())
}

fn generate_requests_for_a_random_cluster() -> anyhow::Result<Vec<Request>> {
    let mut requests: Vec<Request> = Vec::new();

    let random_cluster_id = random_id_of_7_chars();
    let random_hostname = format!("{}.com", random_cluster_id);
    let address = "127.0.0.1:8080".to_string();

    let add_cluster = RequestType::AddCluster(Cluster {
        cluster_id: random_cluster_id.clone(),
        ..Default::default()
    })
    .into();
    requests.push(add_cluster);

    let add_listener =
        RequestType::AddHttpListener(ListenerBuilder::new_http(&address).to_http(None)?).into();
    requests.push(add_listener);

    let add_frontend = RequestType::AddHttpFrontend(RequestHttpFrontend {
        cluster_id: Some(random_cluster_id.clone()),
        address,
        hostname: random_hostname,
        ..Default::default()
    })
    .into();
    requests.push(add_frontend);

    let add_backend_1 = RequestType::AddBackend(AddBackend {
        cluster_id: random_cluster_id.clone(),
        backend_id: format!("{}_backend_1", random_cluster_id),
        address: random_socket_address(),
        ..Default::default()
    })
    .into();
    requests.push(add_backend_1);

    let add_backend_2 = RequestType::AddBackend(AddBackend {
        cluster_id: random_cluster_id.clone(),
        backend_id: format!("{}_backend_2", random_cluster_id),
        address: random_socket_address(),
        ..Default::default()
    })
    .into();
    requests.push(add_backend_2);

    Ok(requests)
}

fn random_id_of_7_chars() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(7)
        .map(char::from)
        .collect()
}

fn random_socket_address() -> String {
    let mut rng = rand::thread_rng();

    let ip_address = format!(
        "{}.{}.{}.{}",
        rng.gen_range(0..256),
        rng.gen_range(0..256),
        rng.gen_range(0..256),
        rng.gen_range(0..256)
    );

    let port = rng.gen_range(1025..65536);

    let socket_address = format!("{}:{}", ip_address, port);

    socket_address
}

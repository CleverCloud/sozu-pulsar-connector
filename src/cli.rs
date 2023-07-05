use clap::Parser;

/// Command line tool to consume requests on a Pulsar topic and write them
/// to a Sōzu instance
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long = "config", help = "configuration file of the pulsar connector")]
    pub config: String,
}

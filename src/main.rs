use bee_herder::{run, Config};
use clap::{App, Arg};
use std::{error::Error, process};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let matches = App::new("bee_herder")
        .version("0.1.0")
        .about("Herding bees 🧲🐝")
        .author("mfw78")
        .arg(
            Arg::with_name("mode")
                .short('m')
                .long("mode")
                .value_name("MODE")
                .help("Sets the mode to upload files or the index")
                .takes_value(true)
                .possible_values(&["files", "manifest", "refresh"])
                .required(true),
        )
        .arg(
            Arg::with_name("path")
                .long("path")
                .value_name("PATH")
                .help("The directory from which to upload files")
                .takes_value(true)
                .required(true),
        )
        .after_long_help(
            "Environment variables: \n
  - BEE_API_URL:        API URL for uploading to Swarm \n
  - BEE_DEBUG_API_URL:  Debug API URL for Swarm \n
  - POSTAGE_BATCH:      Stamp to be used for uploading to Swarm \n
  - BEE_HERDER_DB:      The path to the leveldb database for co-ordinating \n
  - UPLOAD_RATE:        The rate at which to upload files to Swarm (files per second) \n
  - NODE_ID:            When multi-node uploading, specify the node ID to use \n
  - NODE_COUNT:         When multi-node uploading, specify the number of nodes")
        .get_matches();
    // TODO: Write detailed usage instructions
    //

    let config = Config::new(matches).unwrap_or_else(|err| {
        eprintln!("Problem parsing configuration variables: {}", err);
        process::exit(1)
    });

    // run the program
    if let Err(e) = run(config).await {
        eprintln!("Application error: {}", e);
        process::exit(1);
    }

    Ok(())
}

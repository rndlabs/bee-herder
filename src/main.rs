use std::{error::Error, process};

use bee_herder::{import, manifest, migrate, upload, Cli, Commands};
use clap::Parser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    let res = match &cli.subcommand {
        Commands::Import(import) => import::run(import).await,
        Commands::Upload(upload) => upload::run(upload).await,
        Commands::Manifest(manifest) => crate::manifest::run(manifest).await,
        Commands::Migrate(migrate) => migrate::run(migrate).await,
    };

    // run the program
    if let Err(e) = res {
        eprintln!("Application error: {}", e);
        process::exit(1);
    }

    Ok(())
}

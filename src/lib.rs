use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::error;

pub mod import;
pub mod manifest;
pub mod migrate;
pub mod upload;

pub type Result<T> = std::result::Result<T, Box<dyn error::Error + Send>>;

const FILE_PREFIX: &str = "f_";

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum HerdStatus {
    Pending,
    Tagged,
    Uploaded,
    UrlProcessed,
    Syncing,
    Synced,
    Verified,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HerdFile {
    pub file_path: String,
    pub prefix: String,
    pub status: HerdStatus,
    pub tag: Option<u32>,
    pub reference: Option<Vec<u8>>,
    pub mantaray_reference: Option<Vec<u8>>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HerdIndexItem {
    pub data: Vec<u8>,
    pub status: HerdStatus,
}

pub enum HerdMode {
    Import,
    Upload,
    Manifest,
    Refresh,
    Migrate,
}

#[derive(Parser)]
#[clap(name = "bee_herder")]
#[clap(author, version, about)]
pub struct Cli {
    #[clap(subcommand)]
    pub subcommand: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Import a path to the database
    Import(Import),
    /// Uploads any pending files to Swarm 🐝
    Upload(Upload),
    /// Generates a manifest for all the uploaded files
    Manifest(Manifest),
    /// Migrate the database to the new format
    Migrate(Migrate),
}

#[derive(Args)]
pub struct Import {
    #[clap(long, value_parser, help = "Sets the path to the database to use")]
    db: String,
    #[clap(value_parser)]
    path: String,
}

#[derive(Args, Clone)]
pub struct Upload {
    #[clap(long, value_parser, help = "Sets the path to the database to use")]
    db: String,
    #[clap(
        value_parser,
        env = "BEE_API_URL",
        help = "Sets the URI of the Bee API to use"
    )]
    bee_api_uri: String,
    #[clap(
        value_parser,
        env = "BEE_HERDER_BEE_POSTAGE_BATCH",
        help = "Which postage batch to use for uploading"
    )]
    bee_postage_batch: String,
    #[clap(
        value_parser,
        env = "BEE_HERDER_UPLOAD_RATE",
        help = "Rate in files / sec when uploading"
    )]
    upload_rate: Option<u32>,
    #[clap(
        value_parser,
        env = "BEE_HERDER_NODE_ID",
        help = "The node ID when uploading using multiple nodes"
    )]
    node_id: Option<u32>,
    #[clap(
        value_parser,
        env = "BEE_HERDER_NODE_COUNT",
        help = "The total number of nodes in use when uploading using multiple nodes"
    )]
    node_count: Option<u32>,
}

#[derive(Args)]
pub struct Manifest {
    #[clap(long, value_parser, help = "Sets the path to the database to use")]
    db: String,
    #[clap(
        value_parser,
        env = "BEE_API_URL",
        help = "Sets the URI of the Bee API to use"
    )]
    bee_api_uri: String,
    #[clap(
        value_parser,
        env = "BEE_HERDER_BEE_POSTAGE_BATCH",
        help = "Which postage batch to use for uploading"
    )]
    bee_postage_batch: String,
    #[clap(value_parser, help = "Prefices that should be parallelized")]
    parallel_prefixes: Vec<String>,
}

#[derive(Args)]
pub struct Migrate {
    #[clap(long, value_parser, help = "Sets the path to the database to use")]
    db: String,
}

// a function that takes a database and returns a specific key as a number or 0 if it does not exist
fn get_num(db: &sled::Db, key: HerdStatus) -> u64 {
    match db.get(bincode::serialize(&key).unwrap()) {
        Ok(Some(num)) => bincode::deserialize(&num).unwrap(),
        _ => 0,
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn url_test() {
        let path = "lkdsfjklsda/asdf/wer/index.html";
        let url = url::Url::parse(&path).unwrap();
        assert_eq!(url.path(), "lkdsfjklsda/asdf/wer/index.html");
    }
}

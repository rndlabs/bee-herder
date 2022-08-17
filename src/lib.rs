use std::{collections::HashMap, error::Error, env, path::Path, fs};
use bee_api::{UploadConfig, BeeConfig};
use serde::{Serialize, Deserialize};
use indicatif::{ProgressBar, ProgressStyle};
use tokio_stream::StreamExt;

const FILE_PREFIX : &str = "f_";

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum HerdStatus {
    Pending,
    Uploaded,
    Synced,
    Verified,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HerdFile {
    pub path: Vec<u8>,
    pub prefix: Vec<u8>,
    pub status: HerdStatus,
    pub tag: Option<u64>,
    pub reference: Option<Vec<u8>>,
    pub mantaray_reference: Option<Vec<u8>>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HerdIndexItem {
    pub data: Vec<u8>,
    pub status: HerdStatus,
}

pub enum HerdMode {
    Files,
    Manifest,
    Refresh,
}

pub struct Config {
    pub mode: HerdMode,
    pub path: String,
    pub db: String,
    pub stamp: String,
    pub bee_api: String,
    pub bee_debug_api: String,
}

impl Config {
    pub fn new(matches: clap::ArgMatches) -> Result<Config, &'static str> {
        let mode = match matches.value_of("mode").unwrap() {
            "files" => HerdMode::Files,
            "manifest" => HerdMode::Manifest,
            "refresh" => HerdMode::Refresh,
            _ => return Err("Invalid mode"),
        };
        let path = matches.value_of("path").unwrap().to_string();

        // return err if db is not set
        let db = match env::var("BEE_HERDER_DB") {
            Ok(val) => val,
            Err(_) => return Err("Environment variable BEE_HERDER_DB must be set"),
        };

        // return err if stamp is not set
        let stamp = match env::var("POSTAGE_BATCH") {
            Ok(val) => val,
            Err(_) => return Err("Environment variable POSTAGE_BATCH must be set"),
        };

        // return err if bee_api is not set
        let bee_api = match env::var("BEE_API_URL") {
            Ok(val) => val,
            Err(_) => return Err("Environment variable BEE_API_URL must be set"),
        };

        // return err if bee_debug_api is not set
        let bee_debug_api = match env::var("BEE_DEBUG_API_URL") {
            Ok(val) => val,
            Err(_) => return Err("Environment variable BEE_DEBUG_API_URL must be set"),
        };

        Ok(Config {
            mode,
            path,
            db,
            stamp,
            bee_api,
            bee_debug_api,
        })
    }
}

async fn files_upload(config: &Config) -> Result<(), Box<dyn Error>> {

    println!("made it inside the files_upload");
    let client = reqwest::Client::new();

    let db = sled::open(&config.db)?;

    let db_iter = tokio_stream::iter(db.scan_prefix(FILE_PREFIX.as_bytes()));

    tokio::pin!(db_iter);

    let mut to_upload = db_iter
        .map(|item| {
            let (key, value) = item.expect("Failed to read database");
            let file: HerdFile = bincode::deserialize(&value)
                .expect("Failed to deserialize");
            (file, key)
        })
        .filter(|(file, _)| file.status == HerdStatus::Pending);

    // consume the iterator
    while let Some((file, key)) = to_upload.next().await {
        // read the file from the local filesystem
        let path = Path::new(&config.path).join(String::from_utf8(file.path.clone()).unwrap());
        let data = fs::read(path).expect("Failed to read file");

        // upload the file to the swarm
        let hash = bee_api::bytes_post(
            client.clone(),
            config.bee_api.clone(),
            data, &
            UploadConfig {
                stamp: config.stamp.clone(),
                pin: Some(true),
                tag: None,
                deferred: Some(true),
            }).await?;

        // update the database
        let mut file = file;
        file.status = HerdStatus::Uploaded;
        file.tag = Some(0);
        file.reference = Some(hash.ref_.as_bytes().to_vec());
        db.insert(key, bincode::serialize(&file).unwrap()).expect("Failed to update database");

        println!("Uploaded file: {:?}", file);
    }
    
    Ok(())
}

pub async fn run(config: Config) -> Result<(), Box<dyn Error>> {

    println!("Made it inside of the run");

    // if mode is files
    match config.mode {
        HerdMode::Files => files_upload(&config).await?,
        HerdMode::Manifest => todo!(),
        HerdMode::Refresh => todo!(),
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // #[tokio::test]
    // async fn it_works() {
    //     println!("Hello, world!");
    //     files_upload().await.unwrap();
    // }
}
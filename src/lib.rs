use bee_api::UploadConfig;
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, error::Error, fs, path::Path};

use tokio::sync::mpsc;
use tokio_stream::StreamExt;

// type Result<T> = std::result::Result<T, Box<dyn error::Error + Send>>;

const FILE_PREFIX: &str = "f_";

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

async fn files_upload(config: Config) -> Result<(), Box<dyn Error + Send>> {
    let client = reqwest::Client::new();

    // log the start time of the upload
    let start = std::time::Instant::now();

    let db = sled::open(&config.db).unwrap();
    let db_iter = tokio_stream::iter(db.scan_prefix(FILE_PREFIX.as_bytes()));

    let (tx, rx) = mpsc::channel(100);

    let pb = ProgressBar::new(50000 as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})").unwrap()
        .progress_chars("#>-"));

    // create a sync channel for processing completed items
    let handle = tokio::spawn(async move {
        // convert rx to a ReceiverStream
        let mut rx = tokio_stream::wrappers::ReceiverStream::new(rx);
        // consume the channel
        while let Some(result) = rx.next().await {
            match result {
                Ok((file, key)) => {
                    // write the file to the database
                    let value = bincode::serialize(&file).expect("Failed to serialize");
                    db.insert(key, value).expect("Failed to write to database");
                }
                Err(e) => {
                    println!("{}", e);
                }
            }
            pb.inc(1);
        }

        pb.finish_with_message(format!("Upload done in {}s", start.elapsed().as_secs()));
    });

    let compute = tokio::spawn(async move {
        tokio::pin!(db_iter);
        let mut to_upload = db_iter
            .map(|item| {
                let (key, value) = item.expect("Failed to read database");
                let file: HerdFile = bincode::deserialize(&value).expect("Failed to deserialize");
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
                data,
                &UploadConfig {
                    stamp: config.stamp.clone(),
                    pin: Some(true),
                    tag: None,
                    deferred: Some(true),
                },
            )
            .await;

            // set the hash in herdfile and return it to the channel
            // or return an error if the upload failed
            match hash {
                Ok(hash) => {
                    let mut file = file;
                    file.status = HerdStatus::Uploaded;
                    file.reference = Some(hex::decode(hash.ref_).unwrap());
                    tx.send(Ok((file, key))).await.expect("Failed to send to channel");
                }
                Err(e) => {
                    tx.send(Err(e)).await.unwrap();
                }
            }
        }
    });

    handle.await.unwrap();
    compute.await.unwrap();

    Ok(())
}

pub async fn run(config: Config) -> Result<(), Box<dyn Error + Send>> {
    println!("Made it inside of the run");

    // if mode is files
    match config.mode {
        HerdMode::Files => files_upload(config).await?,
        HerdMode::Manifest => todo!(),
        HerdMode::Refresh => todo!(),
    };

    Ok(())
}
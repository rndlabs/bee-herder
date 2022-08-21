use bee_api::UploadConfig;
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, error::Error, fs};

use tokio::sync::mpsc;
use tokio_stream::StreamExt;

// type Result<T> = std::result::Result<T, Box<dyn error::Error + Send>>;

const FILE_PREFIX: &str = "f_";

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum HerdStatus {
    Pending,
    Uploaded,
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

    // read current number of pending entries
    let num_pending = match db
        .get(bincode::serialize(&HerdStatus::Pending).unwrap())
        .unwrap()
    {
        Some(b) => bincode::deserialize(&b).unwrap(),
        None => 0,
    };

    // read current number of pending entries
    let num_uploaded = match db
        .get(bincode::serialize(&HerdStatus::Uploaded).unwrap())
        .unwrap()
    {
        Some(b) => bincode::deserialize(&b).unwrap(),
        None => 0,
    };
    
    // if number of pending entries is 0, then we are done
    if num_pending == 0 {
        println!("No pending entries to upload");
        return Ok(());
    }

    tag_index_generator(&db, &client, &config).await?;

    let (tx, rx) = mpsc::channel(100);

    let pb = ProgressBar::new(num_pending);
    pb.set_style(ProgressStyle::default_bar()
        .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})").unwrap()
        .progress_chars("#>-"));

    // create a sync channel for processing completed items
    let sync_thread_db = db.clone();
    let handle = tokio::spawn(async move {
        // convert rx to a ReceiverStream
        let mut rx = tokio_stream::wrappers::ReceiverStream::new(rx);
        // prepare a batch for writing to the database to minimise IOPS
        let mut batch = sled::Batch::default();
        // consume the channel
        let mut count = 0;
        while let Some(result) = rx.next().await {
            match result {
                Ok((file, key)) => {
                    // write the file to the database
                    let value = bincode::serialize(&file).expect("Failed to serialize");
                    batch.insert(key, value);
                }
                Err(e) => {
                    println!("{}", e);
                }
            }
            
            count += 1;
            if count % 1000 == 0 {
                batch.insert(
                    bincode::serialize(&HerdStatus::Pending).unwrap(),
                    bincode::serialize(&(num_pending - count)).unwrap(),
                );
                batch.insert(
                    bincode::serialize(&HerdStatus::Uploaded).unwrap(),
                    bincode::serialize(&(num_uploaded + count)).unwrap(),
                );
                sync_thread_db.apply_batch(batch).expect("Failed to apply batch");
                batch = sled::Batch::default();
            }

            pb.inc(1);
        }

        // write the final batch
        if num_pending - count > 0 {
            batch.insert(
                bincode::serialize(&HerdStatus::Pending).unwrap(),
                bincode::serialize(&(num_pending - count)).unwrap(),
            );
        } else {
            batch.remove(bincode::serialize(&HerdStatus::Pending).unwrap());
        }

        if num_uploaded + count > 0 {
            batch.insert(
                bincode::serialize(&HerdStatus::Uploaded).unwrap(),
                bincode::serialize(&(num_uploaded + count)).unwrap(),
            );
        } else {
            batch.remove(bincode::serialize(&HerdStatus::Uploaded).unwrap());
        }

        sync_thread_db.apply_batch(batch).expect("Failed to apply batch");

        pb.finish_with_message(format!("Upload done in {}s", start.elapsed().as_secs()));
    });

    let uploader = tokio::spawn(async move {
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
            let data = fs::read(file.file_path.clone()).expect("Failed to read file");

            // upload the file to the swarm
            let hash = bee_api::bytes_post(
                client.clone(),
                config.bee_api.clone(),
                data,
                &UploadConfig {
                    stamp: config.stamp.clone(),
                    pin: Some(true),
                    tag: file.tag,
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
                    tx.send(Ok((file, key)))
                        .await
                        .expect("Failed to send to channel");
                }
                Err(e) => {
                    tx.send(Err(e)).await.unwrap();
                }
            }
        }
    });

    handle.await.unwrap();
    uploader.await.unwrap();

    Ok(())
}

// generate a tags in batches.
// just iterate over the files and generate a tag for each 100 files
async fn tag_index_generator(db: &sled::Db, client: &reqwest::Client, config: &Config) -> Result<(), Box<dyn std::error::Error + Send>> {
    // attempt to retrieve the hashmap from the db
    let index: Vec<u32> = match db.get("cluster_to_tag_index") {
        Ok(Some(index)) => {
            // if it exists, deserialize it
            bincode::deserialize(&index).unwrap()
        }
        _ => {
            let pb = ProgressBar::new(100);
            pb.set_style(ProgressStyle::default_bar()
                .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})").unwrap()
                .progress_chars("#>-"));
            
            pb.set_message("Generating tags");

            // otherwise, create a new one
            let mut index = vec![0; 100];

            // using the bee api, create 100 tags
            for i in 0..100 {
                let tag = bee_api::tag_post(client, config.bee_api.clone()).await.unwrap();
                index[i] = tag.uid;
                pb.inc(1);
            }

            pb.finish_with_message("Tags generated");
            index
        }
    };

    // create a batch to write the index to the database
    let mut batch = sled::Batch::default();
    batch.insert("cluster_to_tag_index", bincode::serialize(&index).unwrap());

    // files start with the f_ prefix in the database
    // get all files from the sled database that are of status pending and do not have a tag
    db
        .scan_prefix(FILE_PREFIX.as_bytes())
        .filter_map(|item| {

            let (key, value) = item.expect("Failed to read database");
            let file: HerdFile = bincode::deserialize(&value).expect("Failed to deserialize");
            if file.status == HerdStatus::Pending && file.tag.is_none() {
                Some((file, key))
            } else {
                None
            }
        })
        // enumerate the files so we can get the index
        .enumerate()
        // map the files to a new tag based on the index
        .map(|(i, (file, key))| {
            let mut file = file;
            let tag = index[i % 100];
            file.tag = Some(tag);
            (file, key)
        })
        // for each file, update the database
        .for_each(|(file, key)| {
            batch.insert(key, bincode::serialize(&file).unwrap());
        });

    // write the batch to the database
    db.apply_batch(batch).expect("Failed to apply batch");
    Ok(())
}

pub async fn run(config: Config) -> Result<(), Box<dyn Error + Send>> {
    // if mode is files
    match config.mode {
        HerdMode::Files => files_upload(config).await?,
        HerdMode::Manifest => todo!(),
        HerdMode::Refresh => todo!(),
    };

    Ok(())
}

use bee_api::UploadConfig;
use governor::{Quota, RateLimiter, Jitter};
use indicatif::{ProgressBar, ProgressStyle};
use mantaray::{persist::BeeLoadSaver, Entry, Manifest};
use serde::{Deserialize, Serialize};
use std::error;
use std::time::Duration;
use std::{collections::HashMap, env, num::NonZeroU32};

use std::sync::Arc;

use thiserror::Error;

use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_stream::StreamExt; // for read_to_end()

type Result<T> = std::result::Result<T, Box<dyn error::Error + Send>>;

#[derive(Error, Debug)]
pub enum BeeHerderError {
    #[error("Environment variable {0} must be set")]
    EnvVarNotSet(String),
    #[error("Invalid mode")]
    InvalidMode,
    #[error("Environment variable {0} must be a number")]
    EnvVarNotNumber(String),
}

const FILE_PREFIX: &str = "f_";
const NUM_UPLOAD_TAGS: usize = 100;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum HerdStatus {
    Pending,
    Tagged,
    Uploaded,
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
    pub metadata: HashMap<String, String>,
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
}

pub struct Config {
    pub mode: HerdMode,
    pub path: String,
    pub db: String,
    pub stamp: String,
    pub bee_api: String,
    pub bee_debug_api: String,
    pub upload_rate: u32,
    pub node_id: u32,
    pub node_count: u32,
}

impl Config {
    pub fn new(matches: clap::ArgMatches) -> Result<Config> {
        let mode = match matches.value_of("mode").unwrap() {
            "import" => HerdMode::Import,
            "upload" => HerdMode::Upload,
            "manifest" => HerdMode::Manifest,
            "refresh" => HerdMode::Refresh,
            _ => return Err(Box::new(BeeHerderError::InvalidMode)),
        };
        let path = matches.value_of("path").unwrap().to_string();

        // return err if db is not set
        let db = match env::var("BEE_HERDER_DB") {
            Ok(val) => val,
            Err(_) => {
                return Err(Box::new(BeeHerderError::EnvVarNotSet(
                    "BEE_HERDER_DB".to_string(),
                )))
            }
        };

        // return err if stamp is not set
        let stamp = match env::var("POSTAGE_BATCH") {
            Ok(val) => val,
            Err(_) => {
                return Err(Box::new(BeeHerderError::EnvVarNotSet(
                    "POSTAGE_BATCH".to_string(),
                )))
            }
        };

        // return err if bee_api is not set
        let bee_api = match env::var("BEE_API_URL") {
            Ok(val) => val,
            Err(_) => {
                return Err(Box::new(BeeHerderError::EnvVarNotSet(
                    "BEE_API_URL".to_string(),
                )))
            }
        };

        // return err if bee_debug_api is not set
        let bee_debug_api = match env::var("BEE_DEBUG_API_URL") {
            Ok(val) => val,
            Err(_) => {
                return Err(Box::new(BeeHerderError::EnvVarNotSet(
                    "BEE_DEBUG_API_URL".to_string(),
                )))
            }
        };

        let upload_rate = match env::var("UPLOAD_RATE") {
            // parse as u32 or return err
            Ok(val) => match val.parse::<u32>() {
                Ok(val) => val,
                Err(_) => {
                    return Err(Box::new(BeeHerderError::EnvVarNotNumber(
                        "UPLOAD_RATE".to_string(),
                    )))
                }
            },
            Err(_) => 50,
        };

        let node_id = match env::var("NODE_ID") {
            // parse as u32 or return err
            Ok(val) => match val.parse::<u32>() {
                Ok(val) => val,
                Err(_) => {
                    return Err(Box::new(BeeHerderError::EnvVarNotNumber(
                        "NODE_ID".to_string(),
                    )))
                }
            },
            Err(_) => 0,
        };

        let node_count = match env::var("NODE_COUNT") {
            // parse as u32 or return err
            Ok(val) => match val.parse::<u32>() {
                Ok(val) => val,
                Err(_) => {
                    return Err(Box::new(BeeHerderError::EnvVarNotNumber(
                        "NODE_COUNT".to_string(),
                    )))
                }
            },
            Err(_) => 1,
        };

        Ok(Config {
            mode,
            path,
            db,
            stamp,
            bee_api,
            bee_debug_api,
            upload_rate,
            node_id,
            node_count,
        })
    }
}

async fn import_dir(config: Config) -> Result<()> {
    let db = sled::open(config.db).expect("Unable to open database");
    // get the number of pending files
    let num_pending = get_num(&db, HerdStatus::Pending);
    println!("{} pending files before import", num_pending);

    // from a directory, recursively import all files into the db
    let mut files = Vec::new();
    let mut dirs = Vec::new();
    let mut dir = tokio::fs::read_dir(&config.path).await.unwrap();
    while let Some(entry) = dir.next_entry().await.unwrap() {
        let path = entry.path();
        if path.is_dir() {
            dirs.push(path);
        } else {
            files.push(path);
        }
    }

    // use loop to avoid recursion
    while !dirs.is_empty() {
        let mut new_dirs = Vec::new();
        for dir in dirs {
            let mut dir = tokio::fs::read_dir(&dir).await.unwrap();
            while let Some(entry) = dir.next_entry().await.unwrap() {
                let path = entry.path();
                if path.is_dir() {
                    new_dirs.push(path);
                } else {
                    files.push(path);
                }
            }
        }
        dirs = new_dirs;
    }

    let mut batch = sled::Batch::default();

    // count files
    let mut count = 0;
    for file in files {
        let file_path = file.to_str().unwrap().to_string();
        // set prefix to the file path relative to the root path excluding the leading slash
        let prefix = file_path
            .strip_prefix(&config.path)
            .unwrap()
            .strip_prefix("/")
            .unwrap()
            .to_string();

        // print the file path
        println!("Importing {} as {}", file_path, prefix);

        let mut metadata = HashMap::new();
        let content_type = match file_path.split(".").last() {
            Some("png") => "image/png",
            Some("jpg") => "image/jpeg",
            Some("jpeg") => "image/jpeg",
            Some("gif") => "image/gif",
            Some("svg") => "image/svg+xml",
            Some("mp4") => "video/mp4",
            Some("webm") => "video/webm",
            Some("mp3") => "audio/mpeg",
            Some("wav") => "audio/wav",
            Some("ogg") => "audio/ogg",
            Some("pdf") => "application/pdf",
            Some("txt") => "text/plain",
            Some("html") => "text/html",
            Some("css") => "text/css",
            Some("json") => "application/json",
            Some("xml") => "application/xml",
            Some("zip") => "application/zip",
            Some("tar") => "application/x-tar",
            Some("gz") => "application/gzip",
            Some("rar") => "application/x-rar-compressed",
            Some("7z") => "application/x-7z-compressed",
            Some("js") => "text/javascript",
            Some(_) => "application/octet-stream",
            None => "application/octet-stream",
        };
        metadata.insert("Content-Type".to_string(), content_type.to_string());

        let herd_file = HerdFile {
            file_path, // absolute file path
            prefix, // should be relative to the path specified in the config
            status: HerdStatus::Pending,
            tag: None,
            reference: None,
            mantaray_reference: None,
            metadata,
        };
        let mut key = "f_".as_bytes().to_vec();
        key.extend_from_slice(herd_file.file_path.as_bytes());
        batch.insert(key, bincode::serialize(&herd_file).unwrap());

        count += 1;
    }

    batch.insert(
        bincode::serialize(&HerdStatus::Pending).unwrap(),
        bincode::serialize(&(num_pending + count)).unwrap(),
    );
    db.apply_batch(batch).expect("Failed to apply batch");

    let num_pending = get_num(&db, HerdStatus::Pending);
    println!("{} pending files after import", num_pending);

    println!("Imported {} files", count);

    Ok(())
}

async fn files_upload(config: Config) -> Result<()> {
    let client = reqwest::Client::new();

    // log the start time of the upload
    let start = std::time::Instant::now();

    let db = sled::open(&config.db).unwrap();
    let mut handles = Vec::new();

    // if there are pending files, make sure they are tagged
    if get_num(&db, HerdStatus::Pending) > 0 {
        tagger(&db, &client, &config).await?;
    }

    // read current number of pending entries
    let num_pending = get_num(&db, HerdStatus::Pending);
    let num_tagged = get_num(&db, HerdStatus::Tagged);
    let num_uploaded = get_num(&db, HerdStatus::Uploaded);

    // if number of tagged entries is 0, then we are done
    if num_tagged == 0 {
        println!("No tagged entries to upload");
        return Ok(());
    }

    let (tx, rx) = mpsc::channel(100);

    let pb = ProgressBar::new(num_tagged);
    pb.set_style(ProgressStyle::default_bar()
        .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})").unwrap()
        .progress_chars("#>-"));

    pb.set_message("Uploading");

    // create a sync channel for processing *completed* items
    let sync_thread_db = db.clone();
    handles.push(tokio::spawn(async move {
        // convert rx to a ReceiverStream
        let mut rx = tokio_stream::wrappers::ReceiverStream::new(rx);
        // prepare a batch for writing to the database to minimise IOPS
        let mut batch = sled::Batch::default();
        // consume the channel
        let mut count = 0;
        let mut failed = 0;
        while let Some(result) = rx.next().await {
            match result {
                Ok((file, key)) => {
                    // write the file to the database
                    let value = bincode::serialize(&file).expect("Failed to serialize");
                    batch.insert(key, value);
                }
                Err(e) => {
                    println!("{}", e);
                    failed += 1;
                }
            }

            count += 1;
            if count % 500 == 0 {
                batch.insert(
                    bincode::serialize(&HerdStatus::Tagged).unwrap(),
                    bincode::serialize(&(num_tagged - count)).unwrap(),
                );
                batch.insert(
                    bincode::serialize(&HerdStatus::Uploaded).unwrap(),
                    bincode::serialize(&(num_uploaded + count)).unwrap(),
                );
                sync_thread_db
                    .apply_batch(batch)
                    .expect("Failed to apply batch");
                batch = sled::Batch::default();
            }

            pb.inc(1);
        }

        // write the final batch
        if num_pending - count > 0 {
            batch.insert(
                bincode::serialize(&HerdStatus::Tagged).unwrap(),
                bincode::serialize(&(num_tagged - count)).unwrap(),
            );
        } else {
            batch.remove(bincode::serialize(&HerdStatus::Tagged).unwrap());
        }

        if num_uploaded + count > 0 {
            batch.insert(
                bincode::serialize(&HerdStatus::Uploaded).unwrap(),
                bincode::serialize(&(num_uploaded + count)).unwrap(),
            );
        } else {
            batch.remove(bincode::serialize(&HerdStatus::Uploaded).unwrap());
        }

        sync_thread_db
            .apply_batch(batch)
            .expect("Failed to apply batch");

        // finish with number of files uploaded, and number of files failed in number of seconds
        pb.finish_with_message(format!(
            "{} files uploaded, {} failed in {} seconds",
            count,
            failed,
            start.elapsed().as_secs()
        ));
    }));

    let uploader_db = db.clone();
    handles.push(tokio::spawn(async move {
        // set the node index and number of nodes uploading
        let node_id = config.node_id;
        let node_count = config.node_count;

        let lim = RateLimiter::direct(Quota::per_second(
            NonZeroU32::new(config.upload_rate).unwrap(),
        ));

        let offset = 2 + config.path.len() + 1; // TODO: Can replace this index with a shorter index in future

        for i in 0..node_count {
            let idx = (node_id + i) % node_count;
            if node_count > 1 {
                println!("Node id {} uploading {}/{}", idx, idx+1, node_count);
            }

            let db_iter = tokio_stream::iter(uploader_db.scan_prefix(FILE_PREFIX.as_bytes()));
            tokio::pin!(db_iter);

            let mut to_upload = db_iter
                .map(|item| {
                    let (key, value) = item.expect("Failed to read database");
                    let file: HerdFile =
                        bincode::deserialize(&value).expect("Failed to deserialize");
                    // decode the key as a string and strip the first two characters
                    // let key_u32 = String::from_utf8(key.to_vec()).expect("Failed to decode key");
                    // let key_u32 = key_u32[offset..].to_string().parse::<u32>().unwrap();
                    (file, key)
                })
                // decode key as u32 and filter out files that have already been uploaded
                .filter(|(file, _)| {
                    file.status == HerdStatus::Tagged
                        // && key_u32 % node_count == idx
                        && file
                            .metadata
                            .get("Content-Type")
                            .map(|ct| ct != "application/octet-stream+xapian") // TODO: Filter at wiki_extractor level
                            .unwrap_or(true)
                });

            // consume the iterator
            while let Some((file, key)) = to_upload.next().await {
                // read the file from the local filesystem
                // print file path being processed
                let mut tokio_file = File::open(file.file_path.clone()).await.unwrap();

                let mut data = vec![];
                tokio_file.read_to_end(&mut data).await.unwrap();

                // ensure we drop the file handle
                drop(tokio_file);

                // throttle the upload rate
                lim.until_ready_with_jitter(Jitter::new(Duration::from_millis(5), Duration::from_millis(50))).await;

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
        }
    }));

    futures::future::join_all(handles).await;

    // At this point all files have been uploaded, so let's check if there are any files that need to be uploaded
    // again
    db.scan_prefix(FILE_PREFIX.as_bytes())
        .map(|item| {
            let (key, value) = item.expect("Failed to read database");
            let file: HerdFile =
                bincode::deserialize(&value).expect("Failed to deserialize");
            (file, key)
        })
        .filter(|(file, _)| file.status == HerdStatus::Tagged)
        .for_each(|(file, key)| {
            println!("Failed to upload {:?}", file);
        });

    Ok(())
}

// generate tags in batches.
// just iterate over the files and generate a tag for each 100 files
async fn tagger(
    db: &sled::Db,
    client: &reqwest::Client,
    config: &Config,
) -> Result<()> {
    // attempt to retrieve the hashmap from the db
    let index: Vec<u32> = match db.get("cluster_to_tag_index") {
        Ok(Some(index)) => {
            // if it exists, deserialize it
            bincode::deserialize(&index).unwrap()
        }
        _ => {
            // otherwise, create a new one
            let pb = ProgressBar::new(NUM_UPLOAD_TAGS.try_into().unwrap());
            pb.set_style(ProgressStyle::default_bar()
                .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})").unwrap()
                .progress_chars("#>-"));

            pb.set_message("Generating tags");

            // otherwise, create a new one
            let mut index = vec![0; NUM_UPLOAD_TAGS];

            // using the bee api, create NUM_UPLOAD_TAGS tags
            for i in 0..NUM_UPLOAD_TAGS {
                let tag = bee_api::tag_post(client, config.bee_api.clone())
                    .await
                    .unwrap();
                index[i] = tag.uid;
                pb.inc(1);
            }

            // write the index to the database
            db.insert("cluster_to_tag_index", bincode::serialize(&index).unwrap())
                .unwrap();

            pb.finish_with_message("Tags generated");
            index
        }
    };

    // create a batch in which to append tagged files to the database
    let mut batch = sled::Batch::default();

    // get starting time
    let start = std::time::Instant::now();

    // get number of pending files
    let num_pending = get_num(&db, HerdStatus::Pending);
    let num_tagged = get_num(&db, HerdStatus::Tagged);

    let pb = ProgressBar::new(num_pending);

    pb.set_style(ProgressStyle::default_bar()
        .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})").unwrap()
        .progress_chars("#>-"));

    pb.set_message("Tagging files");

    // files start with the f_ prefix in the database
    // get all files from the sled database that are of status pending and do not have a tag
    let files = db
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
            let tag = index[i % NUM_UPLOAD_TAGS];
            file.tag = Some(tag);
            file.status = HerdStatus::Tagged;
            (i, file, key)
        });

    // count the number of tagged files
    let mut count = 0;

    // write the tagged files to the database
    for (i, file, key) in files {
        batch.insert(key, bincode::serialize(&file).unwrap());

        if i % 1000 == 0 {
            batch.insert(bincode::serialize(&HerdStatus::Tagged).unwrap(), bincode::serialize(&(num_tagged + count)).unwrap());
            // todo: should guard against the case where the total size is 1000 and num_pending becomes 0
            batch.insert(bincode::serialize(&HerdStatus::Pending).unwrap(), bincode::serialize(&(num_pending - count)).unwrap());
            db.apply_batch(batch).expect("Failed to apply batch");
            batch = sled::Batch::default();
        }

        pb.inc(1);
        count += 1;
    }

    // set the number of tagged files in the database
    batch.insert(bincode::serialize(&HerdStatus::Tagged).unwrap(), bincode::serialize(&(num_tagged + count)).unwrap());
    if num_pending - count > 0 {
        batch.insert(bincode::serialize(&HerdStatus::Pending).unwrap(), bincode::serialize(&(num_pending - count)).unwrap());
    } else {
        batch.remove(bincode::serialize(&HerdStatus::Pending).unwrap());
    }

    // write the remaining files to the database
    db.apply_batch(batch).expect("Failed to apply batch");

    // finish the progress bar with number of items processed and time elapsed
    pb.finish_with_message(format!(
        "Tagged {} files in {:?}",
        num_pending,
        start.elapsed()
    ));
    Ok(())
}

async fn manifest_gen(config: Config) -> Result<()> {
    // log the start time of the upload
    let start = std::time::Instant::now();

    let db = sled::open(&config.db).unwrap();
    let mut handles = Vec::new();

    let db_iter = tokio_stream::iter(db.scan_prefix(FILE_PREFIX.as_bytes()));

    // read current number of pending entries
    let num_uploaded = get_num(&db, HerdStatus::Uploaded);

    // read current number of in syncing status
    let num_syncing = get_num(&db, HerdStatus::Syncing);

    // create static variable to hold beeloadsaver
    // hold beeloadsaver using arc
    let beeloadsaver = Arc::new(BeeLoadSaver::new(
        config.bee_api.clone(),
        bee_api::BeeConfig {
            upload: Some(UploadConfig {
                stamp: config.stamp.clone(),
                pin: Some(true),
                tag: None, // TODO!enable tag generation for upload tracking
                deferred: Some(true),
            }),
        },
    ));

    // a thread to monitor the progress of the indexer
    let (tx, rx) = mpsc::channel::<Result<(HerdFile, IVec)>>(100);

    let pb = ProgressBar::new(num_uploaded);
    pb.set_style(ProgressStyle::default_bar()
        .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})").unwrap()
        .progress_chars("#>-"));

    pb.set_message("Building manifest");

    // create a sync channel for processing *completed* items
    handles.push(tokio::spawn(async move {
        // convert rx to a ReceiverStream
        let mut rx = tokio_stream::wrappers::ReceiverStream::new(rx);
        // consume the channel
        let mut count = 0;
        let mut failed = 0;
        while let Some(result) = rx.next().await {
            if result.is_err() {
                failed += 1;
            }

            count += 1;
            pb.inc(1);
        }

    }));

    // indexer thread for generating the manifest
    handles.push(tokio::spawn(async move {
        tokio::pin!(db_iter);
        let mut to_index = db_iter
            .map(|item| {
                let (key, value) = item.expect("Failed to read database");
                let file: HerdFile = bincode::deserialize(&value).expect("Failed to deserialize");
                (file, key)
            })
            .filter(|(file, _)| file.status == HerdStatus::Uploaded);

        let mut count = 0;
        let mut manifest: Manifest = Manifest::new(Box::new(beeloadsaver.clone()), false);

        // consume the iterator
        while let Some((file, key)) = to_index.next().await {
            // add to the manifest
            manifest
                .add(
                    &file.prefix,
                    Entry {
                        reference: file.reference.as_ref().unwrap().clone(),
                        metadata: file.metadata.clone(),
                    },
                )
                .await
                .unwrap();
            tx.send(Ok((file, key))).await.unwrap();

            count += 1;

            // for every 1000 items, save the manifest to dump out the forks
            if count % 1000 == 0 {
                println!("Saving manifest at {}", count);
                manifest.store().await.unwrap();
                let ref_ = manifest.trie.ref_;

                // reset the manifest
                manifest = Manifest::new_manifest_reference(ref_, Box::new(beeloadsaver.clone())).unwrap();
            }
        }

        // set metadata
        let mut metadata = HashMap::new();
        metadata.insert(
            String::from("website-index-document"),
            String::from("wiki/index"),
        );

        manifest.set_root(metadata).await.unwrap();

        // save the manifest trie
        manifest.store().await.unwrap();
        println!("Manifest root: {:?}", hex::encode(manifest.trie.ref_));
        println!("Processed {} files", count);
    }));

    futures::future::join_all(handles).await;

    Ok(())
}

pub async fn run(config: Config) -> Result<()> {
    // if mode is files
    match config.mode {
        HerdMode::Import => import_dir(config).await?,
        HerdMode::Upload => files_upload(config).await?,
        HerdMode::Manifest => manifest_gen(config).await?,
        HerdMode::Refresh => todo!(),
    };

    Ok(())
}

// a function that takes a database and returns a specific key as a number or 0 if it does not exist
fn get_num(db: &sled::Db, key: HerdStatus) -> u64 {
    match db.get(bincode::serialize(&key).unwrap()) {
        Ok(Some(num)) => bincode::deserialize(&num).unwrap(),
        _ => 0,
    }
}
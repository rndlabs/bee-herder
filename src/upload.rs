use std::{num::NonZeroU32, time::Duration};

use bee_api::UploadConfig;
use governor::{Jitter, Quota, RateLimiter};
use indicatif::{ProgressBar, ProgressStyle};
use tokio::{fs::File, io::AsyncReadExt, sync::mpsc};
use tokio_stream::StreamExt;

use crate::{get_num, HerdFile, HerdStatus, Result, Upload, FILE_PREFIX};

const NUM_UPLOAD_TAGS: usize = 100;

pub async fn run(config: &Upload) -> Result<()> {
    let client = reqwest::Client::new();

    // log the start time of the upload
    let start = std::time::Instant::now();

    let db = sled::open(&config.db).unwrap();
    let mut handles = Vec::new();

    // if there are pending files, make sure they are tagged
    if get_num(&db, HerdStatus::Pending) > 0 {
        tagger(&db, &client, config).await?;
    }

    // read current number of tagged / uploaded entries
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
        let (mut count, mut failed) = (0, 0);
        while let Some(result) = rx.next().await {
            match result {
                Ok((file, key)) => {
                    // write the file to the database
                    let value = bincode::serialize(&file).expect("Failed to serialize");
                    batch.insert(key, value);
                }
                Err(e) => {
                    eprintln!("{}", e);
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
        if num_tagged - count > 0 {
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
        println!();
    }));

    let uploader_db = db.clone();
    let config = config.clone();
    handles.push(tokio::spawn(async move {
        let lim = RateLimiter::direct(Quota::per_second(
            NonZeroU32::new(config.upload_rate.unwrap_or(50)).unwrap(),
        ));

        // let offset = 2 + config.path.len() + 1; // TODO: Can replace this index with a shorter index in future

        let node_id = config.node_id.unwrap_or(0);
        let node_count = config.node_count.unwrap_or(1);

        for i in 0..node_count {
            let idx = (node_id + i) % node_count;
            if node_count > 1 {
                println!("Node id {} uploading {}/{}", idx, idx + 1, node_count);
            }

            let db_iter = tokio_stream::iter(uploader_db.scan_prefix(FILE_PREFIX.as_bytes()));
            tokio::pin!(db_iter);

            let mut to_upload = db_iter
                .map(|item| {
                    let (key, value) = item.expect("Failed to read database");
                    let file: HerdFile =
                        bincode::deserialize(&value).expect("Failed to deserialize");
                    (file, key)
                })
                // decode key as u32 and filter out files that have already been uploaded
                .filter(|(file, _)| file.status == HerdStatus::Tagged);

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
                lim.until_ready_with_jitter(Jitter::new(
                    Duration::from_millis(5),
                    Duration::from_millis(50),
                ))
                .await;

                // upload the file to the swarm
                let hash = bee_api::bytes_post(
                    client.clone(),
                    config.bee_api_uri.clone(),
                    data,
                    &UploadConfig {
                        stamp: config.bee_postage_batch.clone(),
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

    // wait for all handles to finish
    for handle in handles {
        handle.await.unwrap();
    }

    // At this point all files have been uploaded, so let's check if there are any files that need to be uploaded
    // again
    db.scan_prefix(FILE_PREFIX.as_bytes())
        .map(|item| {
            let (key, value) = item.expect("Failed to read database");
            let file: HerdFile = bincode::deserialize(&value).expect("Failed to deserialize");
            (file, key)
        })
        .filter(|(file, _)| file.status == HerdStatus::Tagged)
        .for_each(|(file, _)| {
            println!("Failed to upload {:?}", file);
        });

    Ok(())
}

// generate tags in batches.
// just iterate over the files and generate a tag for each 100 files
async fn tagger(db: &sled::Db, client: &reqwest::Client, config: &Upload) -> Result<()> {
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
            for tag in index.iter_mut().take(NUM_UPLOAD_TAGS) {
                *tag = bee_api::tag_post(client, config.bee_api_uri.clone())
                    .await
                    .unwrap()
                    .uid;
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
    let num_pending = get_num(db, HerdStatus::Pending);
    let num_tagged = get_num(db, HerdStatus::Tagged);

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
            batch.insert(
                bincode::serialize(&HerdStatus::Tagged).unwrap(),
                bincode::serialize(&(num_tagged + count)).unwrap(),
            );
            // todo: should guard against the case where the total size is 1000 and num_pending becomes 0
            batch.insert(
                bincode::serialize(&HerdStatus::Pending).unwrap(),
                bincode::serialize(&(num_pending - count)).unwrap(),
            );
            db.apply_batch(batch).expect("Failed to apply batch");
            batch = sled::Batch::default();
        }

        pb.inc(1);
        count += 1;
    }

    // set the number of tagged files in the database
    batch.insert(
        bincode::serialize(&HerdStatus::Tagged).unwrap(),
        bincode::serialize(&(num_tagged + count)).unwrap(),
    );
    if num_pending - count > 0 {
        batch.insert(
            bincode::serialize(&HerdStatus::Pending).unwrap(),
            bincode::serialize(&(num_pending - count)).unwrap(),
        );
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

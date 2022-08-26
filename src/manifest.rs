use std::{collections::BTreeMap, sync::Arc};

use bee_api::UploadConfig;
use indicatif::{ProgressBar, ProgressStyle};
use mantaray::{persist::BeeLoadSaver, Entry};
use sled::IVec;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use url::Url;

use crate::{get_num, HerdFile, HerdStatus, Result, FILE_PREFIX};

pub async fn run(config: &crate::Manifest) -> Result<()> {
    // log the start time of the upload
    let start = std::time::Instant::now();

    let db = sled::open(&config.db).unwrap();
    let mut handles = Vec::new();

    let db_iter = tokio_stream::iter(db.scan_prefix(FILE_PREFIX.as_bytes()));

    // read current number of pending entries
    let num_uploaded = get_num(&db, HerdStatus::Uploaded);

    // if there are no uploading files, return
    if num_uploaded == 0 {
        println!("No files to index");
        return Ok(());
    }

    // read current number of in syncing status
    let num_syncing = get_num(&db, HerdStatus::Syncing);

    // create static variable to hold beeloadsaver
    // hold beeloadsaver using arc
    let ls = Arc::new(BeeLoadSaver::new(
        config.bee_api_uri.clone(),
        bee_api::BeeConfig { upload: None },
    ));

    // check if there is a tag for the manifest
    let manifest_tag = match db.get("manifest_tag") {
        Ok(Some(tag)) => {
            // if there is, deserialize it
            bincode::deserialize(&tag).unwrap()
        }
        _ => {
            // otherwise, create a new one
            let tag = bee_api::tag_post(&ls.client, config.bee_api_uri.clone())
                .await
                .unwrap();
            // write the tag to the database
            db.insert("manifest_tag", bincode::serialize(&tag).unwrap())
                .unwrap();
            tag
        }
    };

    let ls = Arc::new(BeeLoadSaver::new(
        config.bee_api_uri.clone(),
        bee_api::BeeConfig {
            upload: Some(UploadConfig {
                stamp: config.bee_postage_batch.clone(),
                pin: Some(true),
                tag: Some(manifest_tag.uid),
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
    let monitor_db = db.clone();
    handles.push(tokio::spawn(async move {
        // convert rx to a ReceiverStream
        let mut rx = tokio_stream::wrappers::ReceiverStream::new(rx);
        // consume the channel
        let mut count = 0;
        let mut failed = 0;
        let mut batch = sled::Batch::default();
        while let Some(result) = rx.next().await {
            if let Ok((file, key)) = result {
                // update the database with the new status
                let mut file = file;
                file.status = HerdStatus::Syncing;
                batch.insert(key, bincode::serialize(&file).unwrap());
            } else {
                failed += 1;
            }

            count += 1;
            pb.inc(1);

            if count % 4000 == 0 {
                batch.insert(
                    bincode::serialize(&HerdStatus::Uploaded).unwrap(),
                    bincode::serialize(&(num_uploaded - count)).unwrap(),
                );
                batch.insert(
                    bincode::serialize(&HerdStatus::Syncing).unwrap(),
                    bincode::serialize(&(num_syncing + count)).unwrap(),
                );
                monitor_db
                    .apply_batch(batch)
                    .expect("Failed to apply batch");
                batch = sled::Batch::default();
            }
        }

        // set the number of uploaded files in the database
        if num_uploaded - count > 0 {
            batch.insert(
                bincode::serialize(&HerdStatus::Uploaded).unwrap(),
                bincode::serialize(&(num_uploaded - count)).unwrap(),
            );
        } else {
            batch.remove(bincode::serialize(&HerdStatus::Uploaded).unwrap());
        }
        // set the number of syncing files in the database
        batch.insert(
            bincode::serialize(&HerdStatus::Syncing).unwrap(),
            bincode::serialize(&(num_syncing + count)).unwrap(),
        );
        monitor_db
            .apply_batch(batch)
            .expect("Failed to apply batch");

        pb.finish_with_message(format!(
            "Processed {} items with {} failures in {:?}",
            count,
            failed,
            start.elapsed()
        ));
    }));

    // indexer thread for generating the manifest
    let manifest_db = db.clone();
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

        // if the manifest_root is set in the database, use that as the root for the manifest
        let mut manifest = match manifest_db.get("manifest_root") {
            Ok(Some(root)) => {
                let root: Vec<u8> = bincode::deserialize(&root).unwrap();
                mantaray::Manifest::new_manifest_reference(root, Box::new(ls.clone())).unwrap()
            }
            _ => mantaray::Manifest::new(Box::new(ls.clone()), false),
        };

        // consume the iterator
        while let Some((file, key)) = to_index.next().await {
            // add to the manifest
            // initial messy code to parse the prefix and ensure valid ascii path
            let mut url: String = String::from("http://bee.org/");
            url.push_str(&file.prefix);
            let prefix = Url::parse(&url).unwrap().path().to_string()[1..].to_string();
            manifest
                .add(
                    &prefix,
                    Entry {
                        reference: file.reference.as_ref().unwrap().clone(),
                        metadata: file.metadata.clone(),
                    },
                )
                .await
                .unwrap();
            tx.send(Ok((file, key))).await.unwrap();

            count += 1;

            // for every 4000 items, save the manifest to dump out the forks
            if count % 4000 == 0 {
                println!("Saving manifest at {}", count);
                manifest.store().await.unwrap();
                let ref_ = manifest.trie.ref_;

                // set the manifest root in the database
                manifest_db
                    .insert("manifest_root", bincode::serialize(&ref_).unwrap())
                    .unwrap();

                // reset the manifest
                manifest =
                    mantaray::Manifest::new_manifest_reference(ref_, Box::new(ls.clone())).unwrap();
            }
        }

        // set metadata
        let mut metadata = BTreeMap::new();
        metadata.insert(
            String::from("website-index-document"),
            String::from("index.html"),
        );

        manifest.set_root(metadata).await.unwrap();

        // save the manifest trie
        manifest.store().await.unwrap();
        let root = manifest.trie.ref_.clone();
        println!(
            "Manifest root uploaded at {:?} with monitoring on tag {}",
            hex::encode(&root),
            &manifest_tag.uid
        );

        println!("{}", manifest.trie.to_string());

        println!("Processed {} files", count);
    }));

    futures::future::join_all(handles).await;

    Ok(())
}

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    vec,
};

use bee_api::UploadConfig;
use indicatif::{ProgressBar, ProgressStyle};
use mantaray::{
    node::{Fork, Node},
    persist::BeeLoadSaver, Entry,
    // walker::walk_node,
};
use sled::Batch;
use tokio::{sync::mpsc::{self, Sender}, runtime::Builder};
use tokio_stream::StreamExt;
use url::Url;

use crate::{get_num, HerdFile, HerdStatus, Result, FILE_PREFIX};

pub async fn run(config: &crate::Manifest) -> Result<()> {
    // log the start time of the upload
    let start = std::time::Instant::now();
    let db = sled::open(&config.db).unwrap();

    // read current number of uploaded entries, and if there are no uploading files, return
    let num_uploaded = get_num(&db, HerdStatus::Uploaded);
    let num_url_processed = get_num(&db, HerdStatus::UrlProcessed);
    if (num_uploaded + num_url_processed) == 0 {
        println!("No files to index");
        return Ok(());
    }

    // process all prefixes to be valid prefixes
    url_to_ascii(&db);

    let mut handles = Vec::new();
    let mut parallel_handles = Vec::new();

    // read current number of url processed entries and syncing entries
    let num_url_processed = get_num(&db, HerdStatus::UrlProcessed);
    let num_syncing = get_num(&db, HerdStatus::Syncing);

    // create static variable to hold beeloadsaver
    let ls = Arc::new(BeeLoadSaver::new(
        config.bee_api_uri.clone(),
        bee_api::BeeConfig { upload: None },
    ));

    // check if there is a tag for the manifest, then setup the loadersaver
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

    // channel to send the number of files to be uploaded to the progress bar
    let (tx, rx) = mpsc::channel::<Result<(Batch, u64)>>(100);

    // a thread to monitor the progress of the indexer
    let monitor_db = db.clone();
    handles.push(tokio::spawn(async move {
        // convert rx to a ReceiverStream
        let mut rx = tokio_stream::wrappers::ReceiverStream::new(rx);

        let pb = ProgressBar::new(num_url_processed as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})").unwrap()
            .progress_chars("#>-"));

        pb.set_message("Building manifest");

        // consume the channel
        let mut count: u64 = 0;
        let mut failed_batches: u64 = 0;
        while let Some(result) = rx.next().await {
            if let Ok((mut batch, batch_count)) = result {
                count += batch_count;
                if num_url_processed - count > 0 {
                    batch.insert(
                        bincode::serialize(&HerdStatus::UrlProcessed).unwrap(),
                        bincode::serialize(&(num_url_processed - count)).unwrap(),
                    );
                } else {
                    batch.remove(bincode::serialize(&HerdStatus::UrlProcessed).unwrap());
                }

                // set the number of syncing files in the database
                batch.insert(
                    bincode::serialize(&HerdStatus::Syncing).unwrap(),
                    bincode::serialize(&(num_syncing + count)).unwrap(),
                );

                monitor_db.apply_batch(batch).unwrap();
                pb.inc(batch_count);
            } else {
                failed_batches += 1;
            }
        }

        let mut batch = sled::Batch::default();
        // set the number of uploaded files in the database
        if num_url_processed - count > 0 {
            batch.insert(
                bincode::serialize(&HerdStatus::UrlProcessed).unwrap(),
                bincode::serialize(&(num_url_processed - count)).unwrap(),
            );
        } else {
            batch.remove(bincode::serialize(&HerdStatus::UrlProcessed).unwrap());
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
            "Processed {} items with {} batch failures in {:?}",
            count,
            failed_batches,
            start.elapsed()
        ));
    }));

    let runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("herd")
        .build()
        .unwrap();

    // first use all the hints to process the prefixes
    for p in &config.parallel_prefixes {
        // iterate over the common prefixes and process them
        for c in prefixes(&db, p) {
            let db = db.clone();
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
            let tx = tx.clone();
            let p = p.clone();
            let mut prefix = p.clone();
            prefix.push(c.into());
            let batch_size = config.batch_size.clone();
            parallel_handles.push(runtime.spawn(async move {
                let root = indexer(&db, prefix.clone(), batch_size, ls, tx).await.unwrap();
                // println!("prefix: {} c: {} ref: {:?}", prefix, c, hex::encode(&root));
                (p, c, root)
            }));
        }
    }

    // wait for all the parallel prefixes to finish
    let mut parallel_results: HashMap<String, BTreeMap<u8, Vec<u8>>> = HashMap::new();
    for handle in parallel_handles {
        let (hint, common_prefix, ref_) = handle.await.unwrap();
        let map = parallel_results.entry(hint).or_insert(BTreeMap::new());
        map.insert(common_prefix, ref_);
    }

    // use a single thread to process the remaining prefixes
    let batch_size = config.batch_size.clone();
    handles.push(tokio::spawn(async move {
        let root = indexer(&db, "".to_string(), batch_size, ls.clone(), tx).await.unwrap();
        let mut manifest =
            mantaray::Manifest::new_manifest_reference(root, Box::new(ls.clone())).unwrap();

        // set metadata
        let mut metadata = BTreeMap::new();
        metadata.insert(
            String::from("website-index-document"),
            String::from("index.html"),
        );

        manifest.set_root(metadata).await.unwrap();

        // get the trie node for direct access
        let trie = &mut manifest.trie;

        // iterate over the remaining prefixes and process them
        for hint in parallel_results.keys() {
            // create the node (entry should be nil)
            trie.add(
                hint.as_bytes(),
                &Vec::new(),
                BTreeMap::new(),
                &mut Some(Box::new(ls.clone())),
            )
            .await
            .unwrap();
            let node = trie
                .lookup_node(hint.as_bytes(), &mut Some(Box::new(ls.clone())))
                .await
                .unwrap();

            let parallel_results_iter =
                tokio_stream::iter(parallel_results.get(hint).unwrap().iter());
            tokio::pin!(parallel_results_iter);

            while let Some((common_prefix, ref_)) = parallel_results_iter.next().await {
                // println!(
                //     "Attempting to add forks from root manifest {} for prefix {}",
                //     hex::encode(ref_),
                //     String::from_utf8(vec![*common_prefix]).unwrap()
                // );
                // lookup the node for the common prefix
                let mut n = Node::new_node_ref(ref_);
                n.load(&mut Some(Box::new(ls.clone()))).await.unwrap();
                n.entry = vec![];
                // println!("{}", n.to_string());

                node.forks.insert(
                    *common_prefix,
                    Fork {
                        prefix: vec![*common_prefix],
                        node: n,
                    },
                );
            }

            node.make_edge();
            node.make_not_value();
            // println!("{}", node.to_string());
        }

        // save the manifest trie
        manifest.store().await.unwrap();
        let root = manifest.trie.ref_.clone();
        println!(
            "Manifest root uploaded at {:?} with monitoring on tag {}\n\n\n",
            hex::encode(&root),
            &manifest_tag.uid
        );

        // walk_node(vec![], &mut Some(Box::new(ls.clone())), &mut manifest.trie);

        println!("{}", manifest.trie.to_string());
    }));

    // wait for all handles to finish
    for handle in handles {
        handle.await.unwrap();
    }

    Ok(())
}

async fn indexer(
    db: &sled::Db,
    prefix: String,
    batch_size: usize,
    ls: Arc<BeeLoadSaver>,
    tx: Sender<std::result::Result<(Batch, u64), Box<dyn std::error::Error + Send>>>,
) -> Result<Vec<u8>> {
    // create a manifest root key for this prefix
    let manifest_key = match prefix.as_str() == "" {
        false => format!("manifest_root_{}", prefix),
        true => "manifest_root".to_string(),
    };

    // if the manifest_root is set in the database, use that as the root for the manifest
    let mut manifest = match db.get(manifest_key.as_bytes()) {
        Ok(Some(root)) => {
            let root: Vec<u8> = bincode::deserialize(&root).unwrap();
            mantaray::Manifest::new_manifest_reference(root, Box::new(ls.clone())).unwrap()
        }
        _ => mantaray::Manifest::new(Box::new(ls.clone()), false),
    };

    let mut count = 0;
    let mut count_in_batch = 0;
    let mut batch = sled::Batch::default();
    let db_iter = tokio_stream::iter(db.scan_prefix(FILE_PREFIX.as_bytes()).filter(|item| {
        let (_, file) = item.as_ref().unwrap();
        let file: HerdFile = bincode::deserialize(file).unwrap();
        prefix.is_empty() || file.prefix.starts_with(&prefix)
    }));
    tokio::pin!(db_iter);
    while let Some(value) = db_iter.next().await {
        let (key, value) = value.expect("Failed to read database");
        let mut file: HerdFile = bincode::deserialize(&value).expect("Failed to deserialize");
        // TODO: Move status check to iter filter
        if file.status == HerdStatus::UrlProcessed {
            manifest
                .add(
                    &file.prefix[prefix.len()..],
                    Entry {
                        reference: file.reference.as_ref().unwrap().clone(),
                        metadata: file.metadata.clone(),
                    },
                )
                .await
                .unwrap();
            file.status = HerdStatus::Syncing;
            batch.insert(key, bincode::serialize(&file).unwrap());
            count += 1;
            count_in_batch += 1;
            if count % batch_size == 0 {
                manifest.store().await.unwrap();
                let ref_ = manifest.trie.ref_;
                manifest =
                    mantaray::Manifest::new_manifest_reference(ref_.clone(), Box::new(ls.clone()))
                        .unwrap();

                // set the manifest root in the database
                batch.insert(
                    bincode::serialize(&manifest_key).unwrap(),
                    bincode::serialize(&ref_).unwrap(),
                );
                tx.send(Ok((batch, count_in_batch))).await.unwrap();
                batch = sled::Batch::default();
                count_in_batch = 0;
            }
        }
    }

    manifest.store().await.unwrap();
    // println!("Processed {} items in root: {} for prefix: {}", count, hex::encode(&manifest.trie.ref_), prefix);

    // set the manifest root in the database
    batch.insert(
        bincode::serialize(&manifest_key).unwrap(),
        bincode::serialize(&manifest.trie.ref_).unwrap(),
    );
    tx.send(Ok((batch, count_in_batch))).await.unwrap();

    Ok(manifest.trie.ref_)
}

fn prefixes(db: &sled::Db, common: &String) -> Vec<u8> {
    // create a variable appending common to FILE_PREFIX

    let mut prefixes: HashMap<u8, u32> = HashMap::new();

    // iterate through the database and collect the first character of all file prefies excluding the common prefix
    let iter = db
        .scan_prefix(&FILE_PREFIX.as_bytes())
        .map(|item| {
            let (_, value) = item.expect("Failed to read database");
            let file: HerdFile = bincode::deserialize(&value).expect("Failed to deserialize");
            file
        })
        .filter(|file| file.prefix.starts_with(common));

    for f in iter {
        let first_char = f.prefix.as_bytes()[common.len()];
        let count = prefixes.entry(first_char as u8).or_insert(0);
        *count += 1;
    }

    // return the keys as a vector
    let mut keys: Vec<u8> = prefixes.keys().cloned().collect();
    keys.sort();

    keys
}

fn url_to_ascii(db: &sled::Db) {
    let db_iter = db
        .scan_prefix(&FILE_PREFIX.as_bytes())
        .map(|item| {
            let (key, value) = item.expect("Failed to read database");
            let file: HerdFile = bincode::deserialize(&value).expect("Failed to deserialize");
            (file, key)
        })
        .filter(|(file, _)| file.status == HerdStatus::Uploaded);
    let mut count = 0;
    let mut batch = sled::Batch::default();
    let num_uploaded = get_num(db, HerdStatus::Uploaded);
    let num_url_processed = get_num(db, HerdStatus::UrlProcessed);
    let pb = ProgressBar::new(num_uploaded);
    pb.set_style(ProgressStyle::default_bar()
        .template("{msg} {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})").unwrap()
        .progress_chars("#>-"));

    pb.set_message("Processing URLs");

    for (mut file, key) in db_iter {
        let mut url: String = String::from("http://bee.org/");
        url.push_str(&file.prefix);
        file.prefix = Url::parse(&url).unwrap().path().to_string()[1..].to_string();
        file.status = HerdStatus::UrlProcessed;
        batch.insert(key, bincode::serialize(&file).unwrap());
        count += 1;
        pb.inc(1);
        if count % 10000 == 0 {
            // set the number of uploaded files in the database
            if num_uploaded - count > 0 {
                batch.insert(
                    bincode::serialize(&HerdStatus::Uploaded).unwrap(),
                    bincode::serialize(&(num_uploaded - count)).unwrap(),
                );
            } else {
                batch.remove(bincode::serialize(&HerdStatus::Uploaded).unwrap());
            }

            // set the number of url processed files in the database
            batch.insert(
                bincode::serialize(&HerdStatus::UrlProcessed).unwrap(),
                bincode::serialize(&(num_url_processed + count)).unwrap(),
            );
            db.apply_batch(batch).unwrap();
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
    // set the number of url processed files in the database
    batch.insert(
        bincode::serialize(&HerdStatus::UrlProcessed).unwrap(),
        bincode::serialize(&(num_url_processed + count)).unwrap(),
    );
    db.apply_batch(batch).unwrap();

    pb.finish_with_message(format!("Processed {} URLs", count));
}

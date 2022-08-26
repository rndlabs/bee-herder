use std::collections::BTreeMap;

use crate::{get_num, HerdFile, HerdStatus, Import, Result};

pub async fn run(config: &Import) -> Result<()> {
    let db = sled::open(&config.db).expect("Unable to open database");
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
            .strip_prefix('/')
            .unwrap()
            .to_string();

        // print the file path
        println!("Importing {} as {}", file_path, prefix);

        let mut metadata = BTreeMap::new();
        let content_type = match file_path.split('.').last() {
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
            prefix,    // should be relative to the path specified in the config
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

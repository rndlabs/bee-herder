use std::{collections::HashMap, error::Error};
use reqwest::Client;
use serde::{Serialize, Deserialize};
use indicatif::{ProgressBar, ProgressStyle};
use tokio::stream;
use tokio_stream::StreamExt;

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

#[derive(Debug, Serialize, Deserialize)]
pub struct SwarmReference {
    pub reference: String,
}

pub struct SwarmUploadConfig {
    pub stamp: String,
    pub pin: Option<bool>,
    // pub encrypt: Option<String>,
    pub tag: Option<String>,
    pub deferred: Option<bool>,
}

// upload the data to the swarm
async fn upload(client: Client, data: Vec<u8>, config: SwarmUploadConfig) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut headers = reqwest::header::HeaderMap::new();
    // headers.insert("Content-Type", "application/octet-stream".parse().unwrap());

    // process the config
    headers.insert("swarm-postage-batch-id", config.stamp.parse().unwrap());
    if let Some(pin) = config.pin && pin {
        headers.insert("swarm-pin", "true".parse().unwrap());
    }
    if let Some(tag) = config.tag {
        headers.insert("swarm-tag", tag.parse().unwrap());
    }
    if let Some(deferred) = config.deferred && !deferred {
        headers.insert("swarm-deferred", "false".parse().unwrap());
    }

    let res = client
        .post(format!("{}/bytes", "http://localhost:1633"))
        .body(data)
        .headers(headers)
        .send()
        .await?;
    let reference = res.json::<SwarmReference>().await?;
    Ok(reference.reference.as_bytes().to_vec())
}

async fn list_to_be_completed() -> Result<(), Box<dyn Error>> {
    let client = reqwest::Client::new();

    let db = sled::open("/tmp/wiki/db").expect("Failed to open database");

    let db_iter = tokio_stream::iter(db.scan_prefix("f_".as_bytes()));

    tokio::pin!(db_iter);

    let mut items = db_iter
        .map(|item| {
            let (key, value) = item.expect("Failed to read database");
            let file: HerdFile = bincode::deserialize(&value).expect("Failed to deserialize");
            (file, key)
        })
        .filter(|(file, key)| file.status == HerdStatus::Pending);

    while let Some((file, key)) = items.next().await {
        // read the file from the local filesystem
        let root = "/tmp/wiki";
        let path = std::path::Path::new(root).join(String::from_utf8(file.path.clone()).unwrap());
        let data = std::fs::read(path).expect("Failed to read file");

        // upload the file to the swarm
        let hash = upload(client.clone(), data, SwarmUploadConfig {
            stamp: "0aec09d320b726a4a7d728a398eefb39939633e3e1badc136c45e956d48ab1c3".to_string(),
            pin: Some(true),
            tag: None,
            deferred: Some(true),
        }).await?;

        // update the database
        let mut file = file;
        file.status = HerdStatus::Uploaded;
        file.tag = Some(0);
        file.reference = Some(hash);
        db.insert(key, bincode::serialize(&file).unwrap()).expect("Failed to update database");

        println!("Uploaded file: {:?}", file);
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_works() {
        println!("Hello, world!");
        list_to_be_completed().await.unwrap();
    }
}
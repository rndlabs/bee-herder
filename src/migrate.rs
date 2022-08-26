use crate::{HerdStatus, FILE_PREFIX, Result, get_num, HerdFile, Migrate};

pub async fn run(config: &Migrate) -> Result<()> {
    // connect to the database
    let db = sled::open(&config.db).unwrap();

    // migration required - number of uploaded files wasn't record, let's fix that
    if get_num(&db, HerdStatus::Uploaded) == 0 {
        let mut num_uploaded = get_num(&db, HerdStatus::Uploaded);

        // get all files from the database that are of status uploaded
        let files = db
            .scan_prefix(FILE_PREFIX.as_bytes())
            .filter_map(|item| {
                let (key, value) = item.expect("Failed to read database");
                let file: HerdFile = bincode::deserialize(&value).expect("Failed to deserialize");
                if file.status == HerdStatus::Uploaded || (file.status == HerdStatus::Tagged && file.reference.is_some()) {
                    Some((file, key))
                } else {
                    None
                }
            });

        // count the number of uploaded files
        let mut batch = sled::Batch::default();
        // for all in files, increment the number of uploaded files
        for (count, (file, key)) in files.enumerate() {
            // set this file to uploaded status
            let mut file = file;
            file.status = HerdStatus::Uploaded;
            batch.insert(key, bincode::serialize(&file).unwrap());
            num_uploaded += 1;

            if count % 1000 == 0 {
                batch.insert(bincode::serialize(&HerdStatus::Uploaded).unwrap(), bincode::serialize(&num_uploaded).unwrap());
                db.apply_batch(batch).expect("Failed to apply batch");
                batch = sled::Batch::default();
            }
        }

        // write the num_uploaded to the database
        batch.insert(bincode::serialize(&HerdStatus::Uploaded).unwrap(), bincode::serialize(&num_uploaded).unwrap());
        db.apply_batch(batch).expect("Failed to apply batch");

        println!("Migrated {} files", num_uploaded);

    }
    Ok(())
}
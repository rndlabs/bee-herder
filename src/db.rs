use crate::{Db, Result, get_num, HerdStatus};
use strum::IntoEnumIterator;

pub async fn run(config: &Db) -> Result<()> {
    let db = sled::open(&config.db).unwrap();

    // get the number of entries in the database
    let count = db.len();

    println!("Entries in the database: {}", count);

    for status in HerdStatus::iter() {
        let num = get_num(&db, status.clone());
        println!("{:?}: {}", status.to_string(), num);
    }

    Ok(())
}
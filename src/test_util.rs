use std::env::temp_dir;
use std::path::PathBuf;
use rand::random;
use crate::db::{CheckMode, DB, DBBuilder};
use crate::tx::{TX, TxBuilder};


#[cfg(test)]
pub(crate) fn mock_tx() -> TX {
    let tx = TxBuilder::new().set_writable(true).build();
    tx.0.meta.write().pg_id = 1;
    tx
}

#[cfg(test)]
pub(crate) fn mock_db() -> DBBuilder {
    DBBuilder::new(temp_file()).set_auto_remove(true).set_read_only(true).set_check_mode(CheckMode::PARANOID)
}

#[cfg(test)]
pub(crate) fn temp_file() -> PathBuf {
    temp_dir().join(format!("{}.boltdb.db", rand::random::<u32>()))
}


#[test]
fn open() {
    let db = mock_db().build().unwrap();
}
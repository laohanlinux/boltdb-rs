use crate::db::{CheckMode, DBBuilder, DB, MAGIC};
use crate::tx::{TxBuilder, TX};
use rand::random;
use std::env::temp_dir;
use std::path::PathBuf;

#[cfg(test)]
pub(crate) fn mock_tx() -> TX {
    mock_log();
    let tx = TxBuilder::new().set_writable(true).build();
    tx.0.meta.write().pg_id = 1;
    tx
}

#[cfg(test)]
pub(crate) fn mock_db() -> DBBuilder {
    mock_log();
    DBBuilder::new(temp_file())
        .set_auto_remove(true)
        .set_read_only(false)
        .set_check_mode(CheckMode::PARANOID)
}

#[cfg(test)]
pub(crate) fn temp_file() -> PathBuf {
    temp_dir().join(format!("{}.boltdb.db", rand::random::<u64>()))
}

#[cfg(test)]
pub(crate) fn mock_log() {
    use env_logger::Env;
    let env = Env::default()
        .filter_or("MY_LOG_LEVEL", "debug")
        .write_style_or("MY_LOG_STYLE", "always");
    env_logger::try_init_from_env(env);
}

#[test]
fn open() {
    let db = mock_db().build().unwrap();
    assert_eq!(db.page_size(), page_size::get());
    assert_eq!(db.meta().version, 2);
    assert_eq!(db.meta().magic, MAGIC);
    assert_eq!(db.meta().root.root, 3);
}

#[test]
fn open_existing() {
    mock_log();
    let db = DBBuilder::new("./test_data/remark.db")
        .set_read_only(true)
        .build()
        .unwrap();
    assert_eq!(db.page_size(), page_size::get());
    assert_eq!(db.meta().version, 2);
    assert_eq!(db.meta().magic, MAGIC);
    assert_eq!(db.meta().root.root, 9);
    {
        let tx = db.begin_tx().unwrap();
        let buckets = tx.buckets();
        assert_eq!(buckets.len(), 7);
        tx.check_sync().unwrap();
    }
}

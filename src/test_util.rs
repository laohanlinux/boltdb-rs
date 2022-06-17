use crate::bucket::Bucket;
use crate::db::{CheckMode, DBBuilder, DB, MAGIC};
use crate::node::{Node, NodeBuilder};
use crate::page::PAGE_HEADER_SIZE;
use crate::tx::WeakTx;
use crate::tx::{TxBuilder, TX};
use crate::Page;
use bitflags::_core::borrow::Borrow;
use log::{info, kv::source::as_map, kv::Source};
use rand::random;
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::collections::HashMap;
use std::env::temp_dir;
use std::fmt::{format, Pointer};
use std::path::PathBuf;
use std::ptr::NonNull;

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
pub(crate) fn mock_bucket(tx: WeakTx) -> Bucket {
    Bucket::new(tx)
}

#[cfg(test)]
pub(crate) fn mock_node(bucket: *const Bucket) -> Node {
    NodeBuilder::new(bucket).build()
}

#[cfg(test)]
pub(crate) fn mock_db2(str: String) -> DBBuilder {
    use std::fs::remove_file;

    mock_log();
    remove_file(&str);
    DBBuilder::new(str)
        .set_read_only(false)
        .set_check_mode(CheckMode::PARANOID)
}

#[cfg(test)]
pub(crate) fn temp_file() -> PathBuf {
    temp_dir().join(format!("{}.boltdb.db", rand::random::<u64>()))
}

#[cfg(test)]
pub(crate) fn mock_json_log() {}


#[cfg(test)]
pub(crate) fn color_log_level<'a>(style: &'a mut env_logger::fmt::Style, level: log::Level ) -> env_logger::fmt::StyledValue<'a, &'static str> {
    use env_logger::fmt::{Color, Style};
    use log::{Level::Trace, Level::Warn, Level::Info, Level::Debug, Level::Error};
    match level {
        Trace => style.set_color(Color::Magenta).value("TRACE"),
        Warn => style.set_color(Color::Blue).value("WARN"),
        Info => style.set_color(Color::Green).value("INFO"),
        Debug => style.set_color(Color::Yellow).value("DEBUG"),
        Error => style.set_color(Color::Red).value("ERROR"),
    }
}

#[cfg(test)]
pub(crate) fn mock_log() {
    use chrono::Local;
    use env_logger::Env;
    use log::kv::source::AsMap;
    use log::kv::{Error, Key, ToKey, ToValue, Value};
    use serde::{Deserialize, Serialize};
    use std::io::Write;

    #[derive(Serialize, Deserialize)]
    struct JsonLog {
        level: log::Level,
        ts: String,
        module: String,
        msg: String,
        #[serde(skip_serializing_if = "HashMap::is_empty", flatten)]
        kv: HashMap<String, serde_json::Value>,
    }

    let env = Env::default()
        .filter_or("MY_LOG_LEVEL", "debug")
        .write_style_or("MY_LOG_STYLE", "always");
    let _ = env_logger::Builder::from_env(env)
        .format(|buf, record| {

            let mut l = JsonLog {
                ts: Local::now().format("%Y-%m-%dT%H:%M:%S").to_string(),
                module: record.file().unwrap_or("unknown").to_string()
                    + ":"
                    + &*record.line().unwrap_or(0).to_string(),
                level: record.level(),
                msg: record.args().to_string(),
                kv: Default::default(),
            };
            let kv: AsMap<&dyn Source> = as_map(record.key_values());
            if let Ok(kv) = serde_json::to_string(&kv) {
                let h: HashMap<String, serde_json::Value> = serde_json::from_str(&kv).unwrap();
                l.kv.extend(h.into_iter());
            }
            writeln!(buf, "{}", serde_json::to_string(&l).unwrap())
        })
        .try_init();
    log::info!( is_ok = true; "start init log");
    // env_logger::try_init_from_env(env);
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
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
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

#[test]
fn panic_while_update() {
    use kv_log_macro::info;
    let mut db = mock_db().build().unwrap();

    db.update(|tx| {
        info!("ready to create a bucket");
        let _ = tx.create_bucket(b"bucket").unwrap();
        info!("has created a new bucket");
        Ok(())
    })
    .unwrap();
}

fn insert(h: &mut HashMap<i32, Vec<u8>>) -> *mut Page {
    let mut v = vec![0u8; PAGE_HEADER_SIZE];
    let page = unsafe { v.as_mut_ptr() as *mut Page };
    h.insert(1, v);
    page
}

#[test]
fn double_ref() {
    let mut h = HashMap::default();
    let mut page = insert(&mut h);
    let page = unsafe { &mut *page };
    let v1 = h.get(&1).unwrap().clone();
    page.id = 200;
    let v2 = h.get(&1).unwrap().clone();
    assert_ne!(v1, v2);
}

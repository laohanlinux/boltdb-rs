use memmap::MmapOptions;
use std::fs::File;

use crate::db::DB;

pub fn mmap(mut db: DB, mmap_size: usize) -> ::std::io::Result<DB> {
    // let file = File::open(db.path())?;
    // let mut opt = MmapOptions::new();
    // db.0.mmap = RwLock::new(unsafe { opt.map_exec(&file) }?);
    // db.0.mmap_size = Mutex::new(mmap_size);
    // Ok(db)
    Ok(db)
}

#[test]
fn it_works() {
    let file = File::open("Cargo.toml").unwrap();
    let mut _opt = MmapOptions::new();
    let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
    println!("{:#?}", String::from_utf8_lossy(&mmap[0..199]));
}

use std::ffi::CString;

use leveldb_sys::*;

fn open(path: &str) -> *mut leveldb_t {
    let options = unsafe { leveldb_options_create() };
    let value = 1;
    unsafe { leveldb_options_set_create_if_missing(options, value) };
    let name = CString::new(path).unwrap();
    let mut errdata = core::ptr::null_mut();
    let db = unsafe { leveldb_open(options, name.as_ptr(), &mut errdata) };
    if db == core::ptr::null_mut() {
        let errmsg = unsafe { CString::from_raw(errdata) };
        panic!("{:?}", errmsg);
    }

    db
}

fn close(db: *mut leveldb_t) {
    unsafe { leveldb_close(db) };
}

fn main() {
    let db = open("src/data.db");
    close(db);
}

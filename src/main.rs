use std::{ffi::CString, ptr::NonNull};

use leveldb_sys::*;

struct LdbOptions {
    handle: std::ptr::NonNull<leveldb_options_t>,
}

impl LdbOptions {
    fn new() -> Self {
        let handle = unsafe { leveldb_options_create() };
        unsafe { leveldb_options_set_create_if_missing(handle, 1) };
        Self {
            handle: NonNull::new(handle).unwrap(),
        }
    }
}

impl Drop for LdbOptions {
    fn drop(&mut self) {
        unsafe { leveldb_options_destroy(self.handle.as_ptr()) };
    }
}

struct Ldb {
    handle: std::ptr::NonNull<leveldb_t>,
    path: CString,
}

impl Ldb {
    fn new(path: &str, options: &LdbOptions) -> Self {
        let path = CString::new(path).unwrap();
        let mut errdata = core::ptr::null_mut();
        let db = unsafe { leveldb_open(options.handle.as_ptr(), path.as_ptr(), &mut errdata) };
        if db == core::ptr::null_mut() {
            let errmsg = unsafe { CString::from_raw(errdata) };
            panic!("{:?}", errmsg);
        }
        println!("LevelDB {:?} opened", path);

        Self {
            handle: NonNull::new(db).unwrap(),
            path,
        }
    }

    fn open_or_create(path: &str) -> Self {
        Self::new(path, &LdbOptions::new())
    }
}

impl Drop for Ldb {
    fn drop(&mut self) {
        unsafe { leveldb_close(self.handle.as_ptr()) };
        println!("LevelDB {:?} closed", self.path);
    }
}

fn main() {
    Ldb::open_or_create("src/data.db");
}

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
    // TODO Use Cell, for interior mutability when reading
    errptr: *mut i8,
}

impl Ldb {
    fn new(path: &str, options: &LdbOptions) -> Self {
        let path = CString::new(path).unwrap();
        let mut errptr = core::ptr::null_mut();
        let db = unsafe { leveldb_open(options.handle.as_ptr(), path.as_ptr(), &mut errptr) };
        if db == core::ptr::null_mut() {
            Self::check(errptr);
        }
        println!("LevelDB {:?} opened", path);

        Self {
            handle: NonNull::new(db).unwrap(),
            path,
            errptr,
        }
    }

    fn check(errptr: *mut i8) {
        if errptr != core::ptr::null_mut() {
            let errmsg = unsafe { CString::from_raw(errptr) };
            panic!("{:?}", errmsg);
        }
    }

    fn check_error(&self) {
        Self::check(self.errptr);
    }

    fn write(&mut self, key: &str, val: &str) {
        let options = unsafe { leveldb_writeoptions_create() };
        let batch = unsafe { leveldb_writebatch_create() };
        let keylen = key.len();
        let vallen = val.len();
        let ckey = CString::new(key).unwrap();
        let cval = CString::new(val).unwrap();
        unsafe { leveldb_writebatch_put(batch, ckey.as_ptr(), keylen, cval.as_ptr(), vallen) };

        unsafe { leveldb_write(self.handle.as_ptr(), options, batch, &mut self.errptr) };
        self.check_error();

        println!("Written ({:?}, {:?})", key, val);
    }

    fn read(&mut self, key: &str) -> Result<String, ()> {
        let options = unsafe { leveldb_readoptions_create() };

        let keylen = key.len();
        let mut vallen = 0;
        let ckey = CString::new(key).unwrap();

        let valptr = unsafe {
            leveldb_get(
                self.handle.as_ptr(),
                options,
                ckey.as_ptr(),
                keylen,
                &mut vallen,
                &mut self.errptr,
            )
        };
        self.check_error();

        if vallen == 0 {
            return Err(());
        }

        let val = unsafe { String::from_raw_parts(valptr as _, vallen, vallen) };
        println!("Looking up {:?} -> {:?}", key, val);
        Ok(val)
    }
}

impl Drop for Ldb {
    fn drop(&mut self) {
        unsafe { leveldb_close(self.handle.as_ptr()) };
        println!("LevelDB {:?} closed", self.path);
    }
}

struct Database {
    ldb: Ldb,
}

impl Database {
    fn open_or_create(path: &str) -> Self {
        Self {
            ldb: Ldb::new(path, &LdbOptions::new()),
        }
    }

    fn write(&mut self, key: &str, val: &str) {
        self.ldb.write(key, val);
    }

    fn read(&mut self, key: &str) -> Result<String, ()> {
        self.ldb.read(key)
    }
}

#[test]
fn create_write_read() {
    let mut db = Database::open_or_create("src/data.db");
    db.write("key", "val");
    assert!(db.read("key").unwrap() == "val");
    assert!(db.read("wrong").is_err());
}
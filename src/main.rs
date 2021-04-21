use rocksdb::{DB, ColumnFamilyDescriptor, Options};

fn main() {
    // NB: db is automatically closed at end of lifetime
    let path = "rocks.db";
    let mut cf_opts = Options::default();
    cf_opts.set_max_write_buffer_number(16);
    let cf = ColumnFamilyDescriptor::new("cf1", cf_opts);

    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);

    {
    let mut db = DB::open_cf_descriptors(&db_opts, path, vec![cf]).unwrap();
    let ocf1=db.cf_handle("cf2");
    let cf1=if ocf1.is_none(){
        db.create_cf("cf2", &Options::default()).unwrap();
        db.cf_handle("cf2").unwrap()
    } else {
        ocf1.unwrap()
    };
    db.put_cf(cf1, b"my key", b"my value").unwrap();
    match db.get_cf(cf1,b"my key") {
        Ok(Some(value)) => println!("retrieved value {}", String::from_utf8(value).unwrap()),
        Ok(None) => println!("value not found"),
        Err(e) => println!("operational problem encountered: {}", e),
    }
    db.delete_cf(cf1, b"my key").unwrap();
    }
    let _ = DB::destroy(&Options::default(), path);
}

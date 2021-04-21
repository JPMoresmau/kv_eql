use rocksdb::{DB, Options, Error};
use serde_json::json;
use rust_eql::{RocksDBMetadata, Operation};
use std::{collections::{HashMap, HashSet}, iter, slice};
use serde_json::Value;

#[test]
fn test_basic() -> Result<(),Error>{
    let path = "test_basic.db";
    {
        let db = DB::open_default(path)?;
        let mut meta=RocksDBMetadata::new(db);
        let john = json!({
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        });
        
        meta.insert("type1", b"key1", &john)?;
        
        let ov=meta.get("type1", b"key1")?;
        assert_eq!(ov,Some(john));

        meta.delete("type1", b"key1")?;
        let ov=meta.get("type1", b"key1")?;
        assert_eq!(ov,None);
    }
    let _ = DB::destroy(&Options::default(), path);
    Ok(())
}

#[test]
fn test_scan() -> Result<(),Error>{
    let path = "test_scan.db";
    {
        let db = DB::open_default(path)?;
        let mut meta=RocksDBMetadata::new(db);
        let john = json!({
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        });
        
        meta.insert("type1", b"key1", &john)?;

        let mary = json!({
            "name": "Mary Doe",
            "age": 34
        });
        
        meta.insert("type1", b"key2", &mary)?;

        //let v1=vec![(b"key1",&john),(b"key2",&mary)];
        let v1:Vec<(Box<[u8]>,Value)>=meta.execute(Operation::scan("type1")).collect();
        assert_eq!(2,v1.len());
        assert_eq!(*b"key1",*v1[0].0);
        assert_eq!(*b"key2",*v1[1].0);
        assert_eq!(john,v1[0].1);
        assert_eq!(mary,v1[1].1);
        
        let john2 = json!({
            "name": "John Doe",
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        });
        
       
        let mary2 = json!({
            "name": "Mary Doe",
        });

        let v1:Vec<(Box<[u8]>,Value)>=meta.execute(Operation::extract(&vec!["name","phones"],Operation::scan("type1"))).collect();
        assert_eq!(2,v1.len());
        assert_eq!(*b"key1",*v1[0].0);
        assert_eq!(*b"key2",*v1[1].0);
        assert_eq!(john2,v1[0].1);
        assert_eq!(mary2,v1[1].1);
        

    }
    let _ = DB::destroy(&Options::default(), path);
    Ok(())
}
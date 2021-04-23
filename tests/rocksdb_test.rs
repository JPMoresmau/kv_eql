use serde_json::json;
use rust_eql::{RocksDBEQL, Operation};
use serde_json::Value;
use anyhow::Result;

#[test]
fn test_basic() -> Result<()>{
    let path = "test_basic.db";
    {
        let mut meta=RocksDBEQL::open(path)?;
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
    RocksDBEQL::destroy( path)?;
    Ok(())
}

#[test]
fn test_scan() -> Result<()>{
    let path = "test_scan.db";
    {
        let mut meta=RocksDBEQL::open(path)?;
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

        let v1:Vec<(Box<[u8]>,Value)>=meta.execute(Operation::extract(&["name","phones"],Operation::scan("type1"))).collect();
        assert_eq!(2,v1.len());
        assert_eq!(*b"key1",*v1[0].0);
        assert_eq!(*b"key2",*v1[1].0);
        assert_eq!(john2,v1[0].1);
        assert_eq!(mary2,v1[1].1);

    }
    RocksDBEQL::destroy( path)?;
    Ok(())
}

#[test]
fn text_index_metadata() -> Result<()> {
    let path = "text_index_metadata.db";
    {
        let mut eql=RocksDBEQL::open(path)?;
        eql.add_index("type1", "idx1", vec!["name"])?;
        let md=eql.metadata;
        assert_eq!(true, md.indices.contains_key("type1"));
        let m2=md.indices.get("type1").unwrap();
        assert_eq!(true, m2.contains_key("idx1"));
        let on=m2.get("idx1").unwrap();
        assert_eq!(1,on.len());
        assert_eq!("name",on[0]);
    }
    {
        let eql=RocksDBEQL::open(path)?;
        let md=eql.metadata;
        assert_eq!(true, md.indices.contains_key("type1"));
        let m2=md.indices.get("type1").unwrap();
        assert_eq!(true, m2.contains_key("idx1"));
        let on=m2.get("idx1").unwrap();
        assert_eq!(1,on.len());
        assert_eq!("name",on[0]);
    }
    RocksDBEQL::destroy( path)?;
    Ok(())
}
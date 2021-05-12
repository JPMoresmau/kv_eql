use anyhow::Result;
use kv_eql::{index_lookup_keys, scan, EQLRecord, EQLDB};
use serde_json::json;

#[test]
fn test_space_type() -> Result<()> {
    let path = "test_space_type.db";
    {
        let mut eql = EQLDB::open(path)?;
        let md = &eql.metadata;
        assert_eq!(true, md.indices.is_empty());

        let john = json!({
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        });

        eql.insert("Customer Details", "key1", &john)?;
        let md = &eql.metadata;
        assert_eq!(true, md.indices.contains_key("Customer Details"));
        let m2 = md.indices.get("Customer Details").unwrap();
        assert_eq!(true, m2.is_empty());

        let ov = eql.get("Customer Details", "key1")?;
        assert_eq!(ov, Some(john));

        eql.delete("Customer Details", "key1")?;
        let ov = eql.get("Customer Details", "key1")?;
        assert_eq!(ov, None);
    }
    {
        let mut eql = EQLDB::open(path)?;
        let ov = eql.get("Customer Details", "key1")?;
        assert_eq!(ov, None);
    }
    EQLDB::destroy(path)?;
    Ok(())
}

#[test]
fn test_scan_unknown() -> Result<()> {
    let path = "test_scan_unknown.db";
    {
        let eql = EQLDB::open(path)?;

        let v1: Vec<EQLRecord> = eql.execute(scan("type1"))?.collect();
        assert_eq!(0, v1.len());
    }
    EQLDB::destroy(path)?;
    Ok(())
}

#[test]
fn test_index_lookup_unknown() -> Result<()> {
    let path = "test_index_lookup_unknown.db";
    {
        let mut eql = EQLDB::open(path)?;
        let john = json!({
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        });
        eql.insert("type1", "key1", &john)?;

        let mary = json!({
            "name": "Mary Doe",
            "age": 34
        });

        eql.insert("type1", "key2", &mary)?;

        let v1: Vec<EQLRecord> = eql
            .execute(index_lookup_keys(
                "type1",
                "idx1",
                vec![json!("John Doe")],
                vec!["nameix", "ageix"],
            ))?
            .collect();
        assert_eq!(0, v1.len());
    }
    EQLDB::destroy(path)?;
    Ok(())
}

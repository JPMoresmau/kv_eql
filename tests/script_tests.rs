use anyhow::Result;
use kv_eql::{
   EQLRecord, EQLDB,
};
use serde_json::json;
use serde_json::Value;

#[test]
fn test_scan() -> Result<()> {
    let path = "test_scan.db";
    {
        let mut meta = EQLDB::open(path)?;
        let mary = json!({
            "name": "Mary Doe",
            "age": 34
        });

        meta.insert("type1", "key2", &mary)?;

        let john = json!({
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        });

        meta.insert("type1", "key1", &john)?;

        //let v1=vec![(b"key1",&john),(b"key2",&mary)];
        let v1: Vec<EQLRecord> = meta.execute_script("scan(type1)")?.collect();
        assert_eq!(2, v1.len());
        assert_eq!(Value::from("key1"), v1[0].key);
        assert_eq!(Value::from("key2"), v1[1].key);
        assert_eq!(john, v1[0].value);
        assert_eq!(mary, v1[1].value);

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

        let v1: Vec<EQLRecord> = meta
            .execute_script("extract([\"name\", \"phones\"], scan(type1))")?
            .collect();
        assert_eq!(2, v1.len());
        assert_eq!(Value::from("key1"), v1[0].key);
        assert_eq!(Value::from("key2"), v1[1].key);
        assert_eq!(john2, v1[0].value);
        assert_eq!(mary2, v1[1].value);
    }
    EQLDB::destroy(path)?;
    Ok(())
}

#[test]
fn test_lookup() -> Result<()> {
    let path = "test_lookup.db";
    {
        let mut meta = EQLDB::open(path)?;
        let john = json!({
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        });

        meta.insert("type1", "key1", &john)?;

        let mary = json!({
            "name": "Mary Doe",
            "age": 34
        });

        meta.insert("type1", "key2", &mary)?;

        //let v1=vec![(b"key1",&john),(b"key2",&mary)];
        let v1: Vec<EQLRecord> = meta
            .execute_script("key_lookup(type1, \"key1\")")?
            .collect();
        assert_eq!(1, v1.len());
        assert_eq!(Value::from("key1"), v1[0].key);
        assert_eq!(john, v1[0].value);

        let mary2 = json!({
            "name": "Mary Doe",
        });

        let v1: Vec<EQLRecord> = meta
            .execute_script("extract(
                [name, phones],
                key_lookup(type1, \"key2\"))")?
            .collect();
        assert_eq!(1, v1.len());
        assert_eq!(Value::from("key2"), v1[0].key);
        assert_eq!(mary2, v1[0].value);
    }
    EQLDB::destroy(path)?;
    Ok(())
}

#[test]
fn test_index_lookup() -> Result<()> {
    let path = "test_index_lookup.db";
    {
        let mut eql = EQLDB::open(path)?;
        eql.add_index("type1", "idx1", vec!["/name", "/age"])?;
        let john = json!({
            "name": "John Doe",
            "age": 43.0,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        });
        eql.insert("type1", "key1", &john)?;

        let mary = json!({
            "name": "Mary Doe",
            "age": 34.0
        });

        eql.insert("type1", "key2", &mary)?;

        let john2 = json!({
            "nameix": "John Doe",
            "ageix": 43.0,
        });
        let mary2 = json!({
            "nameix": "Mary Doe",
            "ageix": 34.0,
        });

        let mary3 = json!({
            "ageix": 34.0,
        });

        let v1: Vec<EQLRecord> = eql
            .execute_script("index_lookup(
                type1,
                idx1,
                [\"John Doe\"],
                [nameix, ageix]
            )")?
            .collect();
        assert_eq!(1, v1.len());
        assert_eq!(Value::from("key1"), v1[0].key);
        assert_eq!(john2, v1[0].value);

        let v1: Vec<EQLRecord> = eql
            .execute_script("index_lookup(
                type1,
                idx1,
                [\"John Doe\", 43],
                [nameix, ageix]
            )")?
            .collect();
        assert_eq!(1, v1.len());
        assert_eq!(Value::from("key1"), v1[0].key);
        assert_eq!(john2, v1[0].value);

        let v1: Vec<EQLRecord> = eql
            .execute_script("index_lookup(
                type1,
                idx1,
                [\"John Doe\", 34],
                [nameix, ageix]
            )")?
            .collect();
        assert_eq!(0, v1.len());

        let v1: Vec<EQLRecord> = eql
            .execute_script("index_lookup(
                type1,
                idx1,
                [\"Mary Doe\"],
                [nameix, ageix]
            )")?
            .collect();
        assert_eq!(1, v1.len());
        assert_eq!(Value::from("key2"), v1[0].key);
        assert_eq!(mary2, v1[0].value);

        let v1: Vec<EQLRecord> = eql
            .execute_script("index_lookup(
                type1,
                idx1,
                [\"Mary Doe\"],
                [\"\", ageix]
            )")?
            .collect();
        assert_eq!(1, v1.len());
        assert_eq!(Value::from("key2"), v1[0].key);
        assert_eq!(mary3, v1[0].value);

        let v1: Vec<EQLRecord> = eql
            .execute_script("index_lookup(
                type1,
                idx1,
                [\"John Deer\", 43],
                [nameix, ageix]
            )")?
            .collect();
        assert_eq!(0, v1.len());

        let v1: Vec<EQLRecord> = eql
            .execute_script("index_lookup(
                type1,
                idx1,
                [],
                [nameix, ageix]
            )")?
            .collect();
        assert_eq!(2, v1.len());
        assert_eq!(Value::from("key1"), v1[0].key);
        assert_eq!(john2, v1[0].value);
        assert_eq!(Value::from("key2"), v1[1].key);
        assert_eq!(mary2, v1[1].value);

        eql.delete("type1", "key1")?;
        let v1: Vec<EQLRecord> = eql
            .execute_script("index_lookup(
                type1,
                idx1,
                [\"John Doe\"],
                [nameix, ageix]
            )")?
            .collect();
        assert_eq!(0, v1.len());
    }
    EQLDB::destroy(path)?;
    Ok(())
}

#[test]
fn test_index_nested_loops() -> Result<()> {
    let path = "test_index_nested_loops.db";
    {
        let mut eql = EQLDB::open(path)?;
        eql.add_index("type1", "idx1", vec!["/name", "/age"])?;
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

        let john2 = json!({
            "name": "John Doe",
            "age": 43,
            "ageix": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        });

        let v1: Vec<EQLRecord> = eql
            .execute_script("nested_loops(
                index_lookup(type1, idx1, [\"John Doe\"]),
                #\"key_lookup(\"type1\", rec.key)\"#
            )")?
            .collect();
        assert_eq!(1, v1.len());
        assert_eq!(Value::from("key1"), v1[0].key);
        assert_eq!(john, v1[0].value);

        let v1: Vec<EQLRecord> = eql
            .execute_script("nested_loops(
                index_lookup(type1, idx1, [\"John Doe\"], [\"\", \"ageix\"]),
                    #\"augment(
                        rec.value,
                        key_lookup(\"type1\", rec.key)
                    )\"#
            )")?
            .collect();
        assert_eq!(1, v1.len());
        assert_eq!(Value::from("key1"), v1[0].key);
        assert_eq!(john2, v1[0].value);
    }
    EQLDB::destroy(path)?;
    Ok(())
}

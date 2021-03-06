use std::iter;

use anyhow::Result;
use kv_eql::{
    augment, extract, hash_join, index_lookup, index_lookup_keys, key_lookup, merge,
    nested_loops, process, scan, EQLBatch, EQLRecord, RecordExtract, EQLDB,
};
use serde_json::json;
use serde_json::Value;

mod common;
use common::write_northwind_data;

#[test]
fn test_basic() -> Result<()> {
    let path = "test_basic.db";
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

        eql.insert("type1", "key1", &john)?;
        let md = &eql.metadata;
        assert_eq!(true, md.indices.contains_key("type1"));
        let m2 = md.indices.get("type1").unwrap();
        assert_eq!(true, m2.is_empty());

        let ov = eql.get("type1", "key1")?;
        assert_eq!(ov, Some(john));

        eql.delete("type1", "key1")?;
        let ov = eql.get("type1", "key1")?;
        assert_eq!(ov, None);
    }
    {
        let mut eql = EQLDB::open(path)?;
        let ov = eql.get("type1", "key1")?;
        assert_eq!(ov, None);
    }
    EQLDB::destroy(path)?;
    Ok(())
}

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
        let v1: Vec<EQLRecord> = meta.execute(scan("type1"))?.collect();
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
            .execute(extract(&["name", "phones"], scan("type1")))?
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
        let k1=Value::from("key1");
        //let v1=vec![(b"key1",&john),(b"key2",&mary)];
        let mut v1: Vec<EQLRecord> = meta
            .execute(key_lookup("type1", &k1))?
            .collect();
        assert_eq!(1, v1.len());
        assert_eq!(Value::from("key1"), v1[0].key);
        assert_eq!(john, v1[0].value);

        v1 = meta
            .execute(key_lookup("type1", k1))?
            .collect();
        assert_eq!(1, v1.len());
        assert_eq!(Value::from("key1"), v1[0].key);
        assert_eq!(john, v1[0].value);

        let mary2 = json!({
            "name": "Mary Doe",
        });

        let v1: Vec<EQLRecord> = meta
            .execute(extract(
                &["name", "phones"],
                key_lookup("type1", Value::from("key2")),
            ))?
            .collect();
        assert_eq!(1, v1.len());
        assert_eq!(Value::from("key2"), v1[0].key);
        assert_eq!(mary2, v1[0].value);
    }
    EQLDB::destroy(path)?;
    Ok(())
}

#[test]
fn test_index_metadata() -> Result<()> {
    let path = "test_index_metadata.db";
    {
        let mut eql = EQLDB::open(path)?;
        eql.add_index("type1", "idx1", vec!["/name"])?;
        let md = &eql.metadata;
        assert_eq!(true, md.indices.contains_key("type1"));
        let m2 = md.indices.get("type1").unwrap();
        assert_eq!(true, m2.contains_key("idx1"));
        let on = m2.get("idx1").unwrap();
        assert_eq!(1, on.len());
        assert_eq!("/name", on[0]);
    }
    {
        let mut eql = EQLDB::open(path)?;
        let md = &eql.metadata;
        assert_eq!(true, md.indices.contains_key("type1"));
        let m2 = md.indices.get("type1").unwrap();
        assert_eq!(true, m2.contains_key("idx1"));
        let on = m2.get("idx1").unwrap();
        assert_eq!(1, on.len());
        assert_eq!("/name", on[0]);
        eql.delete_index("type1", "idx1")?;
        let md = &eql.metadata;
        assert_eq!(true, md.indices.contains_key("type1"));
        let m2 = md.indices.get("type1").unwrap();
        assert_eq!(true, m2.is_empty());
    }
    {
        let eql = EQLDB::open(path)?;
        let md = eql.metadata;
        assert_eq!(true, md.indices.contains_key("type1"));
        let m2 = md.indices.get("type1").unwrap();
        assert_eq!(true, m2.is_empty());
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
            "nameix": "John Doe",
            "ageix": 43,
        });
        let mary2 = json!({
            "nameix": "Mary Doe",
            "ageix": 34,
        });

        let mary3 = json!({
            "ageix": 34,
        });

        let v1: Vec<EQLRecord> = eql
            .execute(index_lookup_keys(
                "type1",
                "idx1",
                vec![json!("John Doe")],
                vec!["nameix", "ageix"],
            ))?
            .collect();
        assert_eq!(1, v1.len());
        assert_eq!(Value::from("key1"), v1[0].key);
        assert_eq!(john2, v1[0].value);

        let v1: Vec<EQLRecord> = eql
            .execute(index_lookup_keys(
                "type1",
                "idx1",
                vec![json!("John Doe"), json!(43)],
                vec!["nameix", "ageix"],
            ))?
            .collect();
        assert_eq!(1, v1.len());
        assert_eq!(Value::from("key1"), v1[0].key);
        assert_eq!(john2, v1[0].value);

        let v1: Vec<EQLRecord> = eql
            .execute(index_lookup_keys(
                "type1",
                "idx1",
                vec![json!("John Doe"), json!(34)],
                vec!["nameix", "ageix"],
            ))?
            .collect();
        assert_eq!(0, v1.len());

        let v1: Vec<EQLRecord> = eql
            .execute(index_lookup_keys(
                "type1",
                "idx1",
                vec![json!("Mary Doe")],
                vec!["nameix", "ageix"],
            ))?
            .collect();
        assert_eq!(1, v1.len());
        assert_eq!(Value::from("key2"), v1[0].key);
        assert_eq!(mary2, v1[0].value);

        let v1: Vec<EQLRecord> = eql
            .execute(index_lookup_keys(
                "type1",
                "idx1",
                vec![json!("Mary Doe")],
                vec!["", "ageix"],
            ))?
            .collect();
        assert_eq!(1, v1.len());
        assert_eq!(Value::from("key2"), v1[0].key);
        assert_eq!(mary3, v1[0].value);

        let v1: Vec<EQLRecord> = eql
            .execute(index_lookup_keys(
                "type1",
                "idx1",
                vec![json!("John Deer"), json!(43)],
                vec!["nameix", "ageix"],
            ))?
            .collect();
        assert_eq!(0, v1.len());

        let v1: Vec<EQLRecord> = eql
            .execute(index_lookup_keys(
                "type1",
                "idx1",
                vec![],
                vec!["nameix", "ageix"],
            ))?
            .collect();
        assert_eq!(2, v1.len());
        assert_eq!(Value::from("key1"), v1[0].key);
        assert_eq!(john2, v1[0].value);
        assert_eq!(Value::from("key2"), v1[1].key);
        assert_eq!(mary2, v1[1].value);

        eql.delete("type1", "key1")?;
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
            .execute(nested_loops(
                index_lookup("type1", "idx1", vec![json!("John Doe")]),
                |rec| Ok(key_lookup("type1", &rec.key)),
            ))?
            .collect();
        assert_eq!(1, v1.len());
        assert_eq!(Value::from("key1"), v1[0].key);
        assert_eq!(john, v1[0].value);

        let v1: Vec<EQLRecord> = eql
            .execute(nested_loops(
                index_lookup_keys("type1", "idx1", vec![json!("John Doe")], vec!["", "ageix"]),
                |rec| {
                    Ok(augment(
                        &rec.value,
                        key_lookup("type1", &rec.key),
                    ))
                },
            ))?
            .collect();
        assert_eq!(1, v1.len());
        assert_eq!(Value::from("key1"), v1[0].key);
        assert_eq!(john2, v1[0].value);
    }
    EQLDB::destroy(path)?;
    Ok(())
}

#[test]
fn test_batch() -> Result<()> {
    let path = "test_batch.db";
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
        let mut batch = EQLBatch::default();

        eql.batch_insert(&mut batch, "type1", "key1", &john)?;

        let mary = json!({
            "name": "Mary Doe",
            "age": 34
        });

        eql.batch_insert(&mut batch, "type1", "key2", &mary)?;

        let v1: Vec<EQLRecord> = eql
            .execute(extract(&["name", "phones"], scan("type1")))?
            .collect();
        assert_eq!(0, v1.len());

        eql.write(batch)?;

        //let v1=vec![(b"key1",&john),(b"key2",&mary)];
        let v1: Vec<EQLRecord> = eql.execute(scan("type1"))?.collect();
        assert_eq!(2, v1.len());
        assert_eq!(Value::from("key1"), v1[0].key);
        assert_eq!(Value::from("key2"), v1[1].key);
        assert_eq!(john, v1[0].value);
        assert_eq!(mary, v1[1].value);

        let mut batch = EQLBatch::default();

        eql.batch_delete(&mut batch, "type1", "key1")?;
        eql.batch_delete(&mut batch, "type1", "key2")?;

        let v1: Vec<EQLRecord> = eql.execute(scan("type1"))?.collect();
        assert_eq!(2, v1.len());

        eql.write(batch)?;

        let v1: Vec<EQLRecord> = eql
            .execute(extract(&["name", "phones"], scan("type1")))?
            .collect();
        assert_eq!(0, v1.len());
    }
    EQLDB::destroy(path)?;
    Ok(())
}



#[test]
fn test_two_types_nested_loops() -> Result<()> {
    let path = "test_two_types_nested_loops.db";
    {
        let mut eql = EQLDB::open(path)?;
        write_northwind_data(&mut eql)?;

        let v1: Vec<EQLRecord> = eql
            .execute(nested_loops(
                extract(&["description"], scan("categories")),
                |rec| {
                    Ok(augment(
                        &rec.value,
                        nested_loops(
                            index_lookup("products", "product_category_id", vec![rec.key.clone()]),
                            |rec| Ok(key_lookup("products", &rec.key)),
                        ),
                    ))
                },
            ))?
            .collect();
        assert_eq!(4, v1.len());
        let keys1: Vec<Value> = v1.iter().map(|t| t.key.clone()).collect();
        assert_eq!(true, keys1.contains(&Value::from(1)));
        assert_eq!(true, keys1.contains(&Value::from(2)));
        assert_eq!(true, keys1.contains(&Value::from(3)));
        assert_eq!(true, keys1.contains(&Value::from(4)));
        assert_eq!(
            4,
            v1.iter()
                .filter(|EQLRecord { key, value, .. }| {
                    if let Some(m) = value.as_object() {
                        if key == &Value::from(1) || key == &Value::from(2) {
                            assert_eq!(
                                Some(&Value::from("Soft drinks, coffees, teas, beers, and ales")),
                                m.get("description")
                            );
                        } else {
                            assert_eq!(
                                Some(&Value::from(
                                    "Sweet and savory sauces, relishes, spreads, and seasonings"
                                )),
                                m.get("description")
                            );
                        }
                        return true;
                    }
                    return false;
                })
                .count()
        );
    }
    EQLDB::destroy(path)?;
    Ok(())
}

#[test]
fn test_hash() -> Result<()> {
    let path = "test_hash.db";
    {
        let mut eql = EQLDB::open(path)?;
        write_northwind_data(&mut eql)?;
        let v1: Vec<EQLRecord> = eql
            .execute(hash_join(
                scan("categories"),
                RecordExtract::Key,
                scan("products"),
                RecordExtract::pointer("/category_id"),
                |(o, mut rec)| {
                    Ok(o.map(|rec1| {
                        if let Some(d) = rec1.value.pointer("/description") {
                            if let Value::Object(ref mut map) = rec.value {
                                map.insert(String::from("description"), d.clone());
                            }
                        }
                        rec
                    }))
                },
            ))?
            .collect();
        assert_eq!(4, v1.len());
        let keys1: Vec<Value> = v1.iter().map(|t| t.key.clone()).collect();
        assert_eq!(true, keys1.contains(&Value::from(1)));
        assert_eq!(true, keys1.contains(&Value::from(2)));
        assert_eq!(true, keys1.contains(&Value::from(3)));
        assert_eq!(true, keys1.contains(&Value::from(4)));
        assert_eq!(
            4,
            v1.iter()
                .filter(|EQLRecord { key, value, .. }| {
                    if let Some(m) = value.as_object() {
                        if key == &Value::from(1) || key == &Value::from(2) {
                            assert_eq!(
                                Some(&Value::from("Soft drinks, coffees, teas, beers, and ales")),
                                m.get("description")
                            );
                        } else {
                            assert_eq!(
                                Some(&Value::from(
                                    "Sweet and savory sauces, relishes, spreads, and seasonings"
                                )),
                                m.get("description")
                            );
                        }
                        return true;
                    }
                    return false;
                })
                .count()
        );
    }
    EQLDB::destroy(path)?;
    Ok(())
}

#[test]
fn test_merge() -> Result<()> {
    let path = "test_merge.db";
    {
        let mut eql = EQLDB::open(path)?;
        write_northwind_data(&mut eql)?;
        let v1: Vec<EQLRecord> = eql
            .execute(merge(
                scan("categories"),
                RecordExtract::Key,
                index_lookup_keys(
                    "products",
                    "product_category_id",
                    vec![],
                    vec!["category_id"],
                ),
                RecordExtract::pointer("/category_id"),
                |(orec1, orec2)| {
                    Ok(orec1
                        .map(|rec1| {
                            orec2.map(|rec2| {
                                let mut rec3 = rec2.clone();
                                if let Some(d) = rec1.value.pointer("/description") {
                                    if let Value::Object(ref mut map) = rec3.value {
                                        map.insert(String::from("description"), d.clone());
                                    }
                                }
                                rec3
                            })
                        })
                        .flatten())
                },
            ))?
            .collect();
        assert_eq!(4, v1.len());
        let keys1: Vec<Value> = v1.iter().map(|t| t.key.clone()).collect();
        assert_eq!(true, keys1.contains(&Value::from(1)));
        assert_eq!(true, keys1.contains(&Value::from(2)));
        assert_eq!(true, keys1.contains(&Value::from(3)));
        assert_eq!(true, keys1.contains(&Value::from(4)));
        assert_eq!(
            4,
            v1.iter()
                .filter(|EQLRecord { key, value, .. }| {
                    if let Some(m) = value.as_object() {
                        if key == &Value::from(1) || key == &Value::from(2) {
                            assert_eq!(
                                Some(&Value::from("Soft drinks, coffees, teas, beers, and ales")),
                                m.get("description")
                            );
                        } else {
                            assert_eq!(
                                Some(&Value::from(
                                    "Sweet and savory sauces, relishes, spreads, and seasonings"
                                )),
                                m.get("description")
                            );
                        }
                        return true;
                    }
                    return false;
                })
                .count()
        );
    }
    EQLDB::destroy(path)?;
    Ok(())
}

#[test]
fn test_process() -> Result<()> {
    let path = "test_process.db";
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

        let john2 = json!({
            "name": "John Doe",
            "age": "43",
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        });

        let mary2 = json!({
            "name": "Mary Doe",
            "age": "34",
        });

        // map
        let v1: Vec<EQLRecord> = meta
            .execute(process(
                scan("type1"),
                Box::new(|it: Box<dyn Iterator<Item = EQLRecord>>| {
                    Ok(Box::new(it.map(|mut r| {
                        if let Value::Object(ref mut map) = r.value {
                            if let Some(v) = map.get("age") {
                                if let Some(i) = v.as_i64() {
                                    let v2 = json!(format!("{}", i));
                                    map.insert(String::from("age"), v2);
                                }
                            }
                        }
                        r
                    })))
                }),
            ))?
            .collect();
        assert_eq!(2, v1.len());
        assert_eq!(Value::from("key1"), v1[0].key);
        assert_eq!(Value::from("key2"), v1[1].key);
        assert_eq!(john2, v1[0].value);
        assert_eq!(mary2, v1[1].value);

        // SUM
        let v1: Vec<EQLRecord> = meta
            .execute(process(
                scan("type1"),
                Box::new(|it| {
                    Ok(Box::new(iter::once(EQLRecord {
                        key: Value::Null,
                        value: json!(it.fold(0, |c, r| {
                            if let Some(m) = r.value.as_object() {
                                if let Some(v) = m.get("age") {
                                    if let Some(i) = v.as_i64() {
                                        return c + i;
                                    }
                                }
                            }
                            c
                        })),
                    })))
                }),
            ))?
            .collect();
        assert_eq!(1, v1.len());
        assert_eq!(Value::Null, v1[0].key);
        assert_eq!(json!(77), v1[0].value);
    }
    EQLDB::destroy(path)?;
    Ok(())
}

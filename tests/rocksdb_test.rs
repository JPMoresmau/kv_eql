use std::iter;

use anyhow::Result;
use kv_eql::{EQLBatch, EQLDB, EQLRecord, RecordExtract, augment, extract, hash_lookup, index_lookup, index_lookup_keys, key_lookup, process, merge, nested_loops, scan};
use serde_json::json;
use serde_json::Value;

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

        //let v1=vec![(b"key1",&john),(b"key2",&mary)];
        let v1: Vec<EQLRecord> = meta
            .execute(key_lookup("type1", Value::from("key1")))?
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
                |rec| key_lookup("type1", rec.key.clone()),
            ))?
            .collect();
        assert_eq!(1, v1.len());
        assert_eq!(Value::from("key1"), v1[0].key);
        assert_eq!(john, v1[0].value);

        let v1: Vec<EQLRecord> = eql
            .execute(nested_loops(
                index_lookup_keys("type1", "idx1", vec![json!("John Doe")], vec!["", "ageix"]),
                |rec| augment(rec.value.clone(), key_lookup("type1", rec.key.clone())),
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

fn write_northwind_data(eql: &mut EQLDB) -> Result<()> {
    let bevs = json!({
        "category_name":"Beverages",
        "description":"Soft drinks, coffees, teas, beers, and ales",
    });
    let conds = json!({
        "category_name":"Condiments",
        "description":"Sweet and savory sauces, relishes, spreads, and seasonings",
    });
    eql.insert("categories", 1, &bevs)?;
    eql.insert("categories", 2, &conds)?;

    eql.add_index("products", "product_category_id", vec!["/category_id"])?;

    let chai = json!({
        "product_name":"Chai",
        "category_id":1,
        "quantity_per_unit":"10 boxes x 20 bags",
    });
    eql.insert("products", 1, &chai)?;
    let chang = json!({
        "product_name":"Chang",
        "category_id":1,
        "quantity_per_unit":"24 - 12 oz bottles"
    });
    eql.insert("products", 2, &chang)?;
    let aniseed = json!({
        "product_name":"Aniseed Syrup",
        "category_id":2,
        "quantity_per_unit":"12 - 550 ml bottles"
    });
    eql.insert("products", 3, &aniseed)?;
    let cajun = json!({
        "product_name":"Chef Anton's Cajun Seasoning",
        "category_id":2,
        "quantity_per_unit":"48 - 6 oz jars"
    });
    eql.insert("products", 4, &cajun)?;
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
                    augment(
                        rec.value.clone(),
                        nested_loops(
                            index_lookup("products", "product_category_id", vec![rec.key.clone()]),
                            |rec| key_lookup("products", rec.key.clone()),
                        ),
                    )
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
            .execute(hash_lookup(
                scan("categories"),
                RecordExtract::Key,
                scan("products"),
                RecordExtract::pointer("/category_id"),
                |(o, mut rec)| {
                    o.map(|rec1| {
                        if let Some(d) = rec1
                            .value.pointer("/description"){
                            rec.value
                                .as_object_mut()
                                .unwrap()
                                .insert(String::from("description"), d.clone());
                        }
                        rec
                    })
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
                vec![RecordExtract::Key],
                index_lookup_keys(
                    "products",
                    "product_category_id",
                    vec![],
                    vec!["category_id"],
                ),
                vec![RecordExtract::pointer("/category_id")],
                |(orec1, orec2)| {
                    orec1
                        .map(|rec1| {
                            orec2.map(|rec2| {
                                let mut rec3 = rec2.clone();
                                if let Some(d) = rec1
                                    .value.pointer("/description") {
                                    rec3.value
                                        .as_object_mut()
                                        .unwrap()
                                        .insert(String::from("description"), d.clone());
                                }
                                rec3
                            })
                        })
                        .flatten()
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
        let v1: Vec<EQLRecord> = meta.execute(process(scan("type1"),Box::new(|it:Box<dyn Iterator<Item=EQLRecord>>| {
            Box::new(it.map(|mut r| {
                if let Some(m) = r.value.as_object_mut(){
                    if let Some(v) = m.get("age") {
                        if let Some(i) = v.as_i64() {
                            let v2=json!(format!("{}",i));
                            m.insert(String::from("age"), v2);
                        }
                    }
                }
                r
            }))
        })))?.collect();
        assert_eq!(2, v1.len());
        assert_eq!(Value::from("key1"), v1[0].key);
        assert_eq!(Value::from("key2"), v1[1].key);
        assert_eq!(john2, v1[0].value);
        assert_eq!(mary2, v1[1].value);

        // SUM
        let v1: Vec<EQLRecord> = meta.execute(process(scan("type1"),Box::new(|it| {
            Box::new(iter::once(EQLRecord{key:Value::Null,value:json!(it.fold(0, |c,r| {
                if let Some(m) = r.value.as_object(){
                    if let Some(v) = m.get("age") {
                        if let Some(i) = v.as_i64() {
                            return c+i;
                        }
                    }
                }
                c
            }))}))
        })))?.collect();
        assert_eq!(1, v1.len());
        assert_eq!(Value::Null, v1[0].key);
        assert_eq!(json!(77), v1[0].value);


    }
    EQLDB::destroy(path)?;
    Ok(())
}
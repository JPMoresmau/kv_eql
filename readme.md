 The library provides an "Explicit Query Language" over a key value store

 In a relational database, you write queries in SQL, and the database query planner and optimizer generate the low-level operations,
 like table scans, index lookup, various ways of joining results, etc.
 In Explicit Query Language, you're the optimizer: you write directly the low level operations you want! So you know exactly what level
 of performance to expect!

 What this library does:
 * Encapsulates usage of RocksDB as the underlying data store
 * Provides support for indexing
 * Provides support to define explicit operations on the data and the indices
 * Only mandates that records have both a key and a value that is a JSON Value. Numbers and Strings are good candidate for keys usually, but you're not limited to that.

 # Examples

 ## Connect and operate on data
 ```rust
 use kv_eql::*;
 use serde_json::json;

 let path = "test_basic.db";
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

     let ov = eql.get("type1", "key1")?;
     assert_eq!(ov, Some(john));

     eql.delete("type1", "key1")?;
     let ov = eql.get("type1", "key1")?;
     assert_eq!(ov, None);
 }
 EQLDB::destroy(path)?;
 ```
 
 ## Queries
 In the standard sample Northwind database, there are categories and products. Each product belongs to a category. Suppose you want to list all products
 and add to them the description of their categories. One way to do it would be to scan the categories table, only keep the description, then for each category
 lookup all products for this category and add the description. This is how you would write this in EQL:
 ```rust
 eql
 .execute(nested_loops(
    extract(&["description"], scan("categories")),
    |rec| {
        augment(
            &rec.value,
            nested_loops(
                index_lookup("products", "product_category_id", vec![rec.key.clone()]),
                |rec| key_lookup("products", &rec.key),
            ),
        )
    },
));
 ```
 We scan the categories, extract the description field, and perform a nested loop: for each category, we do an index lookup, retrieve the product and augment 
 the product value with the description
 In this example, it would probably be faster to scan both categories and products since we want all products, and do a hash join
 ```rust
 eql.execute(hash_join(
    scan("categories"),
    RecordExtract::Key,
    scan("products"),
    RecordExtract::pointer("/category_id"),
    |(o, mut rec)| {
        o.map(|rec1| {
            if let Some(d) = rec1.value.pointer("/description"){
                if let Value::Object(ref mut map) = rec.value {
                    map.insert(String::from("description"), d.clone());
                }
            }
            rec
        })
    },
    ));
 ```
 
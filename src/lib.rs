/*!
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
```
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
Ok::<(), anyhow::Error>(())
```

## Queries
In the standard sample Northwind database, there are categories and products. Each product belongs to a category. Suppose you want to list all products
and add to them the description of their categories. One way to do it would be to scan the categories table, only keep the description, then for each category
lookup all products for this category and add the description. This is how you would write this in EQL:
```no_run
# use kv_eql::*;
# let eql = EQLDB::open("")?;
eql
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
));
# Ok::<(), anyhow::Error>(())
```
We scan the categories, extract the description field, and perform a nested loop: for each category, we do an index lookup, retrieve the product and augment 
the product value with the description
In this example, it would probably be faster to scan both categories and products since we want all products, and do a hash join
```no_run
# use kv_eql::*;
# use serde_json::Value;
# 
# let eql = EQLDB::open("")?;
eql.execute(hash_lookup(
   scan("categories"),
   RecordExtract::Key,
   scan("products"),
   RecordExtract::pointer("/category_id"),
   |(o, mut rec)| {
       Ok(o.map(|rec1| {
           if let Some(d) = rec1.value.pointer("/description"){
               if let Value::Object(ref mut map) = rec.value {
                    map.insert(String::from("description"), d.clone());
               }
           }
           rec
       }))
   },
   ));
# Ok::<(), anyhow::Error>(())
```

*/

use rhai::Engine;
use rocksdb::{
    ColumnFamilyDescriptor, Direction, IteratorMode, Options, ReadOptions, WriteBatch, DB,
};

use serde_json::{Map, Value};
use std::{io::{BufReader, BufWriter}};
use std::path::{Path, PathBuf};
use std::{
    cmp::Ordering,
    collections::BTreeMap,
    fs::{File, OpenOptions},
};
use std::{
    collections::{HashMap, HashSet},
    fs::remove_file,
    iter,
};

use anyhow::Result;
use thiserror::Error;

mod ops;
pub use ops::*;

mod parse;
pub use parse::*;

mod script;
pub use script::*;

use nom::Finish;

/// Metadata errors
#[derive(Error, Debug)]
pub enum MetadataError {
    /// Duplicate index name for same table/column family
    #[error("duplicate index {index_name} for record type {rec_type}")]
    DuplicateIndex {
        rec_type: String,
        index_name: String,
    },
}


/// A batch of operations, in the same transaction
#[derive(Default)]
pub struct EQLBatch {
    batch: WriteBatch,
}

/// The database structure
pub struct EQLDB {
    /// The underlying database
    db: DB,
    /// Xhere we store the metadata
    metadata_path: PathBuf,
    /// The metadata
    pub metadata: Metadata,
    /// The scripting engine
    pub scripting_engine: Engine,
}

impl EQLDB {
    /// Opens the database
    /// # Arguments
    /// * `path` - The folder where the database and metadata reside
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mdp = path.as_ref().join("metadata.json");
        let metadata = if mdp.is_file() {
            let file = File::open(mdp.clone())?;
            let reader = BufReader::new(file);
            serde_json::from_reader(reader)?
        } else {
            Metadata::default()
        };

        let mut cfs = vec![];
        for (rec_type, indices) in metadata.indices.iter() {
            let mut cf_opts = Options::default();
            cf_opts.set_max_write_buffer_number(16);
            cfs.push(ColumnFamilyDescriptor::new(rec_type, cf_opts));
            for idx_name in indices.keys() {
                let mut cf_opts = Options::default();
                cf_opts.set_max_write_buffer_number(16);
                let idx_cf = index_cf_name(rec_type, idx_name);
                cfs.push(ColumnFamilyDescriptor::new(idx_cf, cf_opts));
            }
        }

        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let db = DB::open_cf_descriptors(&db_opts, path, cfs).unwrap();
        Ok(EQLDB {
            db,
            metadata_path: mdp,
            metadata,
            scripting_engine:eql_engine(),
        })
    }

     /// Opens the database
    /// # Arguments
    /// * `path` - The folder where the database and metadata reside
    pub fn open_new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::destroy(&path)?;
        Self::open(path)
    }

    /// Destroys fully a database, removing the folder and metadata
    /// # Arguments
    /// * `path` - The folder where the database and metadata reside
    pub fn destroy<P: AsRef<Path>>(path: P) -> Result<()> {
        let mdp = path.as_ref().join("metadata.json");
        if mdp.is_file() {
            remove_file(mdp)?;
        }
        DB::destroy(&Options::default(), path)?;
        Ok(())
    }

    /// Adds an index
    /// # Arguments
    /// * `rec_type` - The record type
    /// * `idx_name` - The index name, must be unique for a given record type
    /// * `on` - The list of JSON expressions to apply to values and index
    pub fn add_index<T: AsRef<str>, IT: AsRef<str>, OT: AsRef<str>>(
        &mut self,
        rec_type: T,
        idx_name: IT,
        on: Vec<OT>,
    ) -> Result<()> {
        let ref_type = String::from(rec_type.as_ref());
        let ref_idx = String::from(idx_name.as_ref());
        let m = self
            .metadata
            .indices
            .entry(ref_type.clone())
            .or_insert_with(HashMap::new);
        if m.contains_key(&ref_idx) {
            return Err(MetadataError::DuplicateIndex {
                rec_type: ref_type,
                index_name: ref_idx,
            }
            .into());
        }
        let idx_cf = index_cf_name(rec_type.as_ref(), &ref_idx);
        self.db.create_cf(&idx_cf, &Options::default())?;
        let cf1 = self.db.cf_handle(&idx_cf).unwrap();
        m.insert(
            ref_idx,
            on.iter().map(|s| String::from(s.as_ref())).collect(),
        );

        self.save_metadata()?;

        self.execute(scan(rec_type.as_ref()))?
            .try_fold(WriteBatch::default(), |mut b, rec| {
                let kv = serde_json::to_vec(&rec.key).unwrap();
                let ix_key = index_key(&on, &kv, &rec.value);

                b.put_cf(cf1, ix_key, &kv);
                if b.len() > 1000 {
                    self.db.write(b)?;
                    return Ok(WriteBatch::default());
                }
                let r: Result<WriteBatch, rocksdb::Error> = Ok(b);
                r
            })?;

        Ok(())
    }

    /// Saves the metadata to a file
    fn save_metadata(&self) -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.metadata_path.clone())?;
        serde_json::to_writer(BufWriter::new(file), &self.metadata)?;
        Ok(())
    }

    /// Deletes an index
    /// # Arguments
    /// * `rec_type` - The record type
    /// * `idx_name` - The index name, must be unique for a given record type
    pub fn delete_index<T: AsRef<str>, IT: AsRef<str>>(
        &mut self,
        rec_type: T,
        idx_name: IT,
    ) -> Result<()> {
        if let Some(m) = self.metadata.indices.get_mut(rec_type.as_ref()) {
            m.remove(idx_name.as_ref());
            let idx_cf = index_cf_name(rec_type.as_ref(), idx_name.as_ref());
            self.db.drop_cf(&idx_cf)?;

            self.save_metadata()?;
        }
        Ok(())
    }

    /// Inserts a record
    /// # Arguments
    /// * `rec_type` - The record type
    /// * `key` - The key
    /// * `value` - A reference to the value to store
    pub fn insert<T: AsRef<str>, V: Into<Value>>(
        &mut self,
        rec_type: T,
        key: V,
        value: &Value,
    ) -> Result<()> {
        let mut batch = EQLBatch::default();
        self.batch_insert(&mut batch, rec_type, key, value)?;
        self.db.write(batch.batch)?;
        Ok(())
    }

    /// Inserts a record into a write batch
    /// # Arguments
    /// * `batch`- The write batch
    /// * `rec_type` - The record type
    /// * `key` - The key
    /// * `value` - A reference to the value to store
    pub fn batch_insert<T: AsRef<str>, V: Into<Value>>(
        &mut self,
        batch: &mut EQLBatch,
        rec_type: T,
        key: V,
        value: &Value,
    ) -> Result<()> {
        let ref_type = rec_type.as_ref();
        let ocf1 = self.db.cf_handle(ref_type);
        let cf = match ocf1 {
            None => {
                self.db.create_cf(ref_type, &Options::default())?;
                let cf1 = self.db.cf_handle(ref_type).unwrap();
                cf1
            }
            Some(cf1) => cf1,
        };
        let kv = serde_json::to_vec(&key.into()).unwrap();
        batch
            .batch
            .put_cf(cf, kv.clone(), serde_json::to_vec(value).unwrap());

        if let Some(idxs) = self.metadata.indices.get(ref_type) {
            for (idx_name, on) in idxs.iter() {
                let idx_cf = index_cf_name(rec_type.as_ref(), idx_name);
                if let Some(cf1) = self.db.cf_handle(&idx_cf) {
                    let ix_key = index_key(on, &kv, value);
                    batch.batch.put_cf(cf1, ix_key, kv.clone());
                }
            }
        } else {
            self.metadata
                .indices
                .insert(String::from(ref_type), HashMap::new());
            self.save_metadata()?;
        }

        Ok(())
    }

    /// Reads a single record
    /// # Arguments
    /// * `rec_type` - The record type
    /// * `key` - The key
    pub fn get<T: AsRef<str>, V: Into<Value>>(
        &mut self,
        rec_type: T,
        key: V,
    ) -> Result<Option<Value>> {
        let ref_type = rec_type.as_ref();
        let ocf1 = self.db.cf_handle(ref_type);
        if let Some(cf1) = ocf1 {
            Ok(self
                .db
                .get_cf(cf1, serde_json::to_vec(&key.into()).unwrap())?
                .map(|v| serde_json::from_slice(&v).unwrap()))
        } else {
            Ok(None)
        }
    }

    /// Deletes a single record
    /// # Arguments
    /// * `rec_type` - The record type
    /// * `key` - The key
    pub fn delete<T: AsRef<str>, V: Into<Value>>(&mut self, rec_type: T, key: V) -> Result<()> {
        let mut batch = EQLBatch::default();
        self.batch_delete(&mut batch, rec_type, key)?;
        self.db.write(batch.batch)?;
        Ok(())
    }

    /// Deletes a single record in batch
    /// # Arguments
    /// * `batch` - The write batch
    /// * `rec_type` - The record type
    /// * `key` - The key
    pub fn batch_delete<T: AsRef<str>, V: Into<Value>>(
        &mut self,
        batch: &mut EQLBatch,
        rec_type: T,
        key: V,
    ) -> Result<()> {
        let ref_type = rec_type.as_ref();
        let ocf1 = self.db.cf_handle(ref_type);
        if let Some(cf1) = ocf1 {
            let kv = serde_json::to_vec(&key.into()).unwrap();
            if let Some(idxs) = self.metadata.indices.get(ref_type) {
                if !idxs.is_empty() {
                    if let Some(value) = self
                        .db
                        .get_cf(cf1, kv.clone())?
                        .map(|v| serde_json::from_slice(&v).unwrap())
                    {
                        for (idx_name, on) in idxs.iter() {
                            let idx_cf = index_cf_name(rec_type.as_ref(), idx_name);
                            if let Some(cf) = self.db.cf_handle(&idx_cf) {
                                let ix_key = index_key(on, &kv, &value);
                                batch.batch.delete_cf(cf, ix_key);
                            }
                        }
                    }
                }
            }
            batch.batch.delete_cf(cf1, kv);
        }
        Ok(())
    }

    /// Writes a batch
    /// # Arguments
    /// * `batch` - The write batch
    pub fn write(&self, batch: EQLBatch) -> Result<()> {
        self.db.write(batch.batch)?;
        Ok(())
    }

    /// Executes an operation and returns an iterator on records
    /// # Arguments
    /// * `operation` - The operation
    pub fn execute<'a>(&'a self, operation: Operation<'a>) -> Result<Box<dyn Iterator<Item = EQLRecord> + 'a>> {
        match operation {
            Operation::Scan { name } => {
                let ocf1 = self.db.cf_handle(&name);
                if let Some(cf1) = ocf1 {
                    let it = self.db.iterator_cf(cf1, IteratorMode::Start).map(|(k, v)| {
                        EQLRecord::new(
                            serde_json::from_slice::<Value>(&k).unwrap(),
                            serde_json::from_slice::<Value>(&v).unwrap(),
                        )
                    });
                    return Ok(Box::new(it));
                }
            }
            Operation::KeyLookup { name, key } => {
                let ocf1 = self.db.cf_handle(&name);
                if let Some(cf1) = ocf1 {
                    let rk = serde_json::to_vec(&key).unwrap();
                    let v =
                        self.db.get_cf(cf1, &rk).unwrap().map(|v| {
                            EQLRecord::new(key, serde_json::from_slice::<Value>(&v).unwrap())
                        });
                    return Ok(Box::new(v.into_iter()));
                }
            }
            Operation::Extract {
                names,
                operation: b_op,
            } => {
                return Ok(Box::new(self.execute(*b_op)?.map(move |rec| EQLRecord {
                    value: extract_from_value(rec.value, &names),
                    ..rec
                })));
            }
            Operation::Augment {
                value,
                operation: b_op,
            } => {
                return Ok(Box::new(self.execute(*b_op)?.map(move |rec| EQLRecord {
                    value: merge_values(&value, rec.value),
                    ..rec
                })));
            }
            Operation::IndexLookup {
                name,
                index_name,
                values,
                keys,
            } => {
                let idx_cf = index_cf_name(&name, &index_name);
                if let Some(cf) = self.db.cf_handle(&idx_cf) {
                    if values.is_empty() {
                        let it = self
                            .db
                            .iterator_cf(cf, IteratorMode::Start)
                            .map(move |(k, v)| {
                                EQLRecord::new(
                                    serde_json::from_slice(&v).unwrap(),
                                    extract_from_index_key(&k, &keys),
                                )
                            });
                        return Ok(Box::new(it));
                    } else {
                        let mut v = vec![];

                        for o in values.iter() {
                            v.append(&mut serde_json::to_vec(o).unwrap());
                            v.push(0);
                        }
                        let mut opts = ReadOptions::default();
                        let mut u = v.clone();
                        u.pop();
                        u.push(1);
                        opts.set_iterate_upper_bound(u);
                        let mode = IteratorMode::From(&v.as_ref(), Direction::Forward);
                        let it = self.db.iterator_cf_opt(cf, opts, mode).map(move |(k, v)| {
                            EQLRecord::new(
                                serde_json::from_slice(&v).unwrap(),
                                extract_from_index_key(&k, &keys),
                            )
                        });
                        return Ok(Box::new(it));
                    }
                }
            }
            Operation::NestedLoops { first, second } => {
                return Ok(Box::new(self.execute(*first)?
                    .map(|rec| second(&rec).and_then(|op| self.execute(op)).map(|i| i.collect::<Vec<EQLRecord>>()))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .flatten()
                ))
                ;

            }
            Operation::HashLookup {
                build,
                build_hash,
                probe,
                probe_hash,
                join,
            } => {
                let map: HashMap<String, EQLRecord> = self
                    .execute(*build)?
                    .flat_map(|rec| build_hash.apply(&rec).map(|s| (format!("{}", s), rec)))
                    .collect();
                return Ok(Box::new(self.execute(*probe)?.flat_map(move |rec| {
                    probe_hash.apply(&rec)
                        .map(|h| {
                            let hash = format!("{}", h);
                            join((map.get(&hash), rec))
                        })
                }).collect::<Result<Vec<Option<EQLRecord>>>>()?.into_iter().filter_map(|c| c)));
            }
            Operation::Merge {
                first,
                first_key,
                second,
                second_key,
                join,
            } => {
                let mut it1 = self.execute(*first)?.peekable();
                let mut it2 = self.execute(*second)?.peekable();
                let mut v = vec![];
                let mut orec1 = it1.next();
                let mut orec2 = it2.next();
                while orec1.is_some() || orec2.is_some() {
                    if let Some(rec1) = &orec1 {
                        if let Some(rec2) = &orec2 {
                            let k1 = serde_json::to_vec(&first_key.apply(rec1).unwrap_or_else(|| Value::Null)).unwrap();
                            let k2 = serde_json::to_vec(&second_key.apply(rec2).unwrap_or_else(|| Value::Null)).unwrap();
                            match k1.cmp(&k2) {
                                Ordering::Less => {
                                    if let Some(rec3) =  join((Some(rec1), None))?{
                                        v.push(rec3);
                                    }
                                    orec1 = it1.next();
                                }
                                Ordering::Greater => {
                                    if let Some(rec3) = join((None, Some(rec2)))?{
                                        v.push(rec3);
                                    }
                                    orec2 = it2.next();
                                }
                                Ordering::Equal => {
                                    if let Some(rec3) = join((Some(rec1), Some(rec2)))?{
                                        v.push(rec3);
                                    }
                                    orec2 = it2.next();
                                }
                            }
                        } else {
                            if let Some(rec3) = join((Some(rec1), None))?{
                                v.push(rec3);
                            }
                            orec1 = it1.next();
                        }
                    } else if let Some(rec2) = &orec2 {
                        if let Some(rec3)= join((None, Some(rec2)))?{
                            v.push(rec3);
                        }
                        orec2 = it2.next();
                    }
                }
                return Ok(Box::new(v.into_iter()));
            },
            Operation::Process {operation, process} => {
               return process(self.execute(*operation)?);
            },
        }
        Ok(Box::new(iter::empty::<EQLRecord>()))
    }

    pub fn execute_script<'a>(&'a self, script: &'a str) -> Result<Box<dyn Iterator<Item = EQLRecord> + 'a>> {
        let r=parse_operation_verbose(script).finish();
        match r {
            Ok((_,sop))=> {
                println!("sop:{:?}",sop);
                let op = sop.into_rust(&self.scripting_engine)?;
                self.execute(op)
            },
            Err(e)=> 
                Err(QueryError::ParseError(format!("{}",e)).into()) ,
        }
        
    }
}

/// Merge JSON values
fn merge_values(first: &Value, mut second: Value) -> Value {
    if let Some(m1) = first.as_object() {
        if !m1.is_empty() {
            if let Some(m2) = second.as_object_mut() {
                for (k, v) in m1.iter() {
                    if !m2.contains_key(k) {
                        m2.insert(k.clone(), v.clone());
                    }
                }
            }
        }
    }
    second
}

/// Given an index key, builds a JSON object from the given key names
fn extract_from_index_key<K: AsRef<[u8]>>(k: K, keys: &[String]) -> Value {
    let mut im: BTreeMap<String, Value> = BTreeMap::new();
    for (part, name) in k.as_ref().split(|u| *u == 0).zip(keys.iter()) {
        if !name.is_empty() {
            im.insert(name.clone(), serde_json::from_slice(part).unwrap());
        }
    }

    serde_json::to_value(im).unwrap()
}

/// Only keep given names in given JSON value
fn extract_from_value(mut value: Value, names: &HashSet<String>) -> Value {
    if let Some(m) = value.as_object_mut(){
        names
            .iter()
            .filter_map(|n| get_map_value(n, m))
            .collect()
    } else {
        value
    }
}

/// Get and remove a value from a map, returning the key and value
fn get_map_value(n: &str, m: &mut Map<String, Value>) -> Option<(String, Value)> {
    m.remove(n).map(|v| (String::from(n), v))
}

/// Get the underlying column family name for a given record type and index name
fn index_cf_name(ref_type: &str, index_name: &str) -> String {
    format!("#idx_{}_{}", ref_type, index_name)
}

/// Builds the key for an index column family
/// The index key is made of the serialized values separated by 0, and the record key
fn index_key<T: AsRef<str>, K: AsRef<[u8]>>(on: &[T], key: &K, value: &Value) -> Vec<u8> {
    let mut v = vec![];
    for o in on {
        if let Some(v2) = value.pointer(o.as_ref()) {
            v.append(&mut serde_json::to_vec(v2).unwrap());
        } else {
            v.append(&mut serde_json::to_vec(&Value::Null).unwrap());
        }
        v.push(0);
    }
    for k in key.as_ref() {
        v.push(*k);
    }
    v
}

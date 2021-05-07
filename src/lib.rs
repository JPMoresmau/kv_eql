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
       augment(
           rec.value.clone(),
           nested_loops(
               index_lookup("products", "product_category_id", vec![rec.key.clone()]),
               |rec| key_lookup("products", rec.key.clone()),
           ),
       )
   },
));
# Ok::<(), anyhow::Error>(())
```
We scan the categories, extract the description field, and perform a nested loop: for each category, we do an index lookup, retrieve the product and augment 
the product value with the description
In this example, it would probably be faster to scan both categories and products since we want all products, and do a hash join
```no_run
# use kv_eql::*;
# let eql = EQLDB::open("")?;
eql.execute(hash_lookup(
   scan("categories"),
   RecordExtract::Key,
   scan("products"),
   RecordExtract::pointer("/category_id"),
   |(o, mut rec)| {
       o.map(|rec1| {
           if let Some(d) = rec1.value.pointer("/description"){
               rec.value.as_object_mut().unwrap().insert(String::from("description"), d.clone());
           }
           rec
       })
   },
   ));
# Ok::<(), anyhow::Error>(())
```

*/

use rocksdb::{
    ColumnFamilyDescriptor, Direction, IteratorMode, Options, ReadOptions, WriteBatch, DB,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::io::{BufReader, BufWriter};
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

/// A specific operation on the data store
pub enum Operation {
    Scan {
        name: String,
    },
    KeyLookup {
        name: String,
        key: Value,
    },
    Extract {
        names: HashSet<String>,
        operation: Box<Operation>,
    },
    Augment {
        value: Value,
        operation: Box<Operation>,
    },
    IndexLookup {
        name: String,
        index_name: String,
        values: Vec<Value>,
        keys: Vec<String>,
    },
    NestedLoops {
        first: Box<Operation>,
        second: Box<dyn Fn(&EQLRecord) -> Operation>,
    },
    HashLookup {
        build: Box<Operation>,
        build_hash: RecordExtract,
        probe: Box<Operation>,
        probe_hash: RecordExtract,
        join: HashJoinFunction,
    },
    Merge {
        first: Box<Operation>,
        first_key: Vec<RecordExtract>,
        second: Box<Operation>,
        second_key: Vec<RecordExtract>,
        join: MergeJoinFunction,
    },
}

/// The underlying type for Hash join function
type HashJoinFunction = Box<dyn Fn((Option<&EQLRecord>, EQLRecord)) -> Option<EQLRecord>>;
/// the underlying type for Merge join function
type MergeJoinFunction = Box<dyn Fn((Option<&EQLRecord>, Option<&EQLRecord>)) -> Option<EQLRecord>>;

/// Builds an operation to scan a whole record type
/// # Arguments
/// * `name` - the name of the record type
///
pub fn scan<N: Into<String>>(name: N) -> Operation {
    Operation::Scan { name: name.into() }
}

/// Builds an operation to perform a single key lookup
/// # Arguments
/// * `name` - the name of the record type
/// * `key` - the key
pub fn key_lookup<N: Into<String>>(name: N, key: Value) -> Operation {
    Operation::KeyLookup {
        name: name.into(),
        key,
    }
}

/// Builds an operation to extract specific keys from the values returned by the wrapped operation
/// # Arguments
/// * `extract` - the names of the keys to extract, the others will be dropped
/// * `operation` - the wrapped operation
pub fn extract(extract: &[&str], operation: Operation) -> Operation {
    let mut hs = HashSet::new();
    for e in extract.iter() {
        hs.insert((*e).into());
    }
    Operation::Extract {
        names: hs,
        operation: Box::new(operation),
    }
}

/// Builds an operation to merge a given Value into the values returned by the wrapped operation
/// # Arguments
/// * `value` - the value to merge with the values from the wrapped operation
/// * `operation` - the wrapped operation
pub fn augment(value: Value, operation: Operation) -> Operation {
    Operation::Augment {
        value,
        operation: Box::new(operation),
    }
}

/// Builds an operation to perform an index lookup
/// # Arguments
/// * `name` - the name of the table/column family
/// * `index_name` - the name of the index
/// * `values` - the values to lookup in the index, in the order the index was built. Null values can be used to indicate
/// all values can be considered at this level in the index. An empty Vec means the full index will be scanned
pub fn index_lookup<N: Into<String>, IN: Into<String>>(
    name: N,
    index_name: IN,
    values: Vec<Value>,
) -> Operation {
    Operation::IndexLookup {
        name: name.into(),
        index_name: index_name.into(),
        values,
        keys: vec![],
    }
}

/// Builds an operation to perform an index lookup and return some index keys
/// # Arguments
/// * `name` - the name of the table/column family
/// * `index_name` - the name of the index
/// * `values` - the values to lookup in the index, in the order the index was built. Null values can be used to indicate
/// all values can be considered at this level in the index. An empty Vec means the full index will be scanned
/// * `keys` - the names to use as keys in the returned Value for each index values section, empty string meaning "ignore this part of the index key"
pub fn index_lookup_keys<N: Into<String>, IN: Into<String>, OT: AsRef<str>>(
    name: N,
    index_name: IN,
    values: Vec<Value>,
    keys: Vec<OT>,
) -> Operation {
    Operation::IndexLookup {
        name: name.into(),
        index_name: index_name.into(),
        values,
        keys: keys.iter().map(|s| String::from(s.as_ref())).collect(),
    }
}

/// Builds an operation to perform nested loops
/// # Arguments
/// * `first` - the initial operation on which we'll iterate
/// * `second` - a function to build an operation given each record from the first operation
pub fn nested_loops<F>(first: Operation, second: F) -> Operation
where
    F: Fn(&EQLRecord) -> Operation + 'static,
{
    Operation::NestedLoops {
        first: Box::new(first),
        second: Box::new(second),
    }
}

/// Builds an operation to perform a hash join lookup
/// # Arguments
/// * `build` - the build operation
/// * `build_hash` - the function to build a value from each record from the first operation, returning None if we want to ignore that record
/// * `probe` - the probe operation
/// * `probe_hash` - the function to build a value from each record from the second operation, returning None if we want to ignore that record
/// * `join` - the function to join the record from the first operation if it exists and the record from the second operation. Two records
/// are joined when they gave the same value via `build_hash` and `probe_hash`
pub fn hash_lookup<F>(
    build: Operation,
    build_hash: RecordExtract,
    probe: Operation,
    probe_hash: RecordExtract,
    join: F,
) -> Operation
where
    F: Fn((Option<&EQLRecord>, EQLRecord)) -> Option<EQLRecord> + 'static,
{
    Operation::HashLookup {
        build: Box::new(build),
        build_hash,
        probe: Box::new(probe),
        probe_hash,
        join: Box::new(join),
    }
}

/// Builds an operation to merge two operations
/// # Arguments
/// * `first` - the first operation
/// * `first_key`- builds the array of values that will be the key for the first records.
/// * `second` - the second operation
/// * `second_key` - builds the array of values that will be the key for the second records
/// * `join` - the function to join records from both operations. There may be only a first record, only a second record, or both.
/// The join uses key comparisons so expects the two sets of keys to be in the same order
pub fn merge<F>(
    first: Operation,
    first_key: Vec<RecordExtract>,
    second: Operation,
    second_key: Vec<RecordExtract>,
    join: F,
) -> Operation
where
    F: Fn((Option<&EQLRecord>, Option<&EQLRecord>)) -> Option<EQLRecord> + 'static,
{
    Operation::Merge {
        first: Box::new(first),
        first_key: first_key,
        second: Box::new(second),
        second_key: second_key,
        join: Box::new(join),
    }
}

/// The metadata we keep track of
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Metadata {
    /// all the indices created: first key is record type, second is index name, the final value are the JSON pointers to index, in order
    pub indices: HashMap<String, HashMap<String, Vec<String>>>,
}

/// A record from an operation. Both keys and values are arbitrary JSON values, but some operations expect the values to be JSON objects
#[derive(Debug, Clone)]
pub struct EQLRecord {
    pub key: Value,
    pub value: Value,
}

impl EQLRecord {
    /// Creates a new record
    pub fn new(key: Value, value: Value) -> Self {
        EQLRecord { key, value }
    }
}


/// Indicates how to extract information from a record
pub enum RecordExtract {
    /// Extracts the key
    Key,
    /// Extracts the full value
    Value,
    /// Extracts the result of the given pointer on the value
    Pointer(String),
    /// Arbitrary function to retrieve a value
    Function(Box<dyn Fn(&EQLRecord) -> Option<Value>>),
}

impl RecordExtract {
    /// Create a pointer extraction
    /// # Arguments
    /// * `pointer` - the JSON pointer expression
    pub fn pointer<N: Into<String>>(pointer: N) -> RecordExtract{
        RecordExtract::Pointer(pointer.into())
    }

    /// Apply the record extract to a record and return the potential result
    /// # Arguments
    /// * `rec` - the record
    fn apply(&self, rec: &EQLRecord) -> Option<Value> {
        match self {
            RecordExtract::Key => Some(rec.key.clone()),
            RecordExtract::Value => Some(rec.value.clone()),
            RecordExtract::Pointer(p) => rec.value.pointer(p).map(|v| v.clone()),
            RecordExtract::Function(f) => f(rec),
        }
    }

    /// Apply multiple extracts to a record, return the values obtained
    /// # Arguments
    /// * `exs` - The extracts
    /// * `rec` - The record
    fn multiple(exs: &[RecordExtract], rec: &EQLRecord ) -> Vec<Value> {
        exs.iter().filter_map(|e| e.apply(rec)).collect()
    }
}

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
        })
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

        self.execute(scan(rec_type.as_ref()))
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
    pub fn execute(&self, operation: Operation) -> Box<dyn Iterator<Item = EQLRecord> + '_> {
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
                    return Box::new(it);
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
                    return Box::new(v.into_iter());
                }
            }
            Operation::Extract {
                names,
                operation: b_op,
            } => {
                return Box::new(self.execute(*b_op).map(move |rec| EQLRecord {
                    value: extract_from_value(rec.value, &names),
                    ..rec
                }));
            }
            Operation::Augment {
                value,
                operation: b_op,
            } => {
                return Box::new(self.execute(*b_op).map(move |rec| EQLRecord {
                    value: merge_values(&value, rec.value),
                    ..rec
                }));
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
                        return Box::new(it);
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
                        return Box::new(it);
                    }
                }
            }
            Operation::NestedLoops { first, second } => {
                return Box::new(
                    self.execute(*first)
                        .flat_map(move |rec| self.execute(second(&rec))),
                );
            }
            Operation::HashLookup {
                build,
                build_hash,
                probe,
                probe_hash,
                join,
            } => {
                let map: HashMap<String, EQLRecord> = self
                    .execute(*build)
                    .flat_map(|rec| build_hash.apply(&rec).map(|s| (format!("{}", s), rec)))
                    .collect();
                return Box::new(self.execute(*probe).flat_map(move |rec| {
                    probe_hash.apply(&rec)
                        .map(|h| {
                            let hash = format!("{}", h);
                            join((map.get(&hash), rec))
                        })
                        .flatten()
                }));
            }
            Operation::Merge {
                first,
                first_key,
                second,
                second_key,
                join,
            } => {
                let mut it1 = self.execute(*first).peekable();
                let mut it2 = self.execute(*second).peekable();
                let mut v = vec![];
                let mut orec1 = it1.next();
                let mut orec2 = it2.next();
                while orec1.is_some() || orec2.is_some() {
                    if let Some(rec1) = &orec1 {
                        if let Some(rec2) = &orec2 {
                            let k1 = values_key(&RecordExtract::multiple(&first_key, rec1));
                            let k2 = values_key(&RecordExtract::multiple(&second_key, rec2));
                            match k1.cmp(&k2) {
                                Ordering::Less => {
                                    if let Some(rec3) = join((Some(rec1), None)) {
                                        v.push(rec3);
                                    }
                                    orec1 = it1.next();
                                }
                                Ordering::Greater => {
                                    if let Some(rec3) = join((None, Some(rec2))) {
                                        v.push(rec3);
                                    }
                                    orec2 = it2.next();
                                }
                                Ordering::Equal => {
                                    if let Some(rec3) = join((Some(rec1), Some(rec2))) {
                                        v.push(rec3);
                                    }
                                    orec2 = it2.next();
                                }
                            }
                        } else {
                            if let Some(rec3) = join((Some(rec1), None)) {
                                v.push(rec3);
                            }
                            orec1 = it1.next();
                        }
                    } else if let Some(rec2) = &orec2 {
                        if let Some(rec3) = join((None, Some(rec2))) {
                            v.push(rec3);
                        }
                        orec2 = it2.next();
                    }
                }
                return Box::new(v.into_iter());
            }
        }
        Box::new(iter::empty::<EQLRecord>())
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
fn extract_from_value(value: Value, names: &HashSet<String>) -> Value {
    match value {
        Value::Object(mut m) => Value::Object(
            names
                .iter()
                .filter_map(|n| get_map_value(n, &mut m))
                .collect(),
        ),
        _ => value,
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

/// Builds a key from a list of values
/// The key is made of the serialized values separated by 0
fn values_key(values: &[Value]) -> Vec<u8> {
    let mut v = vec![];
    for v2 in values {
        v.append(&mut serde_json::to_vec(v2).unwrap());
        v.push(0);
    }
    v
}

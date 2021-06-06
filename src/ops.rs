use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
};
use anyhow::Result;
use thiserror::Error;


#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Script error in nested loops: {0}")]
    NestedLoopsError(String),
    #[error("Script error in hash lookup: {0}")]
    HashLookupError(String),
    #[error("Script error in merge: {0}")]
    MergeError(String),
    #[error("Script error in map: {0}")]
    MapError(String),
    #[error("Script error in reduce: {0}")]
    ReduceError(String),
    #[error("Parse error in scripted operation: {0}")]
    ParseError(String),
    #[error("Error converting value to scripting Dynamic: {0}")]
    DynamicError(String),
}

/// A specific operation on the data store
pub enum Operation<'a> {
    Scan {
        name: String,
    },
    KeyLookup {
        name: String,
        key: Value,
    },
    Extract {
        names: HashSet<String>,
        operation: Box<Operation<'a>>,
    },
    Augment {
        value: Value,
        operation: Box<Operation<'a>>,
    },
    IndexLookup {
        name: String,
        index_name: String,
        values: Vec<Value>,
        keys: Vec<String>,
    },
    NestedLoops {
        first: Box<Operation<'a>>,
        second: Box<dyn Fn(&EQLRecord) -> Result<Operation<'a>> + 'a>,
    },
    HashLookup {
        build: Box<Operation<'a>>,
        build_hash: RecordExtract,
        probe: Box<Operation<'a>>,
        probe_hash: RecordExtract,
        join: HashJoinFunction<'a>,
    },
    Merge {
        first: Box<Operation<'a>>,
        first_key: RecordExtract,
        second: Box<Operation<'a>>,
        second_key: RecordExtract,
        join: MergeJoinFunction<'a>,
    },
    Process {
        operation: Box<Operation<'a>>,
        process: Box<dyn Fn(Box<dyn Iterator<Item=EQLRecord> +'a>) -> Result<Box<dyn Iterator<Item=EQLRecord> +'a>> +'a>
    },
}

/// The underlying type for Hash join function
type HashJoinFunction<'a> = Box<dyn Fn((Option<&EQLRecord>, EQLRecord)) -> Result<Option<EQLRecord>> +'a>;
/// the underlying type for Merge join function
type MergeJoinFunction<'a> = Box<dyn Fn((Option<&EQLRecord>, Option<&EQLRecord>)) -> Result<Option<EQLRecord>> +'a>;

/// Builds an operation to scan a whole record type
/// # Arguments
/// * `name` - the name of the record type
///
pub fn scan<'a, N: Into<String>>(name: N) -> Operation<'a> {
    Operation::Scan { name: name.into() }
}

/// Builds an operation to perform a single key lookup
/// # Arguments
/// * `name` - the name of the record type
/// * `key` - the key
pub fn key_lookup<'a, N: Into<String>>(name: N, key: Value) -> Operation<'a> {
    Operation::KeyLookup {
        name: name.into(),
        key,
    }
}

/// Builds an operation to extract specific keys from the values returned by the wrapped operation
/// # Arguments
/// * `extract` - the names of the keys to extract, the others will be dropped
/// * `operation` - the wrapped operation
pub fn extract<'a>(extract: &[&str], operation: Operation<'a>) -> Operation<'a> {
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
pub fn augment<'a>(value: Value, operation: Operation<'a>) -> Operation <'a>{
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
pub fn index_lookup<'a,N: Into<String>, IN: Into<String>>(
    name: N,
    index_name: IN,
    values: Vec<Value>,
) -> Operation<'a> {
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
pub fn index_lookup_keys<'a, N: Into<String>, IN: Into<String>, OT: AsRef<str>>(
    name: N,
    index_name: IN,
    values: Vec<Value>,
    keys: Vec<OT>,
) -> Operation<'a> {
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
pub fn nested_loops<'a, F>(first: Operation<'a>, second: F) -> Operation<'a>
where
    F: Fn(&EQLRecord) -> Result<Operation<'a>> + 'a,
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
pub fn hash_lookup<'a,F>(
    build: Operation<'a>,
    build_hash: RecordExtract,
    probe: Operation<'a>,
    probe_hash: RecordExtract,
    join: F,
) -> Operation<'a>
where
    F: Fn((Option<&EQLRecord>, EQLRecord)) -> Result<Option<EQLRecord>> + 'a,
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
/// * `first_key` - builds the array of values that will be the key for the first records.
/// * `second` - the second operation
/// * `second_key` - builds the array of values that will be the key for the second records
/// * `join` - the function to join records from both operations. There may be only a first record, only a second record, or both.
/// The join uses key comparisons so expects the two sets of keys to be in the same order
pub fn merge<'a, F>(
    first: Operation<'a>,
    first_key: RecordExtract,
    second: Operation<'a>,
    second_key: RecordExtract,
    join: F,
) -> Operation<'a>
where
    F: Fn((Option<&EQLRecord>, Option<&EQLRecord>)) -> Result<Option<EQLRecord>> +'a,
{
    Operation::Merge {
        first: Box::new(first),
        first_key: first_key,
        second: Box::new(second),
        second_key: second_key,
        join: Box::new(join),
    }
}

/// # Builds an operation to transform an iterator over record into another
/// # Arguments
/// * `operation` - the underlying operation providing the original records
/// * `process` - the function to pass the iterator it
pub fn process<'a>(
    operation: Operation<'a>,
    process: Box<dyn Fn(Box<dyn Iterator<Item=EQLRecord> +'a>) -> Result<Box<dyn Iterator<Item=EQLRecord> +'a>> +'a>,
) -> Operation<'a>
//where F: Fn(Box<dyn Iterator<Item=EQLRecord> +'a>) -> Box<dyn Iterator<Item=EQLRecord> +'a> +'a, 
{
    Operation::Process{operation:Box::new(operation),process}
}

/// The metadata we keep track of
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Metadata {
    /// all the indices created: first key is record type, second is index name, the final value are the JSON pointers to index, in order
    pub indices: HashMap<String, HashMap<String, Vec<String>>>,
}

/// A record from an operation. Both keys and values are arbitrary JSON values, but some operations expect the values to be JSON objects
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EQLRecord {
    pub key: Value,
    pub value: Value,
}

impl EQLRecord {
    /// Creates a new record
    pub fn new(key: Value, value: Value) -> Self {
        EQLRecord { key, value }
    }

    pub fn empty() -> Self {
        EQLRecord { key:Value::Null, value:Value::Null }
    }

    pub fn is_empty(&self) -> bool {
        self.key.is_null() && self.value.is_null()
    }

    pub fn ensure_not_empty(self) -> Option<Self> {
        if self.is_empty(){
            None
        } else {
            Some(self)
        }
    }
}

impl Default for EQLRecord {
    fn default() -> Self {
        EQLRecord::empty()
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
    Function(Box<dyn Fn(&EQLRecord) -> Value>),
    /// Multiple extract
    Multiple(Vec<RecordExtract>),
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
    pub(crate) fn apply(&self, rec: &EQLRecord) -> Option<Value> {
        match self {
            RecordExtract::Key => Some(rec.key.clone()),
            RecordExtract::Value => Some(rec.value.clone()),
            RecordExtract::Pointer(p) => rec.value.pointer(p).map(|v| v.clone()),
            RecordExtract::Function(f) => {
                let v=f(rec);
                if v.is_null() {
                    return None;
                }
                Some(v)
            },
            RecordExtract::Multiple(v) => {
                let vs:Vec<Value>=v.iter().filter_map(|e| e.apply(rec)).collect();
                Some(Value::Array(vs))
            },
        }
    }

}
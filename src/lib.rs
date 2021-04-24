use rocksdb::{ColumnFamilyDescriptor, IteratorMode, Options, DB, ReadOptions, Direction};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::{
    collections::{HashMap, HashSet},
    fs::remove_file,
    iter,
};

use anyhow::Result;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    Scan {
        name: String,
    },
    KeyLookup {
        name: String,
        key: Box<[u8]>,
    },
    Extract {
        names: HashSet<String>,
        operation: Box<Operation>,
    },
    IndexLookup {
        name: String,
        index_name: String,
        values: Vec<Value>,
        keys: Vec<String>,
    }
}

impl Operation {
    pub fn scan<N: Into<String>>(name: N) -> Self {
        Operation::Scan { name: name.into() }
    }

    pub fn key_lookup<N: Into<String>>(name: N, key: Box<[u8]>) -> Self {
        Operation::KeyLookup { name: name.into(),key }
    }

    pub fn extract(extract: &[&str], operation: Operation) -> Self {
        let mut hs = HashSet::new();
        for e in extract.iter() {
            hs.insert((*e).into());
        }
        Operation::Extract {
            names: hs,
            operation: Box::new(operation),
        }
    }

    pub fn index_lookup<N: Into<String>,IN: Into<String>,OT: AsRef<str>>(name: N, index_name:IN, values: Vec<Value>,
        keys: Vec<OT>,) -> Self {
        Operation::IndexLookup{name:name.into(), index_name:index_name.into(), values, keys:keys.iter().map(|s| String::from(s.as_ref())).collect()}
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Metadata {
    pub indices: HashMap<String, HashMap<String, Vec<String>>>,
}

pub struct RocksDBEQL {
    db: DB,
    metadata_path: PathBuf,
    pub metadata: Metadata,
}

#[derive(Error, Debug)]
pub enum MetadataError {
    #[error("duplicate index {index_name} for record type {rec_type}")]
    DuplicateIndex {
        rec_type: String,
        index_name: String,
    },
}

impl RocksDBEQL {
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
        Ok(RocksDBEQL {
            db,
            metadata_path: mdp,
            metadata,
        })
    }

    pub fn destroy<P: AsRef<Path>>(path: P) -> Result<()> {
        let mdp = path.as_ref().join("metadata.json");
        if mdp.is_file() {
            remove_file(mdp)?;
        }
        DB::destroy(&Options::default(), path)?;
        Ok(())
    }

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

        self.execute(Operation::scan(rec_type.as_ref()))
            .try_for_each(|(k, v)| {
                let ix_key = index_key(&on, &k, &v);
                self.db.put_cf(cf1, ix_key, &k)
            })?;

        Ok(())
    }

    fn save_metadata(&self) -> Result<()> {
        let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(self.metadata_path.clone())?;
        serde_json::to_writer(BufWriter::new(file), &self.metadata)?;
        Ok(())
    }

    pub fn delete_index<T: AsRef<str>, IT: AsRef<str>>(
        &mut self,
        rec_type: T,
        idx_name: IT,
    ) -> Result<()> {
        if let Some(m) = self.metadata.indices.get_mut(rec_type.as_ref()){
            m.remove(idx_name.as_ref());
            let idx_cf = index_cf_name(rec_type.as_ref(), idx_name.as_ref());
            self.db.drop_cf(&idx_cf)?;

            self.save_metadata()?;
        }
        Ok(())
    }

    pub fn insert<T: AsRef<str>, K: AsRef<[u8]>>(
        &mut self,
        rec_type: T,
        key: K,
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
        //let cf=RocksDBMetadata::cf_handle(&mut self.db, rec_type)?;
        self.db
            .put_cf(cf, key.as_ref(), serde_json::to_vec(value).unwrap())?;

        if let Some(idxs) = self.metadata.indices.get(ref_type) {
            for (idx_name, on) in idxs.iter() {
                let idx_cf = index_cf_name(rec_type.as_ref(), idx_name);
                if let Some(cf1) = self.db.cf_handle(&idx_cf){
                    let ix_key = index_key(on, key.as_ref(), value);
                    self.db.put_cf(cf1, ix_key, key.as_ref())?;
                }
            }
        } else {
            self.metadata.indices.insert(String::from(ref_type),HashMap::new());
            self.save_metadata()?;
        }

        Ok(())
    }

    pub fn get<T: AsRef<str>, K: AsRef<[u8]>>(
        &mut self,
        rec_type: T,
        key: K,
    ) -> Result<Option<Value>> {
        let ref_type = rec_type.as_ref();
        let ocf1 = self.db.cf_handle(ref_type);
        if let Some(cf1) = ocf1 {
            Ok(self
                .db
                .get_cf(cf1, key)?
                .map(|v| serde_json::from_slice(&v).unwrap()))
        } else {
            Ok(None)
        }
    }

    pub fn delete<T: AsRef<str>, K: AsRef<[u8]>>(&mut self, rec_type: T, key: K) -> Result<()> {
        let ref_type = rec_type.as_ref();
        let ocf1 = self.db.cf_handle(ref_type);
        if let Some(cf1) = ocf1 {
            if let Some(idxs) = self.metadata.indices.get(ref_type) {
                if !idxs.is_empty() {
                    if let Some(value)=self
                        .db
                        .get_cf(cf1, key.as_ref())?
                        .map(|v| serde_json::from_slice(&v).unwrap()){
                        for (idx_name, on) in idxs.iter() {
                            let idx_cf = index_cf_name(rec_type.as_ref(), idx_name);
                            if let Some(cf) = self.db.cf_handle(&idx_cf){
                                let ix_key = index_key(on, key.as_ref(), &value);
                                self.db.delete_cf(cf, ix_key)?;
                            }
                        }
                    }
                }
            }
            self.db.delete_cf(cf1, key)?;
        }
        Ok(())
    }

    pub fn execute(
        &self,
        operation: Operation,
    ) -> Box<dyn Iterator<Item = (Box<[u8]>, Value)> + '_> {
        match operation {
            Operation::Scan { name } => {
                let ocf1 = self.db.cf_handle(&name);
                if let Some(cf1) = ocf1 {
                    let it = self
                        .db
                        .iterator_cf(cf1, IteratorMode::Start)
                        .map(|(k, v)| (k, serde_json::from_slice::<Value>(&v).unwrap()));
                    return Box::new(it);
                }
            },
            Operation::KeyLookup{name, key} => {
                let ocf1 = self.db.cf_handle(&name);
                if let Some(cf1) = ocf1 {
                    let v=self.db.get_cf(cf1, &key).unwrap().map(|v| (key, serde_json::from_slice::<Value>(&v).unwrap()));
                    return Box::new(v.into_iter());
                }
            },
            Operation::Extract {
                names,
                operation: b_op,
            } => {
                return Box::new(
                    self.execute(*b_op)
                        .map(move |(k, v)| (k, extract_from_value(v, &names))),
                );
            },
            Operation::IndexLookup {
                name, index_name, values, keys
            } => {
                let idx_cf=index_cf_name(&name, &index_name);
                if let Some(cf) = self.db.cf_handle(&idx_cf) {
                    let mut v = vec![];
                   
                    for o in values.iter() {
                        v.append(&mut serde_json::to_vec(o).unwrap());
                        v.push(0);
                    }
                    let mut opts = ReadOptions::default();
                    let mut u=v.clone();
                    u.pop();
                    u.push(1);
                    opts.set_iterate_upper_bound(u);
                    let mode = IteratorMode::From(&v.as_ref(), Direction::Forward);
                    let it=self.db.iterator_cf_opt(cf, opts,mode)
                        .map(move |(k, v)| (v,extract_from_index_key(&k, &keys)));
                    return Box::new(it);
                }
            }
        }
        Box::new(iter::empty::<(Box<[u8]>, Value)>())
    }
}

fn extract_from_index_key<K:AsRef<[u8]>>(k: K, keys: &[String]) -> Value {
    let mut im:HashMap<String,Value>=HashMap::new();
    for (part,name) in k.as_ref().split(|u| *u==0).zip(keys.iter()) {
        if !name.is_empty() {
            im.insert(name.clone(),serde_json::from_slice(part).unwrap());
        }
    }

    serde_json::to_value(im).unwrap()
}

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

fn get_map_value(n: &str, m: &mut Map<String, Value>) -> Option<(String, Value)> {
    m.remove(n).map(|v| (String::from(n), v))
}

fn index_cf_name(ref_type: &str, index_name: &str) -> String {
    format!("#idx_{}_{}", ref_type, index_name)
}

fn index_key<T: AsRef<str>, K: AsRef<[u8]>>(on: &[T], key: K, value: &Value) -> Vec<u8> {
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

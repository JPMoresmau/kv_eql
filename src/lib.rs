use rocksdb::{ColumnFamilyDescriptor, DB, Direction, IteratorMode, Options, ReadOptions, WriteBatch};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{collections::BTreeMap, fs::{File, OpenOptions}};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::{
    collections::{HashMap, HashSet},
    fs::remove_file,
    iter,
};

use anyhow::{ Result};
use thiserror::Error;

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
        build_hash: Box<dyn Fn(&EQLRecord) -> Option<Value>>,
        probe: Box<Operation>,
        probe_hash: Box<dyn Fn(&EQLRecord) -> Option<Value>>,
        join: Box<dyn Fn((Option<&EQLRecord>,EQLRecord)) -> Option<EQLRecord>>,
    }
}


pub fn scan<N: Into<String>>(name: N) -> Operation {
    Operation::Scan { name: name.into() }
}

pub fn key_lookup<N: Into<String>>(name: N, key: Value) -> Operation {
    Operation::KeyLookup { name: name.into(),key }
}

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

pub fn augment(value: Value, operation: Operation) -> Operation {
    Operation::Augment{value, operation: Box::new(operation),}
}

pub fn index_lookup<N: Into<String>,IN: Into<String>>(name: N, index_name:IN, values: Vec<Value>) -> Operation {
    Operation::IndexLookup{name:name.into(), index_name:index_name.into(), values, keys:vec![]}
}

pub fn index_lookup_keys<N: Into<String>,IN: Into<String>,OT: AsRef<str>>(name: N, index_name:IN, values: Vec<Value>,
    keys: Vec<OT>,) -> Operation {
    Operation::IndexLookup{name:name.into(), index_name:index_name.into(), values, keys:keys.iter().map(|s| String::from(s.as_ref())).collect()}
}

pub fn nested_loops<F>(first: Operation, second: F) -> Operation 
    where F: Fn(&EQLRecord) -> Operation + 'static {
    Operation::NestedLoops {first:Box::new(first),second:Box::new(second)}
}

pub fn hash_lookup<F1,F2,F3>(build: Operation, build_hash: F1, probe: Operation, probe_hash: F2, join: F3) -> Operation 
    where F1: Fn(&EQLRecord) -> Option<Value>+ 'static, F2: Fn(&EQLRecord) -> Option<Value> + 'static,
            F3: Fn((Option<&EQLRecord>,EQLRecord)) -> Option<EQLRecord> + 'static  {
    Operation::HashLookup{build:Box::new(build),build_hash:Box::new(build_hash),probe:Box::new(probe)
            , probe_hash:Box::new(probe_hash), join: Box::new(join)}
}



#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Metadata {
    pub indices: HashMap<String, HashMap<String, Vec<String>>>,
}


#[derive(Debug)]
pub struct EQLRecord {
    pub key: Value,
    pub value: Value,
}

impl EQLRecord {
    pub fn new(
        key: Value,
        value: Value) -> Self {
            EQLRecord{key,value}
        }
}

#[derive(Error, Debug)]
pub enum MetadataError {
    #[error("duplicate index {index_name} for record type {rec_type}")]
    DuplicateIndex {
        rec_type: String,
        index_name: String,
    },
}

#[derive(Default)]
pub struct EQLBatch {
    batch: WriteBatch,
}

pub struct EQLDB {
    db: DB,
    metadata_path: PathBuf,
    pub metadata: Metadata,
}


impl EQLDB {
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

        self.execute(scan(rec_type.as_ref()))
            .try_fold(WriteBatch::default(), |mut b, rec| {
                let kv=serde_json::to_vec(&rec.key).unwrap();
                let ix_key = index_key(&on, &kv, &rec.value);
                
                b.put_cf(cf1, ix_key, &kv);
                if b.len()>1000 {
                    self.db.write(b)?;
                    return Ok(WriteBatch::default());
                }
                let r:Result<WriteBatch,rocksdb::Error>=Ok(b);
                r
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

    pub fn insert<T: AsRef<str>, V: Into<Value>>(
        &mut self,
        rec_type: T,
        key: V,
        value: &Value,
    ) -> Result<()> {
        let mut batch=EQLBatch::default();
        self.batch_insert(&mut batch,rec_type, key,value)?;
        self.db.write(batch.batch)?;
        Ok(())
    }

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
        batch.batch.put_cf(cf, kv.clone(), serde_json::to_vec(value).unwrap());

        if let Some(idxs) = self.metadata.indices.get(ref_type) {
            for (idx_name, on) in idxs.iter() {
                let idx_cf = index_cf_name(rec_type.as_ref(), idx_name);
                if let Some(cf1) = self.db.cf_handle(&idx_cf){
                    let ix_key = index_key(on, &kv, value);
                    batch.batch.put_cf(cf1, ix_key, kv.clone());
                }
            }
        } else {
            self.metadata.indices.insert(String::from(ref_type),HashMap::new());
            self.save_metadata()?;
        }

        Ok(())
    }


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

    pub fn delete<T: AsRef<str>, V: Into<Value>>(&mut self, rec_type: T, key: V) -> Result<()> {
        let mut batch=EQLBatch::default();
        self.batch_delete(&mut batch,rec_type, key)?;
        self.db.write(batch.batch)?;
        Ok(())
    }

    pub fn batch_delete<T: AsRef<str>, V: Into<Value>>(&mut self,batch: &mut EQLBatch, rec_type: T, key: V) -> Result<()> {
        let ref_type = rec_type.as_ref();
        let ocf1 = self.db.cf_handle(ref_type);
        if let Some(cf1) = ocf1 {
            let kv=serde_json::to_vec(&key.into()).unwrap();
            if let Some(idxs) = self.metadata.indices.get(ref_type) {
                if !idxs.is_empty() {
                    if let Some(value)=self
                        .db
                        .get_cf(cf1, kv.clone())?
                        .map(|v| serde_json::from_slice(&v).unwrap()){
                        for (idx_name, on) in idxs.iter() {
                            let idx_cf = index_cf_name(rec_type.as_ref(), idx_name);
                            if let Some(cf) = self.db.cf_handle(&idx_cf){
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

    pub fn write(&self, batch: EQLBatch) -> Result<()> {
        self.db.write(batch.batch)?;
        Ok(())
    }

    pub fn execute(
        &self,
        operation: Operation,
    ) -> Box<dyn Iterator<Item =EQLRecord> + '_> {
        match operation {
            Operation::Scan { name } => {
                let ocf1 = self.db.cf_handle(&name);
                if let Some(cf1) = ocf1 {
                    let it = self
                        .db
                        .iterator_cf(cf1, IteratorMode::Start)
                        .map(|(k, v)| EQLRecord::new(serde_json::from_slice::<Value>(&k).unwrap(), serde_json::from_slice::<Value>(&v).unwrap()));
                    return Box::new(it);
                }
            },
            Operation::KeyLookup{name, key} => {
                let ocf1 = self.db.cf_handle(&name);
                if let Some(cf1) = ocf1 {
                    let rk=serde_json::to_vec(&key).unwrap();
                    let v=self.db.get_cf(cf1, &rk).unwrap().map(|v| EQLRecord::new(key, serde_json::from_slice::<Value>(&v).unwrap()));
                    return Box::new(v.into_iter());
                }
            },
            Operation::Extract {
                names,
                operation: b_op,
            } => {
                return Box::new(
                    self.execute(*b_op)
                        .map(move |rec| EQLRecord{value:extract_from_value(rec.value, &names),..rec}),
                );
            },
            Operation::Augment{
                value,
                operation: b_op
            } => {
                return Box::new(
                    self.execute(*b_op)
                        .map(move |rec| EQLRecord{value:merge_values(&value, rec.value),..rec}),
                );
            },
            Operation::IndexLookup {
                name, index_name, values, keys
            } => {
                let idx_cf=index_cf_name(&name, &index_name);
                if let Some(cf) = self.db.cf_handle(&idx_cf) {
                    if values.is_empty(){
                        let it=self.db.iterator_cf(cf,IteratorMode::Start)
                            .map(move |(k, v)| EQLRecord::new(serde_json::from_slice(&v).unwrap(),extract_from_index_key(&k, &keys)));
                        return Box::new(it);
                    } else {
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
                            .map(move |(k, v)| EQLRecord::new(serde_json::from_slice(&v).unwrap(),extract_from_index_key(&k, &keys)));
                        return Box::new(it);
                    }
                }
            },
            Operation::NestedLoops{first,second} => {
                return Box::new(
                    self.execute(*first)
                        .flat_map(move |rec| self.execute(second(&rec))),
                );
            },
            Operation::HashLookup{build,build_hash,probe,probe_hash,join} => {
                let map:HashMap<String,EQLRecord> = 
                    self.execute(*build).flat_map(|rec| build_hash(&rec).map(|s| (format!("{}",s),rec))).collect();
                return Box::new(self.execute(*probe)
                    .flat_map(move |rec| {
                       probe_hash(&rec).map(|h| {
                            let hash = format!("{}",h);
                            join((map.get(&hash),rec))
                        }).flatten()
                    }))
            }
        }
        Box::new(iter::empty::<EQLRecord>())
    }
}

fn merge_values(first: &Value, mut second: Value) -> Value {
    if let Some(m1) = first.as_object() {
        if !m1.is_empty(){
            if let Some(m2) = second.as_object_mut() {
                for (k,v) in m1.iter(){
                    if !m2.contains_key(k){
                        m2.insert(k.clone(), v.clone());
                    }
                }
            }
        }
    }
    second
}

fn extract_from_index_key<K:AsRef<[u8]>>(k: K, keys: &[String]) -> Value {
    let mut im:BTreeMap<String,Value>=BTreeMap::new();
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

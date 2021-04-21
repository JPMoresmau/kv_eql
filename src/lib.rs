use std::{collections::{HashMap, HashSet}, iter, slice};
use rocksdb::{ColumnFamily, DB, Error, IteratorMode, Options};
use serde_json::{Map, Value};


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    Scan {
        name: String
    },
    Extract {
        names: HashSet<String>,
        operation: Box<Operation>,
    }
}

impl Operation {
    pub fn scan<N: Into<String>>(name: N) -> Self {
        Operation::Scan{name: name.into()}
    }

    pub fn extract(extract: &[&str], operation: Operation) -> Self {
        let mut hs=HashSet::new();
        for e in extract.iter() {
            hs.insert((*e).into());
        }
        Operation::Extract{names:hs, operation:Box::new(operation)}
    }
}

/*trait Record<F> {

    fn name<'a>(&'a self) -> &'a str;

    fn extract<D:AsRef<[u8]>>(&self, data: D, extract: Extract) -> HashMap<String,F>;

}*/

pub struct RocksDBMetadata {
    db: DB,
    //columns: HashMap<String,ColumnFamily>,
}

impl RocksDBMetadata {

    pub fn new(db: DB) -> Self {
        RocksDBMetadata {
            db//, columns:HashMap::new()
        }
    }

    /*fn cf_handle<'a, T: AsRef<str>>(db: &'a mut DB,rec_type: T) -> Result<&'a ColumnFamily,Error> {
        let ref_type=rec_type.as_ref();
        let ocf1= db.cf_handle(ref_type);
        Ok(if ocf1.is_none(){
            db.create_cf(ref_type, &Options::default())?;
            let cf1=db.cf_handle(ref_type).unwrap();
            cf1
        } else {
            ocf1.unwrap()
        })
    }*/

    pub fn insert<T: AsRef<str>, K: AsRef<[u8]>>(&mut self, rec_type: T, key: K, value: &Value) -> Result<(),Error>{
        let ref_type=rec_type.as_ref();
        let ocf1= self.db.cf_handle(ref_type);
        let cf=if ocf1.is_none(){
            self.db.create_cf(ref_type, &Options::default())?;
            let cf1=self.db.cf_handle(ref_type).unwrap();
            cf1
        } else {
            ocf1.unwrap()
        };
        //let cf=RocksDBMetadata::cf_handle(&mut self.db, rec_type)?;
        self.db.put_cf(cf, key, serde_json::to_vec(value).unwrap())
    }

    pub fn get<T: AsRef<str>, K: AsRef<[u8]>>(&mut self, rec_type: T, key: K) -> Result<Option<Value>,Error>{
        let ref_type=rec_type.as_ref();
        let ocf1= self.db.cf_handle(ref_type);
        if let Some(cf1) = ocf1 {
            self.db.get_cf(cf1, key).map(|ov| ov.map(|v| serde_json::from_slice(&v).unwrap()))
        } else {
            Ok(None)
        }
    }

    pub fn delete<T: AsRef<str>, K: AsRef<[u8]>>(&mut self, rec_type: T, key: K) -> Result<(),Error>{
        let ref_type=rec_type.as_ref();
        let ocf1= self.db.cf_handle(ref_type);
        if let Some(cf1) = ocf1 {
            self.db.delete_cf(cf1, key)?;
        }
        Ok(())
    }

    pub fn execute(&mut self, operation: Operation) -> Box<dyn Iterator<Item=(Box<[u8]>,Value)>+ '_> {
        match operation {
            Operation::Scan{name} => {
                let ocf1= self.db.cf_handle(&name);
                if let Some(cf1) = ocf1 {
                    let it=self.db.iterator_cf(cf1, IteratorMode::Start)
                        .map(|(k,v)| (k,serde_json::from_slice::<Value>(&v).unwrap()));
                    return Box::new(it);
                }
            },
            Operation::Extract { names, operation: b_op } => {
                return Box::new(self.execute(*b_op).map(move |(k,v)| (k,extract_from_value(v,&names))));
            },
        }
        Box::new(iter::empty::<(Box<[u8]>,Value)>())
    }
}

fn extract_from_value(value: Value, names: &HashSet<String>) -> Value {
    match value {
        Value::Object(mut m) => Value::Object(names.iter().filter_map(|n| get_map_value(n,&mut m)).collect()),
        _ => value,
    }
}

fn get_map_value(n: &String, m: &mut Map<String,Value>) -> Option<(String,Value)>{
    if let Some(v) = m.remove(n) {
        Some((n.clone(),v))
    } else {
        None
    }
}
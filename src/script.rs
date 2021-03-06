use std::collections::HashSet;

use crate::ops::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value};
use rhai::{Array, Dynamic, Engine, ImmutableString, Scope, serde::{from_dynamic, to_dynamic}};

use anyhow::Result;

/// Indicates how to extract information from a record, using a script for dynamic operations
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScriptedRecordExtract {
    /// Extracts the key
    Key,
    /// Extracts the full value
    Value,
    /// Extracts the result of the given pointer on the value
    Pointer(String),
    /// Arbitrary function to retrieve a value
    Script(String),
    /// Multiple extraction
    Multiple(Vec<ScriptedRecordExtract>),
  }
  
impl ScriptedRecordExtract {
      /// Create a pointer extraction
      /// # Arguments
      /// * `pointer` - the JSON pointer expression
      pub fn pointer<N: Into<String>>(pointer: N) -> Self{
        ScriptedRecordExtract::Pointer(pointer.into())
     }
  
      /// Create a pointer extraction
      /// # Arguments
      /// * `pointer` - the JSON pointer expression
      pub fn script<N: Into<String>>(script: N) -> Self{
        ScriptedRecordExtract::Script(script.into())
     }
  
      /// Convert a scripted record extraction into an executable one
      pub fn into_rust(self) -> Result<RecordExtract> {
        match self {
          ScriptedRecordExtract::Key => Ok(RecordExtract::Key),
          ScriptedRecordExtract::Value => Ok(RecordExtract::Value),
          ScriptedRecordExtract::Pointer(str) => Ok(RecordExtract::Pointer(str)),
          ScriptedRecordExtract::Script(str) => {
            let engine = Engine::new();
            let ast = engine.compile(&str)?;
            Ok(RecordExtract::Function(Box::new(move |rec| {
              let engine = Engine::new();
              let mut scope = Scope::new();
              scope.push_constant_dynamic("rec", to_dynamic(rec).unwrap());
              match engine.eval_ast_with_scope::<Dynamic>(&mut scope, &ast){
                Ok(ov)=>from_dynamic(&ov).unwrap(),
                Err(e)=>{
                  eprintln!("Error while running record extraction script: {}",e);
                  Value::Null
                },
              }
            })))
          },
          ScriptedRecordExtract::Multiple(v)=> {
            let mut vs=vec![];
            for sor in v.into_iter(){
              vs.push(sor.into_rust()?);
            }
            Ok(RecordExtract::Multiple(vs))
          },
        }
      }
  }

/// A specific operation on the data store, with scripts for dynamic operation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScriptedOperation {
  Scan {
      name: String,
  },
  KeyLookup {
      name: String,
      key: Value,
  },
  Extract {
      names: HashSet<String>,
      operation: Box<ScriptedOperation>,
  },
  Augment {
      value: Value,
      operation: Box<ScriptedOperation>,
  },
  IndexLookup {
      name: String,
      index_name: String,
      values: Vec<Value>,
      keys: Vec<String>,
  },
  NestedLoops {
      first: Box<ScriptedOperation>,
      second: String,
  },
  HashJoin {
      build: Box<ScriptedOperation>,
      build_hash: ScriptedRecordExtract,
      probe: Box<ScriptedOperation>,
      probe_hash: ScriptedRecordExtract,
      join: String,
  },
  Merge {
      first: Box<ScriptedOperation>,
      first_key: ScriptedRecordExtract,
      second: Box<ScriptedOperation>,
      second_key: ScriptedRecordExtract,
      join: String,
  },
  Map {
      operation: Box<ScriptedOperation>,
      process: String
  },
  Reduce {
    operation: Box<ScriptedOperation>,
    process: String
},
}

pub fn eql_engine() -> Engine {
  let mut engine = Engine::new();
  engine.register_result_fn("scan",|str: ImmutableString| to_dynamic(ScriptedOperation::Scan{name:str.into_owned()}));
  engine.register_result_fn("key_lookup",|str: ImmutableString, key: Dynamic| to_dynamic(ScriptedOperation::KeyLookup{name:str.into_owned(), key:from_dynamic::<Value>(&key)?}));
  engine.register_result_fn("extract",|names: Dynamic, op: Dynamic| to_dynamic(ScriptedOperation::Extract{names:from_dynamic(&names)?,operation:Box::new(from_dynamic(&op)?)}));
  engine.register_result_fn("augment",|value: Dynamic, op: Dynamic| to_dynamic(ScriptedOperation::Augment{value:from_dynamic(&value)?,operation:Box::new(from_dynamic(&op)?)}));
  engine.register_result_fn("index_lookup",|name: ImmutableString, index: ImmutableString, values: Dynamic| to_dynamic(ScriptedOperation::IndexLookup{name:name.into_owned(),index_name:index.into_owned(),values:from_dynamic(&values)?,keys:vec![]}));
  engine.register_result_fn("index_lookup",|name: ImmutableString, index: ImmutableString, values: Dynamic, keys: Dynamic| to_dynamic(ScriptedOperation::IndexLookup{name:name.into_owned(),index_name:index.into_owned(),values:from_dynamic(&values)?,keys:from_dynamic(&keys)?}));
  engine.register_result_fn("nested_loops",|op: Dynamic,second: ImmutableString| to_dynamic(ScriptedOperation::NestedLoops{first:Box::new(from_dynamic(&op)?),second:second.into_owned()}));
  engine.register_result_fn("hash_join",|build: Dynamic,build_hash: Dynamic, probe: Dynamic, probe_hash: Dynamic, join: ImmutableString| to_dynamic(ScriptedOperation::HashJoin{build:Box::new(from_dynamic(&build)?),build_hash:from_dynamic(&build_hash)?,probe:Box::new(from_dynamic(&probe)?),probe_hash:from_dynamic(&probe_hash)?,join:join.into_owned()}));
  engine.register_result_fn("merge",|first: Dynamic,first_key: Dynamic, second: Dynamic, second_key: Dynamic, join: ImmutableString| to_dynamic(ScriptedOperation::Merge{first:Box::new(from_dynamic(&first)?),first_key:from_dynamic(&first_key)?,second:Box::new(from_dynamic(&second)?),second_key:from_dynamic(&second_key)?,join:join.into_owned()}));
  engine.register_result_fn("map",|op: Dynamic,process: ImmutableString| to_dynamic(ScriptedOperation::Map{operation:Box::new(from_dynamic(&op)?),process:process.into_owned()}));
  engine.register_result_fn("reduce",|op: Dynamic,process: ImmutableString| to_dynamic(ScriptedOperation::Reduce{operation:Box::new(from_dynamic(&op)?),process:process.into_owned()}));
  engine.register_result_fn("empty_record", || to_dynamic(EQLRecord::empty()));
  engine
}

/// convert a value to a Dynamic, returning an anhow error
fn eql_to_dynamic<T>(value: T) -> Result<Dynamic>
where
    T: Serialize,
{
  to_dynamic(value).map_err(|e| QueryError::DynamicError(format!("{}",e)).into())
}

impl ScriptedOperation {
    /// Convert a scripted record extraction into an executable one
    pub fn into_rust<'a>(self, engine: &'a Engine) -> Result<Operation<'a>> {
      match self {
          ScriptedOperation::Scan{name}=>Ok(Operation::Scan{name}),
          ScriptedOperation::KeyLookup{name, key}=>Ok(Operation::KeyLookup{name,key}),
          ScriptedOperation::Extract{names,operation}=>operation.into_rust(engine).map(|op| Operation::Extract{names,operation:Box::new(op)}),
          ScriptedOperation::Augment{value,operation}=>operation.into_rust(engine).map(|op| Operation::Augment{value,operation:Box::new(op)}),
          ScriptedOperation::IndexLookup{name,index_name, values, keys}=>Ok(Operation::IndexLookup{name,index_name,values,keys}),
          ScriptedOperation::NestedLoops{first,second}=>first.into_rust(engine).and_then(|op| {
            let ast = engine.compile(&second)?;
            Ok(Operation::NestedLoops{first:Box::new(op),second:Box::new(move |rec|{
              let mut scope = Scope::new();
              scope.push_constant_dynamic("rec", eql_to_dynamic(rec)?);
              match engine.eval_ast_with_scope::<Dynamic>(&mut scope, &ast)
                .and_then(|d| from_dynamic::<ScriptedOperation>(&d)){
                Ok(sop)=>match sop.into_rust(engine){
                  Ok(op)=>Ok(op),
                  Err(e)=>Err(QueryError::NestedLoopsError(format!("{}",e)).into()),
                },
                Err(e)=>Err(QueryError::NestedLoopsError(format!("{}",e)).into()),
              }
                            
            })})
          }),
          ScriptedOperation::HashJoin{build,build_hash,probe,probe_hash,join}=>{
            let op1=build.into_rust(engine)?;
            let s1=build_hash.into_rust()?;
            let op2=probe.into_rust(engine)?;
            let s2=probe_hash.into_rust()?;
            let ast = engine.compile(&join)?;

            Ok(Operation::HashJoin{build:Box::new(op1),build_hash:s1,probe:Box::new(op2),probe_hash:s2,join:Box::new(move |(rec1,rec2)|{
              let mut scope = Scope::new();
              let e=EQLRecord::empty();
              scope.push_constant_dynamic("build", eql_to_dynamic(rec1.unwrap_or(&e))?);
              scope.push_dynamic("probe", eql_to_dynamic(rec2)?);
              match engine.eval_ast_with_scope::<Dynamic>(&mut scope, &ast)
                .and_then(|d| from_dynamic::<EQLRecord>(&d)){
                Ok(sop)=>Ok(sop.ensure_not_empty()),
                Err(e)=>Err(QueryError::HashLookupError(format!("{}",e)).into()),
              }
            })})
            }
            ,
          ScriptedOperation::Merge{first, first_key,second,second_key,join}=>{
            let op1=first.into_rust(engine)?;
            let k1=first_key.into_rust()?;
            let op2=second.into_rust(engine)?;
            let k2=second_key.into_rust()?;
            let ast = engine.compile(&join)?;

            Ok(Operation::Merge{first:Box::new(op1),first_key:k1,second:Box::new(op2),second_key:k2,join:Box::new(move |(rec1,rec2)|{
              let mut scope = Scope::new();
              let e=EQLRecord::empty();
              scope.push_constant_dynamic("rec1", eql_to_dynamic(rec1.unwrap_or(&e))?);
              scope.push_constant_dynamic("rec2", eql_to_dynamic(rec2.unwrap_or(&e))?);
              match engine.eval_ast_with_scope::<Dynamic>(&mut scope, &ast)
              .and_then(|d| from_dynamic::<EQLRecord>(&d)){
                Ok(sop)=>Ok(sop.ensure_not_empty()),
                Err(e)=>Err(QueryError::MergeError(format!("{}",e)).into()),
              }
            })})
          },
          ScriptedOperation::Map{operation,process}=>{
            let op1=operation.into_rust(engine)?;
            let ast = engine.compile(&process)?;
            Ok(Operation::Process{operation:Box::new(op1),process:Box::new(move |it|{
              let mut scope = Scope::new();
              let v=it.map(|rec| {
                scope.push_dynamic("rec", eql_to_dynamic(rec)?);
                match engine.eval_ast_with_scope::<Dynamic>(&mut scope, &ast)
                .and_then(|d| from_dynamic::<EQLRecord>(&d)){
                  Ok(sop)=>Ok(sop),
                  Err(e)=>Err(QueryError::MapError(format!("{}",e)).into()),
                }
              }).collect::<Result<Vec<EQLRecord>>>()?;
              Ok(Box::new(v.into_iter()))
            })})
          },
          ScriptedOperation::Reduce{operation,process}=>{
            let op1=operation.into_rust(engine)?;
            let ast = engine.compile(&process)?;
            Ok(Operation::Process{operation:Box::new(op1),process:Box::new(move |it|{
              let mut scope = Scope::new();
              scope.push_constant("recs",it.map(|e| eql_to_dynamic(e)).collect::<Result<Array>>()?);
              scope.push_dynamic("rec",eql_to_dynamic(EQLRecord::empty())?);
              match engine.eval_ast_with_scope::<Dynamic>(&mut scope, &ast)
               {
                  Ok(_)=>{
                    match from_dynamic::<EQLRecord>(&scope.get_value::<Dynamic>("rec").unwrap()){
                      Ok(res)=> Ok(Box::new(std::iter::once(res))),
                      Err(e)=>Err(QueryError::ReduceError(format!("{}",e)).into()),
                    }
                   
                  },
                  Err(e)=>Err(QueryError::ReduceError(format!("{}",e)).into()),
                }              
            })})
          },
      }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json};

    #[test]
    fn test_record_extract_script()-> Result<()> {
        let script=r#"rec.value.k1 + rec.value.k2"#;
        let sr=ScriptedRecordExtract::script(script);
        let r=sr.into_rust()?;
        let rec=EQLRecord{key:json!("key1"),value:json!({"k1":"v1","k2":"v2"})};
        let rec2=r.apply(&rec);
        assert_eq!(Some(json!["v1v2"]),rec2);
        Ok(())
    }

}
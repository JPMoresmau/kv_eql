use std::collections::HashSet;

use crate::ops::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value};
use rhai::{Dynamic, Engine, Scope, serde::{from_dynamic, to_dynamic}};

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
  HashLookup {
      build: Box<ScriptedOperation>,
      build_hash: ScriptedRecordExtract,
      probe: Box<ScriptedOperation>,
      probe_hash: ScriptedRecordExtract,
      join: String,
  },
  Merge {
      first: Box<ScriptedOperation>,
      first_key: Vec<ScriptedRecordExtract>,
      second: Box<ScriptedOperation>,
      second_key: Vec<ScriptedRecordExtract>,
      join: String,
  },
  Process {
      operation: Box<ScriptedOperation>,
      process: String
  },
}

impl ScriptedOperation {
    /// Convert a scripted record extraction into an executable one
    pub fn into_rust<'a>(self) -> Result<Operation<'a>> {
      match self {
          ScriptedOperation::Scan{name}=>Ok(Operation::Scan{name}),
          ScriptedOperation::KeyLookup{name, key}=>Ok(Operation::KeyLookup{name,key}),
          ScriptedOperation::Extract{names,operation}=>operation.into_rust().map(|op| Operation::Extract{names,operation:Box::new(op)}),
          ScriptedOperation::Augment{value,operation}=>operation.into_rust().map(|op| Operation::Augment{value,operation:Box::new(op)}),
          ScriptedOperation::IndexLookup{name,index_name, values, keys}=>Ok(Operation::IndexLookup{name,index_name,values,keys}),
          ScriptedOperation::NestedLoops{first,second}=>first.into_rust().and_then(|op| {
            let engine = Engine::new();
            let ast = engine.compile(&second)?;
            Ok(Operation::NestedLoops{first:Box::new(op),second:Box::new(move |rec|{
              let engine = Engine::new();
              let mut scope = Scope::new();
              scope.push_constant_dynamic("rec", to_dynamic(rec).unwrap());
              match engine.eval_ast_with_scope::<Dynamic>(&mut scope, &ast)
                .and_then(|d| from_dynamic::<ScriptedOperation>(&d)){
                Ok(sop)=>match sop.into_rust(){
                  Ok(op)=>Ok(op),
                  Err(e)=>Err(QueryError::NestedLoopsError(format!("{}",e)).into()),
                },
                Err(e)=>Err(QueryError::NestedLoopsError(format!("{}",e)).into()),
              }
                            
            })})
          }),
          ScriptedOperation::HashLookup{build,build_hash,probe,probe_hash,join}=>{
            let op1=build.into_rust()?;
            let s1=build_hash.into_rust()?;
            let op2=probe.into_rust()?;
            let s2=probe_hash.into_rust()?;
            let engine = Engine::new();
            let ast = engine.compile(&join)?;

            Ok(Operation::HashLookup{build:Box::new(op1),build_hash:s1,probe:Box::new(op2),probe_hash:s2,join:Box::new(move |(rec1,rec2)|{
              let engine = Engine::new();
              let mut scope = Scope::new();
              let e=EQLRecord::empty();
              scope.push_constant_dynamic("build", to_dynamic(rec1.unwrap_or(&e)).unwrap());
              scope.push_constant_dynamic("probe", to_dynamic(rec2).unwrap());
              match engine.eval_ast_with_scope::<Dynamic>(&mut scope, &ast)
                .and_then(|d| from_dynamic::<EQLRecord>(&d)){
                Ok(sop)=>Ok(sop.ensure_not_empty()),
                Err(e)=>Err(QueryError::HashLookupError(format!("{}",e)).into()),
              }
            })})
            }
            ,
          ScriptedOperation::Merge{first, first_key,second,second_key,join}=>{
            let op1=first.into_rust()?;
            let k1=first_key.into_iter().map(|s| s.into_rust()).collect::<Result<Vec<RecordExtract>>>()?;
            let op2=second.into_rust()?;
            let k2=second_key.into_iter().map(|s| s.into_rust()).collect::<Result<Vec<RecordExtract>>>()?;
            let engine = Engine::new();
            let ast = engine.compile(&join)?;

            Ok(Operation::Merge{first:Box::new(op1),first_key:k1,second:Box::new(op2),second_key:k2,join:Box::new(move |(rec1,rec2)|{
              let engine = Engine::new();
              let mut scope = Scope::new();
              let e=EQLRecord::empty();
              scope.push_constant_dynamic("rec1", to_dynamic(rec1.unwrap_or(&e)).unwrap());
              scope.push_constant_dynamic("rec2", to_dynamic(rec2.unwrap_or(&e)).unwrap());
              match engine.eval_ast_with_scope::<Dynamic>(&mut scope, &ast)
              .and_then(|d| from_dynamic::<EQLRecord>(&d)){
                Ok(sop)=>Ok(sop.ensure_not_empty()),
                Err(e)=>Err(QueryError::MergeError(format!("{}",e)).into()),
              }
            })})
          },
          ScriptedOperation::Process{operation,process}=>{
            let op1=operation.into_rust()?;
            let engine = Engine::new();
            let ast = engine.compile(&process)?;
            Ok(Operation::Process{operation:Box::new(op1),process:Box::new(move |it|{
              let engine = Engine::new();
              let mut scope = Scope::new();
              let v=it.map(|rec| {
                scope.push_constant_dynamic("rec", to_dynamic(rec).unwrap());
                match engine.eval_ast_with_scope::<Dynamic>(&mut scope, &ast)
                .and_then(|d| from_dynamic::<EQLRecord>(&d)){
                  Ok(sop)=>Ok(sop),
                  Err(e)=>Err(QueryError::MergeError(format!("{}",e)).into()),
                }
              }).collect::<Result<Vec<EQLRecord>>>()?;
              Ok(Box::new(v.into_iter()))
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
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
                  Ok(op)=>op,
                  Err(e)=>Operation::Error{error:QueryError::NestedLoopsError(format!("{}",e))},
                },
                Err(e)=>Operation::Error{error:QueryError::NestedLoopsError(format!("{}",e))},
              }
                            
            })})
          }),
          ScriptedOperation::HashLookup{build,build_hash,probe,probe_hash,join}=>unimplemented!(),
          ScriptedOperation::Merge{first, first_key,second,second_key,join}=>unimplemented!(),
          ScriptedOperation::Process{operation,process}=>unimplemented!(),
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
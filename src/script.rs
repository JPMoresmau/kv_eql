use crate::ops::*;
use serde_json::{Value};
use rhai::{Dynamic, Engine, Scope, serde::{from_dynamic, to_dynamic}};

use anyhow::Result;

/// Indicates how to extract information from a record, using a script for dynamic operations
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
      pub fn into_raw(self) -> Result<RecordExtract> {
        match self {
          ScriptedRecordExtract::Key => Ok(RecordExtract::Key),
          ScriptedRecordExtract::Value => Ok(RecordExtract::Value),
          ScriptedRecordExtract::Pointer(str) => Ok(RecordExtract::Pointer(str)),
          ScriptedRecordExtract::Script(str) => {
            let engine = Engine::new();
            let ast = engine.compile(&str)?;
            Ok(RecordExtract::Function(Box::new(move |rec| {
              let engine = Engine::new();
              /*engine.register_type::<EQLRecord>()
                .register_get("key",|rec: &mut EQLRecord| rec.key.clone())
                .register_get("value",|rec: &mut EQLRecord| rec.value.clone());
              engine.register_type::<Value>();
             */
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json};

    #[test]
    fn test_record_extract_script()-> Result<()> {
        let script=r#"rec.value.k1 + rec.value.k2"#;
        let sr=ScriptedRecordExtract::script(script);
        let r=sr.into_raw()?;
        let rec=EQLRecord{key:json!("key1"),value:json!({"k1":"v1","k2":"v2"})};
        let rec2=r.apply(&rec);
        assert_eq!(Some(json!["v1v2"]),rec2);
        Ok(())
    }

}
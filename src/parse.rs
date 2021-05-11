use crate::ops::*;

use nom::{
    IResult,
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while, escaped, escaped_transform},
    character::complete::{none_of,char, digit1,multispace0,alphanumeric1 as alphanumeric,one_of},
    combinator::{cut, map, value, opt},
    error::{context, convert_error, ContextError, ErrorKind, ParseError, VerboseError},
    multi::{separated_list0, fold_many0},
    number::complete::{double},
    sequence::{delimited, preceded, separated_pair, terminated, pair},
  };
use serde_json::{Map, Value, json};
use rhai::{Engine, Scope};

use anyhow::Result;
use thiserror::Error;


pub fn parse_operation<'a>(input: &str)  -> IResult<&str, Operation<'a>> {
    alt((parse_scan,parse_key_lookup,parse_extract,parse_augment,parse_index_lookup))(input)
}

fn parse_scan<'a>(input: &str) -> IResult<&str, Operation<'a>> {
    map(preceded(spaced("scan"), 
        preceded(spaced("("),
         cut(
                terminated(
                preceded(sp, parse_string),
                preceded(sp, char(')')),
                )))),|s| Operation::Scan{name:s})(input)
  
}

fn parse_key_lookup<'a>(input: &str) -> IResult<&str, Operation<'a>> {
    map(preceded(spaced("key_lookup"), 
    preceded(spaced("("),
     cut(
            terminated(
            preceded(sp, separated_pair(parse_string,spaced(",") ,preceded(sp,json_value))),
            preceded(sp, char(')')),
            )))),|(s,v)| Operation::KeyLookup{name:s,key:v})(input)

}

fn parse_extract<'a>(input: &str) -> IResult<&str, Operation<'a>> {
  map(preceded(spaced("extract"), 
  preceded(spaced("("),
   cut(
          terminated(
          preceded(sp, separated_pair(string_array,spaced(",") ,preceded(sp,parse_operation))),
          preceded(sp, char(')')),
          )))),|(s,op)| Operation::Extract{names:s.into_iter().collect(),operation:Box::new(op)})(input)
}

fn parse_augment<'a>(input: &str) -> IResult<&str, Operation<'a>> {
  map(preceded(spaced("augment"), 
  preceded(spaced("("),
   cut(
          terminated(
          preceded(sp, separated_pair(json_value,spaced(",") ,preceded(sp,parse_operation))),
          preceded(sp, char(')')),
          )))),|(value,op)| Operation::Augment{value,operation:Box::new(op)})(input)
}

fn parse_index_lookup<'a>(input: &str) -> IResult<&str, Operation<'a>> {
  map(preceded(spaced("index_lookup"), 
  preceded(spaced("("),
   cut(
         terminated(
            preceded(sp, 
             separated_pair(
               separated_pair(
                 parse_string,
                   spaced(","), 
                 preceded(sp, parse_string))
              ,spaced(",") ,
             preceded(sp,pair(array,opt(preceded(spaced(","), string_array)))))),
          preceded(sp, char(')'))
          )))),|((tbl,idx),(values,keys))| Operation::IndexLookup{name:tbl,index_name:idx,values,keys:keys.unwrap_or_else(|| vec![])})(input)
}

fn parse_string<'a,Error: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, String, Error> {
    let string_parser = escaped_transform(none_of(r#""\"#), '\\',     parse_string_control );
    let mut delim_parser = delimited(char('"'),string_parser,char('"'));
    delim_parser(input)
}

fn parse_string_control<'a,Error: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, &'a str, Error> {
    alt((tag("\\"), tag("\""), parse_newline))(input)
}

fn parse_newline<'a,Error: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, &'a str, Error> {
    let (input,_)=tag("n")(input)?;
    Ok((input,"\n"))
}

fn spaced<'a, Error: ParseError<&'a str>>(txt: &'a str) -> impl FnMut(&'a str) -> IResult<&'a str,&'a str, Error> {
    preceded(sp, tag_no_case(txt))
}

fn sp<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, &'a str, E> {
    let chars = " \t\r\n";
    take_while(move |c| chars.contains(c))(i)
  }

fn boolean<'a, E: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, bool, E> {
    let parse_true = value(true, tag("true"));
    let parse_false = value(false, tag("false"));
    alt((parse_true, parse_false))(input)
}

fn string<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
  ) -> IResult<&'a str, String, E> {
    context(
      "string",
      parse_string,
    )(i)
}

fn array<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
  ) -> IResult<&'a str, Vec<Value>, E> {
    context(
      "array",
      preceded(
        char('['),
        cut(terminated(
          separated_list0(preceded(sp, char(',')), json_value),
          preceded(sp, char(']')),
        )),
      ),
    )(i)
  }
  
  fn string_array<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
  ) -> IResult<&'a str, Vec<String>, E> {
    context(
      "array",
      preceded(
        char('['),
        cut(terminated(
          separated_list0(preceded(sp, char(',')), string),
          preceded(sp, char(']')),
        )),
      ),
    )(i)
  }
  

  fn key_value<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
  ) -> IResult<&'a str, (String, Value), E> {
    separated_pair(
      preceded(sp, string),
      cut(preceded(sp, char(':'))),
      json_value,
    )(i)
  }
  
  fn hash<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
  ) -> IResult<&'a str, Map<String, Value>, E> {
    context(
      "map",
      preceded(
        char('{'),
        cut(terminated(
          map(
            separated_list0(preceded(sp, char(',')), key_value),
            |tuple_vec| {
              tuple_vec
                .into_iter()
                .map(|(k, v)| (k, v))
                .collect()
            },
          ),
          preceded(sp, char('}')),
        )),
      ),
    )(i)
  }
  
  /// here, we apply the space parser before trying to parse a value
  fn json_value<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
  ) -> IResult<&'a str, Value, E> {
    preceded(
      sp,
      alt((
        map(hash, Value::Object),
        map(array, Value::Array),
        map(string, |s| Value::String(String::from(s))),
        map(double, |d| Value::from(d)),
        map(boolean, Value::Bool),
      )),
    )(i)
  }
  


#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use serde_json::Value;

    #[test]
    fn test_parse_scan(){
        //assert_eq!(Ok(Operation::Scan("type1")),parse_operation(r#"scan("type1")"#));
        test_parse_scan_type1(r#"scan("type1")"#);
        test_parse_scan_type1(r#"scan ("type1")"#);
        test_parse_scan_type1(r#"scan ( "type1" ) "#);
        test_parse_scan_type1(r#"SCAN("type1")"#);
        test_parse_scan_arb(r#"scan("Customer Details")"#,"Customer Details");
    }

    fn test_parse_scan_type1(input: &str){
        test_parse_scan_arb(input,"type1");
    }

    fn test_parse_scan_arb(input: &str, table: &str){
       match parse_operation(input) {
            Ok(op) => {
                if let Operation::Scan{name} = op.1 {
                        assert_eq!(table,&name);
                } else {
                    panic!("Not scan: {}", input);
                }
            },
            Err(e) =>panic!("Cannot parse: {}: {}", input, e),
        }
    }

    #[test]
    fn test_parse_key_lookup(){
        test_parse_key_lookup_arb(r#"key_lookup("accounts","123")"#,"accounts",json!("123"));
        test_parse_key_lookup_arb(r#"key_lookup("accounts",123)"#,"accounts",json!(123.0));
        test_parse_key_lookup_arb(r#"key_lookup("accounts",{"key":"value"})"#,"accounts",json!({"key":"value"}));
    }

    fn test_parse_key_lookup_arb(input: &str, table: &str, val: Value){
        match parse_operation(input) {
            Ok(op) => {
                if let Operation::KeyLookup{name,key} = op.1 {
                        assert_eq!(table,&name);
                        assert_eq!(val,key);
                        
                } else {
                    panic!("Not key lookup: {}", input);
                }
            },
            Err(e) =>panic!("Cannot parse: {}: {}", input, e),
        }
    }

    #[test]
    fn test_parse_extract(){
      match parse_operation(r#"extract(["name","age"],key_lookup("accounts",123))"#) {
        Ok(op) => {
          if let Operation::Extract{names,operation} = op.1 {  
            assert_eq!(2,names.len());
            assert_eq!(true,names.contains("name"));
            assert_eq!(true,names.contains("age"));
            
            if let Operation::KeyLookup{name,key} = *operation {
              assert_eq!("accounts",&name);
              assert_eq!(json!(123.0),key);
          
            } else {
                panic!("Not key lookup");
            }
            
          } else {
            panic!("Not extract");
          }
        },
        Err(e) =>panic!("Cannot parse: {}", e),
    }
    }

    #[test]
    fn test_parse_augment(){
      match parse_operation(r#"augment({"key":"value"},key_lookup("accounts",123))"#) {
        Ok(op) => {
          if let Operation::Augment{value,operation} = op.1 {  
            assert_eq!(json!({"key":"value"}),value);
            
            if let Operation::KeyLookup{name,key} = *operation {
              assert_eq!("accounts",&name);
              assert_eq!(json!(123.0),key);
          
            } else {
                panic!("Not key lookup");
            }
            
          } else {
            panic!("Not augment");
          }
        },
        Err(e) =>panic!("Cannot parse: {}", e),
    }
    }

    #[test]
    fn test_parse_index_lookup(){
      test_parse_index_lookup_arb(r#"index_lookup("accounts","account_id",["123"])"#,"accounts","account_id",&vec![json!("123")],&vec![]);
      test_parse_index_lookup_arb(r#"index_lookup("accounts","account_id",["123"],["name","age"])"#,"accounts","account_id",&vec![json!("123")],&vec!["name","age"]);
    
    }

    fn test_parse_index_lookup_arb(input: &str, table: &str, idx: &str, val: &[Value], ks:&[&str]){
      match parse_operation(input) {
          Ok(op) => {
              if let Operation::IndexLookup{name,index_name,values,keys} = op.1 {
                  assert_eq!(table,&name);
                  assert_eq!(idx,&index_name);
                  assert_eq!(&val,&values);
                  assert_eq!(&ks,&keys);
              } else {
                  panic!("Not index lookup: {}", input);
              }
          },
          Err(e) =>panic!("Cannot parse: {}: {}", input, e),
      }
  }



}
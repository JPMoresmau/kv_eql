use crate::script::*;

use nom::{IResult, branch::alt, bytes::complete::{escaped_transform, tag, tag_no_case, take, take_while, take_while1}, character::{
        complete::{char, none_of},
        is_alphanumeric,
    }, combinator::{cut, map, opt, value}, error::{ContextError, ParseError, VerboseError, context}, multi::{count, fold_many0, many_till, separated_list0}, number::complete::{double}, sequence::{delimited, pair, preceded, separated_pair, terminated}};

use serde_json::{Map, Value};

pub fn parse_operation<'a, Error: ParseError<&'a str> + ContextError<&'a str>>(input: &'a str) -> IResult<&'a str, ScriptedOperation, Error> {
    alt((
        parse_scan,
        parse_key_lookup,
        parse_extract,
        parse_augment,
        parse_index_lookup,
        parse_nested_loops,
        parse_hash_lookup,
        parse_merge,
        parse_map,
        parse_reduce,
    ))(input)
}

pub fn parse_operation_verbose<'a>(input: &'a str) -> IResult<&'a str, ScriptedOperation, VerboseError<&'a str>> {
    parse_operation(input)
}

fn parse_scan<'a, Error: ParseError<&'a str>>(input: &'a str)  -> IResult<&'a str, ScriptedOperation, Error> {
    map(
        preceded(
            spaced("scan"),
            preceded(
                spaced("("),
                cut(terminated(
                    preceded(sp, parse_eql_string),
                    preceded(sp, char(')')),
                )),
            ),
        ),
        |s| ScriptedOperation::Scan { name: s },
    )(input)
}

fn parse_key_lookup<'a, Error: ParseError<&'a str> + ContextError<&'a str>>(input: &'a str) -> IResult<&'a str, ScriptedOperation, Error> {
    map(
        preceded(
            spaced("key_lookup"),
            preceded(
                spaced("("),
                cut(terminated(
                    preceded(
                        sp,
                        separated_pair(parse_eql_string, spaced(","), preceded(sp, json_value)),
                    ),
                    preceded(sp, char(')')),
                )),
            ),
        ),
        |(s, v)| ScriptedOperation::KeyLookup { name: s, key: v },
    )(input)
}

fn parse_extract<'a, Error: ParseError<&'a str> + ContextError<&'a str>>(input: &'a str) -> IResult<&'a str, ScriptedOperation, Error> {
    map(
        preceded(
            spaced("extract"),
            preceded(
                spaced("("),
                cut(terminated(
                    preceded(
                        sp,
                        separated_pair(string_array, spaced(","), preceded(sp, parse_operation)),
                    ),
                    preceded(sp, char(')')),
                )),
            ),
        ),
        |(s, op)| ScriptedOperation::Extract {
            names: s.into_iter().collect(),
            operation: Box::new(op),
        },
    )(input)
}

fn parse_augment<'a, Error: ParseError<&'a str> + ContextError<&'a str>>(input: &'a str) -> IResult<&'a str, ScriptedOperation, Error> {
    map(
        preceded(
            spaced("augment"),
            preceded(
                spaced("("),
                cut(terminated(
                    preceded(
                        sp,
                        separated_pair(json_value, spaced(","), preceded(sp, parse_operation)),
                    ),
                    preceded(sp, char(')')),
                )),
            ),
        ),
        |(value, op)| ScriptedOperation::Augment {
            value,
            operation: Box::new(op),
        },
    )(input)
}

fn parse_index_lookup<'a, Error: ParseError<&'a str> + ContextError<&'a str>>(input: &'a str) -> IResult<&'a str, ScriptedOperation, Error> {
    map(
        preceded(
            spaced("index_lookup"),
            preceded(
                spaced("("),
                cut(terminated(
                    preceded(
                        sp,
                        separated_pair(
                            separated_pair(
                                parse_eql_string,
                                spaced(","),
                                preceded(sp, parse_eql_string),
                            ),
                            spaced(","),
                            preceded(
                                sp,
                                pair(
                                    array,
                                    opt(preceded(spaced(","), preceded(sp, string_array))),
                                ),
                            ),
                        ),
                    ),
                    preceded(sp, char(')')),
                )),
            ),
        ),
        |((tbl, idx), (values, keys))| ScriptedOperation::IndexLookup {
            name: tbl,
            index_name: idx,
            values,
            keys: keys.unwrap_or_else(|| vec![]),
        },
    )(input)
}

fn parse_nested_loops<'a, Error: ParseError<&'a str> + ContextError<&'a str>>(input: &'a str) -> IResult<&'a str, ScriptedOperation, Error> {
    map(
        preceded(
            spaced("nested_loops"),
            preceded(
                spaced("("),
                cut(terminated(
                    preceded(sp, separated_pair(parse_operation, spaced(","), quoted_str)),
                    preceded(sp, char(')')),
                )),
            ),
        ),
        |(first, second)| ScriptedOperation::NestedLoops {
            first: Box::new(first),
            second: second.into(),
        },
    )(input)
}

fn parse_hash_lookup<'a, Error: ParseError<&'a str> + ContextError<&'a str>>(input: &'a str) -> IResult<&'a str, ScriptedOperation, Error> {
    map(
        preceded(
            spaced("hash_lookup"),
            preceded(
                spaced("("),
                cut(terminated(
                    preceded(sp, 
                        separated_pair(
                        separated_pair(
                            separated_pair(parse_operation, spaced(","), parse_record_extract),
                            spaced(","),
                            separated_pair(parse_operation, spaced(","), parse_record_extract)),
                            spaced(","),
                            quoted_str)
                        ),
                    preceded(sp, char(')')),
                )),
            ),
        )
        ,|(((build,build_hash),(probe,probe_hash)),join)| ScriptedOperation::HashJoin{
            build: Box::new(build),
            build_hash,
            probe: Box::new(probe),
            probe_hash,
            join:join.into()
        }
    )(input)
}

fn parse_merge<'a, Error: ParseError<&'a str> + ContextError<&'a str>>(input: &'a str) -> IResult<&'a str, ScriptedOperation, Error> {
    map(
        preceded(
            spaced("merge"),
            preceded(
                spaced("("),
                cut(terminated(
                    preceded(sp, 
                        separated_pair(
                        separated_pair(
                            separated_pair(parse_operation, spaced(","), parse_record_extract),
                            spaced(","),
                            separated_pair(parse_operation, spaced(","), parse_record_extract)),
                            spaced(","),
                            quoted_str)
                        ),
                    preceded(sp, char(')')),
                )),
            ),
        )
        ,|(((first,first_key),(second,second_key)),join)| ScriptedOperation::Merge{
            first: Box::new(first),
            first_key,
            second: Box::new(second),
            second_key,
            join:join.into()
        }
    )(input)
}

fn parse_map<'a, Error: ParseError<&'a str> + ContextError<&'a str>>(input: &'a str) -> IResult<&'a str, ScriptedOperation, Error> {
    map(
        preceded(
            spaced("map"),
            preceded(
                spaced("("),
                cut(terminated(
                    preceded(sp, 
                        separated_pair(
                            parse_operation,
                            spaced(","),
                            quoted_str)
                        ),
                    preceded(sp, char(')')),
                )),
            )
        )
        , |(op,process)| ScriptedOperation::Map{operation:Box::new(op), process:process.into()} 
    )(input)
}

fn parse_reduce<'a, Error: ParseError<&'a str> + ContextError<&'a str>>(input: &'a str) -> IResult<&'a str, ScriptedOperation, Error> {
    map(
        preceded(
            spaced("reduce"),
            preceded(
                spaced("("),
                cut(terminated(
                    preceded(sp, 
                        separated_pair(
                            parse_operation,
                            spaced(","),
                            quoted_str)
                        ),
                    preceded(sp, char(')')),
                )),
            )
        )
        , |(op,process)| ScriptedOperation::Reduce{operation:Box::new(op), process:process.into()} 
    )(input)
}

fn parse_record_extract<'a, Error: ParseError<&'a str> + ContextError<&'a str>>(input: &'a str) -> IResult<&'a str, ScriptedRecordExtract, Error> {
    alt((parse_record_extract_key,parse_record_extract_value,parse_record_extract_pointer,parse_record_extract_script,parse_record_extract_multiple))(input)
}

fn parse_record_extract_key<'a, Error: ParseError<&'a str> + ContextError<&'a str>>(input: &'a str) -> IResult<&'a str, ScriptedRecordExtract, Error> {
    map(spaced("key"),|_| ScriptedRecordExtract::Key)(input)
}

fn parse_record_extract_value<'a, Error: ParseError<&'a str> + ContextError<&'a str>>(input: &'a str) -> IResult<&'a str, ScriptedRecordExtract, Error> {
    map(spaced("value"),|_| ScriptedRecordExtract::Value)(input)
}

fn parse_record_extract_pointer<'a, Error: ParseError<&'a str> + ContextError<&'a str>>(input: &'a str) -> IResult<&'a str, ScriptedRecordExtract, Error> {
    map(preceded(spaced("pointer"),preceded(spaced("("),terminated(parse_eql_string,spaced(")"))))
    ,|s| ScriptedRecordExtract::Pointer(s))(input)
}

fn parse_record_extract_script<'a, Error: ParseError<&'a str> + ContextError<&'a str>>(input: &'a str) -> IResult<&'a str, ScriptedRecordExtract, Error> {
    map(preceded(spaced("script"),preceded(spaced("("),terminated(parse_eql_string,spaced(")"))))
    ,|s| ScriptedRecordExtract::Pointer(s))(input)
}

fn parse_record_extract_multiple<'a, Error: ParseError<&'a str> + ContextError<&'a str>>(input: &'a str) -> IResult<&'a str, ScriptedRecordExtract, Error> {
    map(preceded(spaced("["),terminated(separated_list0(preceded(sp, char(',')), parse_record_extract),spaced("]")))
    ,|s| ScriptedRecordExtract::Multiple(s))(input)
}


fn parse_eql_string<'a, Error: ParseError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, String, Error> {
    alt((parse_string, map(name, |v| v.into())))(input)
}

fn parse_string<'a, Error: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, String, Error> {
    let string_parser = escaped_transform(none_of(r#""\"#), '\\', parse_string_control);
    let delim_parser = delimited(char('"'), string_parser, char('"'));
    alt((map(tag("\"\""),|_s| String::new()),delim_parser))(input)
}

fn quoted_str<'a, Error: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, &'a str, Error> {
    //https://stackoverflow.com/a/61989390/827593
    let (input, _) = sp(input)?;
    // Count number of leading #
    let (remaining, hash_count) = fold_many0(tag("#"), 0, |acc, _| acc + 1)(input)?;

    // Match "
    let (remaining, _) = tag("\"")(remaining)?;

    // Take until closing " plus # (repeated hash_count times)
    let closing = pair(tag("\""), count(tag("#"), hash_count));
    let (remaining, (inner, _)) = many_till(take(1u32), closing)(remaining)?;

    // Extract inner range
    let offset = hash_count + 1;
    let length = inner.len();

    Ok((remaining, &input[offset..offset + length]))
}

fn parse_string_control<'a, Error: ParseError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, &'a str, Error> {
    alt((tag("\\"), tag("\""), parse_newline))(input)
}

fn parse_newline<'a, Error: ParseError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, &'a str, Error> {
    let (input, _) = tag("n")(input)?;
    Ok((input, "\n"))
}

fn spaced<'a, Error: ParseError<&'a str>>(
    txt: &'a str,
) -> impl FnMut(&'a str) -> IResult<&'a str, &'a str, Error> {
    preceded(sp, tag_no_case(txt))
}

fn sp_char<'a, Error: ParseError<&'a str>>(
    txt: char,
) -> impl FnMut(&'a str) -> IResult<&'a str, char, Error> {
    preceded(sp, char(txt))
}

fn sp<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, &'a str, E> {
    let chars = " \t\r\n";
    take_while(move |c| chars.contains(c))(i)
}

fn name<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, &'a str, E> {
    take_while1(move |c| is_alphanumeric(c as u8) || c == '_')(i)
}

fn boolean<'a, E: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, bool, E> {
    let parse_true = value(true, tag("true"));
    let parse_false = value(false, tag("false"));
    alt((parse_true, parse_false))(input)
}

fn string<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<&'a str, String, E> {
    context("string", parse_string)(i)
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
            sp_char('['),
            cut(terminated(
                separated_list0(sp_char(','), preceded(sp,parse_eql_string)),
                sp_char(']'),
            )),
        ),
    )(i)
}

fn key_value<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<&'a str, (String, Value), E> {
    separated_pair(
        preceded(sp, string),
        cut(sp_char(':')),
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
                    |tuple_vec| tuple_vec.into_iter().map(|(k, v)| (k, v)).collect(),
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
    use serde_json::{json, Value};

    #[test]
    fn test_parse_scan() {
        //assert_eq!(Ok(Operation::Scan("type1")),parse_operation(r#"scan("type1")"#));
        test_parse_scan_type1(r#"scan("type1")"#);
        test_parse_scan_type1(r#"scan ("type1")"#);
        test_parse_scan_type1(r#"scan ( "type1" ) "#);
        test_parse_scan_type1(r#"SCAN("type1")"#);
        test_parse_scan_type1(r#"scan(type1)"#);
        test_parse_scan_type1(r#"scan (type1)"#);
        test_parse_scan_type1(r#"scan ( type1 ) "#);
        test_parse_scan_type1(r#"SCAN(type1)"#);
        test_parse_scan_arb(r#"scan("Customer Details")"#, "Customer Details");
    }

    fn test_parse_scan_type1(input: &str) {
        test_parse_scan_arb(input, "type1");
    }

    fn test_parse_scan_arb(input: &str, table: &str) {
        match parse_operation_verbose(input) {
            Ok(op) => {
                if let ScriptedOperation::Scan { name } = op.1 {
                    assert_eq!(table, &name);
                } else {
                    panic!("Not scan: {}", input);
                }
            }
            Err(e) => panic!("Cannot parse: {}: {}", input, e),
        }
    }

    #[test]
    fn test_parse_key_lookup() {
        test_parse_key_lookup_arb(r#"key_lookup("accounts","123")"#, "accounts", json!("123"));
        test_parse_key_lookup_arb(r#"key_lookup( accounts ,"123")"#, "accounts", json!("123"));
        test_parse_key_lookup_arb(r#"key_lookup("accounts",123)"#, "accounts", json!(123.0));
        test_parse_key_lookup_arb(
            r#"key_lookup("accounts",{"key":"value"})"#,
            "accounts",
            json!({"key":"value"}),
        );
    }

    fn test_parse_key_lookup_arb(input: &str, table: &str, val: Value) {
        match parse_operation_verbose(input) {
            Ok(op) => {
                assert_eq!(
                    ScriptedOperation::KeyLookup {
                        name: table.into(),
                        key: val.into()
                    },
                    op.1
                );
            }
            Err(e) => panic!("Cannot parse: {}: {}", input, e),
        }
    }

    #[test]
    fn test_parse_extract() {
        match parse_operation_verbose(r#"extract(["name", "age"],key_lookup("accounts",123))"#) {
            Ok(op) => {
                if let ScriptedOperation::Extract { names, operation } = op.1 {
                    assert_eq!(2, names.len());
                    assert_eq!(true, names.contains("name"));
                    assert_eq!(true, names.contains("age"));
                    assert_eq!(
                        ScriptedOperation::KeyLookup {
                            name: "accounts".into(),
                            key: json!(123.0)
                        },
                        *operation
                    );
                } else {
                    panic!("Not extract");
                }
            }
            Err(e) => panic!("Cannot parse: {}", e),
        };
        match parse_operation_verbose(r#"extract(["name" , "age" ],key_lookup(accounts,123))"#) {
            Ok(op) => {
                if let ScriptedOperation::Extract { names, operation } = op.1 {
                    assert_eq!(2, names.len());
                    assert_eq!(true, names.contains("name"));
                    assert_eq!(true, names.contains("age"));
                    assert_eq!(
                        ScriptedOperation::KeyLookup {
                            name: "accounts".into(),
                            key: json!(123.0)
                        },
                        *operation
                    );
                } else {
                    panic!("Not extract");
                }
            }
            Err(e) => panic!("Cannot parse: {}", e),
        };
        match parse_operation_verbose(r#"extract([name , age ],key_lookup(accounts,123))"#) {
            Ok(op) => {
                if let ScriptedOperation::Extract { names, operation } = op.1 {
                    assert_eq!(2, names.len());
                    assert_eq!(true, names.contains("name"));
                    assert_eq!(true, names.contains("age"));
                    assert_eq!(
                        ScriptedOperation::KeyLookup {
                            name: "accounts".into(),
                            key: json!(123.0)
                        },
                        *operation
                    );
                } else {
                    panic!("Not extract");
                }
            }
            Err(e) => panic!("Cannot parse: {}", e),
        };
    }

    #[test]
    fn test_parse_augment() {
        match parse_operation_verbose(r#"augment({"key":"value"},key_lookup("accounts",123))"#) {
            Ok(op) => {
                if let ScriptedOperation::Augment { value, operation } = op.1 {
                    assert_eq!(json!({"key":"value"}), value);
                    assert_eq!(
                        ScriptedOperation::KeyLookup {
                            name: "accounts".into(),
                            key: json!(123.0)
                        },
                        *operation
                    );
                } else {
                    panic!("Not augment");
                }
            }
            Err(e) => panic!("Cannot parse: {}", e),
        };
        match parse_operation_verbose(r#"augment({"key":"value"},key_lookup(accounts,123))"#) {
            Ok(op) => {
                if let ScriptedOperation::Augment { value, operation } = op.1 {
                    assert_eq!(json!({"key":"value"}), value);
                    assert_eq!(
                        ScriptedOperation::KeyLookup {
                            name: "accounts".into(),
                            key: json!(123.0)
                        },
                        *operation
                    );
                } else {
                    panic!("Not augment");
                }
            }
            Err(e) => panic!("Cannot parse: {}", e),
        }
    }

    #[test]
    fn test_parse_index_lookup() {
        test_parse_index_lookup_arb(
            r#"index_lookup("accounts","account_id",["123"])"#,
            "accounts",
            "account_id",
            vec![json!("123")],
            vec![],
        );
        test_parse_index_lookup_arb(
            r#"index_lookup("accounts","account_id",["123"],["name","age"])"#,
            "accounts",
            "account_id",
            vec![json!("123")],
            vec!["name", "age"],
        );
        test_parse_index_lookup_arb(
            r#"index_lookup(accounts, account_id, ["123"])"#,
            "accounts",
            "account_id",
            vec![json!("123")],
            vec![],
        );
        test_parse_index_lookup_arb(
            r#"index_lookup(accounts , account_id , ["123"] , ["name","age"])"#,
            "accounts",
            "account_id",
            vec![json!("123")],
            vec!["name", "age"],
        );
        test_parse_index_lookup_arb(
            r#"index_lookup(accounts , account_id , ["123"] , ["","age"])"#,
            "accounts",
            "account_id",
            vec![json!("123")],
            vec!["", "age"],
        );
    }

    fn test_parse_index_lookup_arb(
        input: &str,
        table: &str,
        idx: &str,
        val: Vec<Value>,
        ks: Vec<&str>,
    ) {
        match parse_operation_verbose(input) {
            Ok(op) => {
                assert_eq!(
                    ScriptedOperation::IndexLookup {
                        name: table.into(),
                        index_name: idx.into(),
                        values: val.into(),
                        keys: ks.into_iter().map(|s| s.into()).collect()
                    },
                    op.1
                );
            }
            Err(e) => panic!("Cannot parse: {}: {}", input, e),
        }
    }

    #[test]
    fn test_parse_nested_loops() {
        test_parse_nested_loops_arb("nested_loops(index_lookup(\"accounts\",\"account_id\",[\"123\"],[\"name\",\"age\"]),#\"key_lookup(\"type1\", rec.key)\"#)",
        ScriptedOperation::IndexLookup{name:"accounts".into(),index_name:"account_id".into(),values: vec![json!("123")],keys:vec!["name".into(), "age".into()]},
        r#"key_lookup("type1", rec.key)"#
      );
      test_parse_nested_loops_arb("nested_loops(index_lookup(accounts,account_id,[\"123\"],[\"name\",\"age\"]),  #\"key_lookup(type1, rec.key)\"#)",
        ScriptedOperation::IndexLookup{name:"accounts".into(),index_name:"account_id".into(),values: vec![json!("123")],keys:vec!["name".into(), "age".into()]},
        r#"key_lookup(type1, rec.key)"#
      );
    }

    fn test_parse_nested_loops_arb(input: &str, first: ScriptedOperation, script: &str) {
        match parse_operation_verbose(input) {
            Ok(op) => {
                assert_eq!(
                    ScriptedOperation::NestedLoops {
                        first: Box::new(first),
                        second: script.into()
                    },
                    op.1
                );
            }
            Err(e) => panic!("Cannot parse: {}: {}", input, e),
        }
    }

    #[test]
    fn test_parse_hash_lookup(){
        let input="hash_lookup(scan(categories),key,scan(products),pointer(\"/category_id\"),#\"probe.value[\"description\"]=build.value[\"description\"];probe\"#)";
        match parse_operation_verbose(input) {
            Ok(op) => {
                assert_eq!(
                    ScriptedOperation::HashJoin {
                        build: Box::new(ScriptedOperation::Scan{name:"categories".into()}),
                        build_hash: ScriptedRecordExtract::Key,
                        probe: Box::new(ScriptedOperation::Scan{name:"products".into()}),
                        probe_hash: ScriptedRecordExtract::pointer("/category_id"),
                        join: "probe.value[\"description\"]=build.value[\"description\"];probe".into()
                    },
                    op.1
                );
            }
            Err(e) => panic!("Cannot parse: {}: {}", input, e),
        }
    }

    #[test]
    fn test_parse_merge(){
        let input="merge(scan(categories),key,index_lookup(products,product_category_id,[],[\"category_id\"]),pointer(\"/category_id\"),#\"let rec3=empty_record();if rec2.value!=(){rec3.key=rec2.key;rec3.value=#{description:rec1.value[\"description\"]};rec3.value.fill_with(rec2.value);};rec3\"#)";
        match parse_operation_verbose(input) {
            Ok(op) => {
                assert_eq!(
                    ScriptedOperation::Merge {
                        first: Box::new(ScriptedOperation::Scan{name:"categories".into()}),
                        first_key: ScriptedRecordExtract::Key,
                        second: Box::new(ScriptedOperation::IndexLookup{name:"products".into(),index_name:"product_category_id".into(),values:vec![],keys:vec!["category_id".into()]}),
                        second_key: ScriptedRecordExtract::pointer("/category_id"),
                        join: "let rec3=empty_record();if rec2.value!=(){rec3.key=rec2.key;rec3.value=#{description:rec1.value[\"description\"]};rec3.value.fill_with(rec2.value);};rec3".into()
                    },
                    op.1
                );
            }
            Err(e) => panic!("Cannot parse: {}: {}", input, e),
        }

    }

    #[test]
    fn test_parse_map(){
        let input="map(scan(categories),#\"rec.value.[\"description\"]=\"unknown\";rec\"#)";
        match parse_operation_verbose(input) {
            Ok(op) => {
                assert_eq!(
                    ScriptedOperation::Map {
                        operation: Box::new(ScriptedOperation::Scan{name:"categories".into()}),
                        process: "rec.value.[\"description\"]=\"unknown\";rec".into()
                    },
                    op.1
                );
            }
            Err(e) => panic!("Cannot parse: {}: {}", input, e),
        }

    }

    #[test]
    fn test_parse_reduce(){
        let input="reduce(scan(categories),#\"rec.value[\"count\"]=recs.len();\"#)";
        match parse_operation_verbose(input) {
            Ok(op) => {
                assert_eq!(
                    ScriptedOperation::Reduce {
                        operation: Box::new(ScriptedOperation::Scan{name:"categories".into()}),
                        process: "rec.value[\"count\"]=recs.len();".into()
                    },
                    op.1
                );
            }
            Err(e) => panic!("Cannot parse: {}: {}", input, e),
        }

    }
}

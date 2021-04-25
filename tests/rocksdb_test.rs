use serde_json::json;
use kv_eql::{RocksDBEQL, Operation};
use serde_json::Value;
use anyhow::Result;

#[test]
fn test_basic() -> Result<()>{
    let path = "test_basic.db";
    {
        let mut eql=RocksDBEQL::open(path)?;
        let md=&eql.metadata;
        assert_eq!(true, md.indices.is_empty());

        let john = json!({
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        });
        
        eql.insert("type1", "key1", &john)?;
        let md=&eql.metadata;
        assert_eq!(true, md.indices.contains_key("type1"));
        let m2=md.indices.get("type1").unwrap();
        assert_eq!(true, m2.is_empty());
        
        let ov=eql.get("type1", "key1")?;
        assert_eq!(ov,Some(john));

        eql.delete("type1", "key1")?;
        let ov=eql.get("type1", "key1")?;
        assert_eq!(ov,None);
    }
    {
        let mut eql=RocksDBEQL::open(path)?;
        let ov=eql.get("type1", "key1")?;
        assert_eq!(ov,None);
    }
    RocksDBEQL::destroy( path)?;
    Ok(())
}

#[test]
fn test_scan() -> Result<()>{
    let path = "test_scan.db";
    {
        let mut meta=RocksDBEQL::open(path)?;
        let john = json!({
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        });
        
        meta.insert("type1", "key1", &john)?;

        let mary = json!({
            "name": "Mary Doe",
            "age": 34
        });
        
        meta.insert("type1", "key2", &mary)?;

        //let v1=vec![(b"key1",&john),(b"key2",&mary)];
        let v1:Vec<(Value,Value)>=meta.execute(Operation::scan("type1")).collect();
        assert_eq!(2,v1.len());
        assert_eq!(Value::from("key1"),v1[0].0);
        assert_eq!(Value::from("key2"),v1[1].0);
        assert_eq!(john,v1[0].1);
        assert_eq!(mary,v1[1].1);
        
        let john2 = json!({
            "name": "John Doe",
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        });
       
        let mary2 = json!({
            "name": "Mary Doe",
        });

        let v1:Vec<(Value,Value)>=meta.execute(Operation::extract(&["name","phones"],Operation::scan("type1"))).collect();
        assert_eq!(2,v1.len());
        assert_eq!(Value::from("key1"),v1[0].0);
        assert_eq!(Value::from("key2"),v1[1].0);
        assert_eq!(john2,v1[0].1);
        assert_eq!(mary2,v1[1].1);

    }
    RocksDBEQL::destroy( path)?;
    Ok(())
}

#[test]
fn test_lookup() -> Result<()>{
    let path = "test_lookup.db";
    {
        let mut meta=RocksDBEQL::open(path)?;
        let john = json!({
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        });
        
        meta.insert("type1", "key1", &john)?;

        let mary = json!({
            "name": "Mary Doe",
            "age": 34
        });
        
        meta.insert("type1", "key2", &mary)?;

        //let v1=vec![(b"key1",&john),(b"key2",&mary)];
         let v1:Vec<(Value,Value)>=meta.execute(Operation::key_lookup("type1",Value::from("key1"))).collect();
        assert_eq!(1,v1.len());
        assert_eq!(Value::from("key1"),v1[0].0);
        assert_eq!(john,v1[0].1);
      
        let mary2 = json!({
            "name": "Mary Doe",
        });

        let v1:Vec<(Value,Value)>=meta.execute(Operation::extract(&["name","phones"],Operation::key_lookup("type1",Value::from("key2")))).collect();
        assert_eq!(1,v1.len());
        assert_eq!(Value::from("key2"),v1[0].0);
        assert_eq!(mary2,v1[0].1);

    }
    RocksDBEQL::destroy( path)?;
    Ok(())
}

#[test]
fn test_index_metadata() -> Result<()> {
    let path = "test_index_metadata.db";
    {
        let mut eql=RocksDBEQL::open(path)?;
        eql.add_index("type1", "idx1", vec!["/name"])?;
        let md=&eql.metadata;
        assert_eq!(true, md.indices.contains_key("type1"));
        let m2=md.indices.get("type1").unwrap();
        assert_eq!(true, m2.contains_key("idx1"));
        let on=m2.get("idx1").unwrap();
        assert_eq!(1,on.len());
        assert_eq!("/name",on[0]);
    }
    {
        let mut eql=RocksDBEQL::open(path)?;
        let md=&eql.metadata;
        assert_eq!(true, md.indices.contains_key("type1"));
        let m2=md.indices.get("type1").unwrap();
        assert_eq!(true, m2.contains_key("idx1"));
        let on=m2.get("idx1").unwrap();
        assert_eq!(1,on.len());
        assert_eq!("/name",on[0]);
        eql.delete_index("type1", "idx1")?;
        let md=&eql.metadata;
        assert_eq!(true, md.indices.contains_key("type1"));
        let m2=md.indices.get("type1").unwrap();
        assert_eq!(true, m2.is_empty());
    }
    {
        let eql=RocksDBEQL::open(path)?;
        let md=eql.metadata;
        assert_eq!(true, md.indices.contains_key("type1"));
        let m2=md.indices.get("type1").unwrap();
        assert_eq!(true, m2.is_empty());
    }
    RocksDBEQL::destroy(path)?;
    Ok(())
}

#[test]
fn test_index_lookup() -> Result<()> {
    let path = "test_index_lookup.db";
    {
        let mut eql=RocksDBEQL::open(path)?;
        eql.add_index("type1", "idx1", vec!["/name","/age"])?;
        let john = json!({
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        });
        eql.insert("type1", "key1", &john)?;

        let mary = json!({
            "name": "Mary Doe",
            "age": 34
        });
        
        eql.insert("type1", "key2", &mary)?;


        let john2 = json!({
            "nameix": "John Doe",
            "ageix": 43,
        });
        let mary2 = json!({
            "nameix": "Mary Doe",
            "ageix": 34,
        });

        let mary3 = json!({
            "ageix": 34,
        });


        let v1:Vec<(Value,Value)>=eql.execute(Operation::index_lookup_keys("type1","idx1",vec![json!("John Doe")], vec!["nameix","ageix"])).collect();
        assert_eq!(1,v1.len());
        assert_eq!(Value::from("key1"),v1[0].0);
        assert_eq!(john2,v1[0].1);

        let v1:Vec<(Value,Value)>=eql.execute(Operation::index_lookup_keys("type1","idx1",vec![json!("John Doe"),json!(43)], vec!["nameix","ageix"])).collect();
        assert_eq!(1,v1.len());
        assert_eq!(Value::from("key1"),v1[0].0);
        assert_eq!(john2,v1[0].1);
        
        let v1:Vec<(Value,Value)>=eql.execute(Operation::index_lookup_keys("type1","idx1",vec![json!("John Doe"),json!(34)], vec!["nameix","ageix"])).collect();
        assert_eq!(0,v1.len());

        let v1:Vec<(Value,Value)>=eql.execute(Operation::index_lookup_keys("type1","idx1",vec![json!("Mary Doe")], vec!["nameix","ageix"])).collect();
        assert_eq!(1,v1.len());
        assert_eq!(Value::from("key2"),v1[0].0);
        assert_eq!(mary2,v1[0].1);

        let v1:Vec<(Value,Value)>=eql.execute(Operation::index_lookup_keys("type1","idx1",vec![json!("Mary Doe")], vec!["","ageix"])).collect();
        assert_eq!(1,v1.len());
        assert_eq!(Value::from("key2"),v1[0].0);
        assert_eq!(mary3,v1[0].1);

        let v1:Vec<(Value,Value)>=eql.execute(Operation::index_lookup_keys("type1","idx1",vec![json!("John Deer"),json!(43)], vec!["nameix","ageix"])).collect();
        assert_eq!(0,v1.len());

        eql.delete("type1", "key1")?;
        let v1:Vec<(Value,Value)>=eql.execute(Operation::index_lookup_keys("type1","idx1",vec![json!("John Doe")], vec!["nameix","ageix"])).collect();
        assert_eq!(0,v1.len());
    }
    RocksDBEQL::destroy(path)?;
    Ok(())
}


#[test]
fn test_index_nested_loops() -> Result<()> {
    let path = "test_index_nested_loops.db";
    {
        let mut eql=RocksDBEQL::open(path)?;
        eql.add_index("type1", "idx1", vec!["/name","/age"])?;
        let john = json!({
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        });
        eql.insert("type1", "key1", &john)?;

        let mary = json!({
            "name": "Mary Doe",
            "age": 34
        });
        
        eql.insert("type1", "key2", &mary)?;


        let john2 = json!({
            "name": "John Doe",
            "age": 43,
            "ageix": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        });

        let v1:Vec<(Value,Value)>=eql.execute(
                Operation::nested_loops(
                    Operation::index_lookup("type1","idx1",vec![json!("John Doe")]),
                        |(k,_v)| Operation::key_lookup("type1",k.clone())
                )).collect();
        assert_eq!(1,v1.len());
        assert_eq!(Value::from("key1"),v1[0].0);
        assert_eq!(john,v1[0].1);

        let v1:Vec<(Value,Value)>=eql.execute(
            Operation::nested_loops(
                Operation::index_lookup_keys("type1","idx1",vec![json!("John Doe")],vec!["","ageix"]),
                    |(k,v)| Operation::augment(v.clone(), Operation::key_lookup("type1",k.clone()))
            )).collect();
        assert_eq!(1,v1.len());
        assert_eq!(Value::from("key1"),v1[0].0);
        assert_eq!(john2,v1[0].1);
    }
    RocksDBEQL::destroy(path)?;
    Ok(())
}

#[test]
fn test_two_types_nested_loops() -> Result<()> {
    let path = "test_two_types_nested_loops.db";
    {
        let mut eql=RocksDBEQL::open(path)?;
        let bevs=json!({
            "category_name":"Beverages",
            "description":"Soft drinks, coffees, teas, beers, and ales",
        });
        let conds=json!({
            "category_name":"Condiments",
            "description":"Sweet and savory sauces, relishes, spreads, and seasonings",
        });
        eql.insert("categories", 1, &bevs)?;
        eql.insert("categories", 2, &conds)?;
     
        eql.add_index("products", "product_category_id", vec!["/category_id"])?;

        let chai=json!({
            "product_name":"Chai",
            "category_id":1,
        });
        eql.insert("products", 1, &chai)?;
        let chang=json!({
            "product_name":"Chang",
            "category_id":1,
            "quantity_per_unit":"10 boxes x 30 bags",
        });
        eql.insert("products", 2, &chang)?;
        let chang=json!({
            "product_name":"Chang",
            "category_id":1,
            "quantity_per_unit":"24 - 12 oz bottles"
        });
        eql.insert("products", 2, &chang)?;
        let aniseed=json!({
            "product_name":"Aniseed Syrup",
            "category_id":2,
            "quantity_per_unit":"12 - 550 ml bottles"
        });
        eql.insert("products", 3, &aniseed)?;
        let cajun=json!({
            "product_name":"Chef Anton's Cajun Seasoning",
            "category_id":2,
            "quantity_per_unit":"48 - 6 oz jars"
        });
        eql.insert("products", 4, &cajun)?;

        let v1:Vec<(Value,Value)>=eql.execute(
            Operation::nested_loops(
                Operation::extract(&["description"], 
                        Operation::scan("categories")),
                    |(k,v)| Operation::augment(v.clone(), 
                    Operation::nested_loops(
                        Operation::index_lookup("products","product_category_id", vec![k.clone()]),
                        |(k,_v)| Operation::key_lookup("products",k.clone())
                    )
                    )
            )).collect();
        assert_eq!(4,v1.len());
        let keys1:Vec<Value>=v1.iter().map(|t|t.0.clone()).collect();
        assert_eq!(true, keys1.contains(&Value::from(1)));
        assert_eq!(true, keys1.contains(&Value::from(2)));
        assert_eq!(true, keys1.contains(&Value::from(3)));
        assert_eq!(true, keys1.contains(&Value::from(4)));
        assert_eq!(4,v1.iter().filter(|(k,v)| {
            if let Some(m) = v.as_object() {
                if k==&Value::from(1) || k==&Value::from(2) {
                    assert_eq!(Some(&Value::from("Soft drinks, coffees, teas, beers, and ales")),m.get("description"));
                } else {
                    assert_eq!(Some(&Value::from("Sweet and savory sauces, relishes, spreads, and seasonings")),m.get("description"));
                }
                return true;
            }
            return false;
        }).count());
        

    }
    RocksDBEQL::destroy(path)?;
    Ok(())
}
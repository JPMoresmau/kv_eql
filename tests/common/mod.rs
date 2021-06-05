
use kv_eql::EQLDB;
use anyhow::Result;
use serde_json::json;


pub fn write_northwind_data(eql: &mut EQLDB) -> Result<()> {
    let bevs = json!({
        "category_name":"Beverages",
        "description":"Soft drinks, coffees, teas, beers, and ales",
    });
    let conds = json!({
        "category_name":"Condiments",
        "description":"Sweet and savory sauces, relishes, spreads, and seasonings",
    });
    eql.insert("categories", 1, &bevs)?;
    eql.insert("categories", 2, &conds)?;

    eql.add_index("products", "product_category_id", vec!["/category_id"])?;

    let chai = json!({
        "product_name":"Chai",
        "category_id":1,
        "quantity_per_unit":"10 boxes x 20 bags",
    });
    eql.insert("products", 1, &chai)?;
    let chang = json!({
        "product_name":"Chang",
        "category_id":1,
        "quantity_per_unit":"24 - 12 oz bottles"
    });
    eql.insert("products", 2, &chang)?;
    let aniseed = json!({
        "product_name":"Aniseed Syrup",
        "category_id":2,
        "quantity_per_unit":"12 - 550 ml bottles"
    });
    eql.insert("products", 3, &aniseed)?;
    let cajun = json!({
        "product_name":"Chef Anton's Cajun Seasoning",
        "category_id":2,
        "quantity_per_unit":"48 - 6 oz jars"
    });
    eql.insert("products", 4, &cajun)?;
    Ok(())
}
use datafusion::{
    arrow::{
        array::{
            ArrayRef,
            StringArray,
            Date32Builder,
        },
        datatypes::DataType,
    },
    physical_plan::functions::make_scalar_function,
    logical_expr::Volatility,
    prelude::{
        create_udf,
        SessionContext,
        NdJsonReadOptions,
    },
    error::DataFusionError,
};
use std::sync::Arc;
use chrono::DateTime;
use datafusion::error::Result;


#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // let testdata = datafusion::test_util::parquet_test_data();

    // register parquet file with the execution context
    ctx.register_json(
        "users",
        &format!("./data.jsonl"),
        NdJsonReadOptions::default().
            file_extension(".jsonl"),
    ).await?;

    let to_date = make_scalar_function(to_date);
    let to_date = create_udf(
        "to_date",
        vec![DataType::Utf8],
        Arc::new(DataType::Date32),
        Volatility::Immutable,
        to_date,
    );

    ctx.register_udf(to_date.clone());

    let batches = {
        let df = ctx.sql(
                "SELECT distinct to_date(\"date\") as a from users;").await?;
        df.collect().await?
    };
    for batch in &batches {
        println!("{:?}", batch);
    }
    let list: Vec<_> = datafusion::arrow::json::writer::
        record_batches_to_json_rows(&batches[..])?;
    println!("{:?}", list);
    println!("-------------------------------------");
    println!("-------------------------------------");
    println!("-------------------------------------");
    println!("-------------------------------------");

    let batches1 = {
        let df = ctx.sql(
            "SELECT distinct date from 'users';"
        ).await?;
        df.collect().await?
    };
    for batch in &batches1 {
        println!("{:?}", batch);
    }
    let list: Vec<_> = datafusion::arrow::json::writer::
        record_batches_to_json_rows(&batches1[..])?;
    println!("{:?}", list);


    Ok(())
}

fn to_date(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.is_empty() || args.len() > 1 {
        return Err(DataFusionError::Internal(format!(
            "to_date was called with {} arguments. It requires only 1.",
            args.len()
        )));
    }
    let arg = &args[0].as_any().downcast_ref::<StringArray>().unwrap();
    let mut builder = Date32Builder::new();
    for date_string in arg.iter() {
        let date_time = match DateTime::parse_from_rfc3339(
            date_string.expect("date_string is null"),
        ) {
            Ok(dt) => dt,
            Err(e) => {
                // builder.append_value((date_time.timestamp() / 86400) as i32);
                return Result::Err(DataFusionError::Internal(e.to_string()));
            }
        };
        builder.append_value((date_time.timestamp() / 86400) as i32);
    }
    Ok(Arc::new(builder.finish()))
}


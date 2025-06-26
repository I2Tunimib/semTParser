mod operations;
mod python_helpers;
use crate::{
    operations::{
        logs_from_last_get_table, parse_json, pre_process_operations, process_operations,
    },
    python_helpers::{
        create_base_file, create_extension_operation, create_reconciliation_operation,
        write_table_loader,
    },
};
use clap::Parser;
use dotenv::dotenv;

#[derive(Parser)]
#[command(name = "semTParser")]
#[command(about = "A tool to parse and process semT logs for table operations.")]
struct Args {
    #[arg(short, long, default_value = "./logs.txt")]
    log_file: String,

    #[arg(short, long, default_value = "./table_1.csv")]
    table_file: String,
}

fn main() {
    println!("Hello, world!");
    let args = Args::parse();

    dotenv().ok();
    match logs_from_last_get_table(&args.log_file) {
        Ok(Some(results)) => {
            println!(
                "Found {} lines after the last GET_TABLE entry:",
                results.len()
            );
            // Process the results
            match pre_process_operations(results) {
                Ok(operations) => {
                    let processed_operations = process_operations(operations);
                    let current_timestamp = chrono::Utc::now().format("%Y-%m-%d_%H-%M").to_string();
                    let table_name = format!("test_table-{}", current_timestamp);
                    let path = format!("./base_file_{}.py", current_timestamp);
                    match create_base_file(path.as_str()) {
                        Ok(file_path) => {
                            println!("Base file created at: {}", file_path);
                        }
                        Err(e) => eprintln!("Error creating base file: {}", e),
                    }
                    match write_table_loader(path.as_str(), &args.table_file, table_name.as_str()) {
                        Ok(_) => println!("Table loader written successfully."),
                        Err(e) => eprintln!("Error writing table loader: {}", e),
                    }
                    for operation in &processed_operations {
                        let operation_type = operation.get("OpType").unwrap();
                        match operation_type.as_str() {
                            "RECONCILIATION" => {
                                let reconciler_id = operation.get("Reconciler").unwrap();
                                let col_name = operation.get("ColumnName").unwrap();
                                let res = create_reconciliation_operation(
                                    path.as_str(),
                                    col_name,
                                    reconciler_id,
                                    None,
                                );
                                match res {
                                    Ok(_) => {
                                        println!("Reconciliation operation created successfully.")
                                    }
                                    Err(e) => {
                                        eprintln!("Error creating reconciliation operation: {}", e)
                                    }
                                }
                            }
                            "EXTENSION" => {
                                // Handle extension operation
                                println!("Extension operation detected: {:?}", operation);
                                // Here you can implement the logic for handling extension operations
                                let extender_id = operation.get("Extender").unwrap();
                                let col_name = operation.get("ColumnName").unwrap();

                                let additional_data =
                                    parse_json(operation.get("AdditionalData").unwrap()).unwrap();
                                let props: Vec<String> = additional_data
                                    .get("properties")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .split(' ')
                                    .filter(|s| !s.is_empty())
                                    .map(|s| s.to_string())
                                    .collect();
                                match create_extension_operation(
                                    path.as_str(),
                                    col_name,
                                    extender_id,
                                    props,
                                    None,
                                ) {
                                    Ok(_) => {
                                        println!("Extension operation created successfully.")
                                    }
                                    Err(e) => {
                                        eprintln!("Error creating extension operation: {}", e)
                                    }
                                }
                            }
                            _ => {
                                println!("Unkown Operation type: {}", operation_type);
                                // Here you can handle other operation types as needed
                            }
                        }
                    }
                }
                Err(e) => eprintln!("Error processing operations: {}", e),
            }
        }
        Ok(None) => println!("No GET_TABLE entry found."),
        Err(e) => eprintln!("Error: {}", e),
    }
}

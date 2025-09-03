mod code_helper;
mod notebook_helpers;
mod operations;
mod python_helpers;

use crate::{
    notebook_helpers::create_notebook,
    operations::{logs_from_last_get_table, pre_process_operations, process_operations},
    python_helpers::create_python,
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

    #[arg(short, long, default_value = "python", value_parser = ["python", "notebook"])]
    format: String,
}

fn main() {
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
                    match args.format.as_str() {
                        "python" => {
                            let python_args = python_helpers::Args {
                                table_file: args.table_file.clone(),
                            };
                            match create_python(processed_operations, python_args) {
                                Ok(file_path) => println!("Python file created at: {}", file_path),
                                Err(e) => eprintln!("Error creating Python file: {}", e),
                            }
                        }
                        "notebook" => {
                            let notebook_args = notebook_helpers::Args {
                                table_file: args.table_file.clone(),
                            };
                            match create_notebook(processed_operations, notebook_args) {
                                Ok(file_path) => {
                                    println!("Notebook file created at: {}", file_path)
                                }
                                Err(e) => eprintln!("Error creating notebook file: {}", e),
                            }
                        }
                        _ => eprintln!("Unknown format specified: {}", args.format),
                    }
                }
                Err(e) => eprintln!("Error processing operations: {}", e),
            }
        }
        Ok(None) => {
            println!("No GET_TABLE entry found. Creating base file with no operations.");
            // Create file with empty operations
            let empty_operations = Vec::new();
            match args.format.as_str() {
                "python" => {
                    let python_args = python_helpers::Args {
                        table_file: args.table_file.clone(),
                    };
                    match create_python(empty_operations, python_args) {
                        Ok(file_path) => println!("Python file created at: {}", file_path),
                        Err(e) => eprintln!("Error creating Python file: {}", e),
                    }
                }
                "notebook" => {
                    let notebook_args = notebook_helpers::Args {
                        table_file: args.table_file.clone(),
                    };
                    match create_notebook(empty_operations, notebook_args) {
                        Ok(file_path) => {
                            println!("Notebook file created at: {}", file_path)
                        }
                        Err(e) => eprintln!("Error creating notebook file: {}", e),
                    }
                }
                _ => eprintln!("Unknown format specified: {}", args.format),
            }
        }
        Err(e) => eprintln!("Error: {}", e),
    }
}

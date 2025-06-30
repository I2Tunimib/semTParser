use crate::code_helper::{
    get_base_dataset_loader, get_base_extension_operation, get_base_file_loader_code,
    get_base_reconciliation_operation,
};
use crate::operations::parse_json;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Error, Write},
    path::Path,
};
pub struct Args {
    pub table_file: String,
}

pub fn create_base_file(path: &str) -> Result<String, Error> {
    let path = Path::new(path);
    let mut file = get_file_writer(path)?;
    let formatted_code = get_base_file_loader_code();
    file.write_all(formatted_code.as_bytes())?;
    Ok(path.to_string_lossy().to_string())
}

pub fn write_table_loader(
    file_path_str: &str,
    table_path_str: &str,
    table_name: &str,
) -> Result<(), Error> {
    let table_path = Path::new(table_path_str);
    let file_path = Path::new(file_path_str);
    if table_path.exists() {
        let formatted_code = get_base_dataset_loader(table_path_str, "4", table_name);

        //write to file
        let mut file = get_file_writer(file_path)?;
        file.write_all(formatted_code.as_bytes())?;
        Ok(())
    } else {
        Err(Error::new(
            std::io::ErrorKind::NotFound,
            "Table path does not exist",
        ))
    }
}
pub fn create_extension_operation(
    file_path_str: &str,
    column_name: &str,
    extender_id: &str,
    properties: Vec<String>,
    additional_params: Option<Vec<String>>,
) -> Result<(), Error> {
    let file_path = Path::new(file_path_str);
    let mut file = get_file_writer(file_path)?;

    let formatted_code =
        get_base_extension_operation(column_name, properties, additional_params, extender_id);
    file.write_all(formatted_code.as_bytes())?;
    Ok(())
}

pub fn create_reconciliation_operation(
    file_path_str: &str,
    column_name: &str,
    reconciliator_id: &str,
    additional_columns: Option<Vec<String>>,
) -> Result<(), Error> {
    let file_path = Path::new(file_path_str);
    let mut file = get_file_writer(file_path)?;

    let formatted_code =
        get_base_reconciliation_operation(column_name, additional_columns, reconciliator_id);
    file.write_all(formatted_code.as_bytes())?;
    Ok(())
}

fn get_file_writer(file_path: &Path) -> Result<File, Error> {
    if file_path.exists() {
        OpenOptions::new().write(true).append(true).open(file_path)
    } else {
        File::create(file_path)
    }
}

pub fn create_python(
    operations: Vec<HashMap<String, String>>,
    args: Args,
) -> Result<String, std::io::Error> {
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
    for operation in operations {
        let operation_type = operation.get("OpType").unwrap();
        match operation_type.as_str() {
            "RECONCILIATION" => {
                let reconciler_id = operation.get("Reconciler").unwrap();
                let col_name = operation.get("ColumnName").unwrap();
                let res =
                    create_reconciliation_operation(path.as_str(), col_name, reconciler_id, None);
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

                let additional_data = parse_json(operation.get("AdditionalData").unwrap()).unwrap();
                let props: Vec<String> = additional_data
                    .get("properties")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .split(' ')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .collect();
                match create_extension_operation(path.as_str(), col_name, extender_id, props, None)
                {
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
    Ok(path)
}

use crate::code_helper::{
    get_base_dataset_loader, get_base_dataset_loader_with_column_deletion,
    get_base_extension_operation, get_base_file_loader_code, get_base_reconciliation_operation,
};
use crate::operations::{parse_deleted_columns, parse_json};
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
    dataset_id: &str,
    deleted_columns: Option<Vec<String>>,
) -> Result<(), Error> {
    let table_path = Path::new(table_path_str);
    let file_path = Path::new(file_path_str);

    let formatted_code = if table_path.exists() {
        match deleted_columns {
            Some(cols) if !cols.is_empty() => get_base_dataset_loader_with_column_deletion(
                table_path_str,
                dataset_id,
                table_name,
                cols,
            ),
            _ => get_base_dataset_loader(table_path_str, dataset_id, table_name),
        }
    } else {
        // If table file doesn't exist, still generate the loader code but warn the user
        eprintln!(
            "Warning: Table file '{}' does not exist. Generating code with placeholder path.",
            table_path_str
        );
        match deleted_columns {
            Some(cols) if !cols.is_empty() => get_base_dataset_loader_with_column_deletion(
                table_path_str,
                dataset_id,
                table_name,
                cols,
            ),
            _ => get_base_dataset_loader(table_path_str, dataset_id, table_name),
        }
    };

    //write to file
    let mut file = get_file_writer(file_path)?;
    file.write_all(formatted_code.as_bytes())?;
    Ok(())
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

fn write_operation_separator(
    file_path_str: &str,
    operation: &HashMap<String, String>,
    operation_index: usize,
) -> Result<(), Error> {
    let file_path = Path::new(file_path_str);
    let mut file = get_file_writer(file_path)?;

    // Create a parsable comment separator with operation metadata
    let separator = format!(
        "\n# =============================================================================\n# OPERATION_{}: {}\n# METADATA: {{\n",
        operation_index,
        operation.get("OpType").unwrap_or(&"UNKNOWN".to_string())
    );

    file.write_all(separator.as_bytes())?;

    // Write all operation fields as parsable comments
    for (key, value) in operation {
        let metadata_line = format!("#   \"{}\": \"{}\",\n", key, value.replace("\"", "\\\""));
        file.write_all(metadata_line.as_bytes())?;
    }

    file.write_all(
        b"# }\n# =============================================================================\n\n",
    )?;

    Ok(())
}

fn write_operation_summary(
    file_path_str: &str,
    operations: &[HashMap<String, String>],
) -> Result<(), Error> {
    let file_path = Path::new(file_path_str);
    let mut file = get_file_writer(file_path)?;

    file.write_all(
        b"\n# =============================================================================\n",
    )?;
    file.write_all(b"# OPERATION SUMMARY\n")?;
    file.write_all(
        b"# =============================================================================\n",
    )?;
    file.write_all(format!("# Total operations: {}\n", operations.len()).as_bytes())?;

    for (index, operation) in operations.iter().enumerate() {
        let op_type = operation.get("OpType").map_or("UNKNOWN", |s| s.as_str());
        let column_name = operation.get("ColumnName").map_or("N/A", |s| s.as_str());
        let timestamp = operation.get("timestamp").map_or("N/A", |s| s.as_str());

        file.write_all(
            format!(
                "# Operation {}: {} on column '{}' at {}\n",
                index + 1,
                op_type,
                column_name,
                timestamp
            )
            .as_bytes(),
        )?;
    }

    file.write_all(
        b"# =============================================================================\n",
    )?;

    Ok(())
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

    // Debug: Print only operations with DeletedCols
    operations.iter().for_each(|op| {
        if let Some(deleted_cols) = op.get("DeletedCols") {
            println!("Found operation with DeletedCols:");
            println!("  OpType: {:?}", op.get("OpType"));
            println!("  DeletedCols: {:?}", deleted_cols);
            println!(
                "  Parsed columns: {:?}",
                parse_deleted_columns(deleted_cols)
            );
            println!("  Full operation: {:?}", op);
            println!("---");
        }
    });

    // Look for deleted columns in SAVE_TABLE operations
    let deleted_columns = operations
        .iter()
        .find(|op| op.get("OpType") == Some(&"SAVE_TABLE".to_string()))
        .and_then(|op| op.get("DeletedCols"))
        .map(|deleted_cols_str| parse_deleted_columns(deleted_cols_str))
        .filter(|cols| !cols.is_empty());

    let default_dataset_id = "5".to_string();
    let current_dataset_id = if !operations.is_empty() {
        let first_op = operations[0].clone();
        println!("first operation {:?}", first_op.keys());
        operations[0]
            .get("DatasetId")
            .unwrap_or(&default_dataset_id)
    } else {
        println!("No operations found, using default dataset ID");
        &default_dataset_id
    };
    match write_table_loader(
        path.as_str(),
        &args.table_file,
        table_name.as_str(),
        current_dataset_id,
        deleted_columns,
    ) {
        Ok(_) => println!("Table loader written successfully."),
        Err(e) => eprintln!("Error writing table loader: {}", e),
    }

    for (index, operation) in operations.iter().enumerate() {
        // Write operation separator with metadata
        if let Err(e) = write_operation_separator(&path, operation, index + 1) {
            eprintln!("Error writing operation separator: {}", e);
        }

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
                println!("Unknown Operation type: {}", operation_type);
                // Here you can handle other operation types as needed
            }
        }
    }

    // Write operation summary at the end of the file
    if let Err(e) = write_operation_summary(&path, &operations) {
        eprintln!("Error writing operation summary: {}", e);
    }

    Ok(path)
}

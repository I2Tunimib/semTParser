use crate::code_helper::{
    get_base_dataset_loader, get_base_dataset_loader_with_column_deletion,
    get_base_export_operation, get_base_extension_operation, get_base_file_loader_code,
    get_base_propagation_operation, get_base_reconciliation_operation,
};
use crate::operations::{parse_deleted_columns, parse_json};
use serde_json::Value;
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
pub fn create_propagation_operation(
    file_path_str: &str,
    column_name: &str,
    additional_data: &Value,
) -> Result<(), Error> {
    let file_path = Path::new(file_path_str);
    let mut file = get_file_writer(file_path)?;

    let formatted_code = get_base_propagation_operation(column_name, Some(additional_data));
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
    _operation_index: usize,
    displayed_operation_number: usize,
) -> Result<(), Error> {
    let file_path = Path::new(file_path_str);
    let mut file = get_file_writer(file_path)?;

    // Create a parsable comment separator with operation metadata
    let separator = format!(
        "\n# =============================================================================\n# OPERATION_{}: {}\n# METADATA: {{\n",
        displayed_operation_number,
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

    // Filter operations to only include RECONCILIATION and EXTENSION
    let displayed_operations: Vec<&HashMap<String, String>> = operations
        .iter()
        .filter(|op| {
            let op_type = op.get("OpType").map_or("UNKNOWN", |s| s.as_str());
            op_type == "RECONCILIATION" || op_type == "EXTENSION" || op_type == "PROPAGATE_TYPE"
        })
        .collect();

    file.write_all(
        b"\n# =============================================================================\n",
    )?;
    file.write_all(b"# OPERATION SUMMARY\n")?;
    file.write_all(
        b"# =============================================================================\n",
    )?;
    file.write_all(format!("# Total operations: {}\n", displayed_operations.len()).as_bytes())?;

    for (_index, operation) in displayed_operations.iter().enumerate() {
        let op_type = operation.get("OpType").map_or("UNKNOWN", |s| s.as_str());
        let column_name = operation.get("ColumnName").map_or("N/A", |s| s.as_str());
        let timestamp = operation.get("timestamp").map_or("N/A", |s| s.as_str());

        // Use a bullet style (dot) instead of numbering
        file.write_all(
            format!(
                "# - {} on column '{}' at {}\n",
                op_type, column_name, timestamp
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
            println!(
                "  Parsed columns: {:?}",
                parse_deleted_columns(deleted_cols)
            );
        }
    });

    // Look for deleted columns in SAVE_TABLE operations
    let deleted_columns = operations
        .iter()
        .find(|op| op.get("OpType") == Some(&"SAVE_TABLE".to_string()))
        .and_then(|op| op.get("DeletedCols"))
        .map(|deleted_cols_str| parse_deleted_columns(deleted_cols_str))
        .filter(|cols| !cols.is_empty());

    let default_dataset_id = "0".to_string();
    let current_dataset_id = if !operations.is_empty() {
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
        let operation_type = operation.get("OpType").unwrap();

        match operation_type.as_str() {
            "RECONCILIATION" | "EXTENSION" | "PROPAGATE_TYPE" | "EXPORT" => {
                // Only write separator and generate code for RECONCILIATION, EXTENSION, PROPAGATE_TYPE and EXPORT operations
                // Count displayed operations by filtering up to current position
                let displayed_operation_number = operations[..=index]
                    .iter()
                    .filter(|op| {
                        let op_type = op.get("OpType").map_or("UNKNOWN", |s| s.as_str());
                        op_type == "RECONCILIATION"
                            || op_type == "EXTENSION"
                            || op_type == "PROPAGATE_TYPE"
                            || op_type == "EXPORT"
                    })
                    .count();

                // Write operation separator with metadata
                if let Err(e) = write_operation_separator(
                    &path,
                    operation,
                    index + 1,
                    displayed_operation_number,
                ) {
                    eprintln!("Error writing operation separator: {}", e);
                }
            }
            _ => {
                // Skip writing separators for other operation types
                continue;
            }
        }

        match operation_type.as_str() {
            "RECONCILIATION" => {
                let reconciler_id = operation.get("Reconciler").unwrap();
                let col_name = operation.get("ColumnName").unwrap();

                // Parse additional data to check for additionalColumns
                let additional_columns =
                    if let Some(additional_data_str) = operation.get("AdditionalData") {
                        if let Some(additional_data) = parse_json(additional_data_str) {
                            if let Some(additional_columns_obj) =
                                additional_data.get("additionalColumns")
                            {
                                if let Some(obj) = additional_columns_obj.as_object() {
                                    let column_names: Vec<String> =
                                        obj.keys().map(|k| format!("\"{}\"", k)).collect();
                                    if !column_names.is_empty() {
                                        println!(
                                            "Found additionalColumns in reconciliation: {:?}",
                                            column_names
                                        );
                                        Some(column_names)
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                let res = create_reconciliation_operation(
                    path.as_str(),
                    col_name,
                    reconciler_id,
                    additional_columns,
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
            "PROPAGATE_TYPE" => {
                let col_name = operation.get("ColumnName").unwrap();
                let additional_data = parse_json(operation.get("AdditionalData").unwrap()).unwrap();

                if let Some(data_map) = additional_data.as_object() {
                    let value = Value::Object(data_map.clone());
                    let res = create_propagation_operation(path.as_str(), col_name, &value);
                    match res {
                        Ok(_) => println!("Propagation operation created successfully."),
                        Err(e) => eprintln!("Error creating propagation operation: {}", e),
                    }
                } else {
                    eprintln!("Additional data for PROPAGATE_TYPE is not an object");
                }
            }
            "EXTENSION" => {
                // Handle extension operation
                // Here you can implement the logic for handling extension operations
                let extender_id = operation.get("Extender").unwrap();
                let col_name = operation.get("ColumnName").unwrap();

                let additional_data = parse_json(operation.get("AdditionalData").unwrap()).unwrap();

                // Look for "property" array in additional data and join values with whitespaces
                // If "property" array exists, use its values; otherwise fallback to "properties" string
                let mut props: Vec<String> = additional_data
                    .get("property")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        let property_values: Vec<String> = arr.iter()
                            .filter_map(|item| item.as_str())
                            .map(|s| s.to_string())
                            .collect();
                        println!("Found 'property' array in additional data: {:?}", property_values);
                        property_values
                    })
                    .unwrap_or_else(|| {
                        // Fallback to existing "properties" field for backward compatibility
                        let fallback_props: Vec<String> = additional_data
                            .get("properties")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .split(' ')
                            .filter(|s| !s.is_empty())
                            .map(|s| s.to_string())
                            .collect();
                        if !fallback_props.is_empty() {
                            println!("Using fallback 'properties' field: {:?}", fallback_props);
                        } else {
                            println!("No 'property' array or 'properties' field found in additional data");
                        }
                        fallback_props
                    });

                // Check for weatherParams and add them to properties
                if let Some(weather_params) = additional_data.get("weatherParams") {
                    if let Some(weather_array) = weather_params.as_array() {
                        let weather_props: Vec<String> = weather_array
                            .iter()
                            .filter_map(|item| item.as_str())
                            .map(|s| s.to_string())
                            .collect();
                        props.extend(weather_props);
                        println!("Added weatherParams to properties: {:?}", weather_params);
                    }
                }

                // Check for labels and add them to properties
                if let Some(labels) = additional_data.get("labels") {
                    if let Some(labels_array) = labels.as_array() {
                        let label_props: Vec<String> = labels_array
                            .iter()
                            .filter_map(|item| item.as_str())
                            .map(|s| s.to_string())
                            .collect();
                        props.extend(label_props);
                        println!("Added labels to properties: {:?}", labels);
                    }
                }

                // Check for dates field and extract date column name for other_params
                let mut other_params = Vec::new();
                if let Some(dates) = additional_data.get("dates") {
                    if let Some(dates_obj) = dates.as_object() {
                        // Get the first key and extract the third element from its array
                        if let Some((first_key, first_value)) = dates_obj.iter().next() {
                            if let Some(date_array) = first_value.as_array() {
                                if date_array.len() > 2 {
                                    if let Some(date_column_name) = date_array[2].as_str() {
                                        other_params.push(format!(
                                            "\"date_column_name\": \"{}\"",
                                            date_column_name
                                        ));
                                        println!(
                                            "Found date column name: {} from key: {}",
                                            date_column_name, first_key
                                        );
                                    }
                                }
                            }
                        }
                    }
                }

                let additional_params = if other_params.is_empty() {
                    None
                } else {
                    Some(other_params)
                };

                match create_extension_operation(
                    path.as_str(),
                    col_name,
                    extender_id,
                    props,
                    additional_params,
                ) {
                    Ok(_) => {
                        println!("Extension operation created successfully.")
                    }
                    Err(e) => {
                        eprintln!("Error creating extension operation: {}", e)
                    }
                }
            }
            "EXPORT" => {
                // Handle export operation
                if let Some(additional_data_str) = operation.get("AdditionalData") {
                    if let Some(additional_data) = parse_json(additional_data_str) {
                        if let Some(format) = additional_data.get("format").and_then(|f| f.as_str())
                        {
                            let output_file = additional_data
                                .get("outputFile")
                                .and_then(|f| f.as_str())
                                .unwrap_or("export_output");

                            if let Some(export_code) =
                                get_base_export_operation(format, output_file)
                            {
                                let file_path = Path::new(&path);
                                match get_file_writer(file_path) {
                                    Ok(mut file) => {
                                        use std::io::Write;
                                        if let Err(e) = writeln!(file, "\n{}", export_code) {
                                            eprintln!("Error writing export operation: {}", e);
                                        } else {
                                            println!("Export operation created successfully for format: {}", format);
                                        }
                                    }
                                    Err(e) => eprintln!("Error getting file writer: {}", e),
                                }
                            } else {
                                println!(
                                    "Unsupported export format: {}, skipping export operation",
                                    format
                                );
                            }
                        } else {
                            eprintln!("No format specified in EXPORT AdditionalData");
                        }
                    } else {
                        eprintln!("Could not parse AdditionalData for EXPORT operation");
                    }
                } else {
                    eprintln!("No AdditionalData found for EXPORT operation");
                }
            }
            _ => {
                println!("Unknown Operation type: {}", operation_type);
                // Here you can handle other operation types as needed
            }
        }
    }

    // Check if there's no EXPORT operation, add default JSON export
    let has_export = operations
        .iter()
        .any(|op| op.get("OpType").map_or(false, |t| t == "EXPORT"));

    if !has_export {
        println!("No EXPORT operation found, adding default JSON export");
        if let Some(default_export) = get_base_export_operation("json", "results.json") {
            let file_path = Path::new(&path);
            match get_file_writer(file_path) {
                Ok(mut file) => {
                    use std::io::Write;
                    if let Err(e) = writeln!(file, "\n# Default Export (JSON)\n{}", default_export)
                    {
                        eprintln!("Error writing default export operation: {}", e);
                    }
                }
                Err(e) => eprintln!("Error getting file writer for default export: {}", e),
            }
        }
    }

    // Write operation summary at the end of the file
    if let Err(e) = write_operation_summary(&path, &operations) {
        eprintln!("Error writing operation summary: {}", e);
    }

    Ok(path)
}

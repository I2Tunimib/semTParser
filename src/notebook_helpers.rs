use chrono;
use serde::Serialize;
use std::collections::HashMap;
use uuid::Uuid;

use crate::{
    code_helper::{
        get_base_export_operation, get_base_extension_operation, get_base_modification_operation,
        get_base_notebook_dataset_loader, get_base_notebook_dataset_loader_with_column_deletion,
        get_base_notebook_file_loader_code, get_base_propagation_operation,
        get_base_reconciliation_operation,
    },
    operations::{parse_deleted_columns, parse_json},
};

#[derive(Serialize)]
struct Notebook {
    cells: Vec<Cell>,
    metadata: Metadata,
    nbformat: u32,
    nbformat_minor: u32,
}

pub struct Args {
    pub table_file: String,
}

#[derive(Serialize)]
#[serde(tag = "cell_type", rename_all = "lowercase")]
enum Cell {
    Code {
        id: String,
        metadata: serde_json::Value,
        source: Vec<String>,
        execution_count: Option<u32>,
        outputs: Vec<serde_json::Value>,
    },
    Markdown {
        id: String,
        metadata: serde_json::Value,
        source: Vec<String>,
    },
}

#[derive(Serialize)]
struct Metadata {}

pub fn create_notebook(
    operations: Vec<HashMap<String, String>>,
    args: Args,
) -> Result<String, std::io::Error> {
    let current_timestamp = chrono::Utc::now().format("%Y-%m-%d_%H-%M").to_string();
    let table_name = format!("test_table-{}", current_timestamp);
    let path = format!("./base_notebook_file_{}.ipynb", current_timestamp);
    let default_dataset_id = "1".to_string();
    let used_dataset_id = operations[0]
        .get("DatasetId")
        .unwrap_or(&default_dataset_id);
    // Look for deleted columns in SAVE_TABLE operations
    let deleted_columns = operations
        .iter()
        .find(|op| op.get("OpType") == Some(&"SAVE_TABLE".to_string()))
        .and_then(|op| op.get("DeletedCols"))
        .map(|deleted_cols_str| parse_deleted_columns(deleted_cols_str))
        .filter(|cols| !cols.is_empty());

    // Create base cells - starting with summary as first cell
    let mut cells = vec![];

    // Add operation summary cell as the first cell
    // Filter operations to only include RECONCILIATION and EXTENSION
    let mut displayed_operations: Vec<&HashMap<String, String>> = operations.iter().collect();

    // Sort operations by timestamp in ascending order
    displayed_operations.sort_by(|a, b| {
        let timestamp_a = a.get("timestamp").map(|s| s.as_str()).unwrap_or("");
        let timestamp_b = b.get("timestamp").map(|s| s.as_str()).unwrap_or("");
        timestamp_a.cmp(timestamp_b)
    });

    let summary_metadata = serde_json::json!({
        "semtparser": {
            "cell_type": "summary",
            "total_operations": displayed_operations.len(),
            "operation_types": displayed_operations.iter()
                .map(|op| op.get("OpType").map_or("UNKNOWN".to_string(), |s| s.clone()))
                .collect::<Vec<String>>()
        }
    });

    let mut summary_lines = vec![
        "# Operation Summary\n".to_string(),
        format!(
            "**Total operations processed:** {}\n\n",
            displayed_operations.len()
        ),
    ];

    for (_, operation) in displayed_operations.iter().enumerate() {
        let op_type = operation.get("OpType").map_or("UNKNOWN", |s| s.as_str());
        let column_name = operation.get("ColumnName").map_or("N/A", |s| s.as_str());
        let timestamp = operation.get("timestamp").map_or("N/A", |s| s.as_str());

        // Show extender/reconciler information
        let tool_info = match op_type {
            "RECONCILIATION" => {
                let reconciler_id = operation.get("Reconciler").map_or("N/A", |s| s.as_str());
                format!(" using **{}** reconciler", reconciler_id)
            }
            "EXTENSION" => {
                let extender_id = operation.get("Extender").map_or("N/A", |s| s.as_str());
                format!(" using **{}** extender", extender_id)
            }
            _ => String::new(),
        };

        summary_lines.push(format!(
            "- **{}** on column `{}`{} at `{}`\n",
            op_type, column_name, tool_info, timestamp
        ));
    }

    cells.push(Cell::Markdown {
        id: Uuid::new_v4().to_string(),
        metadata: summary_metadata,
        source: summary_lines,
    });

    // Add Operation 0: Setup and Data Loading
    let operation_0_metadata = serde_json::json!({
        "semtparser": {
            "operation_index": 0,
            "operation_type": "SETUP",
            "operation_data": {
                "description": "Initial setup and data loading"
            }
        }
    });

    cells.push(Cell::Markdown {
        id: Uuid::new_v4().to_string(),
        metadata: operation_0_metadata.clone(),
        source: vec!["## Operation 0: Setup and Data Loading\n".to_string()],
    });

    // Add initial imports cell as part of Operation 0
    cells.push(Cell::Code {
        id: Uuid::new_v4().to_string(),
        metadata: serde_json::json!({}),
        source: get_base_notebook_file_loader_code()
            .lines()
            .map(|line| format!("{}\n", line))
            .collect(),
        execution_count: None,
        outputs: vec![],
    });

    // Data loading cell with optional column deletion as part of Operation 0
    let dataset_loader_code = match deleted_columns {
        Some(ref cols) => get_base_notebook_dataset_loader_with_column_deletion(
            args.table_file.as_str(),
            used_dataset_id,
            table_name.as_str(),
            cols.clone(),
        ),
        None => get_base_notebook_dataset_loader(
            args.table_file.as_str(),
            used_dataset_id,
            table_name.as_str(),
        ),
    };

    cells.push(Cell::Code {
        id: Uuid::new_v4().to_string(),
        metadata: operation_0_metadata,
        source: dataset_loader_code
            .lines()
            .map(|line| format!("{}\n", line))
            .collect(),
        execution_count: None,
        outputs: vec![],
    });

    // Add operation cells
    let mut displayed_operation_counter = 0; // Counter for RECONCILIATION, EXTENSION, PROPAGATE_TYPE and EXPORT operations only

    // Check if there's any EXPORT operation in the operations list
    let has_export_operation = operations
        .iter()
        .any(|op| op.get("OpType") == Some(&"EXPORT".to_string()));

    for (index, operation) in operations.iter().enumerate() {
        let operation_type = operation.get("OpType").unwrap();

        // Create metadata object with all operation information
        let operation_metadata = serde_json::json!({
            "semtparser": {
                "operation_index": index + 1,
                "operation_type": operation_type,
                "operation_data": operation
            }
        });

        match operation_type.as_str() {
            "RECONCILIATION" => {
                displayed_operation_counter += 1; // Increment counter for displayed operations

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

                cells.push(Cell::Markdown {
                    id: Uuid::new_v4().to_string(),
                    metadata: operation_metadata.clone(),
                    source: vec![format!(
                        "## Operation {}: Reconciliation for column {} by {}",
                        displayed_operation_counter, col_name, reconciler_id
                    )],
                });

                cells.push(Cell::Code {
                    id: Uuid::new_v4().to_string(),
                    metadata: operation_metadata,
                    source: get_base_reconciliation_operation(
                        &col_name,
                        additional_columns,
                        reconciler_id,
                    )
                    .lines()
                    .map(|line| format!("{}\n", line))
                    .collect(),
                    execution_count: None,
                    outputs: vec![],
                });
            }
            "EXTENSION" => {
                displayed_operation_counter += 1; // Increment counter for displayed operations

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

                cells.push(Cell::Markdown {
                    id: Uuid::new_v4().to_string(),
                    metadata: operation_metadata.clone(),
                    source: vec![format!(
                        "## Operation {}: Extension for column {} by {}",
                        displayed_operation_counter, col_name, extender_id
                    )],
                });

                cells.push(Cell::Code {
                    id: Uuid::new_v4().to_string(),
                    metadata: operation_metadata,
                    source: get_base_extension_operation(
                        &col_name,
                        props,
                        additional_params,
                        extender_id,
                    )
                    .lines()
                    .map(|line| format!("{}\n", line))
                    .collect(),
                    execution_count: None,
                    outputs: vec![],
                });
            }
            "PROPAGATE_TYPE" => {
                displayed_operation_counter += 1; // Increment counter for displayed operations

                let col_name = operation.get("ColumnName").unwrap();

                let additional_data = parse_json(operation.get("AdditionalData").unwrap()).unwrap();

                if let Some(data_map) = additional_data.as_object() {
                    let value = serde_json::Value::Object(data_map.clone());

                    cells.push(Cell::Markdown {
                        id: Uuid::new_v4().to_string(),
                        metadata: operation_metadata.clone(),
                        source: vec![format!(
                            "## Operation {}: Propagation for column {}",
                            displayed_operation_counter, col_name
                        )],
                    });

                    cells.push(Cell::Code {
                        id: Uuid::new_v4().to_string(),
                        metadata: operation_metadata,
                        source: get_base_propagation_operation(&col_name, Some(&value))
                            .lines()
                            .map(|line| format!("{}\n", line))
                            .collect(),
                        execution_count: None,
                        outputs: vec![],
                    });
                } else {
                    cells.push(Cell::Markdown {
                        id: Uuid::new_v4().to_string(),
                        metadata: operation_metadata,
                        source: vec![format!(
                            "Operation {}: PROPAGATE_TYPE - Invalid additional data for column {}",
                            displayed_operation_counter, col_name
                        )],
                    });
                }
            }
            "EXPORT" => {
                displayed_operation_counter += 1; // Increment counter for displayed operations

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
                                cells.push(Cell::Markdown {
                                    id: Uuid::new_v4().to_string(),
                                    metadata: operation_metadata.clone(),
                                    source: vec![format!(
                                        "## Operation {}: Export as {}",
                                        displayed_operation_counter,
                                        format.to_uppercase()
                                    )],
                                });

                                cells.push(Cell::Code {
                                    id: Uuid::new_v4().to_string(),
                                    metadata: operation_metadata,
                                    source: export_code
                                        .lines()
                                        .map(|line| format!("{}\n", line))
                                        .collect(),
                                    execution_count: None,
                                    outputs: vec![],
                                });
                                println!(
                                    "Export operation created successfully for format: {}",
                                    format
                                );
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
            "MODIFICATION" => {
                displayed_operation_counter += 1; // Increment counter for displayed operations

                let modifier_name = operation.get("Modifier").unwrap();
                let col_name = operation.get("ColumnName").unwrap();

                let additional_data = parse_json(operation.get("AdditionalData").unwrap()).unwrap();

                cells.push(Cell::Markdown {
                    id: Uuid::new_v4().to_string(),
                    metadata: operation_metadata.clone(),
                    source: vec![format!(
                        "## Operation {}: Modification for column {} by {}",
                        displayed_operation_counter, col_name, modifier_name
                    )],
                });

                cells.push(Cell::Code {
                    id: Uuid::new_v4().to_string(),
                    metadata: operation_metadata,
                    source: get_base_modification_operation(
                        &col_name,
                        modifier_name,
                        &additional_data,
                    )
                    .lines()
                    .map(|line| format!("{}\n", line))
                    .collect(),
                    execution_count: None,
                    outputs: vec![],
                });
            }
            "GET_TABLE" | "SAVE_TABLE" => {
                // Skip these operation types as they are not useful for notebook output
                continue;
            }
            _ => {
                cells.push(Cell::Markdown {
                    id: Uuid::new_v4().to_string(),
                    metadata: operation_metadata,
                    source: vec![format!(
                        "Operation {}: Unknown operation type: {}",
                        index + 1,
                        operation_type
                    )],
                });
            }
        }
    }

    // Check if no export operation was found in the logs, add default JSON export
    if !has_export_operation {
        println!("No export code generated, adding default JSON export");
        displayed_operation_counter += 1;

        let default_export_metadata = serde_json::json!({
            "semtparser": {
                "operation_index": operations.len() + 1,
                "operation_type": "EXPORT",
                "operation_data": {
                    "description": "Default JSON export",
                    "format": "json"
                }
            }
        });

        if let Some(default_export) = get_base_export_operation("json", "results.json") {
            cells.push(Cell::Markdown {
                id: Uuid::new_v4().to_string(),
                metadata: default_export_metadata.clone(),
                source: vec![format!(
                    "## Operation {}: Export as JSON (Default)",
                    displayed_operation_counter
                )],
            });

            cells.push(Cell::Code {
                id: Uuid::new_v4().to_string(),
                metadata: default_export_metadata,
                source: default_export
                    .lines()
                    .map(|line| format!("{}\n", line))
                    .collect(),
                execution_count: None,
                outputs: vec![],
            });
        }
    }

    let notebook = Notebook {
        nbformat: 4,
        nbformat_minor: 5,
        metadata: Metadata {},
        cells,
    };

    let json = serde_json::to_string_pretty(&notebook)?;
    std::fs::write(&path, json)?;

    Ok(path)
}

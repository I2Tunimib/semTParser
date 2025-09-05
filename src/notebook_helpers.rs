use chrono;
use serde::Serialize;
use std::collections::HashMap;
use uuid::Uuid;

use crate::{
    code_helper::{
        get_base_dataset_loader, get_base_dataset_loader_with_column_deletion,
        get_base_extension_operation, get_base_file_loader_code, get_base_reconciliation_operation,
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
    // Create base cells - similar to BASE_FILE_CONTENT in python_helpers
    let mut cells = vec![
        // Initial imports cell
        Cell::Code {
            id: Uuid::new_v4().to_string(),
            metadata: serde_json::json!({}),
            source: get_base_file_loader_code()
                .lines()
                .map(|line| format!("{}\n", line))
                .collect(),
            execution_count: None,
            outputs: vec![],
        },
    ];

    // Look for deleted columns in SAVE_TABLE operations
    let deleted_columns = operations
        .iter()
        .find(|op| op.get("OpType") == Some(&"SAVE_TABLE".to_string()))
        .and_then(|op| op.get("DeletedCols"))
        .map(|deleted_cols_str| parse_deleted_columns(deleted_cols_str))
        .filter(|cols| !cols.is_empty());

    // Data loading cell with optional column deletion
    let dataset_loader_code = match deleted_columns {
        Some(ref cols) => get_base_dataset_loader_with_column_deletion(
            args.table_file.as_str(),
            used_dataset_id,
            table_name.as_str(),
            cols.clone(),
        ),
        None => get_base_dataset_loader(
            args.table_file.as_str(),
            used_dataset_id,
            table_name.as_str(),
        ),
    };

    cells.push(Cell::Code {
        id: Uuid::new_v4().to_string(),
        metadata: serde_json::json!({}),
        source: dataset_loader_code
            .lines()
            .map(|line| format!("{}\n", line))
            .collect(),
        execution_count: None,
        outputs: vec![],
    });

    // Add operation cells
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
                let reconciler_id = operation.get("Reconciler").unwrap();
                let col_name = operation.get("ColumnName").unwrap();

                cells.push(Cell::Markdown {
                    id: Uuid::new_v4().to_string(),
                    metadata: operation_metadata.clone(),
                    source: vec![format!(
                        "## Operation {}: Reconciliation for column {} by {}",
                        index + 1,
                        col_name,
                        reconciler_id
                    )],
                });

                cells.push(Cell::Code {
                    id: Uuid::new_v4().to_string(),
                    metadata: operation_metadata,
                    source: get_base_reconciliation_operation(&col_name, None, reconciler_id)
                        .lines()
                        .map(|line| format!("{}\n", line))
                        .collect(),
                    execution_count: None,
                    outputs: vec![],
                });
            }
            "EXTENSION" => {
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

                cells.push(Cell::Markdown {
                    id: Uuid::new_v4().to_string(),
                    metadata: operation_metadata.clone(),
                    source: vec![format!(
                        "## Operation {}: Extension for column {} by {}",
                        index + 1,
                        col_name,
                        extender_id
                    )],
                });

                cells.push(Cell::Code {
                    id: Uuid::new_v4().to_string(),
                    metadata: operation_metadata,
                    source: get_base_extension_operation(&col_name, props, None, extender_id)
                        .lines()
                        .map(|line| format!("{}\n", line))
                        .collect(),
                    execution_count: None,
                    outputs: vec![],
                });
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

    // Add operation summary cell
    let summary_metadata = serde_json::json!({
        "semtparser": {
            "cell_type": "summary",
            "total_operations": operations.len(),
            "operation_types": operations.iter()
                .map(|op| op.get("OpType").map_or("UNKNOWN".to_string(), |s| s.clone()))
                .collect::<Vec<String>>()
        }
    });

    let mut summary_lines = vec![
        "# Operation Summary\n".to_string(),
        format!("**Total operations processed:** {}\n\n", operations.len()),
    ];

    for (index, operation) in operations.iter().enumerate() {
        let op_type = operation.get("OpType").map_or("UNKNOWN", |s| s.as_str());
        let column_name = operation.get("ColumnName").map_or("N/A", |s| s.as_str());
        let timestamp = operation.get("timestamp").map_or("N/A", |s| s.as_str());

        summary_lines.push(format!(
            "{}. **{}** on column `{}` at `{}`\n",
            index + 1,
            op_type,
            column_name,
            timestamp
        ));
    }

    cells.push(Cell::Markdown {
        id: Uuid::new_v4().to_string(),
        metadata: summary_metadata,
        source: summary_lines,
    });

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

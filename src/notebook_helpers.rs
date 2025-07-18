use chrono;
use serde::Serialize;
use std::collections::HashMap;
use uuid::Uuid;

use crate::{
    code_helper::{
        get_base_dataset_loader, get_base_dataset_loader_with_column_deletion, get_base_extension_operation, get_base_file_loader_code,
        get_base_reconciliation_operation,
    },
    operations::{parse_json, parse_deleted_columns},
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
        Some(ref cols) => {
            get_base_dataset_loader_with_column_deletion(args.table_file.as_str(), "4", table_name.as_str(), cols.clone())
        }
        None => {
            get_base_dataset_loader(args.table_file.as_str(), "4", table_name.as_str())
        }
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
    for operation in operations {
        let operation_type = operation.get("OpType").unwrap();
        match operation_type.as_str() {
            "RECONCILIATION" => {
                let reconciler_id = operation.get("Reconciler").unwrap();
                let col_name = operation.get("ColumnName").unwrap();

                cells.push(Cell::Markdown {
                    id: Uuid::new_v4().to_string(),
                    metadata: serde_json::json!({}),
                    source: vec![format!(
                        "## Reconciliation operation for column {} by {}",
                        col_name, reconciler_id
                    )],
                });

                cells.push(Cell::Code {
                    id: Uuid::new_v4().to_string(),
                    metadata: serde_json::json!({}),
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
                    metadata: serde_json::json!({}),
                    source: vec![format!(
                        "## Extension operation for column {} by {}",
                        col_name, extender_id
                    )],
                });

                cells.push(Cell::Code {
                    id: Uuid::new_v4().to_string(),
                    metadata: serde_json::json!({}),
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
                    metadata: serde_json::json!({}),
                    source: vec![format!("Unknown operation type: {}", operation_type)],
                });
            }
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

use chrono;
use serde::Serialize;
use std::collections::HashMap;
use std::env;
use uuid::Uuid;

#[derive(Serialize)]
struct Notebook {
    cells: Vec<Cell>,
    metadata: Metadata,
    nbformat: u32,
    nbformat_minor: u32,
}

pub struct Args {
    pub log_file: String,
    pub table_file: String,
    pub format: String,
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
            source: vec![
                "# This is a base notebook for the SemT operations.\n".to_string(),
                "# Import necessary classes and functions from the SemT_py package\n".to_string(),
                "from SemT_py.token_manager import TokenManager\n".to_string(),
                "from SemT_py.extension_manager import ExtensionManager\n".to_string(),
                "from SemT_py.reconciliation_manager import ReconciliationManager\n".to_string(),
                "from SemT_py.utils import Utility\n".to_string(),
                "from SemT_py.dataset_manager import DatasetManager\n".to_string(),
                "from SemT_py.modification_manager import ModificationManager\n".to_string(),
            ],
            execution_count: None,
            outputs: vec![],
        },
        // Configuration cell
        Cell::Code {
            id: Uuid::new_v4().to_string(),
            metadata: serde_json::json!({}),
            source: vec![
                "# Configuration\n".to_string(),
                format!(
                    "base_url = \"{}\"\n",
                    env::var("BASE_URL").unwrap_or_else(|_| "__BASE_URL__".to_string())
                ),
                format!(
                    "username = \"{}\"\n",
                    env::var("USERNAME").unwrap_or_else(|_| "__USERNAME__".to_string())
                ),
                format!(
                    "password = \"{}\"\n",
                    env::var("PASSWORD").unwrap_or_else(|_| "__PASSWORD__".to_string())
                ),
                format!(
                    "api_url = \"{}\"\n",
                    env::var("API_URL").unwrap_or_else(|_| "__API_URL__".to_string())
                ),
                "\n".to_string(),
                "Auth_manager = TokenManager(api_url, username, password)\n".to_string(),
                "token = Auth_manager.get_token()\n".to_string(),
                "reconciliation_manager = ReconciliationManager(base_url, Auth_manager)\n"
                    .to_string(),
                "dataset_manager = DatasetManager(base_url, Auth_manager)\n".to_string(),
                "extension_manager = ExtensionManager(base_url, token)\n".to_string(),
                "utility = Utility(base_url, token_manager=Auth_manager)\n".to_string(),
            ],
            execution_count: None,
            outputs: vec![],
        },
        // Data loading cell
        Cell::Code {
            id: Uuid::new_v4().to_string(),
            metadata: serde_json::json!({}),
            source: vec![
                "# Load a dataset into a DataFrame\n".to_string(),
                "import pandas as pd\n".to_string(),
                "dataset_id = \"4\"\n".to_string(),
                format!("table_name = \"{}\"\n", table_name),
                format!("df = pd.read_csv('{}')\n", args.table_file),
                "return_data = dataset_manager.add_table_to_dataset(dataset_id, df, table_name)\n"
                    .to_string(),
                "data_dict = return_data[1]  # The dictionary containing table info\n".to_string(),
                "\n".to_string(),
                "# Extract the table ID from the dictionary\n".to_string(),
                "table_id = data_dict['tables'][0]['id']\n".to_string(),
            ],
            execution_count: None,
            outputs: vec![],
        },
    ];

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
                    source: vec![
                        format!("reconciliator_id = \"{}\"\n", reconciler_id),
                        "optional_columns = []  # Replace with actual optional columns if needed\n"
                            .to_string(),
                        format!("column_name = \"{}\"\n", col_name),
                        "table_data = dataset_manager.get_table_by_id(dataset_id, table_id)\n"
                            .to_string(),
                        "reconciled_table, backend_payload = reconciliation_manager.reconcile(\n"
                            .to_string(),
                        "    table_data,\n".to_string(),
                        "    column_name,\n".to_string(),
                        "    reconciliator_id,\n".to_string(),
                        "    optional_columns\n".to_string(),
                        ")\n".to_string(),
                        "payload = backend_payload\n".to_string(),
                        "\n".to_string(),
                        "successMessage, sentPayload = utility.push_to_backend(\n".to_string(),
                        "    dataset_id,\n".to_string(),
                        "    table_id,\n".to_string(),
                        "    payload,\n".to_string(),
                        "    debug=False\n".to_string(),
                        ")\n".to_string(),
                        "\n".to_string(),
                        "print(successMessage)\n".to_string(),
                    ],
                    execution_count: None,
                    outputs: vec![],
                });
            }
            "EXTENSION" => {
                let extender_id = operation.get("Extender").unwrap();
                let col_name = operation.get("ColumnName").unwrap();

                // Parse additional data for properties
                let properties = if let Some(additional_data_str) = operation.get("AdditionalData")
                {
                    // Simple JSON parsing - you may need to adapt this based on your parse_json function
                    let additional_data: serde_json::Value =
                        serde_json::from_str(additional_data_str).unwrap_or(serde_json::json!({}));
                    additional_data
                        .get("properties")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .split(' ')
                        .filter(|s| !s.is_empty())
                        .map(|s| format!("'{}'", s))
                        .collect::<Vec<String>>()
                        .join(", ")
                } else {
                    String::new()
                };

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
                    source: vec![
                        "extended_table, extension_payload = extension_manager.extend_column(\n"
                            .to_string(),
                        "    table=table_data,\n".to_string(),
                        format!("    column_name=\"{}\",\n", col_name),
                        format!("    extender_id=\"{}\",\n", extender_id),
                        "    properties=[\n".to_string(),
                        format!("        {}\n", properties),
                        "    ],\n".to_string(),
                        "    other_params={},\n".to_string(),
                        ")\n".to_string(),
                        "payload = extension_payload\n".to_string(),
                        "\n".to_string(),
                        "successMessage, sentPayload = utility.push_to_backend(\n".to_string(),
                        "    dataset_id,\n".to_string(),
                        "    table_id,\n".to_string(),
                        "    payload,\n".to_string(),
                        "    debug=False\n".to_string(),
                        ")\n".to_string(),
                        "\n".to_string(),
                        "print(successMessage)\n".to_string(),
                    ],
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
fn main() {
    println!(
        "This is a test notebook module. Use the create_notebook function to generate a notebook."
    );
}

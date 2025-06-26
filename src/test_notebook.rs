use serde::Serialize;
use uuid::Uuid;

#[derive(Serialize)]
struct Notebook {
    cells: Vec<Cell>,
    metadata: Metadata,
    nbformat: u32,
    nbformat_minor: u32,
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

fn main() {
    let notebook = Notebook {
        nbformat: 4,
        nbformat_minor: 5, // v4.5+ requires IDs
        metadata: Metadata {},
        cells: vec![
            Cell::Code {
                id: Uuid::new_v4().to_string(),
                metadata: serde_json::json!({}),
                source: vec!["import numpy as np".into()],
                execution_count: None,
                outputs: vec![],
            },
            Cell::Markdown {
                id: Uuid::new_v4().to_string(),
                metadata: serde_json::json!({}),
                source: vec!["# Generated Notebook".into()],
            },
        ],
    };

    let json = serde_json::to_string_pretty(&notebook).unwrap();
    std::fs::write("notebook.ipynb", json).unwrap();
}

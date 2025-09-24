use chrono::DateTime;
use rev_lines::RevLines;
use serde_json::Value;
use std::{
    collections::HashMap,
    fmt::Error,
    fs::File,
    io::{self, BufReader},
};

pub fn logs_from_last_get_table(path: &str) -> Result<Option<Vec<String>>, io::Error> {
    let file = File::open(path)?;
    let rev_reader = RevLines::new(BufReader::new(file));
    let mut start_line: Option<String> = None;
    let mut end_line: Option<String> = None;
    let mut result = Vec::new();

    for line_result in rev_reader {
        match line_result {
            Ok(line) => {
                // Process the line
                if line.contains("GET_TABLE") {
                    start_line = Some(line);
                    break; // Stop searching after finding the first occurrence
                } else if line.contains("SAVE_TABLE") {
                    if end_line.is_none() {
                        println!("Found SAVE_TABLE line: {}", line);
                        // If we haven't set an end line yet, set it to the current line
                        end_line = Some(line.clone());
                        result.push(line);
                    }
                }
            }
            Err(e) => {
                eprintln!("Error reading line: {}", e);
                continue; // Skip to the next line
            }
        }
    }
    let start = match start_line {
        Some(line) => line,
        None => return Ok(None),
    };
    let end = match end_line {
        Some(line) => line,
        None => String::new(), // If no end line is found, we will read until the end of the file
    };
    let content = std::fs::read_to_string(path)?;
    let mut found_start = false;

    for line in content.lines() {
        if line == start {
            found_start = true;
        }
        if line == end {
            break; // Stop reading after reaching the end line
        }
        if found_start {
            result.push(line.to_string());
        }
    }
    if result.is_empty() {
        Ok(None)
    } else {
        Ok(Some(result))
    }
}

pub fn pre_process_operations(
    operations: Vec<String>,
) -> Result<Vec<HashMap<String, String>>, Error> {
    let mut operations_arrays = Vec::new();
    for operation in operations {
        let splitted_operations = operation.split(" -| ");
        let operations_obj: HashMap<String, String> = splitted_operations
            .filter_map(|s| {
                let split: Vec<&str> = s.split(":").collect();
                if split[0].trim() == "AdditionalData" {
                    let json_string = split[1..].join(" : ");
                    Some(("AdditionalData".to_string(), json_string.trim().to_string()))
                } else if split.len() == 2 {
                    Some((split[0].trim().to_string(), split[1].trim().to_string()))
                } else if split.len() >= 3 && s.trim().starts_with("[") && s.trim().ends_with("]") {
                    let cleaned_timestamp = s.trim().replace("[", "").replace("]", "").to_string();
                    match DateTime::parse_from_rfc3339(&cleaned_timestamp) {
                        Ok(datetime) => Some(("timestamp".to_string(), datetime.to_rfc3339())),
                        Err(_) => {
                            eprintln!("Error parsing timestamp: {}", cleaned_timestamp);
                            Some(("timestamp".to_string(), cleaned_timestamp))
                        }
                    }
                } else {
                    None
                }
            })
            .collect();

        operations_arrays.push(operations_obj);
        // Process each operation
    }
    Ok(operations_arrays)
}

fn get_extension_key(operation: &HashMap<String, String>) -> String {
    let column_name = operation.get("ColumnName").map_or("", |v| v);
    let extender = operation.get("Extender").map_or("", |v| v);
    let additional_data = operation.get("AdditionalData").map_or("", |v| v);
    format!("{}:{}:{}", column_name, extender, additional_data)
}

fn find_extension(operations: &[HashMap<String, String>], key: &str) -> Option<usize> {
    operations.iter().position(|op| {
        if op.get("OpType") == Some(&"EXTENSION".to_string()) {
            get_extension_key(op) == key
        } else {
            false
        }
    })
}

pub fn sort_operations_by_timestamp(
    operations: Vec<HashMap<String, String>>,
) -> Vec<HashMap<String, String>> {
    let mut sorted_operations = operations;
    sorted_operations.sort_by(|a, b| {
        let a_timestamp = a.get("timestamp").map(|s| s.as_str()).unwrap_or("");
        let b_timestamp = b.get("timestamp").map(|s| s.as_str()).unwrap_or("");
        let datetime_a = DateTime::parse_from_rfc3339(a_timestamp).unwrap();
        let datetime_b = DateTime::parse_from_rfc3339(b_timestamp).unwrap();
        datetime_b.cmp(&datetime_a)
    });
    sorted_operations
}

pub fn process_operations(
    operations: Vec<HashMap<String, String>>,
) -> Vec<HashMap<String, String>> {
    let sorted_op = sort_operations_by_timestamp(operations);
    let mut filtered_operations: Vec<HashMap<String, String>> = Vec::new();
    for op in sorted_op {
        let col_name = op.get("ColumnName").cloned().unwrap_or_default();
        let timestamp = op.get("timestamp").cloned().unwrap_or_default();
        let operation_type = op.get("OpType").cloned().unwrap_or_default();

        if operation_type == "RECONCILIATION" {
            if !find_reconciliation(filtered_operations.clone(), &col_name) {
                filtered_operations.push(op);
            } else {
                println!(
                    "Skipping reconciliation for column: {} at timestamp: {}",
                    col_name, timestamp
                );
            }
        } else if operation_type == "EXTENSION" {
            let extension_key = get_extension_key(&op);
            if let Some(existing_index) = find_extension(&filtered_operations, &extension_key) {
                // Replace the existing extension operation with the newer one
                filtered_operations[existing_index] = op;
                println!(
                    "Replacing extension for column: {} with extender at timestamp: {}",
                    col_name, timestamp
                );
            } else {
                filtered_operations.push(op);
            }
        } else {
            filtered_operations.push(op);
        }
    }
    filtered_operations.sort_by(|a, b| {
        let a_type = a.get("OpType").map(|s| s.as_str()).unwrap_or("");
        let b_type = b.get("OpType").map(|s| s.as_str()).unwrap_or("");
        match (a_type, b_type) {
            ("RECONCILIATION", "RECONCILIATION") => std::cmp::Ordering::Equal,
            ("RECONCILIATION", _) => std::cmp::Ordering::Less, // RECONCILIATION comes first
            (_, "RECONCILIATION") => std::cmp::Ordering::Greater, // RECONCILIATION comes first
            _ => std::cmp::Ordering::Equal, // Keep original order for non-RECONCILIATION operations
        }
    });
    filtered_operations
}

pub fn find_reconciliation(operations: Vec<HashMap<String, String>>, column_name: &str) -> bool {
    for operation in operations {
        if let Some(op_type) = operation.get("OpType") {
            if op_type == "RECONCILIATION"
                && operation.get("ColumnName") == Some(&column_name.to_string())
            {
                return true;
            }
        }
    }
    false
}

pub fn parse_json(json_string: &str) -> Option<Value> {
    match serde_json::from_str(json_string) {
        Ok(json_value) => Some(json_value),
        Err(e) => {
            eprintln!("Error parsing JSON: {}", e);
            None
        }
    }
}

pub fn parse_deleted_columns(deleted_cols_string: &str) -> Vec<String> {
    // Treat empty string or the sentinel value "NO_DELETED" as no deleted columns
    let trimmed = deleted_cols_string.trim();
    if trimmed.is_empty() || trimmed == "NO_DELETED" {
        return vec![];
    }

    trimmed
        .split("|-|")
        .map(|col| col.trim().to_string())
        .filter(|col| !col.is_empty())
        .collect()
}

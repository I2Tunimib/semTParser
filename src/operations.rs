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
                // Skip empty lines
                if line.trim().is_empty() {
                    continue;
                }
                // Process the line
                if line.contains("GET_TABLE") && !end_line.is_none() {
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
    let mut found_end = false;

    for line in content.lines() {
        if line == start {
            found_start = true;
        }
        if found_start && line == end {
            found_end = true;
            // Don't break here - continue reading to capture EXPORT operations after SAVE_TABLE
        }
        if found_start {
            // Skip empty lines
            if !line.trim().is_empty() {
                result.push(line.to_string());
            }

            // After SAVE_TABLE, continue reading until we hit another GET_TABLE or SAVE_TABLE
            if found_end
                && (line.contains("GET_TABLE") && line != start
                    || line.contains("SAVE_TABLE") && line != end)
            {
                break;
            }
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

pub fn sort_operations_by_timestamp(
    operations: Vec<HashMap<String, String>>,
) -> Vec<HashMap<String, String>> {
    let mut sorted_operations = operations;
    sorted_operations.sort_by(|a, b| {
        let a_timestamp = a.get("timestamp").map(|s| s.as_str()).unwrap_or("");
        let b_timestamp = b.get("timestamp").map(|s| s.as_str()).unwrap_or("");

        // Handle invalid or missing timestamps gracefully
        let datetime_a = DateTime::parse_from_rfc3339(a_timestamp);
        let datetime_b = DateTime::parse_from_rfc3339(b_timestamp);

        match (datetime_a, datetime_b) {
            (Ok(da), Ok(db)) => da.cmp(&db), // Sort in ascending order (oldest first)
            (Ok(_), Err(_)) => std::cmp::Ordering::Less,
            (Err(_), Ok(_)) => std::cmp::Ordering::Greater,
            (Err(_), Err(_)) => std::cmp::Ordering::Equal,
        }
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
            // Check if there's already a reconciliation for this column
            let last_reconciliation_index = filtered_operations
                .iter()
                .enumerate()
                .rev()
                .find(|(_, existing_op)| {
                    existing_op.get("OpType") == Some(&"RECONCILIATION".to_string())
                        && existing_op.get("ColumnName") == Some(&col_name)
                })
                .map(|(index, _)| index);

            if let Some(last_recon_idx) = last_reconciliation_index {
                // Check if there's an extension on this column after the last reconciliation
                let has_extension_after =
                    filtered_operations[last_recon_idx + 1..]
                        .iter()
                        .any(|existing_op| {
                            existing_op.get("OpType") == Some(&"EXTENSION".to_string())
                                && existing_op.get("ColumnName") == Some(&col_name)
                        });

                if has_extension_after {
                    // There's an extension in between, keep this new reconciliation
                    filtered_operations.push(op);
                } else {
                    // No extension in between, replace the previous reconciliation with this one
                    println!(
                        "Replacing reconciliation for column: {} at timestamp: {} (no extension in between)",
                        col_name, timestamp
                    );
                    filtered_operations.remove(last_recon_idx);
                    filtered_operations.push(op);
                }
            } else {
                // First reconciliation for this column, keep it
                filtered_operations.push(op);
            }
        } else if operation_type == "EXTENSION" {
            // Check if the last operation on this column is an identical extension
            let last_op_on_column = filtered_operations
                .iter()
                .rev()
                .find(|existing_op| existing_op.get("ColumnName") == Some(&col_name));

            if let Some(last_op) = last_op_on_column {
                let extension_key = get_extension_key(&op);
                let last_extension_key = get_extension_key(last_op);

                if last_op.get("OpType") == Some(&"EXTENSION".to_string())
                    && extension_key == last_extension_key
                {
                    // Identical extension operation, skip it
                    println!(
                        "Skipping identical extension for column: {} at timestamp: {}",
                        col_name, timestamp
                    );
                } else {
                    // Different operation or different extension, keep it
                    filtered_operations.push(op);
                }
            } else {
                // First operation on this column, keep it
                filtered_operations.push(op);
            }
        } else if operation_type == "MODIFICATION" {
            // Find the last modification for this column
            let last_modification_index = filtered_operations
                .iter()
                .enumerate()
                .rev()
                .find(|(_, existing_op)| {
                    existing_op.get("OpType") == Some(&"MODIFICATION".to_string())
                        && existing_op.get("ColumnName") == Some(&col_name)
                })
                .map(|(index, _)| index);

            if let Some(last_mod_idx) = last_modification_index {
                // Replace the previous modification with this one (keep only the last)
                println!(
                    "Replacing modification for column: {} at timestamp: {}",
                    col_name, timestamp
                );
                filtered_operations.remove(last_mod_idx);
                filtered_operations.push(op);
            } else {
                // First modification for this column, keep it
                filtered_operations.push(op);
            }
        } else if operation_type == "EXPORT" {
            // Check if the last operation is an identical EXPORT
            let last_export = filtered_operations
                .iter()
                .rev()
                .find(|existing_op| existing_op.get("OpType") == Some(&"EXPORT".to_string()));

            if let Some(last_exp) = last_export {
                let current_format = op.get("AdditionalData").map_or("", |v| v);
                let last_format = last_exp.get("AdditionalData").map_or("", |v| v);

                if current_format == last_format {
                    // Identical export operation, skip it
                    println!(
                        "Skipping identical EXPORT operation at timestamp: {}",
                        timestamp
                    );
                } else {
                    // Different export format, keep it
                    filtered_operations.push(op);
                }
            } else {
                // First export operation, keep it
                filtered_operations.push(op);
            }
        } else {
            filtered_operations.push(op);
        }
    }

    // Keep the timestamp order - don't re-sort by priority
    filtered_operations
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

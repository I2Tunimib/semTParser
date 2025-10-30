use serde_json::Value;

const BASE_FILE_CONTENT: &str = r#"
import semt_py
import getpass
from semt_py import AuthManager
from semt_py.extension_manager import ExtensionManager
from semt_py.reconciliation_manager import ReconciliationManager
from semt_py.utils import Utility
from semt_py.dataset_manager import DatasetManager
from semt_py.table_manager import TableManager
from semt_py.modification_manager import ModificationManager

def get_input_with_default(prompt, default):
    user_input = input(f"{prompt} (default: {default}): ").strip()
    return user_input if user_input else default

base_url = get_input_with_default("Enter base URL or press Enter to keep default", "__BASE_URL__")
api_url = base_url + "/api"
username = get_input_with_default("Enter your username", "__USERNAME__")
default_password = "__PASSWORD__"
password_prompt = f"Enter your password (default: use stored password): "
password_input = getpass.getpass(password_prompt)
password = password_input if password_input else default_password

Auth_manager = AuthManager(api_url, username, password)
token = Auth_manager.get_token()
reconciliation_manager = ReconciliationManager(base_url, Auth_manager)
dataset_manager = DatasetManager(base_url, Auth_manager)
table_manager = TableManager(base_url, Auth_manager)
extension_manager = ExtensionManager(base_url, token)
utility = Utility(base_url, Auth_manager)
manager = ModificationManager()

"#;

const BASE_DATASET_LOAD_DATAFRAME: &str = r#"
import pandas as pd

dataset_id = get_input_with_default("Enter dataset_id or press Enter to keep default", "__DATASET_ID__")
table_name = get_input_with_default("Enter table_name or press Enter to keep default", "__TABLE_NAME__")

filename = get_input_with_default("Enter path to CSV file or press Enter to keep default", "__TABLE_PATH__")
df = pd.read_csv(filename)

columns_to_delete = [__COLUMNS_TO_DELETE__]
if columns_to_delete and columns_to_delete != ['']:
    for col in columns_to_delete:
        if col in df.columns:
            df = df.drop(columns=[col])
            print(f"Deleted column: {col}")
        else:
            print(f"Column '{col}' not found in table")
    print(f"Columns deleted: {[col for col in columns_to_delete if col in df.columns]}")

table_id, message, table_data = table_manager.add_table(dataset_id, df, table_name)

print(f"Table loaded successfully: {message}")
try:
    from IPython.display import display
    print("Showing dataframe head:")
    display(df.head())
except Exception as e:
    print(f"Could not display DataFrame head: {e}")
    print(df.head().to_string())
"#;

const BASE_RECONCILE_OPERATION: &str = r#"

reconciliator_id = "__RECONCILIATOR_ID__"
optional_columns = [__OPTIONAL_COLUMNS__]
column_name = "__COLUMN_NAME__"
try:
    table_data = table_manager.get_table(dataset_id, table_id)
    reconciled_table, backend_payload = reconciliation_manager.reconcile(
        table_data,
        column_name,
        reconciliator_id,
        optional_columns
    )
    payload = backend_payload

    successMessage, sentPayload = utility.push_to_backend(
    dataset_id,
    table_id,
    payload,
    debug=False
    )

    print(successMessage)
    html_table = Utility.display_json_table(
        json_table=reconciled_table,
        number_of_rows=4,
        from_row=0,
    )
    if html_table is not None:
        from IPython.display import display
        display(html_table)
except Exception as e:
    print(f"An error occurred during reconciliation: {e}")
"#;

const BASE_PROPAGATION_OPERATION: &str = r#"
try:
    type_obj = __TYPE_TO_PROPAGATE__

    table_data, backend_payload = manager.propagate_type(table_data, '__COL_TO_PROPAGATE__', type_obj)


    successMessage, sentPayload = utility.push_to_backend(
        dataset_id,
        table_id,
        backend_payload,
        debug=False
    )

    print(successMessage)
    html_table = Utility.display_json_table(
        json_table=table_data,
        number_of_rows=4,
        from_row=0,
    )
    if html_table is not None:
        from IPython.display import display
        display(html_table)
except Exception as e:
    print(f"An error occurred during propagation: {e}")

"#;

const BASE_EXTENSION_OPERATION: &str = r#"
try:
    table_data = table_manager.get_table(dataset_id, table_id)

    extended_table, extension_payload = extension_manager.extend_column(
        table=table_data,
        column_name="__COLUMN_NAME__",
        extender_id="__EXTENDER_ID__",
        properties=__EXTENSION_PROPERTIES__,
        other_params={__EXTENSION_PARAMS__}
    )
    payload = extension_payload

    successMessage, sentPayload = utility.push_to_backend(
        dataset_id,
        table_id,
        payload,
        debug=False
    )

    print(successMessage)
    html_table = Utility.display_json_table(
        json_table=extended_table,
        number_of_rows=4,
        from_row=0,
    )
    if html_table is not None:
        from IPython.display import display
        display(html_table)
except Exception as e:
    print(f"An error occurred during extension: {e}")
"#;

const BASE_EXPORT_CSV_OPERATION: &str = r#"
# Export as CSV
try:
    csv_file = utility.download_csv(
        dataset_id=dataset_id,
        table_id=table_id,
        output_file="__OUTPUT_FILE__"
    )
    print(f"✓ CSV downloaded: {csv_file}")
except Exception as e:
    print(f"✗ Error downloading CSV: {e}")
"#;

const BASE_EXPORT_JSON_OPERATION: &str = r#"
# Export as JSON
try:
    json_file = utility.download_json(
        dataset_id=dataset_id,
        table_id=table_id,
        output_file="__OUTPUT_FILE__"
    )
    print(f"✓ JSON downloaded: {json_file}")
except Exception as e:
    print(f"✗ Error downloading JSON: {e}")
"#;

pub fn get_base_file_loader_code() -> String {
    let formatted_code = BASE_FILE_CONTENT
        .replace(
            "__USERNAME__",
            &std::env::var("USERNAME").unwrap_or_default(),
        )
        .replace(
            "__PASSWORD__",
            &std::env::var("PASSWORD").unwrap_or_default(),
        )
        .replace(
            "__BASE_URL__",
            &std::env::var("BASE_URL")
                .unwrap_or("http://vm.chronos.disco.unimib.it:3003".to_string()),
        );
    formatted_code
}

pub fn get_base_dataset_loader(table_path: &str, dataset_id: &str, table_name: &str) -> String {
    let formatted_code = BASE_DATASET_LOAD_DATAFRAME
        .replace("__TABLE_PATH__", table_path)
        .replace("__DATASET_ID__", dataset_id)
        .replace("__TABLE_NAME__", table_name)
        .replace("__COLUMNS_TO_DELETE__", "");
    formatted_code
}

pub fn get_base_dataset_loader_with_column_deletion(
    table_path: &str,
    dataset_id: &str,
    table_name: &str,
    columns_to_delete: Vec<String>,
) -> String {
    let columns_to_delete_str = if columns_to_delete.is_empty() {
        "".to_string()
    } else {
        columns_to_delete
            .iter()
            .map(|col| format!("'{}'", col))
            .collect::<Vec<String>>()
            .join(", ")
    };

    let formatted_code = BASE_DATASET_LOAD_DATAFRAME
        .replace("__TABLE_PATH__", table_path)
        .replace("__DATASET_ID__", dataset_id)
        .replace("__TABLE_NAME__", table_name)
        .replace("__COLUMNS_TO_DELETE__", &columns_to_delete_str);
    formatted_code
}

pub fn get_base_extension_operation(
    column_name: &str,
    properties: Vec<String>,
    additional_params: Option<Vec<String>>,
    extender_id: &str,
) -> String {
    // Format properties as a Python list (e.g. ["P373", "P31", "P625"])
    let properties_str = if properties.is_empty() {
        "[]".to_string()
    } else {
        format!(
            "[{}]",
            properties
                .iter()
                .map(|p| format!("\"{}\"", p))
                .collect::<Vec<String>>()
                .join(", ")
        )
    };

    let additional_params_str = match additional_params {
        Some(params) if !params.is_empty() => params.join(", "),
        _ => String::from(""),
    };
    let formatted_code = BASE_EXTENSION_OPERATION
        .replace("__COLUMN_NAME__", column_name)
        .replace("__EXTENSION_PROPERTIES__", &properties_str)
        .replace("__EXTENSION_PARAMS__", &additional_params_str)
        .replace("__EXTENDER_ID__", extender_id);
    formatted_code
}

pub fn get_base_reconciliation_operation(
    column_name: &str,
    additional_columns: Option<Vec<String>>,
    reconciler_id: &str,
) -> String {
    let additional_columns_str = match additional_columns {
        Some(columns) if !columns.is_empty() => columns.join(", "),
        _ => String::from(""),
    };
    let formatted_code = BASE_RECONCILE_OPERATION
        .replace("__RECONCILIATOR_ID__", reconciler_id) // Replace with actual reconciliator ID
        .replace("__COLUMN_NAME__", column_name)
        .replace("__OPTIONAL_COLUMNS__", &additional_columns_str);
    formatted_code
}

fn value_to_python(value: &Value) -> String {
    match value {
        Value::Null => "None".to_string(),
        Value::Bool(b) => {
            if *b {
                "True".to_string()
            } else {
                "False".to_string()
            }
        }
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("\"{}\"", s.replace("\"", "\\\"")),
        Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(value_to_python).collect();
            format!("[{}]", items.join(", "))
        }
        Value::Object(obj) => {
            let items: Vec<String> = obj
                .iter()
                .map(|(k, v)| format!("\"{}\": {}", k, value_to_python(v)))
                .collect();
            format!("{{{}}}", items.join(", "))
        }
    }
}

pub fn get_base_propagation_operation(
    column_name: &str,
    additional_data: Option<&Value>,
) -> String {
    let type_str = additional_data
        .map(value_to_python)
        .unwrap_or_else(|| "{}".to_string());
    let formatted_code = BASE_PROPAGATION_OPERATION
        .replace("__COL_TO_PROPAGATE__", column_name)
        .replace("__TYPE_TO_PROPAGATE__", &type_str);
    formatted_code
}

pub fn get_base_export_operation(format: &str, output_file: &str) -> Option<String> {
    match format.to_lowercase().as_str() {
        "csv" => {
            let formatted_code = BASE_EXPORT_CSV_OPERATION.replace("__OUTPUT_FILE__", output_file);
            Some(formatted_code)
        }
        "w3c" | "json" => {
            let formatted_code = BASE_EXPORT_JSON_OPERATION.replace("__OUTPUT_FILE__", output_file);
            Some(formatted_code)
        }
        _ => None,
    }
}

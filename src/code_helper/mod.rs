const BASE_FILE_CONTENT: &str = r#"
# This is a base file for the Python helper module.
# Import necessary classes and functions from the semt_py package
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
username = get_input_with_default("Enter your username", "__USERNAME__")
default_password = "__PASSWORD__"
password_prompt = f"Enter your password (default: use stored password): "
password_input = getpass.getpass(password_prompt)
password = password_input if password_input else default_password
api_url = get_input_with_default("Enter API URL or press Enter to keep default", "__API_URL__")

Auth_manager = AuthManager(api_url, username, password)
token = Auth_manager.get_token()
reconciliation_manager = ReconciliationManager(base_url, Auth_manager)
dataset_manager = DatasetManager(base_url, Auth_manager)
table_manager = TableManager(base_url, Auth_manager)
extension_manager = ExtensionManager(base_url, token)
utility = Utility(base_url, Auth_manager)
"#;

const BASE_DATASET_LOAD_DATAFRAME: &str = r#"
# Load a dataset into a DataFrame
import pandas as pd

# Note: get_input_with_default is defined in the main file
# Reusing it here for consistency

dataset_id = get_input_with_default("Enter dataset_id or press Enter to keep default", "__DATASET_ID__")
table_name = get_input_with_default("Enter table_name or press Enter to keep default", "__TABLE_NAME__")

df = pd.read_csv('__TABLE_PATH__')

# Delete specified columns if any
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

# Extract the table ID
# Alternative method if above doesn't work:
# return_data = dataset_manager.add_table_to_dataset(dataset_id, df, table_name)
# data_dict = return_data[1]  # The dictionary containing table info
# table_id = data_dict['tables'][0]['id']
"#;

const BASE_RECONCILE_OPERATION: &str = r#"

reconciliator_id = "__RECONCILIATOR_ID__"
optional_columns = [__OPTIONAL_COLUMNS__]  # Replace with actual optional columns if needed
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
    # Or display with specific parameters (example)
    html_table = Utility.display_json_table(
        json_table=reconciled_table,
        number_of_rows=4,  # Show 4 rows
        from_row=0,        # Start from first row
    )
    if html_table is not None:
        from IPython.display import display
        display(html_table)
except Exception as e:
    print(f"An error occurred during reconciliation: {e}")
    # Handle the exception as needed, e.g., log it or re-raise it
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
    # Or display with specific parameters (example)
    html_table = Utility.display_json_table(
        json_table=extended_table,
        number_of_rows=4,  # Show 4 rows
        from_row=0,        # Start from first row
    )
    if html_table is not None:
        from IPython.display import display
        display(html_table)
except Exception as e:
    print(f"An error occurred during extension: {e}")
"#;

pub fn get_base_file_loader_code() -> String {
    let formatted_code = BASE_FILE_CONTENT
        .replace(
            "__API_URL__",
            &std::env::var("API_URL").unwrap_or("http://localhost:3003/api".to_string()),
        )
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
            &std::env::var("BASE_URL").unwrap_or("http://localhost:3003".to_string()),
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

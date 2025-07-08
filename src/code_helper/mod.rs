const BASE_FILE_CONTENT: &str = r#"
# This is a base file for the Python helper module.
# Import necessary classes and functions from the SemT_py package
from SemT_py.token_manager import TokenManager
from SemT_py.extension_manager import ExtensionManager
from SemT_py.reconciliation_manager import ReconciliationManager
from SemT_py.utils import Utility
from SemT_py.dataset_manager import DatasetManager
from SemT_py.modification_manager import ModificationManager

base_url = "__BASE_URL__"  # Replace with your base URL
username = "__USERNAME__"  # Replace with your username
password = "__PASSWORD__"  # Replace with your password
api_url = "__API_URL__"  # Replace with your API URL
Auth_manager = TokenManager(api_url, username, password)
token = Auth_manager.get_token()
reconciliation_manager = ReconciliationManager(base_url, Auth_manager)
dataset_manager = DatasetManager(base_url, Auth_manager)
extension_manager = ExtensionManager(base_url, token)
utility = Utility(base_url, token_manager=Auth_manager)
"#;

const BASE_DATASET_LOAD_DATAFRAME: &str = r#"
# Load a dataset into a DataFrame
import pandas as pd
dataset_id = "__DATASET_ID__"
table_name = "__TABLE_NAME__"

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

return_data = dataset_manager.add_table_to_dataset(dataset_id, df ,table_name)
data_dict = return_data[1]  # The dictionary containing table info

# Extract the table ID from the dictionary
table_id = data_dict['tables'][0]['id']
# table_id, message, response_data = table_manager.add_table(dataset_id, df, table_name)

"#;

const BASE_RECONCILE_OPERATION: &str = r#"

reconciliator_id = "__RECONCILIATOR_ID__"
optional_columns = [__OPTIONAL_COLUMNS__]  # Replace with actual optional columns if needed
column_name = "__COLUMN_NAME__"
table_data = dataset_manager.get_table_by_id(dataset_id, table_id)
reconciled_table, backend_payload = reconciliation_manager.reconcile(
      table_data,
      column_name,
      reconciliator_id,
      optional_columns
  )
payload=backend_payload

successMessage, sentPayload = utility.push_to_backend(
  dataset_id,
  table_id,
  payload,
  debug=False
)

print(successMessage)
"#;

const BASE_EXTENSION_OPERATION: &str = r#"
extended_table, extension_payload = extension_manager.extend_column(
  table=table_data,
  column_name="__COLUMN_NAME__",
  extender_id="reconciledColumnExtWikidata",
  properties=[
      __EXTENSION_PROPERTIES__
  ],
  other_params={__EXTENSION_PARAMS__},
)
payload=backend_payload

successMessage, sentPayload = utility.push_to_backend(
  dataset_id,
  table_id,
  payload,
  debug=False
)

print(successMessage)
"#;

pub fn get_base_file_loader_code() -> String {
    let formatted_code = BASE_FILE_CONTENT
        .replace("__API_URL__", &std::env::var("API_URL").unwrap_or_default())
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
            &std::env::var("BASE_URL").unwrap_or_default(),
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
    let properties_str = properties
        .iter()
        .map(|item| format!("'{}'", item))
        .collect::<Vec<String>>()
        .join(", ");
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

use std::{
    fs::{File, OpenOptions},
    io::{Error, Write},
    path::Path,
};
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

pub fn create_base_file(path: &str) -> Result<String, Error> {
    let path = Path::new(path);
    let mut file = get_file_writer(path)?;
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
    file.write_all(formatted_code.as_bytes())?;
    Ok(path.to_string_lossy().to_string())
}

pub fn write_table_loader(
    file_path_str: &str,
    table_path_str: &str,
    table_name: &str,
) -> Result<(), Error> {
    let table_path = Path::new(table_path_str);
    let file_path = Path::new(file_path_str);
    if table_path.exists() {
        let formatted_code = BASE_DATASET_LOAD_DATAFRAME
            .replace("__TABLE_PATH__", table_path_str)
            .replace("__TABLE_NAME__", table_name)
            .replace("__DATASET_ID__", "4");

        //write to file
        let mut file = get_file_writer(file_path)?;
        file.write_all(formatted_code.as_bytes())?;
        Ok(())
    } else {
        Err(Error::new(
            std::io::ErrorKind::NotFound,
            "Table path does not exist",
        ))
    }
}
pub fn create_extension_operation(
    file_path_str: &str,
    column_name: &str,
    extender_id: &str,
    properties: Vec<String>,
    additional_params: Option<Vec<String>>,
) -> Result<(), Error> {
    let file_path = Path::new(file_path_str);
    let mut file = get_file_writer(file_path)?;

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
    file.write_all(formatted_code.as_bytes())?;
    Ok(())
}

pub fn create_reconciliation_operation(
    file_path_str: &str,
    column_name: &str,
    reconciliator_id: &str,
    additional_columns: Option<Vec<String>>,
) -> Result<(), Error> {
    let file_path = Path::new(file_path_str);
    let mut file = get_file_writer(file_path)?;

    let additional_columns_str = match additional_columns {
        Some(columns) if !columns.is_empty() => columns.join(", "),
        _ => String::from(""),
    };
    let formatted_code = BASE_RECONCILE_OPERATION
        .replace("__RECONCILIATOR_ID__", reconciliator_id) // Replace with actual reconciliator ID
        .replace("__COLUMN_NAME__", column_name)
        .replace("__OPTIONAL_COLUMNS__", &additional_columns_str);
    file.write_all(formatted_code.as_bytes())?;
    Ok(())
}

fn get_file_writer(file_path: &Path) -> Result<File, Error> {
    if file_path.exists() {
        OpenOptions::new().write(true).append(true).open(file_path)
    } else {
        File::create(file_path)
    }
}

# semTParser

A Rust-based tool for parsing and processing semT (Semantic Table) logs to generate Python code for table operations including reconciliation and extension operations.

---

📄 **Detailed technical documentation about log parsing and code/notebook generation is available here:**  
[docs/parsing_and_generation.md](docs/parsing_and_generation.md)

---

## Overview

semTParser analyzes log files to extract table operations and automatically generates Python scripts that can reproduce those operations. It processes logs to identify reconciliation and extension operations, then creates corresponding Python code to execute these operations on CSV tables.

## Features

- **Log Parsing**: Analyzes semT log files to extract operations between GET_TABLE and SAVE_TABLE entries
- **Operation Processing**: Handles RECONCILIATION and EXTENSION operations with detailed parsing of additional data
- **Python Code Generation**: Automatically creates Python scripts with SemT_py package integration for table operations
- **CSV Table Support**: Works with CSV files as input tables
- **Timestamped Output**: Generates uniquely named output files with timestamps
- **Jupyter Notebook Support**: Includes capabilities for generating Jupyter notebooks for data operations _(WIP)_

## Installation

### Prerequisites

- Rust (edition 2021 or later)
- Cargo package manager
- Python 3.6+
- pip (Python package installer)

### Building from Source

```bash
# Clone and build the Rust application
git clone <repository-url>
cd semTParser
cargo build --release
```

### Python Library Setup

The generated Python scripts require the `semt_py` library, which can be installed from GitHub:

```bash
# Install the semt_py library
pip install git+https://github.com/I2Tunimib/I2T-library.git
```

Alternatively, you can clone the repository and install locally:

```bash
git clone https://github.com/I2Tunimib/I2T-library.git
cd I2T-library
pip install -e .
```

## Usage

### Basic Usage

```bash
cargo run -- --log-file ./logs.txt --table-file ./table_1.csv
```

### Command Line Options

- `-l, --log-file <FILE>`: Specify the log file to parse (default: `./logs.txt`)
- `-t, --table-file <FILE>`: Specify the CSV table file to process (default: `./table_1.csv`)
- `-f, --format <FORMAT>`: Output format, either "python" or "notebook" (default: `python`)

### Examples

```bash
# Parse logs.txt and process table_1.csv (using defaults)
cargo run

# Use custom files
cargo run -- --log-file ./my_logs.txt --table-file ./my_table.csv

# Generate a Jupyter notebook instead of Python script
cargo run -- --log-file ./logs.txt --table-file ./table_1.csv --format notebook

# Build and run with optimized binary
cargo build --release
./target/release/semTParser -l ./logs.txt -t ./table_1.csv -f python
```

### Cross-platform Usage

You can build the tool for different platforms using GitHub Actions or the `cross` tool. See the `.github/workflows/cross-build.yml` file for automated cross-compilation settings.

## How It Works

1. **Log Analysis**: The tool reads the specified log file in reverse to find the most recent GET_TABLE operation
2. **Operation Extraction**: Extracts all operations between the last GET_TABLE and the first SAVE_TABLE entry
3. **Processing**: Parses and categorizes operations (RECONCILIATION, EXTENSION, etc.) with detailed JSON parsing of additional data
4. **Code Generation**: Creates a timestamped Python file with:
   - Integration with SemT_py package for API interactions
   - Table loading code for the specified CSV file using pandas
   - Reconciliation operations with specified reconcilers and columns
   - Extension operations with properties and extenders
   - Authentication and token management setup
   - Dataset and table management functionality

## Generated Output

The tool generates Python files with names like `base_file_2025-06-26_08-33.py` containing:

- **SemT_py Integration**: Imports and setup for SemT_py package components including:
  - TokenManager for authentication
  - ExtensionManager for data extension operations
  - ReconciliationManager for data reconciliation
  - DatasetManager for dataset operations
  - Utility functions
- **CSV Table Loading**: Uses pandas for data manipulation and loading
- **API Configuration**: Base URL, authentication, and API endpoint setup
- **Operation Execution**: Reconciliation operations with specified reconcilers and columns
- **Extension Processing**: Extension operations with properties and extenders parsed from JSON
- **Dataset Management**: Functions to add tables to datasets with proper naming

## Dependencies

- `rev_lines`: For reading files in reverse order
- `chrono`: For timestamp generation
- `serde`/`serde_json`: For JSON parsing and serialization
- `uuid`: For generating unique identifiers
- `dotenv`: For environment variable support
- `clap`: For command-line argument parsing

### External Dependencies

The generated Python code requires:

- `semt_py`: Python package for semantic table operations (installed from the I2T-library repository)
- `pandas`: For data manipulation and CSV handling (automatically installed as a dependency of semt_py)

## Configuration

### Environment Variables

You can configure default credentials and API endpoints through environment variables:

```
BASE_URL=http://localhost:3003
API_URL=http://localhost:3003/api
USERNAME=your_username
PASSWORD=your_password
```

These can be placed in a `.env` file in the project root, or can be provided when running the Python scripts. If not provided, the scripts will prompt for these values interactively.

### Interactive Configuration

When running the generated Python scripts, you'll be prompted to enter or confirm:

1. Base URL (default: http://localhost:3003)
2. API URL (default: http://localhost:3003/api) 
3. Dataset ID
4. Table name
5. Table ID (after table is added)

This allows easy customization of endpoints and parameters without modifying the code.

## Configuration

The tool supports environment variables through `.env` files. Place a `.env` file in the project root for custom configuration.

### Setting up the .env file

Create a `.env` file in the project root directory with the following configuration:

```env
# API Configuration
BASE_URL=http://localhost:3003
API_URL=http://localhost:3003/api

# Authentication credentials
USERNAME=your-email@example.com
PASSWORD=your-password

# Optional: Logging level
RUST_LOG=info
```

#### Configuration Parameters

- **BASE_URL**: The base URL of your semT API server (default: `http://localhost:3003`)
- **API_URL**: The full API endpoint URL (default: `http://localhost:3003/api`)
- **USERNAME**: Your semT account username/email address
- **PASSWORD**: Your semT account password
- **RUST_LOG**: Logging level for the application (`debug`, `info`, `warn`, `error`)

#### Example .env file

```env
BASE_URL=http://localhost:3003
API_URL=http://localhost:3003/api
USERNAME=agazzi.ruben99@gmail.com
PASSWORD=your-secure-password
RUST_LOG=debug
```

**Note**: Never commit your `.env` file to version control as it contains sensitive credentials. The `.env` file should be added to your `.gitignore`.

## Supported Operations

The tool currently processes the following log operation types:

### RECONCILIATION Operations

- Extracts reconciler service IDs (e.g., `wikidataOpenRefine`, `wikidataAlligator`)
- Processes column names and additional reconciliation data
- Generates Python code with appropriate reconciliation manager calls

### EXTENSION Operations

- Parses extender service IDs (e.g., `wikidataPropertySPARQL`)
- Extracts property specifications from additional data
- Creates extension operations with proper property handling

### Log Format

Expected log format:

```
[timestamp] -| OpType: OPERATION_TYPE -| DatasetId: X -| TableId: Y -| ColumnName: column -| Service: service_id -| AdditionalData: {json_data}
```

## Development

### Project Structure

```
src/
├── main.rs              # Main application entry point
├── operations.rs        # Log parsing and operation processing
├── python_helpers.rs    # Python code generation utilities
└── test_notebook.rs     # Jupyter notebook generation (test binary)
```

### Available Binaries

The project includes multiple binary targets:

- `main`: Primary application for parsing logs and generating Python code
- `test`: Utility for generating Jupyter notebooks

### Running Tests

```bash
cargo test
```

### Building Debug Version

```bash
cargo build
```

### Running with Debug Output

```bash
RUST_LOG=debug cargo run
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

[Add your license information here]

## Support

For issues and questions, please [create an issue](https://github.com/your-repo/semTParser/issues) in the repository.

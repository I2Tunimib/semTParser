# semTParser

A Rust-based tool for parsing and processing semT (Semantic Table) logs to generate Python code for table operations including reconciliation and extension operations.

## Overview

semTParser analyzes log files to extract table operations and automatically generates Python scripts that can reproduce those operations. It processes logs to identify reconciliation and extension operations, then creates corresponding Python code to execute these operations on CSV tables.

## Features

- **Log Parsing**: Analyzes semT log files to extract operations between GET_TABLE and SAVE_TABLE entries
- **Operation Processing**: Handles RECONCILIATION and EXTENSION operations
- **Python Code Generation**: Automatically creates Python scripts with table loading and operation execution
- **CSV Table Support**: Works with CSV files as input tables
- **Timestamped Output**: Generates uniquely named output files with timestamps

## Installation

### Prerequisites

- Rust (edition 2021 or later)
- Cargo package manager

### Building from Source

```bash
git clone <repository-url>
cd semTParser
cargo build --release
```

## Usage

### Basic Usage

```bash
cargo run --log-file ./logs.txt --table-file ./table_1.csv
```

### Command Line Options

- `-l, --log-file <FILE>`: Specify the log file to parse (default: `./logs.txt`)
- `-t, --table-file <FILE>`: Specify the CSV table file to process (default: `./table_1.csv`)

### Example

```bash
# Parse logs.txt and process table_1.csv
cargo run

# Use custom files
cargo run -- -l ./my_logs.txt -t ./my_table.csv
```

## How It Works

1. **Log Analysis**: The tool reads the specified log file in reverse to find the most recent GET_TABLE operation
2. **Operation Extraction**: Extracts all operations between the last GET_TABLE and the first SAVE_TABLE entry
3. **Processing**: Parses and categorizes operations (RECONCILIATION, EXTENSION, etc.)
4. **Code Generation**: Creates a timestamped Python file with:
   - Table loading code for the specified CSV file
   - Generated operations based on the parsed logs
   - Appropriate function calls for each operation type

## Generated Output

The tool generates Python files with names like `base_file_2025-06-26_08-33.py` containing:

- CSV table loading functionality
- Reconciliation operations with specified reconcilers and columns
- Extension operations with properties and extenders
- Proper error handling and logging

## Dependencies

- `rev_lines`: For reading files in reverse order
- `chrono`: For timestamp generation
- `serde`/`serde_json`: For JSON parsing and serialization
- `uuid`: For generating unique identifiers
- `dotenv`: For environment variable support
- `clap`: For command-line argument parsing

## Configuration

The tool supports environment variables through `.env` files. Place a `.env` file in the project root for custom configuration.

## Development

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

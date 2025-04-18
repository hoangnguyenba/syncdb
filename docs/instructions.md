# Command Line DB Sync App Design

## Overview
This project is a Golang-based CLI tool for syncing databases through export and import operations. It provides flexibility by allowing configurations via command line flags or a .env file.

## Primary Features
- **Export Database:** Serialize database data to a file.
- **Import Database:** Load serialized data back into the database.

## Storage Options
- **Local File:** Read/write operations on the local filesystem.
- **S3:** Integrate with AWS SDK for file uploads/downloads.
- **Google Drive:** Integrate with the Google Drive API.

## Configuration Options
### Database Connection
- **host:** Database server address.
- **port:** Port number.
- **username:** Database username.
- **password:** Database password.
- **tables:** Option to select specific tables (default: all tables).

### Export/Import Settings
- **Include Schema:** Flag to include the database schema in the export/import (default: false).
- **Condition for Export:** Option to add a WHERE condition (e.g., `created_at > 2024-01-01`) to filter data during export.
- **Upsert on Import:** Option to perform upsert (update if exists, insert if not) when importing data (default: true).
- **Table Exclusions:**
  - **exclude-table:** Exclude both schema and data for specified tables.
  - **exclude-table-schema:** Exclude schema for specified tables.
  - **exclude-table-data:** Exclude data for specified tables.

### Storage Credentials/Settings
- **Local File:** File path for storing the export/import file.
- **S3:** Bucket name, region, and credentials.
- **Google Drive:** Credentials and folder ID.

## Import Configuration

The import configuration can be set using environment variables or command-line flags:

```bash
# Import configuration
SYNCDB_IMPORT_DRIVER=mysql
SYNCDB_IMPORT_HOST=localhost
SYNCDB_IMPORT_PORT=3306
SYNCDB_IMPORT_USERNAME=root
SYNCDB_IMPORT_PASSWORD=example
SYNCDB_IMPORT_DATABASE=commercedb
SYNCDB_IMPORT_TABLES=users,products
SYNCDB_IMPORT_FILEPATH=backup.json
SYNCDB_IMPORT_FOLDER_PATH=backup
SYNCDB_IMPORT_FORMAT=json
SYNCDB_IMPORT_EXCLUDE_TABLE=table1,table2
SYNCDB_IMPORT_EXCLUDE_TABLE_SCHEMA=table3,table4
SYNCDB_IMPORT_EXCLUDE_TABLE_DATA=table5,table6
```

## Export Configuration

The export configuration can be set using environment variables or command-line flags:

```bash
# Export configuration
SYNCDB_EXPORT_DRIVER=mysql
SYNCDB_EXPORT_HOST=localhost
SYNCDB_EXPORT_PORT=3306
SYNCDB_EXPORT_USERNAME=root
SYNCDB_EXPORT_PASSWORD=example
SYNCDB_EXPORT_DATABASE=commercedb
SYNCDB_EXPORT_TABLES=users,products
SYNCDB_EXPORT_FILEPATH=backup.json
SYNCDB_EXPORT_FOLDER_PATH=backup
SYNCDB_EXPORT_FORMAT=json
SYNCDB_EXPORT_EXCLUDE_TABLE=table1,table2
SYNCDB_EXPORT_EXCLUDE_TABLE_SCHEMA=table3,table4
SYNCDB_EXPORT_EXCLUDE_TABLE_DATA=table5,table6
```

## Usage Examples

### Export Data

Export data with schema to a timestamped folder:
```bash
./syncdb export --include-schema --folder-path backup
```

This will create a directory structure like:
```
backup/
  ├── 20250329_150405/    # Timestamp (YYYYMMDD_HHMMSS)
  │   └── backup.json
  └── 20250328_103000/
      └── backup.json
```

### Import Data

Import from a specific file:
```bash
./syncdb import --file-path backup.json
```

Import from the latest backup in a folder:
```bash
./syncdb import --folder-path backup --database commercedb
```

This will automatically find and use the most recent backup from the timestamped subdirectories under the database folder. Note that when using `--folder-path`, you must specify the database name either via `--database` flag or `SYNCDB_IMPORT_DATABASE` environment variable.

## Project Structure
- **cmd/**: Entry point for the CLI application.
- **pkg/config/**: Configuration management (loading .env and parsing CLI flags).
- **pkg/db/**: Database connection and operations (export and import logic).
- **pkg/storage/**: Storage abstraction with implementations for local files, S3, and Google Drive.
- **pkg/cli/**: CLI command definitions and flag parsing using frameworks like Cobra.

## Task Breakdown

### 1. Project Setup
- Initialize a new Go module.
- Set up directories: `cmd/`, `pkg/config/`, `pkg/db/`, `pkg/storage/`, and `pkg/cli/`.

### 2. Configuration Management
- **Environment Variables & CLI Flags:**
  - Use a package like [Viper](https://github.com/spf13/viper) or [godotenv](https://github.com/joho/godotenv) to load .env files.
  - Allow command line flags to override .env values.
- **Parameters to Define:**
  - **Database:** host, port, username, password, tables (default: all).
  - **Export/Import:** include schema (default: false), condition for export, upsert (default: true).
  - **Storage:** Local file path, S3 bucket info, Google Drive credentials.

### 3. CLI Design
- **Framework:** Use Cobra for CLI command structure.
- **Commands:**
  - `export`: To export the database.
  - `import`: To import the database.
- **Flags:**  
  - Flags for all database connection parameters.
  - Flags for storage options.
  - Flags for export/import options (tables selection, include schema, export condition, and upsert).

### 4. Database Operations
- **Connection Module:**  
  - Create functions to connect to the database using the provided credentials.
- **Export Process:**  
  - Query the selected tables (or all tables if none specified).
  - Optionally include the schema if the flag is set.
  - Apply the WHERE condition if provided (e.g., `created_at > 2024-01-01`).
  - Serialize data to a chosen format (SQL dump, JSON, or CSV).
- **Import Process:**  
  - Read and deserialize the file.
  - Insert or upsert data into the database based on the upsert flag.
- **Error Handling & Logging:**  
  - Implement robust logging (e.g., using logrus or Go’s standard log package) and clear error messages.

### 5. Storage Abstraction
- **Unified Storage Interface:**  
  - Define methods such as `Upload()`, `Download()`, and optionally `Delete()`.
- **Local File Module:**  
  - Implement file read/write operations.
- **S3 Module:**  
  - Integrate using AWS SDK for Go.
- **Google Drive Module:**  
  - Integrate using the Google Drive API.

### 6. Sync Workflow
- **Export Workflow:**
  - Connect to the database.
  - Fetch data with optional filtering conditions.
  - Optionally include schema in the export.
  - Serialize and upload the file using the chosen storage option.
- **Import Workflow:**
  - Retrieve the file from the chosen storage.
  - Deserialize and upsert (if enabled) data back into the database.
- **Dry-run Mode:**  
  - (Optional) Implement a dry-run option to simulate operations without making changes.

### 7. Testing & Validation
- **Unit Tests:**  
  - Test configuration parsing, database connectivity, export/import operations, and storage functionality.
- **Integration Tests:**  
  - Mock external services (S3 and Google Drive) for testing.

### 8. Documentation
- **README:**  
  - Setup and configuration instructions.
  - Detailed CLI usage with examples.
  - Information on .env file setup and external service credentials.
- **Diagrams:**  
  - (Optional) Create flow diagrams to illustrate module interactions.

### 9. Packaging & Deployment
- **Build Scripts/Docker:**  
  - Create scripts or a Dockerfile for containerized deployment.
- **Platform Documentation:**  
  - Instructions on compiling and running on different OS platforms.

### 10. Future Enhancements (Optional)
- Add concurrency for faster import/export processes.
- Implement incremental sync capabilities.
- Enhance CLI UI with progress indicators or verbose logging options.

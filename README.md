# SyncDB - Database Synchronization Tool

SyncDB is a command-line tool written in Go that helps you export and import database data across different environments. It supports multiple storage options including local filesystem, AWS S3, and Google Drive.

## Features

- Export database data to various storage locations
- Import data back into databases with upsert support
- Support for multiple database drivers:
  - MySQL (default)
  - PostgreSQL
- Multiple storage options:
  - Local filesystem
  - AWS S3
  - Google Drive
- Flexible configuration via command-line flags or environment variables
- Support for selective table export/import
- Optional schema inclusion in exports
- Conditional data export using WHERE clauses
- Organized export structure with timestamp-based folders
- **Profile Management:** Save and reuse common configurations.

## Installation

### Prerequisites

- Go 1.22 or later
- Access to source and target databases
- Appropriate credentials for chosen storage option (S3/Google Drive)

### Using Go Install

```bash
go install github.com/hoangnguyenba/syncdb/cmd/syncdb@latest
```

Or install a specific version:

```bash
go install github.com/hoangnguyenba/syncdb/cmd/syncdb@v0.1.0
```

### Building from Source

```bash
# Clone the repository
git clone https://github.com/hoangnguyenba/syncdb.git
cd syncdb

# Build the binary
go build -o syncdb cmd/syncdb/*.go
```

## Usage

### Profile Management

SyncDB allows you to save common connection and operation settings into named profiles for easier reuse.

**Profile Storage:**
- Profiles are stored as YAML files (`<profile-name>.yaml`) in a dedicated directory.
- This directory is determined by the `SYNCDB_PATH` environment variable.
- If `SYNCDB_PATH` is not set, it defaults to `$HOME/.config/syncdb/profiles` (or platform equivalent like `~/Library/Application Support/syncdb/profiles` on macOS, `%APPDATA%\syncdb\profiles` on Windows).

**Commands:**

- **Create a new profile:**
  ```bash
  syncdb profile create <profile-name> [flags...]
  ```
  *Example:* Create a profile for a local development MySQL database.
  ```bash
  syncdb profile create dev-local \
    --host localhost \
    --port 3306 \
    --username devuser \
    --password "devpass" \
    --database my_dev_db \
    --driver mysql \
    --tables users,products \
    --profile-include-schema=true \
    --exclude-table-data logs
  # Note: Passwords are stored in plain text in the profile file!
  ```

- **Update an existing profile (or create if missing):**
  ```bash
  syncdb profile update <profile-name> [flags...]
  ```
  *Example:* Update the password and add another excluded table for the `dev-local` profile.
  ```bash
  syncdb profile update dev-local \
    --password "new_dev_pass" \
    --exclude-table-data logs,audit_trail
  ```

- **List available profiles:**
  ```bash
  syncdb profile list
  ```
  *Output Example:*
  ```
  Available Profiles:
  - dev-local
  - staging-pg
  ```

- **Show details of a specific profile:**
  ```bash
  syncdb profile show <profile-name>
  ```
  *Example:* Show the configuration stored in the `dev-local` profile.
  ```bash
  syncdb profile show dev-local
  ```
  *Output Example (YAML format):*
  ```yaml
  --- Profile: dev-local ---
  host: localhost
  port: 3306
  username: devuser
  password: new_dev_pass
  database: my_dev_db
  driver: mysql
  tables:
      - users
      - products
  includeschema: true
  includedata: true
  condition: ""
  excludetable: []
  excludetableschema: []
  excludetabledata:
      - logs
      - audit_trail
  ```

- **Delete a profile:**
  ```bash
  syncdb profile delete <profile-name> --force
  ```
  *Example:* Delete the `staging-pg` profile (requires confirmation via `--force`).
  ```bash
  syncdb profile delete staging-pg --force
  ```

**Using Profiles with Export/Import:**

Use the `--profile <profile-name>` flag with `export` or `import` commands to load settings from a profile.

```bash
# Export using 'dev-local' profile, storing locally
syncdb export --profile dev-local --storage local --path ./dev_backups

# Import using 'staging-pg' profile from S3, overriding the database name for this run
syncdb import --profile staging-pg --storage s3 --s3-bucket staging-backups --database temp_staging_restore
```

**Configuration Loading Priority:**

Settings are determined in the following order (highest priority first):
1.  Command-line flags (e.g., `--port 3307`)
2.  Environment variables (e.g., `SYNCDB_PORT=3308`)
3.  Settings from the specified `--profile <profile-name>` file.
4.  Default values defined in the application.

---

### Export Data

```bash
# Basic MySQL export with default folder structure
syncdb export \
  --host localhost \
  --port 3306 \
  --username myuser \
  --password mypass \
  --database mydb \
  --driver mysql \
  --storage local

# Export to custom path
syncdb export \
  --database mydb \
  --path /path/to/exports

# Export specific tables with schema
syncdb export \
  --database mydb \
  --tables table1,table2 \
  --include-schema \
  --path ./backups

# Export without data
syncdb export \
  --database mydb \
  --include-data=false \
  --path ./backups

# Export with condition
syncdb export \
  --database mydb \
  --condition "created_at > '2024-01-01'" \
  --path ./backups

# Export to S3
syncdb export \
  --database mydb \
  --storage s3 \
  --s3-bucket my-bucket \
  --s3-region us-west-2

# Export to Google Drive
syncdb export \
  --database mydb \
  --storage gdrive \
  --gdrive-folder folder_id
```

The export command will create a folder structure like this:
```
<path>/
└── <timestamp>/
      ├── 0_metadata.json
      ├── 0_schema.sql (if --include-schema is used)
      ├── 1_users.sql
      ├── 2_products.sql
      ├── 3_orders.sql
      └── ...
```

### Import Data

```bash
# Basic MySQL import from local file
syncdb import \
  --host localhost \
  --port 3306 \
  --username myuser \
  --password mypass \
  --database mydb \
  --driver mysql \
  --storage local \
  --file-path ./backup.json

# Basic PostgreSQL import from local file
syncdb import \
  --host localhost \
  --port 5432 \
  --username myuser \
  --password mypass \
  --database mydb \
  --driver postgres \
  --storage local \
  --file-path ./backup.json

# Import specific tables
syncdb import \
  --database mydb \
  --tables table1,table2 \
  --file-path ./backup.json

# Import without upsert (insert only)
syncdb import \
  --database mydb \
  --upsert=false \
  --file-path ./backup.json

# Import from S3
syncdb import \
  --database mydb \
  --storage s3 \
  --s3-bucket my-bucket \
  --s3-region us-west-2

# Import from Google Drive
syncdb import \
  --database mydb \
  --storage gdrive \
  --gdrive-folder folder_id
```

### Table Pattern Matching (Wildcards)

All table-related parameters (such as `--tables`, `--exclude-table`, `--exclude-table-schema`, `--exclude-table-data`) support simple wildcard patterns:

- `*` matches all tables
- `*_archival` matches all tables ending with `_archival`
- `bk_*` matches all tables starting with `bk_`
- `*foo*` matches all tables containing `foo`

You can combine multiple patterns separated by commas. For example:

```bash
syncdb export --exclude-table logs,*_archival,bk_*
```
This will exclude the `logs` table, all tables ending with `_archival`, and all tables starting with `bk_`.

This logic applies to all table selection/exclusion parameters in both export and import commands.

## Configuration

Flags can be used to configure database connections, export/import behavior, and storage options. They can also be set via environment variables (see below).

- `--profile`: Name of the configuration profile to use for default settings.

### Database Connection

- `--host`: Database server address (default: "localhost")
- `--port`: Port number (default: 3306 for MySQL, 5432 for PostgreSQL)
- `--username`: Database username
- `--password`: Database password
- `--database`: Database name (required)
- `--driver`: Database driver (mysql, postgres) (default: "mysql")
- `--tables`: Comma-separated list of tables (default: all tables)

### Export Settings

- `--include-schema`: Include database schema in export
- `--include-data`: Include data in export (default: true)
- `--condition`: WHERE condition for filtering data during export
- `--path`: Path for export files (default: .)
- `--format`: Output format (json, sql) (default: "sql")
- `--exclude-table`: Exclude both schema and data for specified tables
- `--exclude-table-schema`: Exclude schema for specified tables
- `--exclude-table-data`: Exclude data for specified tables
- `--from-table-index`: Resume export from a specific table index (for resuming interrupted exports)
- `--from-chunk-index`: Resume export from a specific chunk within a table (for resuming interrupted exports)

### Import Settings

- `--upsert`: Perform upsert instead of insert (default: true)
- `--from-table-index`: Resume import from a specific table index (for resuming interrupted imports)
- `--from-chunk-index`: Resume import from a specific chunk within a table (for resuming interrupted imports)

### Storage Settings

#### Local Storage
- `--storage local`: Use local filesystem

#### S3 Storage
- `--storage s3`: Use AWS S3
- `--s3-bucket`: S3 bucket name
- `--s3-region`: AWS region

#### Google Drive Storage
- `--storage gdrive`: Use Google Drive
- `--gdrive-folder`: Google Drive folder ID

### Environment Variables

Most command-line flags can also be set using environment variables (except for profile-specific boolean flags like `--profile-include-schema`). Environment variables have lower priority than flags but higher priority than profile settings. The format is:
```
SYNCDB_<FLAG_NAME>=value
```

For example:
```bash
export SYNCDB_HOST=localhost
export SYNCDB_PORT=3306
export SYNCDB_USERNAME=myuser
export SYNCDB_PASSWORD=mypass
export SYNCDB_DATABASE=mydb
export SYNCDB_DRIVER=mysql
export SYNCDB_FOLDER_PATH=/path/to/exports
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with [Cobra](https://github.com/spf13/cobra) for CLI functionality
- Uses [AWS SDK for Go](https://github.com/aws/aws-sdk-go) for S3 integration
- Uses [Google Drive API](https://developers.google.com/drive/api/v3/reference) for Google Drive integration

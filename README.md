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

## Installation

### Prerequisites

- Go 1.19 or later
- Access to source and target databases
- Appropriate credentials for chosen storage option (S3/Google Drive)

### Building from Source

```bash
# Clone the repository
git clone https://github.com/hoangnguyenba/syncdb.git
cd syncdb

# Build the binary
go build -o syncdb cmd/syncdb/*.go
```

## Usage

### Export Data

```bash
# Basic MySQL export to local file
syncdb export \
  --host localhost \
  --port 3306 \
  --username myuser \
  --password mypass \
  --database mydb \
  --driver mysql \
  --storage local \
  --file-path ./backup.json

# Basic PostgreSQL export to local file
syncdb export \
  --host localhost \
  --port 5432 \
  --username myuser \
  --password mypass \
  --database mydb \
  --driver postgres \
  --storage local \
  --file-path ./backup.json

# Export specific tables
syncdb export \
  --database mydb \
  --tables table1,table2 \
  --file-path ./backup.json

# Export with schema
syncdb export \
  --database mydb \
  --include-schema \
  --file-path ./backup.json

# Export with condition
syncdb export \
  --database mydb \
  --condition "created_at > '2024-01-01'" \
  --file-path ./backup.json

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

## Configuration

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
- `--condition`: WHERE condition for filtering data during export

### Import Settings

- `--upsert`: Perform upsert instead of insert (default: true)

### Storage Settings

#### Local Storage
- `--storage local`: Use local filesystem
- `--file-path`: Path to store/read the file

#### S3 Storage
- `--storage s3`: Use AWS S3
- `--s3-bucket`: S3 bucket name
- `--s3-region`: AWS region

#### Google Drive Storage
- `--storage gdrive`: Use Google Drive
- `--gdrive-folder`: Google Drive folder ID

### Environment Variables

All command-line flags can also be set using environment variables. The format is:
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
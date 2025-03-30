package main

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/hoangnguyenba/syncdb/pkg/config"
	"github.com/hoangnguyenba/syncdb/pkg/db"
	"github.com/hoangnguyenba/syncdb/pkg/storage"
	"github.com/spf13/cobra"
)

var sqlInsertRegex = regexp.MustCompile(`INSERT INTO (\w+) \((.*?)\) VALUES \((.*?)\);`)

func getLatestTimestampDir(basePath string, dbName string) (string, error) {
	// Check if base directory exists
	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		return "", fmt.Errorf("base directory not found: %s", basePath)
	}

	// Get all subdirectories in the base directory
	entries, err := os.ReadDir(basePath)
	if err != nil {
		return "", fmt.Errorf("failed to read directory: %v", err)
	}

	var latestTime time.Time
	var latestDir string

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		// Try to parse directory name as timestamp
		dirTime, err := time.Parse("20060102_150405", entry.Name())
		if err != nil {
			continue
		}

		if latestDir == "" || dirTime.After(latestTime) {
			latestTime = dirTime
			latestDir = entry.Name()
		}
	}

	if latestDir == "" {
		return "", fmt.Errorf("no valid timestamp directories found in %s", basePath)
	}

	return filepath.Join(basePath, latestDir), nil
}

func getLatestZipFile(basePath string) (string, error) {
	// Check if base directory exists
	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		return "", fmt.Errorf("base directory not found: %s", basePath)
	}

	// Get all files in the base directory
	entries, err := os.ReadDir(basePath)
	if err != nil {
		return "", fmt.Errorf("failed to read directory: %v", err)
	}

	var latestTime time.Time
	var latestZip string

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Check if file is a zip file
		if !strings.HasSuffix(entry.Name(), ".zip") {
			continue
		}

		// Try to parse filename (without .zip) as timestamp
		timestamp := strings.TrimSuffix(entry.Name(), ".zip")
		fileTime, err := time.Parse("20060102_150405", timestamp)
		if err != nil {
			continue
		}

		if latestZip == "" || fileTime.After(latestTime) {
			latestTime = fileTime
			latestZip = entry.Name()
		}
	}

	if latestZip == "" {
		return "", fmt.Errorf("no valid zip files found in %s", basePath)
	}

	return filepath.Join(basePath, latestZip), nil
}

func unzipFile(zipPath string, destPath string) error {
	fmt.Printf("Opening zip file: %s\n", zipPath)
	// Open the zip file
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return fmt.Errorf("failed to open zip file: %v", err)
	}
	defer reader.Close()

	fmt.Printf("Found %d files in zip archive\n", len(reader.File))

	// Extract each file
	extractedCount := 0
	for _, file := range reader.File {
		// Get file info
		fileInfo := file.FileInfo()

		// Skip directories
		if fileInfo.IsDir() {
			continue
		}

		// Open the file in the zip
		rc, err := file.Open()
		if err != nil {
			return fmt.Errorf("failed to open file in zip: %v", err)
		}

		// Create the file path, preserving timestamp directory structure
		path := filepath.Join(destPath, file.Name)

		// Create parent directory if it doesn't exist
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			rc.Close()
			return fmt.Errorf("failed to create directory: %v", err)
		}

		// Create the file
		outFile, err := os.Create(path)
		if err != nil {
			rc.Close()
			return fmt.Errorf("failed to create file: %v", err)
		}

		// Copy the contents
		written, err := io.Copy(outFile, rc)
		outFile.Close()
		rc.Close()
		if err != nil {
			return fmt.Errorf("failed to write file: %v", err)
		}

		extractedCount++
		fmt.Printf("Extracted: %s (%d bytes)\n", file.Name, written)
	}

	fmt.Printf("Successfully extracted %d files\n", extractedCount)
	return nil
}

func newImportCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "import",
		Short: "Import database data",
		Long:  `Import database data from a file.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Load config from environment
			cfg, err := config.LoadConfig()
			if err != nil {
				return fmt.Errorf("failed to load config: %v", err)
			}

			// Get flags, use config as defaults
			host, _ := cmd.Flags().GetString("host")
			if host == "localhost" { // default value
				host = cfg.Import.Host
			}

			port, _ := cmd.Flags().GetInt("port")
			if port == 3306 { // default value
				port = cfg.Import.Port
			}

			username, _ := cmd.Flags().GetString("username")
			if username == "" {
				username = cfg.Import.Username
			}

			password, _ := cmd.Flags().GetString("password")
			if password == "" {
				password = cfg.Import.Password
			}

			dbName, _ := cmd.Flags().GetString("database")
			if dbName == "" {
				dbName = cfg.Import.Database
			}

			dbDriver, _ := cmd.Flags().GetString("driver")
			if dbDriver == "mysql" { // default value
				dbDriver = cfg.Import.Driver
			}

			tables, _ := cmd.Flags().GetStringSlice("tables")
			if len(tables) == 0 {
				tables = cfg.Import.Tables
			}

			filePath, _ := cmd.Flags().GetString("file-path")
			if filePath == "" {
				filePath = cfg.Import.Filepath
			}

			folderPath, _ := cmd.Flags().GetString("folder-path")
			if folderPath == "" {
				folderPath = cfg.Import.FolderPath
			}

			format, _ := cmd.Flags().GetString("format")
			if format == "json" { // default value
				format = cfg.Import.Format
			}

			truncate, _ := cmd.Flags().GetBool("truncate")
			includeSchema, _ := cmd.Flags().GetBool("include-schema")
			includeData, _ := cmd.Flags().GetBool("include-data")
			includeViewData, _ := cmd.Flags().GetBool("include-view-data")

			// Get storage flags
			storageType, _ := cmd.Flags().GetString("storage")
			if storageType == "" {
				storageType = cfg.Import.Storage
			}

			// Initialize storage
			var store storage.Storage
			switch storageType {
			case "local":
				store = storage.NewLocalStorage(folderPath)
			case "s3":
				s3Bucket, _ := cmd.Flags().GetString("s3-bucket")
				if s3Bucket == "" {
					s3Bucket = cfg.Import.S3Bucket
				}

				s3Region, _ := cmd.Flags().GetString("s3-region")
				if s3Region == "" {
					s3Region = cfg.Import.S3Region
				}

				if s3Bucket == "" {
					return fmt.Errorf("s3-bucket is required when storage is set to s3")
				}
				if s3Region == "" {
					return fmt.Errorf("s3-region is required when storage is set to s3")
				}

				store = storage.NewS3Storage(s3Bucket, s3Region)
				if store == nil {
					return fmt.Errorf("failed to initialize S3 storage. Please ensure AWS credentials are set in environment")
				}
			default:
				return fmt.Errorf("unsupported storage type: %s", storageType)
			}

			// Validate required values
			if dbName == "" {
				return fmt.Errorf("database name is required (set via --database flag or SYNCDB_IMPORT_DATABASE env)")
			}

			// Check if folder path is provided
			if folderPath != "" {
				var importDir string
				useZip, _ := cmd.Flags().GetBool("zip")

				if useZip {
					// Create temporary directory for files
					importDir, err = os.MkdirTemp("", "syncdb_import_*")
					if err != nil {
						return fmt.Errorf("failed to create temporary directory: %v", err)
					}
					defer os.RemoveAll(importDir) // Clean up temp directory when done

					if storageType == "s3" {
						fmt.Printf("Searching for latest zip file in S3 with prefix: %s\n", folderPath)
						// Find and download latest zip from S3
						files, err := store.ListObjects(folderPath)
						if err != nil {
							return fmt.Errorf("failed to list objects in S3: %v", err)
						}

						// Find latest zip file
						var latestZip string
						for _, file := range files {
							if strings.HasSuffix(file, ".zip") {
								if latestZip == "" || file > latestZip {
									latestZip = file
								}
							}
						}

						if latestZip == "" {
							return fmt.Errorf("no zip files found in S3 bucket under path: %s", folderPath)
						}

						fmt.Printf("Found latest zip file: %s\n", latestZip)
						fmt.Printf("Downloading zip file from S3...\n")

						// Download the zip file
						zipData, err := store.Download(latestZip)
						if err != nil {
							return fmt.Errorf("failed to download zip from S3: %v", err)
						}
						fmt.Printf("Successfully downloaded %d bytes from S3\n", len(zipData))

						// Save zip file locally
						zipPath := filepath.Join(importDir, "backup.zip")
						if err := os.WriteFile(zipPath, zipData, 0644); err != nil {
							return fmt.Errorf("failed to save zip file: %v", err)
						}
						fmt.Printf("Saved zip file to: %s\n", zipPath)

						fmt.Printf("Unzipping file to: %s\n", importDir)
						// Unzip the file
						if err := unzipFile(zipPath, importDir); err != nil {
							return err
						}

						fmt.Println("Looking for metadata file...")
						// Find the timestamp directory by looking for the metadata file
						err = filepath.Walk(importDir, func(path string, info os.FileInfo, err error) error {
							if err != nil {
								return err
							}
							if !info.IsDir() && strings.HasSuffix(path, "0_metadata.json") {
								// Get parent directory of metadata file
								importDir = filepath.Dir(path)
								return filepath.SkipAll
							}
							return nil
						})
						if err != nil {
							return fmt.Errorf("failed to find metadata file: %v", err)
						}

						if importDir == "" {
							return fmt.Errorf("no metadata file found in zip file")
						}

						fmt.Printf("Using directory with metadata: %s\n", importDir)

						// List all files after unzip
						fmt.Println("\nFiles available in import directory:")
						err = filepath.Walk(importDir, func(path string, info os.FileInfo, err error) error {
							if err != nil {
								return err
							}
							if !info.IsDir() {
								relPath, err := filepath.Rel(importDir, path)
								if err != nil {
									return err
								}
								fmt.Printf("  - %s (%d bytes)\n", relPath, info.Size())
							}
							return nil
						})
						if err != nil {
							fmt.Printf("Warning: failed to list unzipped files: %v\n", err)
						}
						fmt.Println()
					} else {
						// Find latest zip file locally
						zipFile, err := getLatestZipFile(folderPath)
						if err != nil {
							return err
						}

						// Unzip the file
						if err := unzipFile(zipFile, importDir); err != nil {
							return err
						}

						fmt.Println("Looking for metadata file...")
						// Find the timestamp directory by looking for the metadata file
						err = filepath.Walk(importDir, func(path string, info os.FileInfo, err error) error {
							if err != nil {
								return err
							}
							if !info.IsDir() && strings.HasSuffix(path, "0_metadata.json") {
								// Get parent directory of metadata file
								importDir = filepath.Dir(path)
								return filepath.SkipAll
							}
							return nil
						})
						if err != nil {
							return fmt.Errorf("failed to find metadata file: %v", err)
						}

						if importDir == "" {
							return fmt.Errorf("no metadata file found in zip file")
						}

						fmt.Printf("Using directory with metadata: %s\n", importDir)

						// List all files after unzip
						fmt.Println("\nFiles available in import directory:")
						err = filepath.Walk(importDir, func(path string, info os.FileInfo, err error) error {
							if err != nil {
								return err
							}
							if !info.IsDir() {
								relPath, err := filepath.Rel(importDir, path)
								if err != nil {
									return err
								}
								fmt.Printf("  - %s (%d bytes)\n", relPath, info.Size())
							}
							return nil
						})
						if err != nil {
							fmt.Printf("Warning: failed to list unzipped files: %v\n", err)
						}
						fmt.Println()
					}
				} else {
					if storageType == "s3" {
						// Create temporary directory for files
						importDir, err = os.MkdirTemp("", "syncdb_import_*")
						if err != nil {
							return fmt.Errorf("failed to create temporary directory: %v", err)
						}
						defer os.RemoveAll(importDir)

						// List files in S3 to find latest timestamp directory
						files, err := store.ListObjects(folderPath)
						if err != nil {
							return fmt.Errorf("failed to list objects in S3: %v", err)
						}

						// Find latest timestamp directory
						var latestTimestamp string
						for _, file := range files {
							// Skip non-SQL and non-JSON files
							if !strings.HasSuffix(file, ".sql") && !strings.HasSuffix(file, ".json") {
								continue
							}

							// Extract timestamp from file path
							parts := strings.Split(file, "/")
							if len(parts) < 2 {
								continue
							}
							timestamp := parts[len(parts)-2]

							if latestTimestamp == "" || timestamp > latestTimestamp {
								latestTimestamp = timestamp
							}
						}

						if latestTimestamp == "" {
							return fmt.Errorf("no timestamp directories found in S3")
						}

						// Download required files from S3
						if includeSchema {
							schemaData, err := store.Download(fmt.Sprintf("%s/0_schema.sql", latestTimestamp))
							if err != nil {
								return fmt.Errorf("failed to download schema from S3: %v", err)
							}
							if err := os.WriteFile(filepath.Join(importDir, "0_schema.sql"), schemaData, 0644); err != nil {
								return fmt.Errorf("failed to save schema file: %v", err)
							}
						}

						// Download metadata
						metadataData, err := store.Download(fmt.Sprintf("%s/0_metadata.json", latestTimestamp))
						if err != nil {
							return fmt.Errorf("failed to download metadata from S3: %v", err)
						}
						if err := os.WriteFile(filepath.Join(importDir, "0_metadata.json"), metadataData, 0644); err != nil {
							return fmt.Errorf("failed to save metadata file: %v", err)
						}

						// Read metadata to get table list
						var metadata struct {
							ExportedAt   time.Time `json:"exported_at"`
							DatabaseName string    `json:"database_name"`
							Tables       []string  `json:"tables"`
							Schema       bool      `json:"include_schema"`
							ViewData     bool      `json:"include_view_data"`
							Base64       bool      `json:"base64"`
						}
						if err := json.Unmarshal(metadataData, &metadata); err != nil {
							return fmt.Errorf("failed to parse metadata file: %v", err)
						}

						// Download each table file
						for i, table := range metadata.Tables {
							tableData, err := store.Download(fmt.Sprintf("%s/%d_%s.sql", latestTimestamp, i+1, table))
							if err != nil {
								return fmt.Errorf("failed to download table %s from S3: %v", table, err)
							}
							if err := os.WriteFile(filepath.Join(importDir, fmt.Sprintf("%d_%s.sql", i+1, table)), tableData, 0644); err != nil {
								return fmt.Errorf("failed to save table file: %v", err)
							}
						}
					} else {
						// Get latest timestamp directory locally
						importDir, err = getLatestTimestampDir(folderPath, dbName)
						if err != nil {
							return err
						}
					}
				}

				// Initialize database connection
				database, err := db.InitDB(dbDriver, host, port, username, password, dbName)
				if err != nil {
					return fmt.Errorf("failed to connect to database: %v", err)
				}
				defer database.Close()

				// Create a Connection instance
				conn := &db.Connection{
					DB: database,
					Config: db.ConnectionConfig{
						Driver:   dbDriver,
						Host:     host,
						Port:     port,
						User:     username,
						Password: password,
						Database: dbName,
					},
				}

				// Get tables if not specified
				if len(tables) == 0 {
					tables, err = db.GetTables(conn)
					if err != nil {
						return fmt.Errorf("failed to get tables: %v", err)
					}
				}

				// Get table dependencies and sort tables
				deps := make(map[string][]string)
				for _, table := range tables {
					deps[table], err = db.GetTableDependencies(conn, table)
					if err != nil {
						return fmt.Errorf("failed to get dependencies for table %s: %v", table, err)
					}
				}
				tables = db.SortTablesByDependencies(tables, deps)

				// Handle table exclusions
				var excludeTables []string
				var excludeTableSchema []string
				var excludeTableData []string

				if cmd.Flags().Changed("exclude-table") {
					excludeTables, _ = cmd.Flags().GetStringSlice("exclude-table")
				} else {
					excludeTables = cfg.Import.ExcludeTable
				}

				if cmd.Flags().Changed("exclude-table-schema") {
					excludeTableSchema, _ = cmd.Flags().GetStringSlice("exclude-table-schema")
				} else {
					excludeTableSchema = cfg.Import.ExcludeTableSchema
				}

				if cmd.Flags().Changed("exclude-table-data") {
					excludeTableData, _ = cmd.Flags().GetStringSlice("exclude-table-data")
				} else {
					excludeTableData = cfg.Import.ExcludeTableData
				}

				// Create a map for faster lookup
				excludeTableMap := make(map[string]bool)
				excludeSchemaMap := make(map[string]bool)
				excludeDataMap := make(map[string]bool)

				for _, t := range excludeTables {
					excludeTableMap[t] = true
					excludeSchemaMap[t] = true
					excludeDataMap[t] = true
				}
				for _, t := range excludeTableSchema {
					excludeSchemaMap[t] = true
				}
				for _, t := range excludeTableData {
					excludeDataMap[t] = true
				}

				// Import schema if requested
				if includeSchema {
					// Read schema file directly
					schemaFilePath := filepath.Join(importDir, "0_schema.sql")
					schemaContent, err := os.ReadFile(schemaFilePath)
					if err != nil {
						return fmt.Errorf("failed to read schema file: %v", err)
					}

					// Execute schema directly
					_, err = conn.DB.Exec(string(schemaContent))
					if err != nil {
						return fmt.Errorf("failed to execute schema: %v", err)
					}

					fmt.Println("Schema imported successfully")
				}

				// Truncate tables if requested (regardless of whether schema import is enabled)
				if truncate {
					for _, table := range tables {
						if excludeDataMap[table] {
							continue
						}

						// Check if it's a view
						isView, err := db.IsView(conn, table)
						if err != nil {
							// If table doesn't exist yet, it's not a view
							if strings.Contains(err.Error(), "doesn't exist") {
								continue // Skip tables that don't exist
							} else {
								return fmt.Errorf("failed to check if %s is a view: %v", table, err)
							}
						}

						// Don't truncate views
						if isView {
							continue
						}

						if err := db.TruncateTable(conn, table); err != nil {
							return fmt.Errorf("failed to truncate table %s: %v", table, err)
						}

						fmt.Printf("Truncated table '%s'\n", table)
					}
				}

				// Import data if requested
				if includeData {
					for i, table := range tables {
						if excludeDataMap[table] {
							continue
						}

						// Check if it's a view
						isView, err := db.IsView(conn, table)
						if err != nil {
							// If table doesn't exist yet, it's not a view
							if strings.Contains(err.Error(), "doesn't exist") {
								isView = false
							} else {
								return fmt.Errorf("failed to check if %s is a view: %v", table, err)
							}
						}

						if isView && !includeViewData {
							continue
						}

						fmt.Printf("\nImporting data for table %s...\n", table)

						// Check if we need to decode base64 values
						useBase64, _ := cmd.Flags().GetBool("base64")

						// Read metadata file to check if export was done with base64
						metadataFile := filepath.Join(importDir, "0_metadata.json")
						metadataBytes, err := os.ReadFile(metadataFile)
						if err == nil {
							var metadata struct {
								Base64 bool `json:"base64"`
							}
							if err := json.Unmarshal(metadataBytes, &metadata); err == nil {
								// If metadata has base64 flag set to true, override the flag
								if metadata.Base64 {
									fmt.Println("Metadata indicates base64 encoding was used during export. Enabling base64 decoding automatically.")
									useBase64 = true
								}
							}
						}

						// Read SQL file content
						sqlFile := filepath.Join(importDir, fmt.Sprintf("%d_%s.sql", i+1, table))
						sqlBytes, err := os.ReadFile(sqlFile)
						if err != nil {
							return fmt.Errorf("failed to read SQL file %s: %v", sqlFile, err)
						}

						// Convert to string for processing
						sqlContent := string(sqlBytes)

						// Try to decode Base64 values in SQL if the base64 flag is enabled
						if useBase64 {
							sqlContent = decodeBase64Values(sqlContent)
						}

						// Split into individual SQL statements
						sqlStatements := strings.Split(sqlContent, ";")

						// Execute each statement separately
						for _, stmt := range sqlStatements {
							stmt = strings.TrimSpace(stmt)
							if stmt == "" {
								continue
							}

							_, err = conn.DB.Exec(stmt + ";")
							if err != nil {
								return fmt.Errorf("failed to import data to table %s: %v", table, err)
							}
						}

						fmt.Println("done!")
					}
				}

				return nil
			} else if filePath == "" {
				return fmt.Errorf("either --file-path or --folder-path is required")
			}

			// If using file-path, continue with existing JSON/SQL file import logic
			fileData, err := os.ReadFile(filePath)
			if err != nil {
				return fmt.Errorf("failed to read file: %v", err)
			}

			var importData ExportData

			switch format {
			case "json":
				if err := json.Unmarshal(fileData, &importData); err != nil {
					return fmt.Errorf("failed to parse JSON import file: %v", err)
				}
			case "sql":
				// Parse SQL file
				sqlStatements := strings.Split(string(fileData), "\n\n")
				importData = ExportData{
					Data: make(map[string][]map[string]interface{}),
				}

				for _, stmt := range sqlStatements {
					stmt = strings.TrimSpace(stmt)
					if stmt == "" || !strings.HasPrefix(strings.ToUpper(stmt), "INSERT") {
						continue
					}

					// Extract table name and values
					matches := sqlInsertRegex.FindStringSubmatch(stmt)
					if len(matches) != 4 {
						continue
					}

					tableName := matches[1]
					columns := strings.Split(matches[2], ",")
					for i := range columns {
						columns[i] = strings.TrimSpace(columns[i])
					}

					// Parse values
					values := strings.Split(matches[3], ",")
					for i := range values {
						values[i] = strings.TrimSpace(values[i])
					}

					// Create row data
					rowData := make(map[string]interface{})
					for i, col := range columns {
						val := values[i]
						if val == "NULL" {
							rowData[col] = nil
						} else if strings.HasPrefix(val, "'") && strings.HasSuffix(val, "'") {
							// String value - remove outer quotes
							strVal := strings.Trim(val, "'")

							// Try to parse as datetime first
							if t, err := time.Parse("2006-01-02 15:04:05", strVal); err == nil {
								rowData[col] = t
								continue
							}

							// Check if this might be a JSON string
							if strings.HasPrefix(strVal, "{") || strings.HasPrefix(strVal, "[") {
								// Try to parse as JSON
								var jsonVal interface{}
								if err := json.Unmarshal([]byte(strVal), &jsonVal); err == nil {
									rowData[col] = jsonVal
								} else {
									// If not valid JSON, use as regular string
									rowData[col] = strVal
								}
							} else {
								rowData[col] = strVal
							}
						} else {
							// Try to parse as number first
							if strings.Contains(val, ".") {
								// Try as float
								if f, err := strconv.ParseFloat(val, 64); err == nil {
									rowData[col] = f
									continue
								}
							} else {
								// Try as integer
								if i, err := strconv.ParseInt(val, 10, 64); err == nil {
									rowData[col] = i
									continue
								}
							}
							// If not a number, use as is
							rowData[col] = val
						}
					}

					if importData.Data[tableName] == nil {
						importData.Data[tableName] = make([]map[string]interface{}, 0)
					}
					importData.Data[tableName] = append(importData.Data[tableName], rowData)
				}
			default:
				return fmt.Errorf("unsupported format: %s (supported formats: json, sql)", format)
			}

			// Initialize database connection
			database, err := db.InitDB(dbDriver, host, port, username, password, dbName)
			if err != nil {
				return fmt.Errorf("failed to connect to database: %v", err)
			}
			defer database.Close()

			// Create a Connection instance
			conn := &db.Connection{
				DB: database,
				Config: db.ConnectionConfig{
					Driver:   dbDriver,
					Host:     host,
					Port:     port,
					User:     username,
					Password: password,
					Database: dbName,
				},
			}

			// Get base64 flag and check if it's set in metadata (for JSON format)
			useBase64, _ := cmd.Flags().GetBool("base64")
			if format == "json" && importData.Metadata.Base64 && !useBase64 {
				fmt.Println("Metadata indicates base64 encoding was used during export. Enabling base64 decoding automatically.")
				useBase64 = true
			}

			// Filter tables if specified
			importTables := importData.Metadata.Tables
			if len(tables) > 0 {
				importTables = tables
			}

			// Check if data should be imported based on metadata
			if !importData.Metadata.IncludeData && includeData {
				return fmt.Errorf("cannot import data: original export did not include data")
			}

			// Skip data import if include-data is false
			if !includeData {
				fmt.Println("Skipping data import as --include-data is set to false")
				return nil
			}

			// Import data for each table
			for _, table := range importTables {
				// Get current row count
				currentCount, err := db.GetTableRowCount(conn, table)
				if err != nil {
					return fmt.Errorf("failed to get row count for table %s: %v", table, err)
				}
				fmt.Printf("Table %s: %d rows before import\n", table, currentCount)

				// Truncate if requested
				if truncate {
					if err := db.TruncateTable(conn, table); err != nil {
						return fmt.Errorf("failed to truncate table %s: %v", table, err)
					}
				}

				// Import data
				data, ok := importData.Data[table]
				if !ok {
					return fmt.Errorf("table %s not found in import file", table)
				}

				// Check if we need to handle base64 decoding for string values in JSON data
				if useBase64 && format == "json" {
					for i, row := range data {
						for k, v := range row {
							if strVal, ok := v.(string); ok {
								// Try to decode base64 values
								if isBase64(strVal) {
									if decodedBytes, err := base64.StdEncoding.DecodeString(strVal); err == nil {
										data[i][k] = string(decodedBytes)
									}
								}
							}
						}
					}
				}

				// Create a buffer to store the data
				var buf bytes.Buffer
				encoder := json.NewEncoder(&buf)
				for _, row := range data {
					if err := encoder.Encode(row); err != nil {
						return fmt.Errorf("failed to encode data row for table %s: %v", table, err)
					}
				}

				if err := db.ImportTableData(conn, table, &buf); err != nil {
					return fmt.Errorf("failed to import data to table %s: %v", table, err)
				}

				// Get final row count
				finalCount, err := db.GetTableRowCount(conn, table)
				if err != nil {
					return fmt.Errorf("failed to get row count for table %s: %v", table, err)
				}
				fmt.Printf("Table %s: %d rows after import\n", table, finalCount)
			}

			return nil
		},
	}

	// Add flags
	flags := cmd.Flags()
	flags.StringP("host", "H", "localhost", "Database host")
	flags.IntP("port", "P", 3306, "Database port")
	flags.StringP("username", "u", "", "Database username")
	flags.StringP("password", "p", "", "Database password")
	flags.StringP("database", "d", "", "Database name")
	flags.StringP("driver", "D", "mysql", "Database driver (mysql, postgres)")
	flags.StringP("path", "i", "", "Path to import files or zip file")
	flags.StringP("folder-path", "o", "", "Folder path for temporary files (when importing from s3)")
	flags.StringP("storage", "s", "local", "Storage type (local, s3)")
	flags.String("s3-bucket", "", "S3 bucket name")
	flags.String("s3-region", "", "S3 region")
	flags.String("s3-key", "", "S3 key (path to zip file)")
	flags.Bool("truncate", false, "Truncate tables before importing")
	flags.Bool("skip-schema", false, "Skip schema import (only import data)")
	flags.Bool("base64", false, "Decode values from base64 format")
	flags.StringSlice("tables", []string{}, "Tables to import")
	flags.String("file-path", "", "File path to import from")
	flags.String("format", "json", "File format (json, sql)")
	flags.Bool("include-schema", false, "Include schema in import")
	flags.Bool("include-data", true, "Import data from export (default: true)")
	flags.Bool("include-view-data", true, "Include view data in import (default: true)")
	flags.StringSlice("exclude-table", []string{}, "Tables to exclude")
	flags.StringSlice("exclude-table-schema", []string{}, "Tables to exclude schema")
	flags.StringSlice("exclude-table-data", []string{}, "Tables to exclude data")
	flags.Bool("zip", false, "Import from latest zip file")

	return cmd
}

// Helper function to extract table name from schema statement
func extractTableNameFromSchema(stmt string) string {
	// Common patterns for table creation and alteration
	createTableRegex := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[` + "`" + `]?(\w+)[` + "`" + `]?`)
	alterTableRegex := regexp.MustCompile(`(?i)ALTER\s+TABLE\s+[` + "`" + `]?(\w+)[` + "`" + `]?`)

	// Try to match CREATE TABLE
	if matches := createTableRegex.FindStringSubmatch(stmt); len(matches) > 1 {
		return matches[1]
	}

	// Try to match ALTER TABLE
	if matches := alterTableRegex.FindStringSubmatch(stmt); len(matches) > 1 {
		return matches[1]
	}

	return ""
}

// isBase64 checks if a string is Base64 encoded
func isBase64(s string) bool {
	_, err := base64.StdEncoding.DecodeString(s)
	return err == nil && len(s) > 8 && strings.TrimRight(s, "=") != s[:len(s)-1]
}

// decodeBase64Values decodes Base64 encoded values in a SQL statement
func decodeBase64Values(sql string) string {
	// Define regex to match SQL values
	valueRegex := regexp.MustCompile(`'([^']*)'`)

	// Find all values and try to decode them if they're Base64 encoded
	decoded := valueRegex.ReplaceAllStringFunc(sql, func(match string) string {
		// Extract value without quotes
		value := match[1 : len(match)-1]

		// Check if it's Base64 encoded
		if isBase64(value) {
			decodedBytes, err := base64.StdEncoding.DecodeString(value)
			if err == nil {
				// Replace with decoded value
				return "'" + string(decodedBytes) + "'"
			}
		}

		// Return original value if not Base64 or decoding failed
		return match
	})

	return decoded
}

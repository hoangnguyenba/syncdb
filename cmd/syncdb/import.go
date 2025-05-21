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
		RunE: func(cmd *cobra.Command, cmdLineArgs []string) error { // Renamed original 'args' to 'cmdLineArgs'
			// Load config from environment
			cfg, err := config.LoadConfig()
			if err != nil {
				return fmt.Errorf("failed to load config: %v", err)
			}

			// Get profile name from flag
			profileName, _ := cmd.Flags().GetString("profile")

			// Populate arguments from flags, config, and profile
			cmdArgs, err := populateCommonArgsFromFlagsAndConfig(cmd, cfg.Import.CommonConfig, profileName) // Use 'cmdArgs'
			if err != nil {
				return err // Return error from profile loading/parsing
			}

			// Get import-specific flags
			truncate, _ := cmd.Flags().GetBool("truncate")
			dropDatabase, _ := cmd.Flags().GetBool("drop-database")
			filePath := getStringFlagWithConfigFallback(cmd, "file-path", cfg.Import.Filepath) // File path is not part of profile

			// Initialize storage (Storage settings are not part of profile)
			var store storage.Storage
			switch cmdArgs.Storage { // Use cmdArgs
			case "local":
				store = storage.NewLocalStorage(cmdArgs.FolderPath) // Use cmdArgs.FolderPath
			case "s3":
				if cmdArgs.S3Bucket == "" { // Use cmdArgs
					return fmt.Errorf("s3-bucket is required when storage is set to s3 (flag or SYNCDB_IMPORT_S3_BUCKET env)")
				}
				if cmdArgs.S3Region == "" { // Use cmdArgs
					return fmt.Errorf("s3-region is required when storage is set to s3 (flag or SYNCDB_IMPORT_S3_REGION env)")
				}
				store = storage.NewS3Storage(cmdArgs.S3Bucket, cmdArgs.S3Region) // Use cmdArgs.S3Bucket, cmdArgs.S3Region
				if store == nil {
					return fmt.Errorf("failed to initialize S3 storage. Please ensure AWS credentials are set in environment")
				}
			default:
				return fmt.Errorf("unsupported storage type: %s", cmdArgs.Storage) // Use cmdArgs
			}

			// Validate required values (Database name should now be resolved considering profile)
			if cmdArgs.Database == "" { // Use cmdArgs
				return fmt.Errorf("database name is required (set via --database flag, SYNCDB_IMPORT_DATABASE env, or profile)")
			}

			// Check if folder path is provided (Folder path is not part of profile)
			if cmdArgs.FolderPath != "" { // Use cmdArgs
				var importDir string

				if cmdArgs.Zip { // Use cmdArgs
					// Create temporary directory for files
					importDir, err = os.MkdirTemp("", "syncdb_import_*")
					if err != nil {
						return fmt.Errorf("failed to create temporary directory: %v", err)
					}
					defer os.RemoveAll(importDir) // Clean up temp directory when done

					if cmdArgs.Storage == "s3" { // Use cmdArgs
						fmt.Printf("Searching for latest zip file in S3 with prefix: %s\n", cmdArgs.FolderPath) // Use cmdArgs
						// Find and download latest zip from S3
						files, err := store.ListObjects(cmdArgs.FolderPath) // Use cmdArgs
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
							return fmt.Errorf("no zip files found in S3 bucket under path: %s", cmdArgs.FolderPath) // Use cmdArgs
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
					} else { // Local storage
						// Find latest zip file locally
						zipFile, err := getLatestZipFile(cmdArgs.FolderPath) // Use cmdArgs
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
				} else { // Not using zip
					if cmdArgs.Storage == "s3" { // Use cmdArgs
						// Create temporary directory for files
						importDir, err = os.MkdirTemp("", "syncdb_import_*")
						if err != nil {
							return fmt.Errorf("failed to create temporary directory: %v", err)
						}
						defer os.RemoveAll(importDir)

						// List files in S3 to find latest timestamp directory
						files, err := store.ListObjects(cmdArgs.FolderPath) // Use cmdArgs
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
						if cmdArgs.IncludeSchema { // Use cmdArgs
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
					} else { // Local storage, not zip
						// Get latest timestamp directory locally
						importDir, err = getLatestTimestampDir(cmdArgs.FolderPath, cmdArgs.Database) // Use cmdArgs
						if err != nil {
							return err
						}
					}
				} // Drop and recreate database if requested
				if dropDatabase {
					fmt.Printf("Dropping database %s...\n", cmdArgs.Database)
					// Connect to server without database to drop/create
					serverConn, err := db.InitDB(cmdArgs.Driver, cmdArgs.Host, cmdArgs.Port, cmdArgs.Username, cmdArgs.Password, "")
					if err != nil {
						return fmt.Errorf("failed to connect to server: %v", err)
					}

					// Use appropriate escaping based on driver
					var dropSQL, createSQL string
					if cmdArgs.Driver == "mysql" {
						dropSQL = fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", cmdArgs.Database)
						createSQL = fmt.Sprintf("CREATE DATABASE `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", cmdArgs.Database)
					} else {
						// PostgreSQL uses double quotes for identifiers
						dropSQL = fmt.Sprintf(`DROP DATABASE IF EXISTS "%s"`, cmdArgs.Database)
						createSQL = fmt.Sprintf(`CREATE DATABASE "%s" ENCODING = 'UTF8' LC_COLLATE = 'en_US.UTF-8' LC_CTYPE = 'en_US.UTF-8' TEMPLATE = template0`, cmdArgs.Database)
					}

					// Drop database if exists
					fmt.Printf("Dropping database if exists...\n")
					if _, err := serverConn.Exec(dropSQL); err != nil {
						serverConn.Close()
						return fmt.Errorf("failed to drop database: %v", err)
					}

					// Create database
					fmt.Printf("Creating new database...\n")
					if _, err := serverConn.Exec(createSQL); err != nil {
						serverConn.Close()
						return fmt.Errorf("failed to create database: %v", err)
					}

					serverConn.Close()
					fmt.Printf("Database %s dropped and recreated successfully\n", cmdArgs.Database)

					// Brief pause to allow database server to catch up
					time.Sleep(1 * time.Second)
				}

				// Initialize database connection
				fmt.Printf("Connecting to database %s...\n", cmdArgs.Database)
				database, err := db.InitDB(cmdArgs.Driver, cmdArgs.Host, cmdArgs.Port, cmdArgs.Username, cmdArgs.Password, cmdArgs.Database)
				if err != nil {
					return fmt.Errorf("failed to connect to database: %v", err)
				}
				defer database.Close()

				// Create a Connection instance
				conn := &db.Connection{
					DB: database,
					Config: db.ConnectionConfig{
						Driver:   cmdArgs.Driver,
						Host:     cmdArgs.Host,
						Port:     cmdArgs.Port,
						User:     cmdArgs.Username,
						Password: cmdArgs.Password,
						Database: cmdArgs.Database,
					},
				}

				// Use tables from cmdArgs
				currentTables := cmdArgs.Tables
				if len(currentTables) == 0 {
					currentTables, err = db.GetTables(conn)
					if err != nil {
						return fmt.Errorf("failed to get tables: %v", err)
					}
				}

				// Get table dependencies and sort tables
				deps := make(map[string][]string)
				for _, table := range currentTables {
					deps[table], err = db.GetTableDependencies(conn, table)
					if err != nil {
						return fmt.Errorf("failed to get dependencies for table %s: %v", table, err)
					}
				}
				sortedTables := db.SortTablesByDependencies(currentTables, deps)

				// Create maps for faster lookup
				allTables, err := db.GetTables(conn)
				if err != nil {
					return fmt.Errorf("failed to get all tables: %v", err)
				}

				expandedInclude := expandTablePatterns(allTables, cmdArgs.Tables)
				excludeTableMap := expandTablePatterns(allTables, cmdArgs.ExcludeTable)
				excludeSchemaMap := expandTablePatterns(allTables, cmdArgs.ExcludeTableSchema)
				excludeDataMap := expandTablePatterns(allTables, cmdArgs.ExcludeTableData)
				for t := range excludeTableMap {
					excludeSchemaMap[t] = true
					excludeDataMap[t] = true
				}

				importTables := []string{}
				for _, t := range sortedTables {
					if !excludeTableMap[t] && (len(expandedInclude) == 0 || expandedInclude[t]) {
						importTables = append(importTables, t)
					}
				}

				// Import schema if requested
				if cmdArgs.IncludeSchema {
					// Read schema file directly
					schemaFilePath := filepath.Join(importDir, "0_schema.sql")
					schemaContent, err := os.ReadFile(schemaFilePath)
					if err != nil {
						return fmt.Errorf("failed to read schema file: %v", err)
					}

					fmt.Printf("Importing schema from file (%d bytes)...\n", len(schemaContent))
					if err := importSchema(conn, schemaContent); err != nil {
						return fmt.Errorf("failed to import schema: %v", err)
					}
				} else {
					fmt.Println("Skipping schema import as --include-schema is set to false")
				}

				// Truncate tables if requested (regardless of whether schema import is enabled)
				if truncate { // truncate is import-specific, read directly
					for _, table := range sortedTables {
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
				if cmdArgs.IncludeData { // Use cmdArgs
					for _, table := range importTables {
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

						if isView && !cmdArgs.IncludeViewData { // Use cmdArgs
							continue
						}

						fmt.Printf("\nImporting data for table %s...\n", table)

						// Find the index of the table in metadata
						tableIndex := -1
						var metadata struct {
							Tables []string `json:"tables"`
						}

						// Read metadata to find correct file number
						metadataFile := filepath.Join(importDir, "0_metadata.json")
						metadataBytes, err := os.ReadFile(metadataFile)
						if err != nil {
							return fmt.Errorf("failed to read metadata file: %v", err)
						}
						if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
							return fmt.Errorf("failed to parse metadata file: %v", err)
						}

						// Find table's position in exported order
						for i, t := range metadata.Tables {
							if t == table {
								tableIndex = i + 1 // Use 1-based index as files are numbered from 1
								break
							}
						}

						if tableIndex == -1 {
							return fmt.Errorf("table %s not found in metadata", table)
						}

						// Read SQL file content with correct file number
						sqlFile := filepath.Join(importDir, fmt.Sprintf("%d_%s.sql", tableIndex, table))
						sqlBytes, err := os.ReadFile(sqlFile)
						if err != nil {
							return fmt.Errorf("failed to read SQL file %s: %v", sqlFile, err)
						}

						// Get the base64 flag value from cmdArgs
						useBase64 := cmdArgs.Base64

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
			} else if filePath == "" { // Check import-specific filePath
				return fmt.Errorf("either --file-path or --folder-path is required")
			}

			// --- Legacy File Path Import Logic ---
			// This section handles the older --file-path mechanism.
			// It might be worth considering deprecating this in favor of the folder-path approach.

			fmt.Printf("Using legacy --file-path import: %s\n", filePath)

			fileData, err := os.ReadFile(filePath)
			if err != nil {
				return fmt.Errorf("failed to read file: %v", err)
			}

			var importData ExportData // Assuming ExportData struct is still relevant here

			switch cmdArgs.Format { // Use cmdArgs.Format
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
				return fmt.Errorf("unsupported format: %s (supported formats: json, sql)", cmdArgs.Format) // Use cmdArgs
			}

			// Already handled database drop/recreate at the beginning of command

			// Initialize database connection
			database, err := db.InitDB(cmdArgs.Driver, cmdArgs.Host, cmdArgs.Port, cmdArgs.Username, cmdArgs.Password, cmdArgs.Database) // Use cmdArgs
			if err != nil {
				return fmt.Errorf("failed to connect to database: %v", err)
			}
			defer database.Close()

			// Create a Connection instance
			conn := &db.Connection{
				DB: database,
				Config: db.ConnectionConfig{
					Driver:   cmdArgs.Driver,   // Use cmdArgs
					Host:     cmdArgs.Host,     // Use cmdArgs
					Port:     cmdArgs.Port,     // Use cmdArgs
					User:     cmdArgs.Username, // Use cmdArgs
					Password: cmdArgs.Password, // Use cmdArgs
					Database: cmdArgs.Database, // Use cmdArgs
				},
			}

			// Get base64 flag from cmdArgs and check if it's set in metadata (for JSON format)
			useBase64 := cmdArgs.Base64                                               // Use cmdArgs
			if cmdArgs.Format == "json" && importData.Metadata.Base64 && !useBase64 { // Use cmdArgs
				fmt.Println("Metadata indicates base64 encoding was used during export. Enabling base64 decoding automatically.")
				useBase64 = true
			}

			// Filter tables if specified
			importTables := importData.Metadata.Tables // Use tables from the imported file's metadata
			if len(cmdArgs.Tables) > 0 {               // Override with tables from command line/config if provided // Use cmdArgs
				importTables = cmdArgs.Tables // Use cmdArgs
			}

			// Check if data should be imported based on metadata vs flag
			if !importData.Metadata.IncludeData && cmdArgs.IncludeData { // Use cmdArgs
				return fmt.Errorf("cannot import data: original export did not include data, but --include-data flag is true")
			}

			// Skip data import if include-data is false
			if !cmdArgs.IncludeData { // Use cmdArgs
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

				// Truncate if requested (using import-specific flag)
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
				if useBase64 && cmdArgs.Format == "json" { // Use cmdArgs
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

	// Add shared flags
	AddSharedFlags(cmd, true) // Pass true for import command

	// Add import-specific flags
	flags := cmd.Flags()
	flags.String("s3-key", "", "S3 key (path to zip file)")
	flags.Bool("truncate", false, "Truncate tables before importing")
	flags.Bool("drop-database", false, "Drop and recreate database before importing")
	flags.String("file-path", "", "File path to import from (alternative to folder-path)")

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

func importSchema(conn *db.Connection, schemaContent []byte) error {
	// First pass: collect all CREATE TABLE statements
	createTableStatements := make(map[string]string)
	var currentStatement strings.Builder

	lines := strings.Split(string(schemaContent), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}

		currentStatement.WriteString(line)
		currentStatement.WriteString("\n")

		if strings.HasSuffix(line, ";") {
			stmt := currentStatement.String()
			if strings.Contains(strings.ToUpper(stmt), "CREATE TABLE") {
				// Extract table name and validate it exists
				tableName := extractTableNameFromSchema(stmt)
				if tableName != "" {
					createTableStatements[tableName] = stmt
					fmt.Printf("Found CREATE TABLE for %s\n", tableName)
				}
			}
			currentStatement.Reset()
		}
	}

	if len(createTableStatements) == 0 {
		return fmt.Errorf("no CREATE TABLE statements found in schema")
	}

	// Build dependency graph
	deps := make(map[string][]string)
	for tableName, stmt := range createTableStatements {
		if strings.Contains(stmt, "FOREIGN KEY") {
			// Look for all REFERENCES clauses
			refRegex := regexp.MustCompile(`(?i)FOREIGN\s+KEY\s*\([^)]+\)\s*REFERENCES\s+[\x60"']?(\w+)[\x60"']?\s*\([^)]+\)`)
			matches := refRegex.FindAllStringSubmatch(stmt, -1)
			for _, match := range matches {
				if len(match) > 1 {
					referencedTable := match[1]
					deps[tableName] = append(deps[tableName], referencedTable)
					fmt.Printf("Table %s depends on %s\n", tableName, referencedTable)
				}
			}
		}
	}

	// Sort tables by dependencies
	var tables []string
	for t := range createTableStatements {
		tables = append(tables, t)
	}
	sortedTables := db.SortTablesByDependencies(tables, deps)

	// Start a transaction for schema changes
	tx, err := conn.DB.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %v", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Execute statements in dependency order with retry mechanism
	executedTables := make(map[string]bool)
	maxRetries := 5
	for attempt := 0; attempt < maxRetries; attempt++ {
		skippedTables := []string{}

		for _, tableName := range sortedTables {
			if executedTables[tableName] {
				continue
			}

			// Check if all dependencies are met
			canCreate := true
			for _, dep := range deps[tableName] {
				if !executedTables[dep] {
					canCreate = false
					break
				}
			}

			if !canCreate {
				skippedTables = append(skippedTables, tableName)
				continue
			}

			stmt := createTableStatements[tableName]
			fmt.Printf("Creating table %s... (attempt %d)\n", tableName, attempt+1)

			// Try to create the table
			_, err = tx.Exec(stmt)
			if err != nil {
				if strings.Contains(err.Error(), "Error 1824") ||
					strings.Contains(err.Error(), "errno 150") ||
					strings.Contains(strings.ToLower(err.Error()), "foreign key constraint fails") {
					skippedTables = append(skippedTables, tableName)
					fmt.Printf("Warning: Failed to create table %s (dependency issue), will retry\n", tableName)
					continue
				}
				return fmt.Errorf("failed to create table %s: %v", tableName, err)
			}

			executedTables[tableName] = true
			fmt.Printf("Successfully created table %s\n", tableName)
		}

		// If no tables were skipped or no progress was made, we're done
		if len(skippedTables) == 0 {
			break
		}

		// Check if we made any progress this iteration
		if attempt > 0 && len(skippedTables) == len(sortedTables) {
			return fmt.Errorf("failed to resolve table dependencies after %d attempts. Remaining tables: %v", attempt+1, skippedTables)
		}

		// Update sorted tables to only include remaining tables
		sortedTables = skippedTables
	}

	// Commit transaction if all is well
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit schema changes: %v", err)
	}

	fmt.Printf("Schema import completed successfully. Created %d tables.\n", len(executedTables))
	return nil
}

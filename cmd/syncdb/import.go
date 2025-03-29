package main

import (
	"archive/zip"
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
	// Open the zip file
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return fmt.Errorf("failed to open zip file: %v", err)
	}
	defer reader.Close()

	// Create destination directory
	if err := os.MkdirAll(destPath, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %v", err)
	}

	// Extract each file
	for _, file := range reader.File {
		// Open the file in the zip
		rc, err := file.Open()
		if err != nil {
			return fmt.Errorf("failed to open file in zip: %v", err)
		}

		// Create the file path
		path := filepath.Join(destPath, file.Name)

		if file.FileInfo().IsDir() {
			// Create directory
			if err := os.MkdirAll(path, 0755); err != nil {
				rc.Close()
				return fmt.Errorf("failed to create directory: %v", err)
			}
		} else {
			// Create the file
			outFile, err := os.Create(path)
			if err != nil {
				rc.Close()
				return fmt.Errorf("failed to create file: %v", err)
			}

			// Copy the contents
			_, err = io.Copy(outFile, rc)
			outFile.Close()
			rc.Close()
			if err != nil {
				return fmt.Errorf("failed to write file: %v", err)
			}
		}
	}

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
			upsert, _ := cmd.Flags().GetBool("upsert")
			includeSchema, _ := cmd.Flags().GetBool("include-schema")

			// Validate required values
			if dbName == "" {
				return fmt.Errorf("database name is required (set via --database flag or SYNCDB_IMPORT_DATABASE env)")
			}

			// Check if folder path is provided
			if folderPath != "" {
				var importDir string
				useZip, _ := cmd.Flags().GetBool("zip")

				if useZip {
					// Find latest zip file
					zipFile, err := getLatestZipFile(folderPath)
					if err != nil {
						return err
					}

					// Create temporary directory for unzipped files
					importDir, err = os.MkdirTemp("", "syncdb_import_*")
					if err != nil {
						return fmt.Errorf("failed to create temporary directory: %v", err)
					}
					defer os.RemoveAll(importDir) // Clean up temp directory when done

					// Unzip the file
					if err := unzipFile(zipFile, importDir); err != nil {
						return err
					}
				} else {
					// Get latest timestamp directory
					importDir, err = getLatestTimestampDir(folderPath, dbName)
					if err != nil {
						return err
					}
				}

				// Initialize database connection
				database, err := db.InitDB(dbDriver, host, port, username, password, dbName)
				if err != nil {
					return fmt.Errorf("failed to connect to database: %v", err)
				}
				defer database.Close()

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
					schemaPath := filepath.Join(importDir, "0_schema.sql")
					schemaData, err := os.ReadFile(schemaPath)
					if err != nil {
						return fmt.Errorf("failed to read schema file: %v", err)
					}

					// Split schema into individual statements
					schemaStatements := strings.Split(string(schemaData), ";")
					for _, stmt := range schemaStatements {
						stmt = strings.TrimSpace(stmt)
						if stmt == "" {
							continue
						}

						// Skip schema for excluded tables
						tableName := extractTableNameFromSchema(stmt)
						if tableName != "" && excludeSchemaMap[tableName] {
							continue
						}

						if _, err := database.Exec(stmt); err != nil {
							return fmt.Errorf("failed to execute schema statement: %v", err)
						}
					}
				}

				// First read metadata file
				metadataPath := filepath.Join(importDir, "0_metadata.json")
				metadataData, err := os.ReadFile(metadataPath)
				if err != nil {
					return fmt.Errorf("failed to read metadata file: %v", err)
				}

				var metadata struct {
					ExportedAt   time.Time `json:"exported_at"`
					DatabaseName string    `json:"database_name"`
					Tables       []string  `json:"tables"`
					Schema      bool      `json:"include_schema"`
					ViewData    bool      `json:"include_view_data"`
				}

				if err := json.Unmarshal(metadataData, &metadata); err != nil {
					return fmt.Errorf("failed to parse metadata file: %v", err)
				}

				// Import table data
				for i, table := range metadata.Tables {
					// Skip tables that are completely excluded
					if excludeTableMap[table] {
						continue
					}

					// Skip tables that are excluded from data import
					if excludeDataMap[table] {
						continue
					}

					tableFile := filepath.Join(importDir, fmt.Sprintf("%d_%s.sql", i+1, table))
					tableData, err := os.ReadFile(tableFile)
					if err != nil {
						return fmt.Errorf("failed to read table file %s: %v", table, err)
					}

					// Split into individual statements
					statements := strings.Split(string(tableData), "\n")
					for _, stmt := range statements {
						stmt = strings.TrimSpace(stmt)
						if stmt == "" || !strings.HasPrefix(strings.ToUpper(stmt), "INSERT") {
							continue
						}

						if _, err := database.Exec(stmt); err != nil {
							return fmt.Errorf("failed to execute statement for table %s: %v", table, err)
						}
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
				sqlStatements := strings.Split(string(fileData), "\n")
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

			// Filter tables if specified
			importTables := importData.Metadata.Tables
			if len(tables) > 0 {
				importTables = tables
			}

			// Import data for each table
			for _, table := range importTables {
				// Get current row count
				currentCount, err := db.GetTableRowCount(database, table)
				if err != nil {
					return fmt.Errorf("failed to get row count for table %s: %v", table, err)
				}
				fmt.Printf("Table %s: %d rows before import\n", table, currentCount)

				// Truncate if requested
				if truncate {
					if err := db.TruncateTable(database, table); err != nil {
						return fmt.Errorf("failed to truncate table %s: %v", table, err)
					}
				}

				// Import data
				data, ok := importData.Data[table]
				if !ok {
					return fmt.Errorf("table %s not found in import file", table)
				}

				if err := db.ImportTableData(database, table, data, upsert, dbDriver); err != nil {
					return fmt.Errorf("failed to import data to table %s: %v", table, err)
				}

				// Get final row count
				finalCount, err := db.GetTableRowCount(database, table)
				if err != nil {
					return fmt.Errorf("failed to get row count for table %s: %v", table, err)
				}
				fmt.Printf("Table %s: %d rows after import\n", table, finalCount)
			}

			return nil
		},
	}

	// Database connection flags
	cmd.Flags().String("host", "localhost", "Database host")
	cmd.Flags().Int("port", 3306, "Database port")
	cmd.Flags().String("username", "", "Database username")
	cmd.Flags().String("password", "", "Database password")
	cmd.Flags().String("database", "", "Database name")
	cmd.Flags().String("driver", "mysql", "Database driver (mysql, postgres)")
	cmd.Flags().StringSlice("tables", []string{}, "Tables to import")
	cmd.Flags().String("file-path", "", "File path to import from")
	cmd.Flags().String("folder-path", "", "Folder path to import from")
	cmd.Flags().String("format", "json", "File format (json, sql)")
	cmd.Flags().Bool("truncate", false, "Truncate tables before import")
	cmd.Flags().Bool("upsert", false, "Use upsert instead of insert")
	cmd.Flags().Bool("include-schema", false, "Include schema in import")
	cmd.Flags().StringSlice("exclude-table", []string{}, "Tables to exclude")
	cmd.Flags().StringSlice("exclude-table-schema", []string{}, "Tables to exclude schema")
	cmd.Flags().StringSlice("exclude-table-data", []string{}, "Tables to exclude data")
	cmd.Flags().Bool("zip", false, "Import from latest zip file")

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

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
	"strings"
	"time"

	"github.com/hoangnguyenba/syncdb/pkg/config"
	"github.com/hoangnguyenba/syncdb/pkg/db"
	"github.com/hoangnguyenba/syncdb/pkg/storage"
	"github.com/spf13/cobra"
)

type ExportData struct {
	Metadata struct {
		ExportedAt   time.Time `json:"exported_at"`
		DatabaseName string    `json:"database_name"`
		Tables       []string  `json:"tables"`
		Schema       bool      `json:"include_schema"`
		ViewData     bool      `json:"include_view_data"`
		IncludeData  bool      `json:"include_data"`
		Base64       bool      `json:"base64"`
	} `json:"metadata"`
	Schema map[string]string                   `json:"schema,omitempty"`
	Data   map[string][]map[string]interface{} `json:"data"`
}

func newExportCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export database data",
		Long:  `Export database data to a file.`,
		RunE: func(cmd *cobra.Command, cmdLineArgs []string) error { // Renamed original 'args' to 'cmdLineArgs'
			// Load config from environment
			cfg, err := config.LoadConfig()
			if err != nil {
				return fmt.Errorf("failed to load config: %v", err)
			}

			// Populate arguments from flags and config
			cmdArgs := populateCommonArgsFromFlagsAndConfig(cmd, cfg.Export.CommonConfig) // Use 'cmdArgs'

			// Get export-specific flags/config
			batchSize := getIntFlagWithConfigFallback(cmd, "batch-size", cfg.Export.BatchSize)

			// Validate required values
			if cmdArgs.Database == "" { // Use cmdArgs
				return fmt.Errorf("database name is required (set via --database flag or SYNCDB_EXPORT_DATABASE env)")
			}

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
					Driver:   cmdArgs.Driver, // Use cmdArgs
					Host:     cmdArgs.Host, // Use cmdArgs
					Port:     cmdArgs.Port, // Use cmdArgs
					User:     cmdArgs.Username, // Use cmdArgs
					Password: cmdArgs.Password, // Use cmdArgs
					Database: cmdArgs.Database, // Use cmdArgs
				},
			}

			// Use tables from cmdArgs
			currentTables := cmdArgs.Tables // Use cmdArgs
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

			// Use exclusion lists from cmdArgs
			excludeTables := cmdArgs.ExcludeTable // Use cmdArgs
			excludeTableSchema := cmdArgs.ExcludeTableSchema // Use cmdArgs
			excludeTableData := cmdArgs.ExcludeTableData // Use cmdArgs

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

			// Filter out fully excluded tables
			var filteredTables []string
			for _, t := range sortedTables { // Use sortedTables here
				if !excludeTableMap[t] {
					filteredTables = append(filteredTables, t)
				}
			}
			finalTables := filteredTables // Rename for clarity

			// Create timestamp for folder
			timestamp := time.Now().Format("20060102_150405")
			exportPath := filepath.Join(cmdArgs.FolderPath, timestamp) // Use cmdArgs.FolderPath

			// Use zip flag from cmdArgs
			createZip := cmdArgs.Zip // Use cmdArgs

			// Create directory structure
			if err = os.MkdirAll(exportPath, 0755); err != nil {
				return fmt.Errorf("failed to create directory structure: %v", err)
			}

			// Create export data structure
			exportData := ExportData{
				Metadata: struct {
					ExportedAt   time.Time `json:"exported_at"`
					DatabaseName string    `json:"database_name"`
					Tables       []string  `json:"tables"`
					Schema       bool      `json:"include_schema"`
					ViewData     bool      `json:"include_view_data"`
					IncludeData  bool      `json:"include_data"`
					Base64       bool      `json:"base64"`
				}{
					ExportedAt:   time.Now(),
					DatabaseName: cmdArgs.Database, // Use cmdArgs.Database
					Tables:       finalTables,   // Use finalTables
					Schema:       cmdArgs.IncludeSchema, // Use cmdArgs
					ViewData:     cmdArgs.IncludeViewData, // Use cmdArgs
					IncludeData:  cmdArgs.IncludeData, // Use cmdArgs
					Base64:       cmdArgs.Base64, // Use cmdArgs.Base64
				},
				Schema: make(map[string]string),
				Data:   make(map[string][]map[string]interface{}),
			}

			fmt.Printf("Starting export of %d tables from database '%s'\n", len(finalTables), cmdArgs.Database) // Use cmdArgs

			// Write metadata to a separate file (with 0_ prefix)
			metadataData, err := json.MarshalIndent(exportData.Metadata, "", "  ")
			if err != nil {
				return fmt.Errorf("failed to marshal metadata: %v", err)
			}
			metadataFile := filepath.Join(exportPath, "0_metadata.json")
			if err = os.WriteFile(metadataFile, metadataData, 0644); err != nil {
				return fmt.Errorf("failed to write metadata file: %v", err)
			}

			// Export schema if requested
			if cmdArgs.IncludeSchema { // Use cmdArgs
				for _, table := range finalTables {
					if excludeSchemaMap[table] {
						continue
					}

					schema, err := db.GetTableSchema(conn, table)
					if err != nil {
						return fmt.Errorf("failed to get schema for table %s: %v", table, err)
					}
					exportData.Schema[table] = schema.Definition
				}

				// Write schema based on format (with 0_ prefix)
				var schemaData []byte
				var schemaFile string
				if cmdArgs.Format == "sql" { // Use cmdArgs
					// Convert schema map to slice
					var schemaOutput []string
					for table, definition := range exportData.Schema {
						schemaOutput = append(schemaOutput, fmt.Sprintf("-- Table structure for %s\n%s\n", table, definition))
					}
					schemaData = []byte(strings.Join(schemaOutput, "\n"))
					schemaFile = filepath.Join(exportPath, "0_schema.sql")
				} else {
					schemaData, err = json.MarshalIndent(exportData.Schema, "", "  ")
					if err != nil {
						return fmt.Errorf("failed to marshal schema: %v", err)
					}
					schemaFile = filepath.Join(exportPath, "0_schema.json")
				}

				if err = os.WriteFile(schemaFile, schemaData, 0644); err != nil {
					return fmt.Errorf("failed to write schema file: %v", err)
				}
			}

			// Export data if requested
			if cmdArgs.IncludeData { // Use cmdArgs
				for _, table := range finalTables {
					if excludeDataMap[table] {
						continue
					}

					// Check if it's a view
					isView, err := db.IsView(conn, table)
					if err != nil {
						return fmt.Errorf("failed to check if %s is a view: %v", table, err)
					}

					if isView && !cmdArgs.IncludeViewData { // Use cmdArgs
						continue
					}

					fmt.Printf("Exporting table '%s'...", table)

					// Create a buffer to store the data
					var buf bytes.Buffer
					if err := db.ExportTableData(conn, table, &buf); err != nil {
						return fmt.Errorf("failed to export data for table %s: %v", table, err)
					}

					// Decode the data from the buffer
					var operations []db.DataOperation
					decoder := json.NewDecoder(&buf)
					for {
						var op db.DataOperation
						if err := decoder.Decode(&op); err == io.EOF {
							break
						}
						if err != nil {
							return fmt.Errorf("failed to decode operation for table %s: %v", table, err)
						}
						operations = append(operations, op)
					}

					// Convert operations to data
					data := make([]map[string]interface{}, len(operations))
					for i, op := range operations {
						data[i] = op.Data
					}

					exportData.Data[table] = data
					fmt.Printf("done (%d records)\n", len(data))
				}
			}

			// Write data files for each table
			if cmdArgs.IncludeData { // Use cmdArgs
				for i, table := range finalTables {
					if excludeDataMap[table] {
						continue
					}

					data, ok := exportData.Data[table]
					if !ok {
						continue
					}

					// Convert data to SQL format
					var sqlStatements []string

					// Get columns from database schema to ensure consistency
					tableSchema, err := db.GetTableSchema(conn, table)
					if err != nil {
						return fmt.Errorf("failed to get schema for table %s: %v", table, err)
					}

					allColumns := tableSchema.Columns

					// Add backticks to column names
					backtickedColumns := make([]string, len(allColumns))
					for i, col := range allColumns {
						backtickedColumns[i] = fmt.Sprintf("`%s`", col)
					}

					// Process in batches of 100 rows for bulk insert
					for i := 0; i < len(data); i += batchSize {
						end := i + batchSize
						if end > len(data) {
							end = len(data)
						}

						batch := data[i:end]
						if len(batch) == 0 {
							continue
						}

						// Start the INSERT statement
						insertStmt := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES\n",
							table,
							strings.Join(backtickedColumns, ", "))

						valueStrings := make([]string, 0, len(batch))

						// Generate value sets for each row
						for _, row := range batch {
							values := make([]string, len(allColumns))
							for j, col := range allColumns {
								val, exists := row[col]
								if !exists || val == nil {
									values[j] = "NULL"
								} else {
									switch v := val.(type) {
									case string:
										if cmdArgs.Base64 { // Use cmdArgs.Base64
											// Encode string values to base64 when the flag is enabled
											encodedValue := base64.StdEncoding.EncodeToString([]byte(v))
											values[j] = fmt.Sprintf("'%s'", encodedValue)
										} else {
											// Escape single quotes for SQL
											escapedString := strings.ReplaceAll(v, "'", "''")
											// Further escape backslashes if needed, depending on SQL dialect/driver behavior
											// escapedString = strings.ReplaceAll(escapedString, "\\", "\\\\")
											values[j] = fmt.Sprintf("'%s'", escapedString)
										}
									case time.Time:
										values[j] = fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05"))
									default:
										values[j] = fmt.Sprintf("%v", v)
									}
								}
							}
							valueStrings = append(valueStrings, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
						}

						// Complete the statement
						insertStmt += strings.Join(valueStrings, ",\n") + ";"
						sqlStatements = append(sqlStatements, insertStmt)
					}

					// Write data to file
					dataFile := filepath.Join(exportPath, fmt.Sprintf("%d_%s.sql", i+1, table))
					if err := os.WriteFile(dataFile, []byte(strings.Join(sqlStatements, "\n\n")), 0644); err != nil {
						return fmt.Errorf("failed to write data file for table %s: %v", table, err)
					}
					fmt.Printf("Wrote %d records to %s\n", len(data), dataFile)
				}
			}

			// Calculate total records
			totalRecords := 0
			for _, data := range exportData.Data {
				totalRecords += len(data)
			}
			fmt.Printf("Exported %d tables with a total of %d records\n", len(finalTables), totalRecords)

			// If zip flag is enabled, create a zip file
			if createZip {
				zipFileName := filepath.Join(cmdArgs.FolderPath, timestamp+".zip") // Use cmdArgs.FolderPath
				zipFile, err := os.Create(zipFileName)
				if err != nil {
					return fmt.Errorf("failed to create zip file: %v", err)
				}
				defer zipFile.Close()

				zipWriter := zip.NewWriter(zipFile)
				defer zipWriter.Close()

				// Walk through the export directory and add files to zip
				err = filepath.Walk(exportPath, func(path string, info os.FileInfo, err error) error {
					if err != nil {
						return err
					}

					// Skip directories
					if info.IsDir() {
						return nil
					}

					// Create a new file header
					header, err := zip.FileInfoHeader(info)
					if err != nil {
						return fmt.Errorf("failed to create zip header for %s: %v", path, err)
					}

					// Preserve directory structure by including the timestamp directory
					relPath, err := filepath.Rel(filepath.Dir(exportPath), path)
					if err != nil {
						return fmt.Errorf("failed to get relative path for %s: %v", path, err)
					}
					header.Name = relPath

					// Create writer for this file within zip
					writer, err := zipWriter.CreateHeader(header)
					if err != nil {
						return fmt.Errorf("failed to create zip entry for %s: %v", path, err)
					}

					// Open and copy the file content
					file, err := os.Open(path)
					if err != nil {
						return fmt.Errorf("failed to open file %s: %v", path, err)
					}
					defer file.Close()

					_, err = io.Copy(writer, file)
					if err != nil {
						return fmt.Errorf("failed to write file %s to zip: %v", path, err)
					}

					return nil
				})

				if err != nil {
					return fmt.Errorf("failed to create zip archive: %v", err)
				}

				// Close the zip writer before uploading to S3
				zipWriter.Close()

				// If storage is s3, upload to S3
				if cmdArgs.Storage == "s3" { // Use cmdArgs
					if cmdArgs.S3Bucket == "" { // Use cmdArgs
						return fmt.Errorf("s3-bucket is required when storage is set to s3")
					}
					if cmdArgs.S3Region == "" { // Use cmdArgs
						return fmt.Errorf("s3-region is required when storage is set to s3")
					}

					// Initialize S3 storage
					s3Store := storage.NewS3Storage(cmdArgs.S3Bucket, cmdArgs.S3Region) // Use cmdArgs
					if s3Store == nil {
						return fmt.Errorf("failed to initialize S3 storage. Please ensure AWS credentials are set in environment")
					}

					// Upload zip file to S3
					zipData, err := os.ReadFile(zipFileName)
					if err != nil {
						return fmt.Errorf("failed to read zip file for S3 upload: %v", err)
					}

					s3Key := filepath.Join(cmdArgs.FolderPath, filepath.Base(zipFileName)) // Use cmdArgs.FolderPath
					if err := s3Store.Upload(zipData, s3Key); err != nil {
						return fmt.Errorf("failed to upload zip file to S3: %v", err)
					}

					fmt.Printf("Uploaded %s to s3://%s/%s\n", zipFileName, cmdArgs.S3Bucket, s3Key) // Use cmdArgs.S3Bucket

					// Clean up the exported directory after successful zip creation
					// Clean up the local exported directory after successful zip creation and S3 upload
					if err := os.RemoveAll(exportPath); err != nil {
						fmt.Printf("Warning: failed to clean up export directory %s: %v\n", exportPath, err)
					}
					// Clean up the local zip file
					if err := os.Remove(zipFileName); err != nil {
						fmt.Printf("Warning: failed to clean up zip file %s: %v\n", zipFileName, err)
					}
				} else { // Local storage with zip
					fmt.Printf("Created zip archive: %s\n", zipFileName)
					// Clean up the exported directory after successful zip creation
					if err := os.RemoveAll(exportPath); err != nil {
						fmt.Printf("Warning: failed to clean up export directory %s: %v\n", exportPath, err)
					}
				}
			} else if cmdArgs.Storage == "s3" { // Not zipping, but using S3 // Use cmdArgs
				// If not zipping but using S3 storage, upload individual files
				if cmdArgs.S3Bucket == "" { // Use cmdArgs
					return fmt.Errorf("s3-bucket is required when storage is set to s3")
				}
				if cmdArgs.S3Region == "" { // Use cmdArgs
					return fmt.Errorf("s3-region is required when storage is set to s3")
				}

				// Initialize S3 storage
				s3Store := storage.NewS3Storage(cmdArgs.S3Bucket, cmdArgs.S3Region) // Use cmdArgs
				if s3Store == nil {
					return fmt.Errorf("failed to initialize S3 storage. Please ensure AWS credentials are set in environment")
				}

				// Upload individual files to S3
				err = filepath.Walk(exportPath, func(path string, info os.FileInfo, err error) error {
					if err != nil {
						return err
					}

					// Skip directories
					if info.IsDir() {
						return nil
					}

					// Open file
					file, err := os.Open(path)
					if err != nil {
						return fmt.Errorf("failed to open file %s: %v", path, err)
					}
					defer file.Close()

					// Create S3 key (path relative to exportPath base, include folder path and timestamp)
					relPath, err := filepath.Rel(filepath.Dir(exportPath), path) // Relative to parent of timestamp dir
					if err != nil {
						return fmt.Errorf("failed to get relative path for %s: %v", path, err)
					}
					// Ensure S3 key structure includes the base folder path if provided
					s3KeyBase := cmdArgs.FolderPath // Use cmdArgs
					s3Key := filepath.Join(s3KeyBase, relPath) // Joins folderPath + timestamp/file.sql

					// Upload to S3
					fileData, err := io.ReadAll(file)
					if err != nil {
						return fmt.Errorf("failed to read file %s: %v", path, err)
					}

					if err := s3Store.Upload(fileData, s3Key); err != nil {
						return fmt.Errorf("failed to upload file %s to S3 key %s: %v", path, s3Key, err)
					}

					fmt.Printf("Uploaded %s to s3://%s/%s\n", path, cmdArgs.S3Bucket, s3Key) // Use cmdArgs.S3Bucket
					return nil
				})

				if err != nil {
					return fmt.Errorf("failed to upload files to S3: %v", err)
				}

				fmt.Printf("Successfully uploaded all files to S3 bucket: %s\n", cmdArgs.S3Bucket) // Use cmdArgs.S3Bucket

				// Clean up the local exported directory after successful S3 upload
				if err := os.RemoveAll(exportPath); err != nil {
					fmt.Printf("Warning: failed to clean up export directory %s: %v\n", exportPath, err)
				}
			} else { // Local storage, no zip
				fmt.Printf("Successfully exported %d tables to %s\n", len(finalTables), exportPath)
			}

			return nil
		},
	}

	// Add shared flags
	AddSharedFlags(cmd, false) // Pass false for export command

	// Add export-specific flags
	flags := cmd.Flags()
	flags.Int("batch-size", 500, "Number of records to process in a batch")

	return cmd
}

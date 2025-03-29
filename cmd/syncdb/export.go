package main

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/hoangnguyenba/syncdb/pkg/config"
	"github.com/hoangnguyenba/syncdb/pkg/db"
	"github.com/spf13/cobra"
)

type ExportData struct {
	Metadata struct {
		ExportedAt   time.Time `json:"exported_at"`
		DatabaseName string    `json:"database_name"`
		Tables       []string  `json:"tables"`
		Schema       bool      `json:"include_schema"`
		ViewData     bool      `json:"include_view_data"`
	} `json:"metadata"`
	Schema map[string]string                   `json:"schema,omitempty"`
	Data   map[string][]map[string]interface{} `json:"data"`
}

func newExportCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export database data",
		Long:  `Export database data to a file.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Load config from environment
			cfg, err := config.LoadConfig()
			if err != nil {
				return fmt.Errorf("failed to load config: %v", err)
			}

			// Get flags, use config as defaults if flags are not explicitly set
			var host string
			if cmd.Flags().Changed("host") {
				host, _ = cmd.Flags().GetString("host")
			} else {
				host = cfg.Export.Host
			}

			var port int
			if cmd.Flags().Changed("port") {
				port, _ = cmd.Flags().GetInt("port")
			} else {
				port = cfg.Export.Port
			}

			var username string
			if cmd.Flags().Changed("username") {
				username, _ = cmd.Flags().GetString("username")
			} else {
				username = cfg.Export.Username
			}

			var password string
			if cmd.Flags().Changed("password") {
				password, _ = cmd.Flags().GetString("password")
			} else {
				password = cfg.Export.Password
			}

			var dbName string
			if cmd.Flags().Changed("database") {
				dbName, _ = cmd.Flags().GetString("database")
			} else {
				dbName = cfg.Export.Database
			}

			var dbDriver string
			if cmd.Flags().Changed("driver") {
				dbDriver, _ = cmd.Flags().GetString("driver")
			} else {
				dbDriver = cfg.Export.Driver
			}

			var tables []string
			if cmd.Flags().Changed("tables") {
				tables, _ = cmd.Flags().GetStringSlice("tables")
			} else {
				tables = cfg.Export.Tables
			}

			var format string
			if cmd.Flags().Changed("format") {
				format, _ = cmd.Flags().GetString("format")
			} else {
				format = cfg.Export.Format
			}

			// Get folder path, default to database name if not provided
			var folderPath string
			if cmd.Flags().Changed("folder-path") {
				folderPath, _ = cmd.Flags().GetString("folder-path")
			} else {
				folderPath = ""
			}

			// Get storage options
			var storage string
			if cmd.Flags().Changed("storage") {
				storage, _ = cmd.Flags().GetString("storage")
			} else {
				storage = cfg.Export.Storage
			}

			var s3Bucket string
			if cmd.Flags().Changed("s3-bucket") {
				s3Bucket, _ = cmd.Flags().GetString("s3-bucket")
			} else {
				s3Bucket = cfg.Export.S3Bucket
			}

			var s3Region string
			if cmd.Flags().Changed("s3-region") {
				s3Region, _ = cmd.Flags().GetString("s3-region")
			} else {
				s3Region = cfg.Export.S3Region
			}

			// Validate required values
			if dbName == "" {
				return fmt.Errorf("database name is required (set via --database flag or SYNCDB_EXPORT_DATABASE env)")
			}

			// Initialize database connection
			database, err := db.InitDB(dbDriver, host, port, username, password, dbName)
			if err != nil {
				return fmt.Errorf("failed to connect to database: %v", err)
			}
			defer database.Close()

			// Get tables if not specified
			if len(tables) == 0 {
				tables, err = db.GetTables(database, dbName, dbDriver)
				if err != nil {
					return fmt.Errorf("failed to get tables: %v", err)
				}
			}

			// Handle table exclusions
			var excludeTables []string
			var excludeTableSchema []string
			var excludeTableData []string

			if cmd.Flags().Changed("exclude-table") {
				excludeTables, _ = cmd.Flags().GetStringSlice("exclude-table")
			} else {
				excludeTables = cfg.Export.ExcludeTable
			}

			if cmd.Flags().Changed("exclude-table-schema") {
				excludeTableSchema, _ = cmd.Flags().GetStringSlice("exclude-table-schema")
			} else {
				excludeTableSchema = cfg.Export.ExcludeTableSchema
			}

			if cmd.Flags().Changed("exclude-table-data") {
				excludeTableData, _ = cmd.Flags().GetStringSlice("exclude-table-data")
			} else {
				excludeTableData = cfg.Export.ExcludeTableData
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

			// Filter out fully excluded tables
			var filteredTables []string
			for _, t := range tables {
				if !excludeTableMap[t] {
					filteredTables = append(filteredTables, t)
				}
			}
			tables = filteredTables

			// Create timestamp for folder
			timestamp := time.Now().Format("20060102_150405")
			exportPath := filepath.Join(folderPath, timestamp)

			// Get zip flag
			createZip, _ := cmd.Flags().GetBool("zip")

			// Create directory structure
			if err = os.MkdirAll(exportPath, 0755); err != nil {
				return fmt.Errorf("failed to create directory structure: %v", err)
			}

			// Initialize export data structure for metadata
			exportData := ExportData{
				Data: make(map[string][]map[string]interface{}),
			}
			exportData.Metadata.ExportedAt = time.Now()
			exportData.Metadata.DatabaseName = dbName
			exportData.Metadata.Tables = tables

			// Write metadata to a separate file (with 0_ prefix)
			metadataData, err := json.MarshalIndent(exportData.Metadata, "", "  ")
			if err != nil {
				return fmt.Errorf("failed to marshal metadata: %v", err)
			}
			metadataFile := filepath.Join(exportPath, "0_metadata.json")
			if err = os.WriteFile(metadataFile, metadataData, 0644); err != nil {
				return fmt.Errorf("failed to write metadata file: %v", err)
			}

			// Get schema if requested
			includeSchema, _ := cmd.Flags().GetBool("include-schema")
			if includeSchema {
				exportData.Schema = make(map[string]string)
				var schemaOutput []string
				for _, table := range tables {
					// Skip schema for excluded tables
					if excludeSchemaMap[table] {
						continue
					}

					schema, err := db.GetTableSchema(database, table, dbDriver)
					if err != nil {
						return fmt.Errorf("failed to get schema for table %s: %v", table, err)
					}
					exportData.Schema[table] = schema
					if format == "sql" {
						schemaOutput = append(schemaOutput, fmt.Sprintf("-- Table structure for %s\n%s\n", table, schema))
					}
				}
				exportData.Metadata.Schema = true

				// Write schema based on format (with 0_ prefix)
				var schemaData []byte
				var schemaFile string
				if format == "sql" {
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

			// Get include-view-data flag
			includeViewData, _ := cmd.Flags().GetBool("include-view-data")

			// Export data for each table to separate files
			for i, table := range tables {
				// Check if it's a view
				isView, err := db.IsView(database, table, dbDriver)
				if err != nil {
					return fmt.Errorf("failed to check if %s is a view: %v", table, err)
				}

				// Skip data export for views unless include-view-data is true
				if isView && !includeViewData {
					continue
				}

				// Skip data for excluded tables
				if excludeDataMap[table] {
					continue
				}

				data, orderedColumns, err := db.ExportTableData(database, table, "", dbDriver)

				if err != nil {
					return fmt.Errorf("failed to export data from table %s: %v", table, err)
				}

				fmt.Printf("orderedColumns %v\n", orderedColumns)

				var outputData []byte
				switch format {
				case "json":
					outputData, err = json.MarshalIndent(data, "", "  ")
					if err != nil {
						return fmt.Errorf("failed to marshal data: %v", err)
					}
				case "sql":
					var sqlStatements []string
					// Skip if no data
					if len(data) == 0 {
						break
					}

					for _, row := range data {
						values := make([]string, 0, len(orderedColumns))
						for _, col := range orderedColumns {
							val := row[col] // Use the escaped column name directly since that's what we store in the map
							if val == nil {
								values = append(values, "NULL")
							} else {
								switch v := val.(type) {
								case string:
									values = append(values, fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''")))
								case time.Time:
									values = append(values, fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05")))
								case map[string]interface{}, []interface{}:
									jsonBytes, err := json.Marshal(v)
									if err != nil {
										return fmt.Errorf("failed to marshal JSON value: %v", err)
									}
									jsonStr := string(jsonBytes)
									values = append(values, fmt.Sprintf("'%s'", strings.ReplaceAll(jsonStr, "'", "''")))
								case float64:
									values = append(values, fmt.Sprintf("%f", v))
								case int64:
									values = append(values, fmt.Sprintf("%d", v))
								case bool:
									if v {
										values = append(values, "1")
									} else {
										values = append(values, "0")
									}
								default:
									values = append(values, fmt.Sprintf("%v", v))
								}
							}
						}

						stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
							table,
							strings.Join(orderedColumns, ", "),
							strings.Join(values, ", "))
						sqlStatements = append(sqlStatements, stmt)
					}
					outputData = []byte(strings.Join(sqlStatements, "\n") + "\n")
				default:
					return fmt.Errorf("unsupported format: %s (supported formats: json, sql)", format)
				}

				// Write table data to file with index prefix (starting from 1)
				tableFile := filepath.Join(exportPath, fmt.Sprintf("%d_%s.%s", i+1, table, format))
				if err = os.WriteFile(tableFile, outputData, 0644); err != nil {
					return fmt.Errorf("failed to write table file %s: %v", table, err)
				}
			}

			// If zip flag is enabled, create a zip file
			if createZip {
				zipFileName := filepath.Join(folderPath, timestamp+".zip")
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
					relPath, err := filepath.Rel(exportPath, path)
					if err != nil {
						return fmt.Errorf("failed to get relative path for %s: %v", path, err)
					}

					header, err := zip.FileInfoHeader(info)
					if err != nil {
						return fmt.Errorf("failed to create zip header for %s: %v", path, err)
					}

					// Set relative path in zip
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
				if storage == "s3" {
					if s3Bucket == "" {
						return fmt.Errorf("s3-bucket is required when storage is set to s3")
					}
					if s3Region == "" {
						return fmt.Errorf("s3-region is required when storage is set to s3")
					}

					// Create AWS session
					sess, err := session.NewSession(&aws.Config{
						Region: aws.String(s3Region),
					})
					if err != nil {
						return fmt.Errorf("failed to create AWS session: %v", err)
					}

					// Create S3 service client
					svc := s3.New(sess)

					// Upload zip file to S3
					file, err := os.Open(zipFileName)
					if err != nil {
						return fmt.Errorf("failed to open zip file for S3 upload: %v", err)
					}
					defer file.Close()

					s3Key := filepath.Join(folderPath, filepath.Base(zipFileName))
					_, err = svc.PutObject(&s3.PutObjectInput{
						Bucket: aws.String(s3Bucket),
						Key:    aws.String(s3Key),
						Body:   file,
					})
					if err != nil {
						return fmt.Errorf("failed to upload zip file to S3: %v", err)
					}

					fmt.Printf("Uploaded %s to s3://%s/%s\n", zipFileName, s3Bucket, s3Key)
				}

				// Clean up the exported directory after successful zip creation
				if err := os.RemoveAll(exportPath); err != nil {
					fmt.Printf("Warning: failed to clean up export directory: %v\n", err)
				}
			} else if storage == "s3" {
				// If not zipping but using S3 storage, upload individual files
				if s3Bucket == "" {
					return fmt.Errorf("s3-bucket is required when storage is set to s3")
				}
				if s3Region == "" {
					return fmt.Errorf("s3-region is required when storage is set to s3")
				}

				// Create AWS session
				sess, err := session.NewSession(&aws.Config{
					Region: aws.String(s3Region),
				})
				if err != nil {
					return fmt.Errorf("failed to create AWS session: %v", err)
				}

				// Create S3 service client
				svc := s3.New(sess)

				// Upload files to S3
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

					// Create S3 key (path relative to exportPath, include folder path)
					relPath, err := filepath.Rel(exportPath, path)
					if err != nil {
						return fmt.Errorf("failed to get relative path: %v", err)
					}
					s3Key := filepath.Join(folderPath, timestamp, relPath)

					// Upload to S3
					_, err = svc.PutObject(&s3.PutObjectInput{
						Bucket: aws.String(s3Bucket),
						Key:    aws.String(s3Key),
						Body:   file,
					})
					if err != nil {
						return fmt.Errorf("failed to upload file %s to S3: %v", path, err)
					}

					fmt.Printf("Uploaded %s to s3://%s/%s\n", path, s3Bucket, s3Key)
					return nil
				})

				if err != nil {
					return fmt.Errorf("failed to upload files to S3: %v", err)
				}

				fmt.Printf("Successfully uploaded all files to S3 bucket: %s\n", s3Bucket)

				// Clean up the exported directory after successful S3 upload
				if err := os.RemoveAll(exportPath); err != nil {
					fmt.Printf("Warning: failed to clean up export directory: %v\n", err)
				}
			}

			fmt.Printf("Successfully exported %d tables to %s\n", len(tables), exportPath)
			return nil
		},
	}

	// Add flags
	cmd.Flags().String("host", "", "Database host")
	cmd.Flags().Int("port", 0, "Database port")
	cmd.Flags().String("username", "", "Database username")
	cmd.Flags().String("password", "", "Database password")
	cmd.Flags().String("database", "", "Database name")
	cmd.Flags().String("driver", "", "Database driver (mysql or postgres)")
	cmd.Flags().StringSlice("tables", []string{}, "Tables to export (comma-separated)")
	cmd.Flags().String("format", "", "Export format (sql or json)")
	cmd.Flags().String("folder-path", "", "Folder path for export")
	cmd.Flags().Bool("include-schema", false, "Include schema in export")
	cmd.Flags().Bool("include-view-data", false, "Include view data in export")
	cmd.Flags().Bool("zip", false, "Create zip archive")
	cmd.Flags().StringSlice("exclude-table", []string{}, "Tables to exclude from export (comma-separated)")
	cmd.Flags().StringSlice("exclude-table-schema", []string{}, "Tables to exclude schema from export (comma-separated)")
	cmd.Flags().StringSlice("exclude-table-data", []string{}, "Tables to exclude data from export (comma-separated)")
	cmd.Flags().String("storage", "", "Storage type (local or s3)")
	cmd.Flags().String("s3-bucket", "", "S3 bucket name")
	cmd.Flags().String("s3-region", "", "S3 region")

	return cmd
}

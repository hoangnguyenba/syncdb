package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

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
				folderPath = dbName
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

			// Create timestamp for folder
			timestamp := time.Now().Format("20060102_150405")
			exportPath := filepath.Join(folderPath, dbName, timestamp)

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

			// Export data for each table to separate files
			for i, table := range tables {
				data, err := db.ExportTableData(database, table, "")
				if err != nil {
					return fmt.Errorf("failed to export data from table %s: %v", table, err)
				}

				var outputData []byte
				switch format {
				case "json":
					outputData, err = json.MarshalIndent(data, "", "  ")
					if err != nil {
						return fmt.Errorf("failed to marshal data: %v", err)
					}
				case "sql":
					var sqlStatements []string
					// Get ordered column names
					var orderedColumns []string
					if len(data) > 0 {
						for col := range data[0] {
							orderedColumns = append(orderedColumns, col)
						}
						sort.Strings(orderedColumns)
					}

					for _, row := range data {
						values := make([]string, 0, len(orderedColumns))
						for _, col := range orderedColumns {
							val := row[col]
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

			fmt.Printf("Successfully exported %d tables to %s\n", len(tables), exportPath)
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
	cmd.Flags().StringSlice("tables", []string{}, "Tables to export (default: all)")
	cmd.Flags().String("format", "sql", "Output format (json, sql)")
	cmd.Flags().Bool("include-schema", false, "Include database schema in export")
	cmd.Flags().String("folder-path", "", "Base folder path for export (default: database name)")

	return cmd
}

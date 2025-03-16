package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

			var filePath string
			if cmd.Flags().Changed("file-path") {
				filePath, _ = cmd.Flags().GetString("file-path")
			} else {
				filePath = cfg.Export.Filepath
			}

			var format string
			if cmd.Flags().Changed("format") {
				format, _ = cmd.Flags().GetString("format")
			} else {
				format = cfg.Export.Format
			}

			// Validate required values
			if dbName == "" {
				return fmt.Errorf("database name is required (set via --database flag or SYNCDB_EXPORT_DATABASE env)")
			}

			if filePath == "" {
				return fmt.Errorf("file path is required (set via --file-path flag or SYNCDB_EXPORT_FILEPATH env)")
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

			// Initialize export data structure
			exportData := ExportData{
				Data: make(map[string][]map[string]interface{}),
			}
			exportData.Metadata.ExportedAt = time.Now()
			exportData.Metadata.DatabaseName = dbName
			exportData.Metadata.Tables = tables

			// Export data for each table
			for _, table := range tables {
				data, err := db.ExportTableData(database, table, "")
				if err != nil {
					return fmt.Errorf("failed to export data from table %s: %v", table, err)
				}
				exportData.Data[table] = data
			}

			var outputData []byte

			switch format {
			case "json":
				outputData, err = json.MarshalIndent(exportData, "", "  ")
				if err != nil {
					return fmt.Errorf("failed to marshal export data to JSON: %v", err)
				}
			case "sql":
				// Generate SQL statements
				var sqlStatements []string
				for table, rows := range exportData.Data {
					for _, row := range rows {
						columns := make([]string, 0)
						values := make([]string, 0)
						for col, val := range row {
							columns = append(columns, col)
							if val == nil {
								values = append(values, "NULL")
							} else {
								switch v := val.(type) {
								case string:
									values = append(values, fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''")))
								case time.Time:
									values = append(values, fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05")))
								default:
									values = append(values, fmt.Sprintf("%v", v))
								}
							}
						}
						stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
							table,
							strings.Join(columns, ", "),
							strings.Join(values, ", "))
						sqlStatements = append(sqlStatements, stmt)
					}
				}
				outputData = []byte(strings.Join(sqlStatements, "\n") + "\n")
			default:
				return fmt.Errorf("unsupported format: %s (supported formats: json, sql)", format)
			}

			// Create directory if it doesn't exist
			dir := filepath.Dir(filePath)
			if err = os.MkdirAll(dir, 0755); err != nil {
				return fmt.Errorf("failed to create directory: %v", err)
			}

			// Write to file
			if err = os.WriteFile(filePath, outputData, 0644); err != nil {
				return fmt.Errorf("failed to write file: %v", err)
			}

			fmt.Printf("Successfully exported %d tables to %s in %s format\n", len(tables), filePath, format)
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
	cmd.Flags().String("file-path", "", "Output file path")
	cmd.Flags().String("format", "json", "Output format (json, sql)")

	return cmd
}

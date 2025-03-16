package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

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
		Long:  `Export database data to a file using various storage options (local, S3, or Google Drive).`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Get all flags
			host, _ := cmd.Flags().GetString("host")
			port, _ := cmd.Flags().GetInt("port")
			username, _ := cmd.Flags().GetString("username")
			password, _ := cmd.Flags().GetString("password")
			dbName, _ := cmd.Flags().GetString("database")
			dbDriver, _ := cmd.Flags().GetString("driver")
			tables, _ := cmd.Flags().GetStringSlice("tables")
			includeSchema, _ := cmd.Flags().GetBool("include-schema")
			condition, _ := cmd.Flags().GetString("condition")
			filePath, _ := cmd.Flags().GetString("file-path")

			// Validate required flags
			if dbName == "" {
				return fmt.Errorf("database name is required")
			}

			if filePath == "" {
				return fmt.Errorf("file path is required")
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
			exportData.Metadata.Schema = includeSchema

			// Export schema if requested
			if includeSchema {
				exportData.Schema = make(map[string]string)
				for _, table := range tables {
					schema, err := db.GetTableSchema(database, table, dbDriver)
					if err != nil {
						return fmt.Errorf("failed to get schema for table %s: %v", table, err)
					}
					exportData.Schema[table] = schema
				}
			}

			// Export data for each table
			for _, table := range tables {
				data, err := db.ExportTableData(database, table, condition)
				if err != nil {
					return fmt.Errorf("failed to export data from table %s: %v", table, err)
				}
				exportData.Data[table] = data
			}

			// Convert to JSON
			jsonData, err := json.MarshalIndent(exportData, "", "  ")
			if err != nil {
				return fmt.Errorf("failed to marshal export data: %v", err)
			}

			// Create directory if it doesn't exist
			dir := filepath.Dir(filePath)
			if err := os.MkdirAll(dir, 0755); err != nil {
				return fmt.Errorf("failed to create directory: %v", err)
			}

			// Write to file
			if err := os.WriteFile(filePath, jsonData, 0644); err != nil {
				return fmt.Errorf("failed to write file: %v", err)
			}

			fmt.Printf("Successfully exported %d tables to %s\n", len(tables), filePath)
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

	// Export settings flags
	cmd.Flags().Bool("include-schema", false, "Include schema in export")
	cmd.Flags().String("condition", "", "WHERE condition for export")
	cmd.Flags().String("file-path", "", "Output file path")

	// Mark required flags
	cmd.MarkFlagRequired("database")
	cmd.MarkFlagRequired("file-path")

	return cmd
}

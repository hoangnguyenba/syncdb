package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/hoangnguyenba/syncdb/pkg/db"
	"github.com/spf13/cobra"
)

func newImportCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "import",
		Short: "Import database data",
		Long:  `Import database data from a file.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Get all flags
			host, _ := cmd.Flags().GetString("host")
			port, _ := cmd.Flags().GetInt("port")
			username, _ := cmd.Flags().GetString("username")
			password, _ := cmd.Flags().GetString("password")
			dbName, _ := cmd.Flags().GetString("database")
			dbDriver, _ := cmd.Flags().GetString("driver")
			tables, _ := cmd.Flags().GetStringSlice("tables")
			upsert, _ := cmd.Flags().GetBool("upsert")
			filePath, _ := cmd.Flags().GetString("file-path")
			truncate, _ := cmd.Flags().GetBool("truncate")

			// Validate required flags
			if dbName == "" {
				return fmt.Errorf("database name is required")
			}

			if filePath == "" {
				return fmt.Errorf("file path is required")
			}

			// Read the export file
			jsonData, err := os.ReadFile(filePath)
			if err != nil {
				return fmt.Errorf("failed to read file: %v", err)
			}

			// Parse the JSON data
			var exportData ExportData
			if err := json.Unmarshal(jsonData, &exportData); err != nil {
				return fmt.Errorf("failed to parse JSON: %v", err)
			}

			// Initialize database connection
			database, err := db.InitDB(dbDriver, host, port, username, password, dbName)
			if err != nil {
				return fmt.Errorf("failed to connect to database: %v", err)
			}
			defer database.Close()

			// Filter tables if specified
			importTables := exportData.Metadata.Tables
			if len(tables) > 0 {
				// Create a map for quick lookup
				tableMap := make(map[string]bool)
				for _, t := range tables {
					tableMap[t] = true
				}

				// Filter tables
				var filtered []string
				for _, t := range importTables {
					if tableMap[t] {
						filtered = append(filtered, t)
					}
				}
				importTables = filtered
			}

			// Import schema if included
			if exportData.Schema != nil {
				fmt.Println("Importing schema is not implemented yet")
			}

			// Import data for each table
			for _, table := range importTables {
				// Get initial row count
				initialCount, err := db.GetTableRowCount(database, table)
				if err != nil {
					return fmt.Errorf("failed to get initial row count for table %s: %v", table, err)
				}

				// Truncate table if requested
				if truncate {
					if err := db.TruncateTable(database, table); err != nil {
						return fmt.Errorf("failed to truncate table %s: %v", table, err)
					}
					fmt.Printf("Truncated table %s\n", table)
				}

				// Import data
				data := exportData.Data[table]
				if err := db.ImportTableData(database, table, data, upsert, dbDriver); err != nil {
					return fmt.Errorf("failed to import data into table %s: %v", table, err)
				}

				// Get final row count
				finalCount, err := db.GetTableRowCount(database, table)
				if err != nil {
					return fmt.Errorf("failed to get final row count for table %s: %v", table, err)
				}

				fmt.Printf("Table %s: %d rows before import, %d rows after import\n",
					table, initialCount, finalCount)
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
	cmd.Flags().StringSlice("tables", []string{}, "Tables to import (default: all)")

	// Import settings flags
	cmd.Flags().Bool("upsert", true, "Perform upsert instead of insert")
	cmd.Flags().Bool("truncate", false, "Truncate tables before import")
	cmd.Flags().String("file-path", "", "Input file path")

	// Mark required flags
	cmd.MarkFlagRequired("database")
	cmd.MarkFlagRequired("file-path")

	return cmd
}

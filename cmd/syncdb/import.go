package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/hoangnguyenba/syncdb/pkg/config"
	"github.com/hoangnguyenba/syncdb/pkg/db"
	"github.com/spf13/cobra"
)

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

			truncate, _ := cmd.Flags().GetBool("truncate")
			upsert, _ := cmd.Flags().GetBool("upsert")

			// Validate required values
			if dbName == "" {
				return fmt.Errorf("database name is required (set via --database flag or SYNCDB_IMPORT_DATABASE env)")
			}

			if filePath == "" {
				return fmt.Errorf("file path is required (set via --file-path flag or SYNCDB_IMPORT_FILEPATH env)")
			}

			// Read import file
			fileData, err := os.ReadFile(filePath)
			if err != nil {
				return fmt.Errorf("failed to read file: %v", err)
			}

			var importData ExportData
			if err := json.Unmarshal(fileData, &importData); err != nil {
				return fmt.Errorf("failed to parse import file: %v", err)
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
	cmd.Flags().StringSlice("tables", []string{}, "Tables to import (default: all)")
	cmd.Flags().String("file-path", "", "Input file path")

	// Import settings flags
	cmd.Flags().Bool("truncate", false, "Truncate tables before import")
	cmd.Flags().Bool("upsert", true, "Use upsert instead of insert")

	return cmd
}

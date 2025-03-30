package main

import (
	"github.com/spf13/cobra"
)

// AddSharedFlags defines flags common to both import and export commands.
// based on the command context.
func AddSharedFlags(cmd *cobra.Command) {
	flags := cmd.Flags()

	// Database connection flags
	flags.StringP("host", "H", "localhost", "Database host")
	flags.IntP("port", "P", 3306, "Database port")
	flags.StringP("username", "u", "", "Database username")
	flags.StringP("password", "p", "", "Database password")
	flags.StringP("database", "d", "", "Database name")
	flags.StringP("driver", "D", "mysql", "Database driver (mysql, postgres)")

	// Table selection flags
	flags.StringSliceP("tables", "t", []string{}, "Tables to export (comma-separated)")

	// Path and Storage flags
	flags.StringP("folder-path", "o", "", "Folder path for export files or temporary import files")
	flags.StringP("storage", "s", "local", "Storage type (local, s3)")
	flags.String("s3-bucket", "", "S3 bucket name")
	flags.String("s3-region", "", "S3 region")

	// Content flags
	flags.Bool("include-schema", false, "Include schema in operation") // Default true for export, false for import
	flags.Bool("include-data", true, "Include table data in operation")
	flags.Bool("include-view-data", false, "Include view data in operation") // Default true for import, false for export

	// Exclusion flags
	flags.StringSlice("exclude-table", []string{}, "Tables to exclude from operation")
	flags.StringSlice("exclude-table-schema", []string{}, "Tables to exclude schema from operation")
	flags.StringSlice("exclude-table-data", []string{}, "Tables to exclude data from operation")

	// Format/Encoding flags
	flags.StringP("format", "f", "sql", "Export format (sql, json)")
	flags.Bool("base64", false, "Encode string values in base64 format during export")

	// Zip flag
	flags.Bool("zip", false, "Create/Use zip file")
}

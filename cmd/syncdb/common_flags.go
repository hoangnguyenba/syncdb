package main

import (
	"github.com/spf13/cobra"
)

// AddSharedFlags defines flags common to both import and export commands.
// The isImportCmd parameter allows for setting different defaults or descriptions
// based on the command context.
func AddSharedFlags(cmd *cobra.Command, isImportCmd bool) {
	flags := cmd.Flags()

	// Database connection flags
	flags.StringP("host", "H", "localhost", "Database host")
	flags.IntP("port", "P", 3306, "Database port")
	flags.StringP("username", "u", "", "Database username")
	flags.StringP("password", "p", "", "Database password")
	flags.StringP("database", "d", "", "Database name")
	flags.StringP("driver", "D", "mysql", "Database driver (mysql, postgres)")

	// Table selection flags (different short flag for export)
	if isImportCmd {
		flags.StringSlice("tables", []string{}, "Tables to import (comma-separated)")
	} else {
		flags.StringSliceP("tables", "t", []string{}, "Tables to export (comma-separated)")
	}

	// Path and Storage flags
	flags.StringP("folder-path", "o", "", "Folder path for export files or temporary import files")
	flags.StringP("storage", "s", "local", "Storage type (local, s3)")
	flags.String("s3-bucket", "", "S3 bucket name")
	flags.String("s3-region", "", "S3 region")

	// Content flags (different defaults)
	flags.Bool("include-schema", !isImportCmd, "Include schema in operation") // Default true for export, false for import
	flags.Bool("include-data", true, "Include table data in operation")
	flags.Bool("include-view-data", isImportCmd, "Include view data in operation") // Default true for import, false for export

	// Exclusion flags
	flags.StringSlice("exclude-table", []string{}, "Tables to exclude from operation")
	flags.StringSlice("exclude-table-schema", []string{}, "Tables to exclude schema from operation")
	flags.StringSlice("exclude-table-data", []string{}, "Tables to exclude data from operation")

	// Format/Encoding flags (different defaults, short flag, description)
	if isImportCmd {
		flags.String("format", "json", "File format to import (json, sql)")
		flags.Bool("base64", false, "Decode values from base64 format during import")
	} else {
		flags.StringP("format", "f", "sql", "Export format (sql, json)")
		flags.Bool("base64", false, "Encode string values in base64 format during export")
	}

	// Zip flag (different defaults)
	flags.Bool("zip", !isImportCmd, "Create/Use zip file") // Default true for export, false for import
}

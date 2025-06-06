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
	flags.StringP("host", "H", "", "Database host")
	flags.IntP("port", "P", 0, "Database port")
	flags.StringP("username", "u", "", "Database username")
	flags.StringP("password", "p", "", "Database password")
	flags.StringP("database", "d", "", "Database name")
	flags.StringP("driver", "D", "", "Database driver (mysql, postgres)")

	// Table selection flags (different short flag for export)
	flags.StringSliceP("tables", "t", []string{}, "Tables to export (comma-separated)")

	// Path and Storage flags
	flags.StringP("path", "o", "", "Path for export files (file/folder path)")
	flags.StringP("storage", "s", "", "Storage type (local, s3, gdrive)")
	flags.String("s3-bucket", "", "S3 bucket name")
	flags.String("s3-region", "", "S3 region")
	flags.String("gdrive-credentials", "", "Google Drive service account credentials file path")
	flags.String("gdrive-folder", "", "Google Drive folder ID to store files in")

	// Content flags (different defaults)
	flags.Bool("include-schema", false, "Include schema in operation")
	flags.Bool("include-data", true, "Include table data in operation")
	flags.Bool("include-view-data", false, "Include view data in operation")

	// Exclusion flags
	flags.StringSlice("exclude-table", []string{}, "Tables to exclude from operation")
	flags.StringSlice("exclude-table-schema", []string{}, "Tables to exclude schema from operation")
	flags.StringSlice("exclude-table-data", []string{}, "Tables to exclude data from operation")

	// Format/Encoding flags (different defaults, short flag, description)
	flags.StringP("format", "f", "", "Export format (sql, json)")
	flags.Bool("base64", false, "Encode string values in base64 format during export")

	// Zip flag (different defaults)
	flags.Bool("zip", false, "Create/Use zip file")

	// Profile flag
	flags.String("profile", "", "Name of the profile to use for default settings")

	flags.Int("from-table-index", 0, "Resume from a specific table index (for resuming interrupted import/export)")
	flags.Int("from-chunk-index", 0, "Resume from a specific chunk within a table (for resuming interrupted import/export)")
}

// CommonArgs holds arguments derived from flags and config for command execution.
// This helps pass validated/merged arguments to core logic functions.
type CommonArgs struct {
	Host                   string
	Port                   int
	Username               string
	Password               string
	Database               string
	Driver                 string
	Tables                 []string
	Path                   string
	Storage                string
	S3Bucket               string
	S3Region               string
	GdriveCredentials      string
	GdriveFolder           string
	Format                 string
	IncludeSchema          bool
	IncludeData            bool
	IncludeViewData        bool
	Zip                    bool
	Base64                 bool // Meaning differs for import (decode) / export (encode)
	ExcludeTable           []string
	ExcludeTableSchema     []string
	ExcludeTableData       []string
	RecordLimit            int    // Maximum number of records to export per table (0 means no limit)
	DisableForeignKeyCheck bool   // Temporarily disable foreign key checks during import
	FileName               string // Name for export folder/zip (default: {database name}_yyyymmdd_hhmmss)
	QuerySeparator         string // String used to separate SQL queries in export/import
	// Import-specific fields
	Truncate       bool // Truncate tables before import
	Drop           bool // Drop and recreate database before import
	FromTableIndex int  // Resume from a specific table index
	FromChunkIndex int  // Resume from a specific chunk within a table
}

// addProfileConfigFlags adds flags to a command for all fields in ProfileConfig.
// Used by 'profile create' and 'profile update'.
func addProfileConfigFlags(cmd *cobra.Command) {
	flags := cmd.Flags()
	// Defaults should generally be empty or zero for create/update,
	// letting the profile file store the actual desired value.
	flags.String("host", "", "Database host")
	flags.Int("port", 0, "Database port (e.g., 3306 for MySQL, 5432 for PostgreSQL)")
	flags.String("username", "", "Database username")
	flags.String("password", "", "Database password (will be stored in plain text!)")
	flags.String("database", "", "Database name") // Required for create, optional for update
	flags.String("driver", "", "Database driver (e.g., mysql, postgres)")
	flags.StringSlice("tables", []string{}, "Tables to include (comma-separated, default: all)")
	// Use different names for bool flags to avoid conflict with export/import flags if they differ
	flags.Bool("profile-include-schema", false, "Include schema definition in operations using this profile")
	flags.Bool("profile-include-data", true, "Include table data in operations using this profile") // Default true makes sense
	flags.String("condition", "", "WHERE condition for filtering data during export")
	flags.StringSlice("exclude-table", []string{}, "Tables to fully exclude")
	flags.StringSlice("exclude-table-schema", []string{}, "Tables to exclude schema from")
	flags.StringSlice("exclude-table-data", []string{}, "Tables to exclude data from")
}

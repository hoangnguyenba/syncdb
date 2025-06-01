package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// CommonConfig holds configuration options shared between import and export
type CommonConfig struct {
	Driver             string
	Host               string
	Port               int
	Username           string
	Password           string
	Database           string
	Tables             []string
	Path               string // Path for export/import files
	Format             string
	ExcludeTable       []string
	ExcludeTableSchema []string
	ExcludeTableData   []string
	Storage            string
	S3Bucket           string
	S3Region           string
	GdriveCredentials  string // Path to Google Drive service account credentials file
	GdriveFolder       string // Google Drive folder ID for storing files
}

// Config holds the overall application configuration
type Config struct {
	Import struct {
		CommonConfig
		// Import-specific fields can go here if any arise
	}
	Export struct {
		CommonConfig
		BatchSize int // Export-specific field
	}
}

// loadCommonConfig populates a CommonConfig struct using Viper with a specific prefix.
func loadCommonConfig(prefix string) CommonConfig {
	cfg := CommonConfig{}
	cfg.Driver = getViperString(prefix+"driver", "mysql")
	cfg.Host = getViperString(prefix+"host", "localhost")
	cfg.Port = getViperInt(prefix+"port", 3306)
	cfg.Username = getViperString(prefix+"username", "")
	cfg.Password = getViperString(prefix+"password", "")
	cfg.Database = getViperString(prefix+"database", "")
	cfg.Format = getViperString(prefix+"format", "sql") // Default 'sql' might need adjustment based on context
	cfg.Path = getViperString(prefix+"path", "")
	cfg.S3Bucket = getViperString(prefix+"s3_bucket", "")
	cfg.S3Region = getViperString(prefix+"s3_region", "")
	cfg.Storage = getViperString(prefix+"storage", "local")
	cfg.GdriveCredentials = getViperString(prefix+"gdrive_credentials", "")
	cfg.GdriveFolder = getViperString(prefix+"gdrive_folder", "")

	// Handle tables
	if tables := getViperString(prefix+"tables", ""); tables != "" {
		cfg.Tables = strings.Split(tables, ",")
	}

	// Handle table exclusions
	if tables := getViperString(prefix+"exclude_table", ""); tables != "" {
		cfg.ExcludeTable = strings.Split(tables, ",")
	}
	if tables := getViperString(prefix+"exclude_table_schema", ""); tables != "" {
		cfg.ExcludeTableSchema = strings.Split(tables, ",")
	}
	if tables := getViperString(prefix+"exclude_table_data", ""); tables != "" {
		cfg.ExcludeTableData = strings.Split(tables, ",")
	}
	return cfg
}

func LoadConfig() (*Config, error) {
	// Set up Viper to read from both .env file and environment variables
	viper.SetConfigName(".env")
	viper.SetConfigType("env")
	viper.AddConfigPath(".")

	// Read .env file if it exists (ignore error if it doesn't)
	if err := viper.ReadInConfig(); err != nil {
		fmt.Printf("Debug: Error reading config file: %v\n", err)
	} else {
		fmt.Printf("Debug: Successfully read config from: %s\n", viper.ConfigFileUsed())
	}

	// Enable environment variable reading
	viper.AutomaticEnv()
	// No need for SetEnvPrefix("SYNCDB") as we use full keys like "syncdb_import_host"
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // Keep this if you use nested keys in .env

	config := &Config{}

	// Load common config for Import and Export using prefixes
	config.Import.CommonConfig = loadCommonConfig("syncdb_import_")
	config.Export.CommonConfig = loadCommonConfig("syncdb_export_")

	// Load export-specific config
	config.Export.BatchSize = getViperInt("syncdb_export_batch_size", 500)

	// Adjust default format for import if needed (common loader defaults to 'sql')
	if !viper.IsSet("syncdb_import_format") {
		config.Import.Format = "json" // Set import-specific default if not overridden
	}

	// Debug output (optional, adjust as needed)
	fmt.Printf("Debug: Import Config Loaded: %+v\n", config.Import)
	fmt.Printf("Debug: Export Config Loaded: %+v\n", config.Export)
	// fmt.Printf("Debug: Export Database = %s\n", config.Export.Database)
	// fmt.Printf("Debug: Export Driver = %s\n", config.Export.Driver)
	// fmt.Printf("Debug: Export Host = %s\n", config.Export.Host)
	// fmt.Printf("Debug: Export Port = %d\n", config.Export.Port)
	// fmt.Printf("Debug: Export Tables = %v\n", config.Export.Tables)
	// fmt.Printf("Debug: Export Folder Path = %s\n", config.Export.FolderPath)
	// fmt.Printf("Debug: Export Storage = %s\n", config.Export.Storage)
	// fmt.Printf("Debug: Export S3 Bucket = %s\n", config.Export.S3Bucket)
	// fmt.Printf("Debug: Export S3 Region = %s\n", config.Export.S3Region)
	// fmt.Printf("Debug: Export Batch Size = %d\n", config.Export.BatchSize)

	// Note: AWS credentials should be set as environment variables (e.g. AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
	// or using a shared credentials file (~/.aws/credentials). See https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html

	return config, nil
}

func getViperString(key, defaultValue string) string {
	if viper.IsSet(key) {
		return viper.GetString(key)
	}
	return defaultValue
}

func getViperInt(key string, defaultValue int) int {
	if viper.IsSet(key) {
		return viper.GetInt(key)
	}
	return defaultValue
}

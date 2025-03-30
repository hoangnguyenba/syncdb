package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Import struct {
		Driver             string
		Host               string
		Port               int
		Username           string
		Password           string
		Database           string
		Tables             []string
		Filepath           string
		Format             string
		FolderPath         string
		ExcludeTable       []string
		ExcludeTableSchema []string
		ExcludeTableData   []string
		Storage            string
		S3Bucket           string
		S3Region           string
	}
	Export struct {
		Driver             string
		Host               string
		Port               int
		Username           string
		Password           string
		Database           string
		Tables             []string
		Filepath           string
		Format             string
		FolderPath         string
		ExcludeTable       []string
		ExcludeTableSchema []string
		ExcludeTableData   []string
		Storage            string
		S3Bucket           string
		S3Region           string
		BatchSize          int
	}
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
	viper.SetEnvPrefix("SYNCDB")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	config := &Config{}

	// Import config
	config.Import.Driver = getViperString("syncdb_import_driver", "mysql")
	config.Import.Host = getViperString("syncdb_import_host", "localhost")
	config.Import.Port = getViperInt("syncdb_import_port", 3306)
	config.Import.Username = getViperString("syncdb_import_username", "")
	config.Import.Password = getViperString("syncdb_import_password", "")
	config.Import.Database = getViperString("syncdb_import_database", "")
	config.Import.Filepath = getViperString("syncdb_import_filepath", "")
	config.Import.Format = getViperString("syncdb_import_format", "sql")
	config.Import.FolderPath = getViperString("syncdb_import_folder_path", "")
	config.Import.S3Bucket = getViperString("syncdb_import_s3_bucket", "")
	config.Import.S3Region = getViperString("syncdb_import_s3_region", "")
	config.Import.Storage = getViperString("syncdb_import_storage", "local")

	// Handle import tables
	if tables := getViperString("syncdb_import_tables", ""); tables != "" {
		config.Import.Tables = strings.Split(tables, ",")
	}

	// Handle import table exclusions
	if tables := getViperString("syncdb_import_exclude_table", ""); tables != "" {
		config.Import.ExcludeTable = strings.Split(tables, ",")
	}
	if tables := getViperString("syncdb_import_exclude_table_schema", ""); tables != "" {
		config.Import.ExcludeTableSchema = strings.Split(tables, ",")
	}
	if tables := getViperString("syncdb_import_exclude_table_data", ""); tables != "" {
		config.Import.ExcludeTableData = strings.Split(tables, ",")
	}

	// Export config
	config.Export.Driver = getViperString("syncdb_export_driver", "mysql")
	config.Export.Host = getViperString("syncdb_export_host", "localhost")
	config.Export.Port = getViperInt("syncdb_export_port", 3306)
	config.Export.Username = getViperString("syncdb_export_username", "")
	config.Export.Password = getViperString("syncdb_export_password", "")
	config.Export.Database = getViperString("syncdb_export_database", "")
	config.Export.Filepath = getViperString("syncdb_export_filepath", "")
	config.Export.Format = getViperString("syncdb_export_format", "sql")
	config.Export.FolderPath = getViperString("syncdb_export_folder_path", "")
	config.Export.S3Bucket = getViperString("syncdb_export_s3_bucket", "")
	config.Export.S3Region = getViperString("syncdb_export_s3_region", "")
	config.Export.Storage = getViperString("syncdb_export_storage", "local")
	config.Export.BatchSize = getViperInt("syncdb_export_batch_size", 500)

	// Handle export tables
	if tables := getViperString("syncdb_export_tables", ""); tables != "" {
		config.Export.Tables = strings.Split(tables, ",")
	}

	// Handle export table exclusions
	if tables := getViperString("syncdb_export_exclude_table", ""); tables != "" {
		config.Export.ExcludeTable = strings.Split(tables, ",")
	}
	if tables := getViperString("syncdb_export_exclude_table_schema", ""); tables != "" {
		config.Export.ExcludeTableSchema = strings.Split(tables, ",")
	}
	if tables := getViperString("syncdb_export_exclude_table_data", ""); tables != "" {
		config.Export.ExcludeTableData = strings.Split(tables, ",")
	}

	// Debug output
	fmt.Printf("Debug: Export Database = %s\n", config.Export.Database)
	fmt.Printf("Debug: Export Driver = %s\n", config.Export.Driver)
	fmt.Printf("Debug: Export Host = %s\n", config.Export.Host)
	fmt.Printf("Debug: Export Port = %d\n", config.Export.Port)
	fmt.Printf("Debug: Export Tables = %v\n", config.Export.Tables)
	fmt.Printf("Debug: Export Folder Path = %s\n", config.Export.FolderPath)
	fmt.Printf("Debug: Export Storage = %s\n", config.Export.Storage)
	fmt.Printf("Debug: Export S3 Bucket = %s\n", config.Export.S3Bucket)
	fmt.Printf("Debug: Export S3 Region = %s\n", config.Export.S3Region)
	fmt.Printf("Debug: Export Batch Size = %d\n", config.Export.BatchSize)

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

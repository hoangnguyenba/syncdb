package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Import struct {
		Driver   string
		Host     string
		Port     int
		Username string
		Password string
		Database string
		Tables   []string
		Filepath string
		Format   string
	}
	Export struct {
		Driver   string
		Host     string
		Port     int
		Username string
		Password string
		Database string
		Tables   []string
		Filepath string
		Format   string
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

	// Handle import tables
	if tables := getViperString("syncdb_import_tables", ""); tables != "" {
		config.Import.Tables = strings.Split(tables, ",")
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

	// Handle export tables
	if tables := getViperString("syncdb_export_tables", ""); tables != "" {
		config.Export.Tables = strings.Split(tables, ",")
	}

	// Debug output
	fmt.Printf("Debug: Export Database = %s\n", config.Export.Database)
	fmt.Printf("Debug: Export Driver = %s\n", config.Export.Driver)
	fmt.Printf("Debug: Export Host = %s\n", config.Export.Host)
	fmt.Printf("Debug: Export Port = %d\n", config.Export.Port)
	fmt.Printf("Debug: Export Tables = %v\n", config.Export.Tables)

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

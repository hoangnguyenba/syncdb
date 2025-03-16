package config

import (
	"time"

	"github.com/spf13/viper"
)

type DatabaseConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	Tables   []string
}

type ExportImportConfig struct {
	IncludeSchema bool
	ExportCondition *time.Duration
	UpsertOnImport bool
}

type StorageConfig struct {
	LocalFile  string
	S3Bucket   string
	GoogleDriveID string
}

func Load() (*DatabaseConfig, *ExportImportConfig, *StorageConfig) {
	viper.SetConfigType("env")
 viper.AddConfigPath(".")
	return nil, nil, nil
}

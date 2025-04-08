package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to create a temporary .env file
func createTempEnvFile(t *testing.T, content string) (string, func()) {
	t.Helper()
	tmpFile, err := os.CreateTemp("", ".env_test")
	require.NoError(t, err)
	_, err = tmpFile.WriteString(content)
	require.NoError(t, err)
	err = tmpFile.Close()
	require.NoError(t, err)

	// Return the file path and a cleanup function
	return tmpFile.Name(), func() {
		os.Remove(tmpFile.Name())
	}
}

func TestLoadConfig(t *testing.T) {
	// Store original env vars to restore later
	originalEnvVars := make(map[string]string)
	envVarsToSet := []string{
		"SYNCDB_EXPORT_HOST", "SYNCDB_EXPORT_PORT", "SYNCDB_EXPORT_DATABASE",
		"SYNCDB_IMPORT_HOST", "SYNCDB_IMPORT_PORT", "SYNCDB_IMPORT_DATABASE",
		"SYNCDB_EXPORT_TABLES", "SYNCDB_IMPORT_TABLES",
		"SYNCDB_EXPORT_STORAGE", "SYNCDB_IMPORT_STORAGE",
		"SYNCDB_EXPORT_S3_BUCKET", "SYNCDB_IMPORT_S3_BUCKET",
		"SYNCDB_EXPORT_S3_REGION", "SYNCDB_IMPORT_S3_REGION",
		"SYNCDB_EXPORT_FOLDER_PATH", "SYNCDB_IMPORT_FOLDER_PATH",
		"SYNCDB_EXPORT_FORMAT", "SYNCDB_IMPORT_FORMAT",
		"SYNCDB_EXPORT_BATCH_SIZE",
	}
	for _, key := range envVarsToSet {
		originalEnvVars[key] = os.Getenv(key)
		os.Unsetenv(key) // Clear existing env vars for the test
	}
	// Cleanup function to restore env vars
	defer func() {
		for key, val := range originalEnvVars {
			if val == "" {
				os.Unsetenv(key)
			} else {
				os.Setenv(key, val)
			}
		}
	}()

	t.Run("No .env file, no env vars", func(t *testing.T) {
		// Ensure no .env file is loaded by pointing LoadConfig away temporarily
		// (LoadConfig currently looks for ".env" in the current dir)
		// A better approach might be to modify LoadConfig to accept a path or reader

		cfg, err := LoadConfig() // Assumes it won't find a .env file
		require.NoError(t, err)

		// Check defaults (assuming defaults are zero/empty strings in the struct definition)
		assert.Equal(t, "", cfg.Export.Host)
		assert.Equal(t, 0, cfg.Export.Port)
		assert.Equal(t, "", cfg.Export.Database)
		assert.Equal(t, "", cfg.Import.Host)
		assert.Equal(t, 0, cfg.Import.Port)
		assert.Equal(t, "", cfg.Import.Database)
		assert.Equal(t, 0, cfg.Export.BatchSize) // Check default batch size if defined
	})

	t.Run("Load from .env file only", func(t *testing.T) {
		envContent := `
SYNCDB_EXPORT_HOST=env_export_host
SYNCDB_EXPORT_PORT=1111
SYNCDB_EXPORT_DATABASE=env_export_db
SYNCDB_IMPORT_HOST=env_import_host
SYNCDB_IMPORT_PORT=2222
SYNCDB_IMPORT_DATABASE=env_import_db
SYNCDB_EXPORT_TABLES=env_table1,env_table2
SYNCDB_EXPORT_BATCH_SIZE=100
`
		envFilePath, cleanupEnv := createTempEnvFile(t, envContent)
		defer cleanupEnv()

		// Temporarily move to the directory of the temp .env file
		// or modify LoadConfig to take a path
		originalWd, _ := os.Getwd()
		os.Chdir(filepath.Dir(envFilePath))
		// Rename the temp file to ".env" so LoadConfig finds it
		os.Rename(envFilePath, ".env")
		defer func() {
			os.Remove(".env") // Clean up the renamed file
			os.Chdir(originalWd)
		}()

		cfg, err := LoadConfig()
		require.NoError(t, err)

		assert.Equal(t, "env_export_host", cfg.Export.Host)
		assert.Equal(t, 1111, cfg.Export.Port)
		assert.Equal(t, "env_export_db", cfg.Export.Database)
		assert.Equal(t, []string{"env_table1", "env_table2"}, cfg.Export.Tables)
		assert.Equal(t, "env_import_host", cfg.Import.Host)
		assert.Equal(t, 2222, cfg.Import.Port)
		assert.Equal(t, "env_import_db", cfg.Import.Database)
		assert.Equal(t, 100, cfg.Export.BatchSize)
	})

	t.Run("Load from environment variables only", func(t *testing.T) {
		os.Setenv("SYNCDB_EXPORT_HOST", "os_export_host")
		os.Setenv("SYNCDB_EXPORT_PORT", "3333")
		os.Setenv("SYNCDB_EXPORT_DATABASE", "os_export_db")
		os.Setenv("SYNCDB_IMPORT_HOST", "os_import_host")
		os.Setenv("SYNCDB_IMPORT_PORT", "4444")
		os.Setenv("SYNCDB_IMPORT_DATABASE", "os_import_db")
		os.Setenv("SYNCDB_EXPORT_TABLES", "os_tableA,os_tableB")
		os.Setenv("SYNCDB_EXPORT_BATCH_SIZE", "200")

		cfg, err := LoadConfig() // Assumes no .env file present
		require.NoError(t, err)

		assert.Equal(t, "os_export_host", cfg.Export.Host)
		assert.Equal(t, 3333, cfg.Export.Port)
		assert.Equal(t, "os_export_db", cfg.Export.Database)
		assert.Equal(t, []string{"os_tableA", "os_tableB"}, cfg.Export.Tables)
		assert.Equal(t, "os_import_host", cfg.Import.Host)
		assert.Equal(t, 4444, cfg.Import.Port)
		assert.Equal(t, "os_import_db", cfg.Import.Database)
		assert.Equal(t, 200, cfg.Export.BatchSize)
	})

	t.Run("Environment variables override .env file", func(t *testing.T) {
		// .env file settings
		envContent := `
SYNCDB_EXPORT_HOST=env_export_host
SYNCDB_EXPORT_PORT=1111
SYNCDB_EXPORT_DATABASE=env_export_db
SYNCDB_EXPORT_BATCH_SIZE=100
`
		envFilePath, cleanupEnv := createTempEnvFile(t, envContent)
		defer cleanupEnv()

		// Environment variable settings (should override)
		os.Setenv("SYNCDB_EXPORT_HOST", "os_export_host")
		os.Setenv("SYNCDB_EXPORT_PORT", "3333")
		// Database not set in OS env, should take from .env
		os.Setenv("SYNCDB_EXPORT_BATCH_SIZE", "200")

		// Temporarily move to the directory of the temp .env file
		originalWd, _ := os.Getwd()
		os.Chdir(filepath.Dir(envFilePath))
		os.Rename(envFilePath, ".env")
		defer func() {
			os.Remove(".env")
			os.Chdir(originalWd)
		}()

		cfg, err := LoadConfig()
		require.NoError(t, err)

		assert.Equal(t, "os_export_host", cfg.Export.Host)    // OS overrides .env
		assert.Equal(t, 3333, cfg.Export.Port)                // OS overrides .env
		assert.Equal(t, "env_export_db", cfg.Export.Database) // OS not set, uses .env
		assert.Equal(t, 200, cfg.Export.BatchSize)            // OS overrides .env
	})

	// Note: Testing the full priority (Flag > Env Var > Profile > Default)
	// requires testing within the context of the cmd package (e.g., config_helpers_test.go)
	// because flags and profile loading are handled there.
	// These tests focus solely on the LoadConfig function's handling of .env and OS env vars.
}

// TODO: Add tests for config_helpers.go in a cmd/syncdb/config_helpers_test.go file
// These tests would involve:
// - Mocking cobra commands and flags.
// - Mocking profile loading (perhaps by creating temp profile files).
// - Calling populateCommonArgsFromFlagsAndConfig with various combinations of flags set,
//   env vars set, and profiles loaded to verify the priority logic in the resolve* functions.

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hoangnguyenba/syncdb/pkg/config"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create a temporary directory for profile testing
func setupTestProfileDir(t *testing.T) (string, func()) {
	t.Helper()
	baseTmpDir, err := os.MkdirTemp("", "syncdb_cmd_tests_")
	require.NoError(t, err, "Failed to create base temp dir")
	profilesDir := filepath.Join(baseTmpDir, "profiles")
	err = os.MkdirAll(profilesDir, 0755)
	require.NoError(t, err, "Failed to create profiles subdir")
	return baseTmpDir, func() { os.RemoveAll(baseTmpDir) }
}

// Helper function to create a dummy profile file
func createDummyCmdProfile(t *testing.T, profileDir string, profileName string, content string) string {
	t.Helper()
	filePath := filepath.Join(profileDir, profileName+".yaml")
	err := os.WriteFile(filePath, []byte(content), 0644)
	require.NoError(t, err, "Failed to write dummy profile file")
	return filePath
}

// setupTestCmd creates a dummy cobra command with shared flags for testing
func setupTestCmd() *cobra.Command {
	cmd := &cobra.Command{}
	// Add flags similar to export/import commands
	// Use the actual AddSharedFlags function if possible, or replicate flags here
	flags := cmd.Flags()
	flags.StringP("host", "H", "localhost", "Database host")
	flags.IntP("port", "P", 3306, "Database port")
	flags.StringP("username", "u", "", "Database username")
	flags.StringP("password", "p", "", "Database password")
	flags.StringP("database", "d", "", "Database name")
	flags.StringP("driver", "D", "mysql", "Database driver (mysql, postgres)")
	flags.StringSliceP("tables", "t", []string{}, "Tables to export (comma-separated)")
	flags.StringP("path", "o", "", "Path for export files (file/folder path)")
	flags.StringP("storage", "s", "local", "Storage type (local, s3)")
	flags.String("s3-bucket", "", "S3 bucket name")
	flags.String("s3-region", "", "S3 region")
	flags.Bool("include-schema", true, "Include schema in operation")
	flags.Bool("include-data", true, "Include table data in operation")
	flags.Bool("include-view-data", false, "Include view data in operation")
	flags.StringSlice("exclude-table", []string{}, "Tables to exclude from operation")
	flags.StringSlice("exclude-table-schema", []string{}, "Tables to exclude schema from operation")
	flags.StringSlice("exclude-table-data", []string{}, "Tables to exclude data from operation")
	flags.StringP("format", "f", "sql", "Export format (sql, json)")
	flags.Bool("base64", false, "Encode string values in base64 format during export")
	flags.Bool("zip", false, "Create/Use zip file")
	flags.String("profile", "", "Name of the profile to use for default settings") // The crucial flag

	// Add profile-specific flags used by resolveBoolValueProfile logic
	flags.Bool("profile-include-schema", false, "Profile setting for include schema")
	flags.Bool("profile-include-data", true, "Profile setting for include data")

	return cmd
}

func TestPopulateCommonArgsFromFlagsAndConfig(t *testing.T) {
	baseTmpDir, cleanupProfileDir := setupTestProfileDir(t)
	defer cleanupProfileDir()
	profileDir := filepath.Join(baseTmpDir, "profiles")

	// --- Setup Environment ---
	// Store original env vars and SYNCDB_PATH
	originalEnvVars := make(map[string]string)
	envVarsToSet := []string{
		"SYNCDB_HOST", "SYNCDB_PORT", "SYNCDB_DATABASE", "SYNCDB_TABLES",
		"SYNCDB_EXCLUDE_TABLE",
	} // Add more as needed for tests
	originalSyncDBPath := os.Getenv("SYNCDB_PATH")

	// Populate originalEnvVars and unset for the test
	for _, key := range envVarsToSet {
		originalEnvVars[key] = os.Getenv(key)
		os.Unsetenv(key)
	}

	defer func() { // Cleanup function
		for key, val := range originalEnvVars {
			if val == "" {
				os.Unsetenv(key)
			} else {
				os.Setenv(key, val)
			}
		}
		os.Setenv("SYNCDB_PATH", originalSyncDBPath)
	}()

	// Helper to reset env vars and SYNCDB_PATH for each subtest
	resetEnv := func() {
		for key := range originalEnvVars { // Unset all tracked vars
			os.Unsetenv(key)
		}
		os.Setenv("SYNCDB_PATH", baseTmpDir) // Point to our temp profile dir
	}

	// --- Test Cases ---

	t.Run("Defaults only", func(t *testing.T) {
		resetEnv()
		cmd := setupTestCmd()
		cfg := config.CommonConfig{} // Empty base config
		profileName := ""

		args, err := populateCommonArgsFromFlagsAndConfig(cmd, cfg, profileName)
		require.NoError(t, err)

		assert.Equal(t, "localhost", args.Host) // Default from flag definition
		assert.Equal(t, 3306, args.Port)        // Default from flag definition
		assert.Equal(t, "mysql", args.Driver)   // Default from flag definition
		assert.Equal(t, "", args.Database)
		assert.Empty(t, args.Tables)
		assert.True(t, args.IncludeSchema) // Default from flag definition
		assert.True(t, args.IncludeData)   // Default from flag definition
	})

	t.Run("Profile only", func(t *testing.T) {
		resetEnv()
		profileName := "profile-only"
		profileContent := `
database: profile_db
host: profile_host
port: 1234
driver: postgres
tables: [prof_t1, prof_t2]
exclude-table: [prof_ex1]
profile-include-schema: false # Note: uses profile-specific flag name
profile-include-data: false   # Note: uses profile-specific flag name
`
		createDummyCmdProfile(t, profileDir, profileName, profileContent)

		cmd := setupTestCmd()
		cfg := config.CommonConfig{}

		args, err := populateCommonArgsFromFlagsAndConfig(cmd, cfg, profileName)
		require.NoError(t, err)

		assert.Equal(t, "profile_host", args.Host)
		assert.Equal(t, 1234, args.Port)
		assert.Equal(t, "profile_db", args.Database)
		assert.Equal(t, "postgres", args.Driver)
		assert.Equal(t, []string{"prof_t1", "prof_t2"}, args.Tables)
		assert.Equal(t, []string{"prof_ex1"}, args.ExcludeTable)
		assert.False(t, args.IncludeSchema) // Should take profile value
		assert.False(t, args.IncludeData)   // Should take profile value
	})

	t.Run("Env Vars only", func(t *testing.T) {
		resetEnv()
		os.Setenv("SYNCDB_HOST", "env_host")
		os.Setenv("SYNCDB_PORT", "5555")
		os.Setenv("SYNCDB_DATABASE", "env_db")
		os.Setenv("SYNCDB_TABLES", "env_t1,env_t2")
		os.Setenv("SYNCDB_EXCLUDE_TABLE", "env_ex1")

		cmd := setupTestCmd()
		cfg := config.CommonConfig{} // LoadConfig would populate this, but we simulate it here
		cfg.Host = "env_host"
		cfg.Port = 5555
		cfg.Database = "env_db"
		cfg.Tables = []string{"env_t1", "env_t2"}
		cfg.ExcludeTable = []string{"env_ex1"}

		profileName := ""

		args, err := populateCommonArgsFromFlagsAndConfig(cmd, cfg, profileName)
		require.NoError(t, err)

		assert.Equal(t, "env_host", args.Host)
		assert.Equal(t, 5555, args.Port)
		assert.Equal(t, "env_db", args.Database)
		assert.Equal(t, []string{"env_t1", "env_t2"}, args.Tables)
		assert.Equal(t, []string{"env_ex1"}, args.ExcludeTable)
		assert.True(t, args.IncludeSchema) // Default
		assert.True(t, args.IncludeData)   // Default
	})

	t.Run("Flags only", func(t *testing.T) {
		resetEnv()
		cmd := setupTestCmd()
		cmd.Flags().Set("host", "flag_host")
		cmd.Flags().Set("port", "9876")
		cmd.Flags().Set("database", "flag_db")
		cmd.Flags().Set("tables", "flag_t1,flag_t2")
		cmd.Flags().Set("exclude-table", "flag_ex1")
		cmd.Flags().Set("include-schema", "false")
		cmd.Flags().Set("include-data", "false")

		cfg := config.CommonConfig{}
		profileName := ""

		args, err := populateCommonArgsFromFlagsAndConfig(cmd, cfg, profileName)
		require.NoError(t, err)

		assert.Equal(t, "flag_host", args.Host)
		assert.Equal(t, 9876, args.Port)
		assert.Equal(t, "flag_db", args.Database)
		assert.Equal(t, []string{"flag_t1", "flag_t2"}, args.Tables)
		assert.Equal(t, []string{"flag_ex1"}, args.ExcludeTable)
		assert.False(t, args.IncludeSchema)
		assert.False(t, args.IncludeData)
	})

	t.Run("Flag > Env > Profile > Default", func(t *testing.T) {
		resetEnv()
		// Profile
		profileName := "priority-test"
		profileContent := `
database: profile_db
host: profile_host
port: 1111
tables: [prof_t1]
exclude-table: [prof_ex1]
profile-include-schema: true # Profile says true
`
		createDummyCmdProfile(t, profileDir, profileName, profileContent)

		// Env Vars (overrides profile)
		os.Setenv("SYNCDB_HOST", "env_host")
		os.Setenv("SYNCDB_PORT", "2222")
		os.Setenv("SYNCDB_TABLES", "env_t1,env_t2")
		// No env var for database or exclude-table

		// Simulate loaded config from env vars
		cfg := config.CommonConfig{}
		cfg.Host = "env_host"
		cfg.Port = 2222
		cfg.Tables = []string{"env_t1", "env_t2"}
		// cfg.Database remains empty
		// cfg.ExcludeTable remains empty

		// Flags (overrides env and profile)
		cmd := setupTestCmd()
		cmd.Flags().Set("port", "3333")
		cmd.Flags().Set("database", "flag_db")
		cmd.Flags().Set("exclude-table", "flag_ex1")
		cmd.Flags().Set("include-schema", "false") // Flag says false

		args, err := populateCommonArgsFromFlagsAndConfig(cmd, cfg, profileName)
		require.NoError(t, err)

		assert.Equal(t, "env_host", args.Host)                     // Env > Profile
		assert.Equal(t, 3333, args.Port)                           // Flag > Env > Profile
		assert.Equal(t, "flag_db", args.Database)                  // Flag > Profile
		assert.Equal(t, "mysql", args.Driver)                      // Default
		assert.Equal(t, []string{"env_t1", "env_t2"}, args.Tables) // Env > Profile
		assert.Equal(t, []string{"flag_ex1"}, args.ExcludeTable)   // Flag > Profile
		assert.False(t, args.IncludeSchema)                        // Flag > Profile
		assert.True(t, args.IncludeData)                           // Default (profile didn't set it, flag didn't set it)
	})

	t.Run("Profile not found", func(t *testing.T) {
		resetEnv()
		cmd := setupTestCmd()
		cfg := config.CommonConfig{}
		profileName := "does-not-exist"

		_, err := populateCommonArgsFromFlagsAndConfig(cmd, cfg, profileName)
		require.Error(t, err) // Expect error when profile is specified but not found
		assert.Contains(t, err.Error(), "failed to load profile")
	})

}

func addTestFlags(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.StringP("host", "H", "", "Database host")
	flags.IntP("port", "P", 0, "Database port")
	flags.StringP("username", "u", "", "Database username")
	flags.StringP("password", "p", "", "Database password")
	flags.StringP("database", "d", "", "Database name")
	flags.StringP("driver", "D", "", "Database driver (mysql, postgres)")
	flags.StringSliceP("tables", "t", []string{}, "Tables to export (comma-separated)")
	flags.StringP("path", "o", "", "Path for export files (file/folder path)")
	flags.StringP("storage", "s", "", "Storage type (local, s3)")
	flags.String("s3-bucket", "", "S3 bucket name")
	flags.String("s3-region", "", "S3 region")
	flags.Bool("include-schema", false, "Include schema in operation")
	flags.Bool("include-data", true, "Include table data in operation")
	flags.Bool("include-view-data", false, "Include view data in operation")
	flags.String("profile", "", "Name of the profile to use for settings")
}

func TestResolveCommonArgs(t *testing.T) {
	// Set up temp profile directory
	tmpDir, cleanup := setupTestProfileDir(t)
	defer cleanup()
	os.Setenv("SYNCDB_PATH", tmpDir)
	defer os.Unsetenv("SYNCDB_PATH")

	// Create a dummy config profile
	createDummyCmdProfile(t, filepath.Join(tmpDir, "profiles"), "testprofile", `
database: profiledb
host: profilehost
port: 5678
username: profileuser
password: profilepass
driver: postgres
tables: [table1, table2]
includeschema: true
includedata: true
`)

	// Create command and add test flags
	cmd := &cobra.Command{}
	addTestFlags(cmd)

	// Create initial config
	cfg := config.CommonConfig{
		Driver: "mysql",
		Host:   "confighost",
		Port:   3306,
	}

	// Set some flag values
	cmd.Flags().Set("host", "localhost")
	cmd.Flags().Set("path", "./export")
	cmd.Flags().Set("storage", "s3")
	cmd.Flags().Set("s3-bucket", "mybucket")
	cmd.Flags().Set("s3-region", "us-west-2")

	// Loading config without profile
	args, err := populateCommonArgsFromFlagsAndConfig(cmd, cfg, "")

	// Verify values from flags override config values
	require.NoError(t, err)
	assert.Equal(t, "localhost", args.Host)
	assert.Equal(t, "./export", args.Path)
	assert.Equal(t, "s3", args.Storage)
	assert.Equal(t, "mybucket", args.S3Bucket)
	assert.Equal(t, "us-west-2", args.S3Region)

	// Test with profile
	args, err = populateCommonArgsFromFlagsAndConfig(cmd, cfg, "testprofile")
	require.NoError(t, err)
	// Flag values should still override profile values
	assert.Equal(t, "localhost", args.Host)  // Flag value
	assert.Equal(t, 5678, args.Port)         // Profile value
	assert.Equal(t, "./export", args.Path)   // Flag value
	assert.Equal(t, "postgres", args.Driver) // Profile value
}

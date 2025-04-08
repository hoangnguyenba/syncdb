package profile

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create a temporary directory for testing
func setupTestDir(t *testing.T) (string, func()) {
	t.Helper()
	// Create a base temp dir for all profile tests
	baseTmpDir, err := os.MkdirTemp("", "syncdb_profile_tests_")
	require.NoError(t, err, "Failed to create base temp dir")

	// Create the 'profiles' subdirectory within it
	profilesDir := filepath.Join(baseTmpDir, "profiles")
	err = os.MkdirAll(profilesDir, 0755)
	require.NoError(t, err, "Failed to create profiles subdir")

	// Return the base temp dir path and a cleanup function
	return baseTmpDir, func() {
		os.RemoveAll(baseTmpDir)
	}
}

// Helper function to create a dummy profile file
func createDummyProfile(t *testing.T, profileDir string, profileName string, content string) string {
	t.Helper()
	filePath := filepath.Join(profileDir, profileName+".yaml")
	err := os.WriteFile(filePath, []byte(content), 0644)
	require.NoError(t, err, "Failed to write dummy profile file")
	return filePath
}

func TestGetProfileDir(t *testing.T) {
	t.Run("SYNCDB_PATH not set", func(t *testing.T) {
		// Unset SYNCDB_PATH temporarily
		originalPath := os.Getenv("SYNCDB_PATH")
		os.Unsetenv("SYNCDB_PATH")
		defer os.Setenv("SYNCDB_PATH", originalPath) // Restore original value

		// Determine expected default path (this is OS-dependent)
		// For simplicity, we'll just check it doesn't return an error and is absolute.
		// A more robust test would mock os.UserConfigDir()
		dir, err := GetProfileDir()
		assert.NoError(t, err)
		assert.True(t, filepath.IsAbs(dir), "Expected absolute path")
		assert.Contains(t, dir, "syncdb", "Expected path to contain 'syncdb'")
		assert.Contains(t, dir, "profiles", "Expected path to contain 'profiles'")
	})

	t.Run("SYNCDB_PATH is set", func(t *testing.T) {
		// Set SYNCDB_PATH temporarily
		originalPath := os.Getenv("SYNCDB_PATH")
		testPath := "/tmp/custom_syncdb_path_test"
		os.Setenv("SYNCDB_PATH", testPath)
		defer os.Setenv("SYNCDB_PATH", originalPath) // Restore original value

		expectedDir := filepath.Join(testPath, "profiles")
		dir, err := GetProfileDir()
		assert.NoError(t, err)
		assert.Equal(t, expectedDir, dir)
	})
}

func TestGetProfilePath(t *testing.T) {
	// This largely depends on GetProfileDir, so we test a simple case
	t.Run("Valid profile name", func(t *testing.T) {
		// Unset SYNCDB_PATH temporarily
		originalPath := os.Getenv("SYNCDB_PATH")
		os.Unsetenv("SYNCDB_PATH")
		defer os.Setenv("SYNCDB_PATH", originalPath)

		profileName := "my-test-profile"
		path, err := GetProfilePath(profileName)
		assert.NoError(t, err)
		assert.True(t, filepath.IsAbs(path), "Expected absolute path")
		assert.Contains(t, path, "profiles", "Expected path to contain 'profiles'")
		assert.Contains(t, path, profileName+".yaml", "Expected path to end with profile name and .yaml extension")
	})
}

func TestLoadProfile(t *testing.T) {
	baseTmpDir, cleanup := setupTestDir(t)
	defer cleanup()
	profileDir := filepath.Join(baseTmpDir, "profiles")

	// Set SYNCDB_PATH to our temp dir for these tests
	originalPath := os.Getenv("SYNCDB_PATH")
	os.Setenv("SYNCDB_PATH", baseTmpDir)
	defer os.Setenv("SYNCDB_PATH", originalPath)

	t.Run("Profile exists and is valid", func(t *testing.T) {
		profileName := "valid-profile"
		content := `
database: testdb
host: localhost
port: 3306
driver: mysql
tables: [users, orders]
profile-include-schema: true
`
		createDummyProfile(t, profileDir, profileName, content)

		cfg, err := LoadProfile(profileName)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Equal(t, "testdb", cfg.Database)
		assert.Equal(t, "localhost", cfg.Host)
		assert.Equal(t, 3306, cfg.Port)
		assert.Equal(t, "mysql", cfg.Driver)
		assert.Equal(t, []string{"users", "orders"}, cfg.Tables)
		require.NotNil(t, cfg.IncludeSchema)
		assert.True(t, *cfg.IncludeSchema)
		assert.Nil(t, cfg.IncludeData) // Not set in file, should be nil
	})

	t.Run("Profile does not exist", func(t *testing.T) {
		profileName := "non-existent-profile"
		_, err := LoadProfile(profileName)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
		assert.True(t, os.IsNotExist(err), "Expected a 'not exist' error type")
	})

	t.Run("Profile exists but is invalid YAML", func(t *testing.T) {
		profileName := "invalid-yaml"
		content := `database: testdb\nhost: localhost\nport: badport:` // Invalid YAML
		createDummyProfile(t, profileDir, profileName, content)

		_, err := LoadProfile(profileName)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to unmarshal")
	})

	t.Run("Profile exists but has incorrect types", func(t *testing.T) {
		profileName := "wrong-types"
		content := `
database: testdb
port: "not-a-number" # Port should be int
tables: "not-a-slice" # Tables should be slice
profile-include-schema: "not-a-bool" # IncludeSchema should be bool
`
		createDummyProfile(t, profileDir, profileName, content)

		_, err := LoadProfile(profileName)
		// Depending on the YAML library, this might error during unmarshal or result in zero values.
		// We expect an error here.
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to unmarshal")
	})
}

func TestSaveProfile(t *testing.T) {
	baseTmpDir, cleanup := setupTestDir(t)
	defer cleanup()
	profileDir := filepath.Join(baseTmpDir, "profiles")

	// Set SYNCDB_PATH to our temp dir for these tests
	originalPath := os.Getenv("SYNCDB_PATH")
	os.Setenv("SYNCDB_PATH", baseTmpDir)
	defer os.Setenv("SYNCDB_PATH", originalPath)

	t.Run("Save new profile", func(t *testing.T) {
		profileName := "new-save-profile"
		includeSchema := true
		cfg := &ProfileConfig{
			Database:      "saved_db",
			Host:          "savehost",
			Port:          5432,
			Username:      "saveuser",
			Driver:        "postgres",
			Tables:        []string{"products"},
			IncludeSchema: &includeSchema,
			// IncludeData is nil
		}

		err := SaveProfile(profileName, cfg)
		require.NoError(t, err)

		// Verify file content
		filePath := filepath.Join(profileDir, profileName+".yaml")
		contentBytes, err := os.ReadFile(filePath)
		require.NoError(t, err)
		content := string(contentBytes)

		assert.Contains(t, content, "database: saved_db")
		assert.Contains(t, content, "host: savehost")
		assert.Contains(t, content, "port: 5432")
		assert.Contains(t, content, "username: saveuser")
		assert.Contains(t, content, "driver: postgres")
		assert.Contains(t, content, "tables:")
		assert.Contains(t, content, "- products")
		assert.Contains(t, content, "includeschema: true")
		assert.NotContains(t, content, "includedata:") // Should not be present if nil
		assert.NotContains(t, content, "password:")    // Should not be present if empty
	})

	t.Run("Overwrite existing profile", func(t *testing.T) {
		profileName := "overwrite-profile"
		initialContent := "database: initial_db"
		createDummyProfile(t, profileDir, profileName, initialContent)

		cfg := &ProfileConfig{
			Database: "overwritten_db",
			Host:     "newhost",
		}

		err := SaveProfile(profileName, cfg)
		require.NoError(t, err)

		// Verify file content
		filePath := filepath.Join(profileDir, profileName+".yaml")
		contentBytes, err := os.ReadFile(filePath)
		require.NoError(t, err)
		content := string(contentBytes)

		assert.Contains(t, content, "database: overwritten_db")
		assert.Contains(t, content, "host: newhost")
		assert.NotContains(t, content, "initial_db") // Initial content should be gone
	})

	t.Run("Save profile with password", func(t *testing.T) {
		profileName := "password-profile"
		cfg := &ProfileConfig{
			Database: "pw_db",
			Password: "supersecret", // Password should be saved
		}

		err := SaveProfile(profileName, cfg)
		require.NoError(t, err)

		filePath := filepath.Join(profileDir, profileName+".yaml")
		contentBytes, err := os.ReadFile(filePath)
		require.NoError(t, err)
		content := string(contentBytes)

		assert.Contains(t, content, "database: pw_db")
		assert.Contains(t, content, "password: supersecret") // Verify password is included
	})

	// Add test case for directory not existing initially (SaveProfile should create it)
	t.Run("Save profile when directory does not exist", func(t *testing.T) {
		// Use a different base temp dir that doesn't have 'profiles' subdir initially
		nonExistentBaseDir, cleanupNonExistent := setupTestDir(t)
		defer cleanupNonExistent()
		os.RemoveAll(filepath.Join(nonExistentBaseDir, "profiles")) // Remove the profiles subdir

		// Set SYNCDB_PATH to this new base dir
		originalPath := os.Getenv("SYNCDB_PATH")
		os.Setenv("SYNCDB_PATH", nonExistentBaseDir)
		defer os.Setenv("SYNCDB_PATH", originalPath)

		profileName := "create-dir-profile"
		cfg := &ProfileConfig{Database: "created_dir_db"}

		err := SaveProfile(profileName, cfg)
		require.NoError(t, err, "SaveProfile should create the directory")

		// Verify file exists
		profilePath, _ := GetProfilePath(profileName) // Get path based on the temp SYNCDB_PATH
		_, err = os.Stat(profilePath)
		assert.NoError(t, err, "Profile file should exist after saving")
	})
}

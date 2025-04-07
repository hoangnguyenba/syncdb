package profile

// Note: Need to run 'go get gopkg.in/yaml.v3' later

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"gopkg.in/yaml.v3"
)

// ProfileConfig holds the configuration parameters stored within a profile.
type ProfileConfig struct {
	Host               string   `yaml:"host,omitempty"`
	Port               int      `yaml:"port,omitempty"`
	Username           string   `yaml:"username,omitempty"`
	Password           string   `yaml:"password,omitempty"` // Stored in plain text
	Database           string   `yaml:"database"`           // Required field
	Driver             string   `yaml:"driver,omitempty"`
	Tables             []string `yaml:"tables,omitempty"`
	IncludeSchema      *bool    `yaml:"include_schema,omitempty"` // Pointer to distinguish between false and not set
	IncludeData        *bool    `yaml:"include_data,omitempty"`   // Pointer to distinguish between false and not set
	Condition          string   `yaml:"condition,omitempty"`
	ExcludeTable       []string `yaml:"exclude_table,omitempty"`
	ExcludeTableSchema []string `yaml:"exclude_table_schema,omitempty"`
	ExcludeTableData   []string `yaml:"exclude_table_data,omitempty"`
}

// GetProfileDir determines the directory where profile files are stored.
// It checks the SYNCDB_PATH environment variable first, then falls back
// to a default location based on the operating system.
// It also ensures the directory exists, creating it if necessary.
func GetProfileDir() (string, error) {
	// Check environment variable first
	if syncDBPath := os.Getenv("SYNCDB_PATH"); syncDBPath != "" {
		profileDir := filepath.Join(syncDBPath, "profiles")
		if err := os.MkdirAll(profileDir, 0750); err != nil {
			return "", fmt.Errorf("failed to create profile directory specified by SYNCDB_PATH (%s): %w", profileDir, err)
		}
		return profileDir, nil
	}

	// Fallback to default location based on OS
	var configDir string
	var err error

	switch runtime.GOOS {
	case "windows":
		configDir, err = os.UserConfigDir()
		if err != nil {
			return "", fmt.Errorf("failed to get user config directory on Windows: %w", err)
		}
	case "darwin":
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("failed to get user home directory on macOS: %w", err)
		}
		configDir = filepath.Join(homeDir, "Library", "Application Support")
	case "linux":
		configDir, err = os.UserConfigDir() // Uses XDG_CONFIG_HOME or defaults to ~/.config
		if err != nil {
			// Fallback if UserConfigDir fails (e.g., in minimal environments)
			homeDir, err := os.UserHomeDir()
			if err != nil {
				return "", fmt.Errorf("failed to get user home directory on Linux: %w", err)
			}
			configDir = filepath.Join(homeDir, ".config")
		}
	default: // Other OS (e.g., BSD) - default to ~/.config
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("failed to get user home directory: %w", err)
		}
		configDir = filepath.Join(homeDir, ".config")
	}

	profileDir := filepath.Join(configDir, "syncdb", "profiles")
	if err := os.MkdirAll(profileDir, 0750); err != nil { // Use 0750 for permissions
		return "", fmt.Errorf("failed to create default profile directory (%s): %w", profileDir, err)
	}

	return profileDir, nil
}

// GetProfilePath constructs the full path to a specific profile file.
func GetProfilePath(profileName string) (string, error) {
	if profileName == "" {
		return "", errors.New("profile name cannot be empty")
	}
	profileDir, err := GetProfileDir()
	if err != nil {
		return "", err // Error already formatted by GetProfileDir
	}
	// Basic validation for profile name (prevent path traversal, etc.)
	// For now, just ensure it's not empty and join it. More robust validation could be added.
	fileName := fmt.Sprintf("%s.yaml", profileName)
	return filepath.Join(profileDir, fileName), nil
}

// LoadProfile reads and unmarshals a profile configuration file.
func LoadProfile(profileName string) (*ProfileConfig, error) {
	filePath, err := GetProfilePath(profileName)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("profile '%s' not found at %s", profileName, filePath)
		}
		return nil, fmt.Errorf("failed to read profile file %s: %w", filePath, err)
	}

	var config ProfileConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse profile file %s: %w", filePath, err)
	}

	// Basic validation after loading
	if config.Database == "" {
		return nil, fmt.Errorf("profile '%s' is invalid: missing required 'database' field", profileName)
	}

	return &config, nil
}

// SaveProfile marshals and saves a profile configuration to a file.
func SaveProfile(profileName string, config *ProfileConfig) error {
	if config == nil {
		return errors.New("cannot save a nil profile config")
	}
	if config.Database == "" {
		return errors.New("cannot save profile: missing required 'database' field")
	}

	filePath, err := GetProfilePath(profileName)
	if err != nil {
		return err
	}

	// Ensure the directory exists (GetProfilePath should handle this, but double-check)
	if err := os.MkdirAll(filepath.Dir(filePath), 0750); err != nil {
		return fmt.Errorf("failed to ensure profile directory exists for %s: %w", filePath, err)
	}

	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal profile config for '%s': %w", profileName, err)
	}

	// Write with appropriate permissions (e.g., 0600 if sensitive, 0640/0644 otherwise)
	// Using 0640 assuming group read might be useful, owner write.
	if err := os.WriteFile(filePath, data, 0640); err != nil {
		return fmt.Errorf("failed to write profile file %s: %w", filePath, err)
	}

	return nil
}

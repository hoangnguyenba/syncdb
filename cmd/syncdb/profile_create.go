package main

import (
	"fmt"
	"os"
	"strings" // Ensure strings is imported

	"github.com/hoangnguyenba/syncdb/pkg/profile"
	"github.com/spf13/cobra"
)

func newProfileCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <profile-name>",
		Short: "Create a new configuration profile",
		Long:  `Creates a new named configuration profile by saving the provided flags to a file.`,
		Args:  cobra.ExactArgs(1), // Requires exactly one argument: the profile name
		RunE:  runProfileCreate,
	}

	// Add flags corresponding to ProfileConfig fields
	addProfileConfigFlags(cmd)

	return cmd
}

// Note: addProfileConfigFlags is now defined in common_flags.go

func runProfileCreate(cmd *cobra.Command, args []string) error {
	profileName := args[0]
	flags := cmd.Flags()

	// --- Basic Validation ---
	if profileName == "" {
		return fmt.Errorf("profile name cannot be empty")
	}
	// Add more validation for profileName if needed (e.g., allowed characters)

	// Check if profile already exists
	_, err := profile.LoadProfile(profileName)
	if err == nil {
		// Profile exists, return error
		return fmt.Errorf("profile '%s' already exists. Use 'profile update' to modify.", profileName)
	} else {
		// Check if the error is *not* a "not found" error
		profilePath, _ := profile.GetProfilePath(profileName) // Get path for error message
		if !os.IsNotExist(err) && !strings.Contains(err.Error(), fmt.Sprintf("profile '%s' not found", profileName)) {
			// A different error occurred during loading (e.g., permissions, parsing error on existing file?)
			return fmt.Errorf("error checking for existing profile '%s' at %s: %w", profileName, profilePath, err)
		}
		// If it is a "not found" error, that's expected, so we continue.
	}

	// --- Populate ProfileConfig from flags ---
	cfg := profile.ProfileConfig{}

	// Required field validation
	cfg.Database, _ = flags.GetString("database")
	if cfg.Database == "" {
		return fmt.Errorf("flag --database is required to create a profile")
	}

	// Optional fields
	cfg.Host, _ = flags.GetString("host")
	cfg.Port, _ = flags.GetInt("port")
	cfg.Username, _ = flags.GetString("username")
	cfg.Password, _ = flags.GetString("password")
	cfg.Driver, _ = flags.GetString("driver")
	cfg.Tables, _ = flags.GetStringSlice("tables")
	cfg.Condition, _ = flags.GetString("condition")
	cfg.ExcludeTable, _ = flags.GetStringSlice("exclude-table")
	cfg.ExcludeTableSchema, _ = flags.GetStringSlice("exclude-table-schema")
	cfg.ExcludeTableData, _ = flags.GetStringSlice("exclude-table-data")

	// Handle boolean flags (need to check if they were set)
	if flags.Changed("profile-include-schema") {
		val, _ := flags.GetBool("profile-include-schema")
		cfg.IncludeSchema = &val
	}
	if flags.Changed("profile-include-data") {
		val, _ := flags.GetBool("profile-include-data")
		cfg.IncludeData = &val
	}

	// --- Save Profile ---
	err = profile.SaveProfile(profileName, &cfg)
	if err != nil {
		return fmt.Errorf("failed to save profile '%s': %w", profileName, err)
	}

	fmt.Printf("Successfully created profile '%s'.\n", profileName)
	if cfg.Password != "" {
		fmt.Println("Warning: Password was saved in plain text in the profile file.")
	}

	return nil
}

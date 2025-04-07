package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/hoangnguyenba/syncdb/pkg/profile"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// Note: addProfileConfigFlags is now defined in common_flags.go

func newProfileUpdateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update <profile-name>",
		Short: "Update an existing configuration profile or create it if it doesn't exist",
		Long:  `Modifies an existing profile configuration file with the provided flags. If the profile doesn't exist, it will be created.`,
		Args:  cobra.ExactArgs(1), // Requires exactly one argument: the profile name
		RunE:  runProfileUpdate,
	}

	// Add flags corresponding to ProfileConfig fields
	addProfileConfigFlags(cmd)

	return cmd
}

func runProfileUpdate(cmd *cobra.Command, args []string) error {
	profileName := args[0]
	flags := cmd.Flags()

	// --- Basic Validation ---
	if profileName == "" {
		return fmt.Errorf("profile name cannot be empty")
	}

	// --- Load existing profile or create new ---
	cfg, err := profile.LoadProfile(profileName)
	if err != nil {
		// If error is "not found", create a new empty config
		profilePath, _ := profile.GetProfilePath(profileName) // Get path for error message
		if os.IsNotExist(err) || strings.Contains(err.Error(), fmt.Sprintf("profile '%s' not found", profileName)) {
			fmt.Printf("Profile '%s' not found, creating a new one.\n", profileName)
			cfg = &profile.ProfileConfig{} // Initialize empty config
		} else {
			// A different error occurred during loading
			return fmt.Errorf("error loading profile '%s' from %s: %w", profileName, profilePath, err)
		}
	}

	// --- Update fields based on changed flags ---
	flags.Visit(func(f *pflag.Flag) {
		// Use Visit instead of Changed because Changed doesn't work well with default values
		// We only update fields explicitly provided by the user via flags
		switch f.Name {
		case "host":
			cfg.Host, _ = flags.GetString("host")
		case "port":
			cfg.Port, _ = flags.GetInt("port")
		case "username":
			cfg.Username, _ = flags.GetString("username")
		case "password":
			cfg.Password, _ = flags.GetString("password")
		case "database":
			cfg.Database, _ = flags.GetString("database")
		case "driver":
			cfg.Driver, _ = flags.GetString("driver")
		case "tables":
			cfg.Tables, _ = flags.GetStringSlice("tables")
		case "profile-include-schema":
			val, _ := flags.GetBool("profile-include-schema")
			cfg.IncludeSchema = &val
		case "profile-include-data":
			val, _ := flags.GetBool("profile-include-data")
			cfg.IncludeData = &val
		case "condition":
			cfg.Condition, _ = flags.GetString("condition")
		case "exclude-table":
			cfg.ExcludeTable, _ = flags.GetStringSlice("exclude-table")
		case "exclude-table-schema":
			cfg.ExcludeTableSchema, _ = flags.GetStringSlice("exclude-table-schema")
		case "exclude-table-data":
			cfg.ExcludeTableData, _ = flags.GetStringSlice("exclude-table-data")
		}
	})

	// --- Save Profile ---
	err = profile.SaveProfile(profileName, cfg)
	if err != nil {
		return fmt.Errorf("failed to save profile '%s': %w", profileName, err)
	}

	fmt.Printf("Successfully updated profile '%s'.\n", profileName)
	// Check if the password flag was explicitly set during this update
	if flags.Changed("password") && cfg.Password != "" {
		fmt.Println("Warning: Password was saved in plain text in the profile file.")
	}

	return nil
}

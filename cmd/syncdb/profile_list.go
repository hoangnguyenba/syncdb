package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/hoangnguyenba/syncdb/pkg/profile"
	"github.com/spf13/cobra"
)

func newProfileListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all available configuration profiles",
		Long:  `Lists the names of all saved configuration profiles found in the profile directory.`,
		Args:  cobra.NoArgs, // No arguments expected
		RunE:  runProfileList,
	}
	// No flags needed for list command currently
	return cmd
}

func runProfileList(cmd *cobra.Command, args []string) error {
	profileDir, err := profile.GetProfileDir()
	if err != nil {
		// If the error is that the directory doesn't exist, that's fine, just means no profiles.
		if os.IsNotExist(err) {
			fmt.Println("No profiles found (profile directory does not exist).")
			return nil
		}
		// Otherwise, it's a real error getting the path
		return fmt.Errorf("could not determine profile directory: %w", err)
	}

	files, err := os.ReadDir(profileDir)
	if err != nil {
		// Handle case where directory exists but cannot be read
		if os.IsNotExist(err) {
			fmt.Println("No profiles found (profile directory does not exist).")
			return nil
		}
		return fmt.Errorf("could not read profile directory '%s': %w", profileDir, err)
	}

	var profileNames []string
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".yaml") {
			profileName := strings.TrimSuffix(file.Name(), ".yaml")
			profileNames = append(profileNames, profileName)
		}
	}

	if len(profileNames) == 0 {
		fmt.Printf("No profiles found in %s.\n", profileDir)
		return nil
	}

	fmt.Println("Available Profiles:")
	for _, name := range profileNames {
		// Optional Enhancement: Load key details (driver, database)
		// cfg, loadErr := profile.LoadProfile(name)
		// if loadErr == nil {
		//  fmt.Printf("- %s (%s, %s)\n", name, cfg.Driver, cfg.Database)
		// } else {
		//  fmt.Printf("- %s (Error loading details: %v)\n", name, loadErr)
		// }
		fmt.Printf("- %s\n", name) // Simple listing for now
	}

	return nil
}

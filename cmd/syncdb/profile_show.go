package main

import (
	"fmt"

	"github.com/hoangnguyenba/syncdb/pkg/profile"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3" // Import YAML library
)

func newProfileShowCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show <profile-name>",
		Short: "Show the configuration details of a specific profile",
		Long:  `Loads and displays the contents of the specified profile file in YAML format.`,
		Args:  cobra.ExactArgs(1), // Requires exactly one argument: the profile name
		RunE:  runProfileShow,
	}
	// No flags needed for show command
	return cmd
}

func runProfileShow(cmd *cobra.Command, args []string) error {
	profileName := args[0]

	// Basic validation
	if profileName == "" {
		return fmt.Errorf("profile name cannot be empty")
	}

	// Load the profile
	cfg, err := profile.LoadProfile(profileName)
	if err != nil {
		// Error is already formatted by LoadProfile (includes path)
		return fmt.Errorf("failed to load profile '%s': %w", profileName, err)
	}

	// Marshal the loaded config back to YAML
	yamlData, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to marshal profile '%s' to YAML: %w", profileName, err)
	}

	// Print the YAML output
	fmt.Printf("--- Profile: %s ---\n", profileName)
	fmt.Println(string(yamlData))

	return nil
}

package main

import (
	"fmt"
	"os"

	"github.com/hoangnguyenba/syncdb/pkg/profile"
	"github.com/spf13/cobra"
)

func newProfileDeleteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <profile-name>",
		Short: "Delete a configuration profile",
		Long:  `Deletes the specified configuration profile file. Requires the --force flag to proceed.`,
		Args:  cobra.ExactArgs(1), // Requires exactly one argument: the profile name
		RunE:  runProfileDelete,
	}

	// Add the required --force flag
	cmd.Flags().Bool("force", false, "Required flag to confirm deletion")
	cmd.MarkFlagRequired("force") // Make the --force flag mandatory

	return cmd
}

func runProfileDelete(cmd *cobra.Command, args []string) error {
	profileName := args[0]
	force, _ := cmd.Flags().GetBool("force") // Already marked as required

	// Basic validation
	if profileName == "" {
		return fmt.Errorf("profile name cannot be empty")
	}

	// Double-check force flag (although Cobra should enforce it)
	if !force {
		return fmt.Errorf("must use the --force flag to delete profile '%s'", profileName)
	}

	// Get the expected path
	profilePath, err := profile.GetProfilePath(profileName)
	if err != nil {
		// Error determining path (e.g., home dir issue)
		return fmt.Errorf("could not determine path for profile '%s': %w", profileName, err)
	}

	// Check if the profile file actually exists
	if _, err := os.Stat(profilePath); os.IsNotExist(err) {
		return fmt.Errorf("profile '%s' not found at %s", profileName, profilePath)
	} else if err != nil {
		// Other error checking file (e.g., permissions)
		return fmt.Errorf("error checking profile file '%s': %w", profilePath, err)
	}

	// Proceed with deletion
	err = os.Remove(profilePath)
	if err != nil {
		return fmt.Errorf("failed to delete profile file '%s': %w", profilePath, err)
	}

	fmt.Printf("Successfully deleted profile '%s' (%s).\n", profileName, profilePath)
	return nil
}

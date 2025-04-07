package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	if err := Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

var (
	rootCmd = &cobra.Command{
		Use:   "syncdb",
		Short: "A CLI tool for syncing databases through export and import operations.",
		Long:  `This tool allows you to export and import database data using various storage options.`,
	}
)

// newProfileCommand creates the parent 'profile' command
func newProfileCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "profile",
		Short: "Manage configuration profiles",
		Long:  `Create, update, and list configuration profiles for easier command execution.`,
		// Run: func(cmd *cobra.Command, args []string) { // Optional: Add help text if called without subcommand
		// 	cmd.Help()
		// },
	}
	// Add subcommands
	cmd.AddCommand(newProfileCreateCommand())
	cmd.AddCommand(newProfileUpdateCommand())
	cmd.AddCommand(newProfileListCommand())
	cmd.AddCommand(newProfileDeleteCommand())
	cmd.AddCommand(newProfileShowCommand()) // Add show command
	return cmd
}

func init() {
	rootCmd.AddCommand(newExportCommand())
	rootCmd.AddCommand(newImportCommand())
	rootCmd.AddCommand(newProfileCommand()) // Add the profile command
}

func Execute() error {
	return rootCmd.Execute()
}

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

func init() {
	rootCmd.AddCommand(newExportCommand())
	rootCmd.AddCommand(newImportCommand())
}

func Execute() error {
	return rootCmd.Execute()
}

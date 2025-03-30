package main

import (
	"github.com/hoangnguyenba/syncdb/pkg/config"
	"github.com/spf13/cobra"
)

// getStringFlagWithConfigFallback gets a string value, prioritizing the command flag
// over the configuration value if the flag was explicitly set.
func getStringFlagWithConfigFallback(cmd *cobra.Command, flagName string, configValue string) string {
	if cmd.Flags().Changed(flagName) {
		val, _ := cmd.Flags().GetString(flagName)
		return val
	}
	return configValue
}

// getIntFlagWithConfigFallback gets an integer value, prioritizing the command flag
// over the configuration value if the flag was explicitly set.
func getIntFlagWithConfigFallback(cmd *cobra.Command, flagName string, configValue int) int {
	if cmd.Flags().Changed(flagName) {
		val, _ := cmd.Flags().GetInt(flagName)
		return val
	}
	return configValue
}

// getBoolFlagWithConfigFallback gets a boolean value, prioritizing the command flag
// over the configuration value if the flag was explicitly set.
// Note: Cobra doesn't track 'Changed' for bools if they match the default.
// This helper assumes if the flag exists, its value should be used,
// otherwise fallback to config (which might not be ideal for all bools).
// Consider if a different logic is needed for specific boolean flags where config should override a default flag.
func getBoolFlagWithConfigFallback(cmd *cobra.Command, flagName string, configValue bool) bool {
	if cmd.Flags().Changed(flagName) {
		val, _ := cmd.Flags().GetBool(flagName)
		return val
	}
	// If flag wasn't explicitly changed, return the config value.
	// This assumes config takes precedence over the flag's default value.
	return configValue
}


// getStringSliceFlagWithConfigFallback gets a string slice, prioritizing the command flag
// over the configuration value if the flag was explicitly set.
func getStringSliceFlagWithConfigFallback(cmd *cobra.Command, flagName string, configValue []string) []string {
	if cmd.Flags().Changed(flagName) {
		val, _ := cmd.Flags().GetStringSlice(flagName)
		return val
	}
	// Return a copy to avoid modifying the original config slice
	if configValue == nil {
		return nil
	}
	ret := make([]string, len(configValue))
	copy(ret, configValue)
	return ret
}

// populateCommonArgsFromFlagsAndConfig fills a CommonArgs struct by reading flags
// and falling back to the provided CommonConfig.
func populateCommonArgsFromFlagsAndConfig(cmd *cobra.Command, cfg config.CommonConfig) CommonArgs {
	args := CommonArgs{}
	args.Host = getStringFlagWithConfigFallback(cmd, "host", cfg.Host)
	args.Port = getIntFlagWithConfigFallback(cmd, "port", cfg.Port)
	args.Username = getStringFlagWithConfigFallback(cmd, "username", cfg.Username)
	args.Password = getStringFlagWithConfigFallback(cmd, "password", cfg.Password)
	args.Database = getStringFlagWithConfigFallback(cmd, "database", cfg.Database)
	args.Driver = getStringFlagWithConfigFallback(cmd, "driver", cfg.Driver)
	args.Tables = getStringSliceFlagWithConfigFallback(cmd, "tables", cfg.Tables)
	args.FolderPath = getStringFlagWithConfigFallback(cmd, "folder-path", cfg.FolderPath)
	args.Storage = getStringFlagWithConfigFallback(cmd, "storage", cfg.Storage)
	args.S3Bucket = getStringFlagWithConfigFallback(cmd, "s3-bucket", cfg.S3Bucket)
	args.S3Region = getStringFlagWithConfigFallback(cmd, "s3-region", cfg.S3Region)
	args.Format = getStringFlagWithConfigFallback(cmd, "format", cfg.Format)

	// Boolean flags: Read directly from flags as they control operation, not persistent config.
	// The default values are set correctly in AddSharedFlags based on isImportCmd.
	args.IncludeSchema, _ = cmd.Flags().GetBool("include-schema")
	args.IncludeData, _ = cmd.Flags().GetBool("include-data")
	args.IncludeViewData, _ = cmd.Flags().GetBool("include-view-data")
	args.Zip, _ = cmd.Flags().GetBool("zip")
	args.Base64, _ = cmd.Flags().GetBool("base64")

	// Exclusions use helpers as they can come from config or flags
	args.ExcludeTable = getStringSliceFlagWithConfigFallback(cmd, "exclude-table", cfg.ExcludeTable)
	args.ExcludeTableSchema = getStringSliceFlagWithConfigFallback(cmd, "exclude-table-schema", cfg.ExcludeTableSchema)
	args.ExcludeTableData = getStringSliceFlagWithConfigFallback(cmd, "exclude-table-data", cfg.ExcludeTableData)

	return args
}

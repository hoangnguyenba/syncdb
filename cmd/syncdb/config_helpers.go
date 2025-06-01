package main

import (
	"fmt"

	"github.com/hoangnguyenba/syncdb/pkg/config"
	"github.com/hoangnguyenba/syncdb/pkg/profile" // Import the profile package
	"github.com/spf13/cobra"
)

// Helper function to determine the final string value based on priority
func resolveStringValue(cmd *cobra.Command, flagName string, envValue string, profileValue string, defaultValue string) string {
	if cmd.Flags().Changed(flagName) {
		val, _ := cmd.Flags().GetString(flagName)
		return val // Flag has highest priority
	}
	if envValue != "" && envValue != defaultValue { // Check if env var is set and different from default
		return envValue // Env var has next priority
	}
	if profileValue != "" {
		return profileValue // Profile has next priority
	}
	return defaultValue // Default value is the lowest priority
}

// Helper function to determine the final int value based on priority
func resolveIntValue(cmd *cobra.Command, flagName string, envValue int, profileValue int, defaultValue int) int {
	if cmd.Flags().Changed(flagName) {
		val, _ := cmd.Flags().GetInt(flagName)
		return val // Flag has highest priority
	}
	if envValue != 0 && envValue != defaultValue { // Check if env var is set and different from default
		return envValue // Env var has next priority
	}
	// Check if profileValue is explicitly set (assuming 0 might be a valid profile value, though unlikely for port)
	// For simplicity, if profileValue is non-zero, use it. Adjust if 0 is a valid profile setting.
	if profileValue != 0 {
		return profileValue // Profile has next priority
	}
	return defaultValue // Default value is the lowest priority
}

// Helper function to determine the final string slice value based on priority
func resolveStringSliceValue(cmd *cobra.Command, flagName string, envValue []string, profileValue []string) []string {
	if cmd.Flags().Changed(flagName) {
		val, _ := cmd.Flags().GetStringSlice(flagName)
		return val // Flag has highest priority
	}
	// Check if env var slice is non-empty
	if len(envValue) > 0 {
		// Return a copy to avoid modifying the original config slice
		ret := make([]string, len(envValue))
		copy(ret, envValue)
		return ret // Env var has next priority
	}
	// Check if profile slice is non-empty
	if len(profileValue) > 0 {
		// Return a copy
		ret := make([]string, len(profileValue))
		copy(ret, profileValue)
		return ret // Profile has next priority
	}
	return []string{} // Default to empty slice
}

// Helper function to determine the final bool value based on priority: Flag > Profile > Default
// Environment variables are not considered here for flags like include-schema/data as they
// are not part of the base CommonConfig loaded from env.
func resolveBoolValueProfile(cmd *cobra.Command, flagName string, profileValue *bool, defaultValue bool) bool {
	flag := cmd.Flags().Lookup(flagName)
	// Prioritize flag if it was explicitly set
	if flag != nil && cmd.Flags().Changed(flagName) {
		val, _ := cmd.Flags().GetBool(flagName)
		return val
	}
	// Check profile value if the flag wasn't explicitly set
	if profileValue != nil {
		return *profileValue
	}
	// Fallback to the default value defined for the flag
	return defaultValue
}

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

// populateCommonArgsFromFlagsAndConfig fills a CommonArgs struct by reading flags, environment variables (via cfg),
// and profile settings, respecting the priority: Flag > Env Var > Profile > Default.
// It now returns an error if profile loading fails.
func populateCommonArgsFromFlagsAndConfig(cmd *cobra.Command, cfg config.CommonConfig, profileName string) (CommonArgs, error) {
	args := CommonArgs{}
	var loadedProfile *profile.ProfileConfig
	var err error

	// Load profile if specified
	if profileName != "" {
		loadedProfile, err = profile.LoadProfile(profileName)
		if err != nil {
			// Return error if profile specified but not found/parsable
			return args, fmt.Errorf("failed to load profile '%s': %w", profileName, err)
		}
		fmt.Printf("Loaded profile '%s'\n", profileName) // Debug/Info message
	}

	// --- Resolve values based on priority ---
	profileHost := ""
	profilePort := 0
	profileUsername := ""
	profilePassword := ""
	profileDatabase := ""
	profileDriver := ""
	var profileTables []string
	var profileIncludeSchema *bool
	var profileIncludeData *bool
	var profileExcludeTable []string
	var profileExcludeTableSchema []string
	var profileExcludeTableData []string

	if loadedProfile != nil {
		profileHost = loadedProfile.Host
		profilePort = loadedProfile.Port
		profileUsername = loadedProfile.Username
		profilePassword = loadedProfile.Password
		profileDatabase = loadedProfile.Database
		profileDriver = loadedProfile.Driver
		profileTables = loadedProfile.Tables
		profileIncludeSchema = loadedProfile.IncludeSchema
		profileIncludeData = loadedProfile.IncludeData
		profileExcludeTable = loadedProfile.ExcludeTable
		profileExcludeTableSchema = loadedProfile.ExcludeTableSchema
		profileExcludeTableData = loadedProfile.ExcludeTableData
	}

	// Database connection
	args.Host = resolveStringValue(cmd, "host", cfg.Host, profileHost, "localhost")
	args.Port = resolveIntValue(cmd, "port", cfg.Port, profilePort, 3306) // Assuming 3306 is default
	args.Username = resolveStringValue(cmd, "username", cfg.Username, profileUsername, "")
	args.Password = resolveStringValue(cmd, "password", cfg.Password, profilePassword, "") // Handle password securely later if needed
	args.Database = resolveStringValue(cmd, "database", cfg.Database, profileDatabase, "") // Database is required, validation happens later
	args.Driver = resolveStringValue(cmd, "driver", cfg.Driver, profileDriver, "mysql")    // Assuming mysql is default

	// Table selection
	args.Tables = resolveStringSliceValue(cmd, "tables", cfg.Tables, profileTables)

	// Path and Storage (Storage related flags are NOT part of profile)
	args.Path = resolveStringValue(cmd, "path", "", "", "")                                               // Not in profile
	args.Storage = resolveStringValue(cmd, "storage", cfg.Storage, "", "local")                           // Not in profile
	args.S3Bucket = resolveStringValue(cmd, "s3-bucket", cfg.S3Bucket, "", "")                            // Not in profile
	args.S3Region = resolveStringValue(cmd, "s3-region", cfg.S3Region, "", "")                            // Not in profile


	// Format/Encoding (Format is NOT part of profile)
	args.Format = resolveStringValue(cmd, "format", cfg.Format, "", "sql") // Not in profile
	// Base64 is a command-time flag, not stored in profile
	args.Base64, _ = cmd.Flags().GetBool("base64")

	// Content flags (These ARE part of profile)
	// Get the default values from the flag definitions
	includeSchemaDefault, _ := cmd.Flags().GetBool("include-schema")
	includeDataDefault, _ := cmd.Flags().GetBool("include-data")
	// Use resolveBoolValueProfile which has priority: Flag > Profile > Default
	args.IncludeSchema = resolveBoolValueProfile(cmd, "include-schema", profileIncludeSchema, includeSchemaDefault)
	args.IncludeData = resolveBoolValueProfile(cmd, "include-data", profileIncludeData, includeDataDefault)

	// IncludeViewData is a command-time flag, not stored in profile
	args.IncludeViewData, _ = cmd.Flags().GetBool("include-view-data")

	// Exclusions (These ARE part of profile)
	args.ExcludeTable = resolveStringSliceValue(cmd, "exclude-table", cfg.ExcludeTable, profileExcludeTable)
	args.ExcludeTableSchema = resolveStringSliceValue(cmd, "exclude-table-schema", cfg.ExcludeTableSchema, profileExcludeTableSchema)
	args.ExcludeTableData = resolveStringSliceValue(cmd, "exclude-table-data", cfg.ExcludeTableData, profileExcludeTableData)

	// Zip is a command-time flag, not stored in profile
	args.Zip, _ = cmd.Flags().GetBool("zip")
	// Import-specific flags (not stored in profile)
	args.DisableForeignKeyCheck, _ = cmd.Flags().GetBool("disable-foreign-key-check")
	args.Drop, _ = cmd.Flags().GetBool("drop")
	args.Truncate, _ = cmd.Flags().GetBool("truncate")
	// Note: The 'Condition' field from the profile (loadedProfile.Condition) is not directly mapped to CommonArgs.
	// We leave it for specific handling in export.go

	// FileName: only from flag, not from config/profile
	args.FileName, _ = cmd.Flags().GetString("file-name")
	args.QuerySeparator = getStringFlagWithConfigFallback(cmd, "query-separator", "\n--SYNCDB_QUERY_SEPARATOR--\n")
	return args, nil
}

// --- Remove the invalid helper methods defined on profile.ProfileConfig ---
// func (p *profile.ProfileConfig) GetHost() string { ... }
// ... and so on for GetPort, GetUsername, etc. ...

package db

import (
	"database/sql"
	"fmt"
	"strings"
)

// Common database schemas
const (
	SchemaMySQL    = "DATABASE()"
	SchemaPostgres = "public"
)

// Common database placeholders
const (
	PlaceholderMySQL    = "?"
	PlaceholderPostgres = "$%d"
)

// GetDriverConfig returns the configuration for a specific database driver
func GetDriverConfig(driver string) (schema, placeholder string, err error) {
	switch driver {
	case DriverMySQL:
		return SchemaMySQL, PlaceholderMySQL, nil
	case DriverPostgres:
		return SchemaPostgres, PlaceholderPostgres, nil
	default:
		return "", "", fmt.Errorf("%w: %s", ErrUnsupportedDriver, driver)
	}
}

// EscapeIdentifier escapes a database identifier based on the driver
func EscapeIdentifier(driver, identifier string) string {
	switch driver {
	case DriverMySQL:
		return fmt.Sprintf("`%s`", identifier)
	case DriverPostgres:
		return fmt.Sprintf(`"%s"`, identifier)
	default:
		return identifier
	}
}

// BuildPlaceholders creates a string of placeholders for SQL queries
func BuildPlaceholders(driver string, count int) string {
	switch driver {
	case DriverMySQL:
		return strings.Repeat("?,", count-1) + "?"
	case DriverPostgres:
		placeholders := make([]string, count)
		for i := 0; i < count; i++ {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}
		return strings.Join(placeholders, ",")
	default:
		return strings.Repeat("?,", count-1) + "?"
	}
}

// ValidateTableName checks if a table name is valid
func ValidateTableName(tableName string) error {
	if tableName == "" {
		return fmt.Errorf("%w: table name cannot be empty", ErrInvalidQuery)
	}
	if strings.ContainsAny(tableName, "`\"'") {
		return fmt.Errorf("%w: table name contains invalid characters", ErrInvalidQuery)
	}
	return nil
}

// TableExists checks if a table exists in the database
func TableExists(db *sql.DB, driver, tableName string) (bool, error) {
	schema, _, err := GetDriverConfig(driver)
	if err != nil {
		return false, err
	}

	var query string
	switch driver {
	case DriverMySQL:
		query = fmt.Sprintf(`
			SELECT COUNT(*)
			FROM information_schema.tables
			WHERE table_name = ?
			AND table_schema = %s
		`, schema)
	case DriverPostgres:
		query = fmt.Sprintf(`
			SELECT COUNT(*)
			FROM information_schema.tables
			WHERE table_name = $1
			AND table_schema = %s
		`, schema)
	default:
		return false, fmt.Errorf("%w: %s", ErrUnsupportedDriver, driver)
	}

	var count int
	err = db.QueryRow(query, tableName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check if table exists: %w", err)
	}

	return count > 0, nil
}

// SanitizeSQL sanitizes SQL input to prevent SQL injection
func SanitizeSQL(input string) string {
	// Remove common SQL injection patterns
	patterns := []string{
		"--",
		";",
		"/*",
		"*/",
		"@@",
		"xp_",
		"sp_",
		"exec",
		"execute",
		"union",
		"select",
		"insert",
		"update",
		"delete",
		"drop",
		"alter",
		"create",
		"truncate",
	}

	result := input
	for _, pattern := range patterns {
		result = strings.ReplaceAll(result, pattern, "")
	}

	return result
}

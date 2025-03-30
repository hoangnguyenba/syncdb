package db

import (
	"database/sql"
	"fmt"
	"strings"
)

// SchemaInfo contains information about a database table or view
type SchemaInfo struct {
	Name       string
	IsView     bool
	Definition string
	Columns    []string
}

// GetSchema retrieves the schema information for a table or view
func GetSchema(conn *Connection, tableName string) (*SchemaInfo, error) {
	isView, err := checkTableIsView(conn.DB, tableName, conn.Config.Driver)
	if err != nil {
		return nil, fmt.Errorf("failed to check if table is view: %w", err)
	}

	columns, err := getSchemaColumnNames(conn.DB, tableName, conn.Config.Driver)
	if err != nil {
		return nil, fmt.Errorf("failed to get table columns: %w", err)
	}

	definition, err := getDefinition(conn, tableName, isView)
	if err != nil {
		return nil, fmt.Errorf("failed to get definition: %w", err)
	}

	return &SchemaInfo{
		Name:       tableName,
		IsView:     isView,
		Definition: definition,
		Columns:    columns,
	}, nil
}

// getDefinition retrieves the CREATE TABLE/VIEW statement
func getDefinition(conn *Connection, tableName string, isView bool) (string, error) {
	if isView {
		return getViewDefinition(conn, tableName)
	}
	return getTableDefinition(conn, tableName)
}

// getViewDefinition retrieves the CREATE VIEW statement
func getViewDefinition(conn *Connection, tableName string) (string, error) {
	var viewDef string
	var query string

	switch conn.Config.Driver {
	case DriverMySQL:
		query = "SELECT VIEW_DEFINITION FROM information_schema.views WHERE table_name = ?"
	case DriverPostgres:
		query = "SELECT view_definition FROM information_schema.views WHERE table_name = $1 AND table_schema = 'public'"
	default:
		return "", fmt.Errorf("%w: %s", ErrUnsupportedDriver, conn.Config.Driver)
	}

	err := conn.DB.QueryRow(query, tableName).Scan(&viewDef)
	if err != nil {
		return "", fmt.Errorf("failed to get view definition: %w", err)
	}

	return fmt.Sprintf("CREATE VIEW %s AS %s;", tableName, viewDef), nil
}

// getTableDefinition retrieves the CREATE TABLE statement
func getTableDefinition(conn *Connection, tableName string) (string, error) {
	var query string
	switch conn.Config.Driver {
	case DriverMySQL:
		query = fmt.Sprintf("SHOW CREATE TABLE %s", tableName)
	case DriverPostgres:
		query = fmt.Sprintf(`
			SELECT 
				'CREATE TABLE ' || table_name || ' (' ||
				string_agg(
					column_name || ' ' || data_type ||
					CASE 
						WHEN character_maximum_length IS NOT NULL 
						THEN '(' || character_maximum_length || ')'
						ELSE ''
					END ||
					CASE 
						WHEN is_nullable = 'NO' 
						THEN ' NOT NULL'
						ELSE ''
					END,
					', '
				) || ');'
			FROM information_schema.columns
			WHERE table_name = $1
			GROUP BY table_name
		`)
	default:
		return "", fmt.Errorf("%w: %s", ErrUnsupportedDriver, conn.Config.Driver)
	}

	var schema string
	var dummy string // for MySQL's extra column in SHOW CREATE TABLE

	if conn.Config.Driver == DriverMySQL {
		err := conn.DB.QueryRow(query).Scan(&dummy, &schema)
		if err != nil {
			return "", fmt.Errorf("failed to get schema: %w", err)
		}
		if !strings.HasSuffix(schema, ";") {
			schema += ";"
		}
	} else {
		err := conn.DB.QueryRow(query, tableName).Scan(&schema)
		if err != nil {
			return "", fmt.Errorf("failed to get schema: %w", err)
		}
	}

	return schema, nil
}

// getSchemaColumnNames returns a list of column names for a table
func getSchemaColumnNames(db *sql.DB, tableName string, driver string) ([]string, error) {
	var query string
	switch driver {
	case DriverMySQL:
		query = `
			SELECT COLUMN_NAME
			FROM INFORMATION_SCHEMA.COLUMNS
			WHERE TABLE_SCHEMA = DATABASE()
			AND TABLE_NAME = ?
			ORDER BY ORDINAL_POSITION`
	case DriverPostgres:
		query = `
			SELECT column_name
			FROM information_schema.columns
			WHERE table_name = $1
			ORDER BY ordinal_position`
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedDriver, driver)
	}

	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}

	return columns, rows.Err()
}

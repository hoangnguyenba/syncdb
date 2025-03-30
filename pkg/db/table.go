package db

import (
	"database/sql"
	"fmt"
)

// TableInfo contains information about a database table
type TableInfo struct {
	Name         string
	RowCount     int64
	IsView       bool
	Dependencies []string
}

// ListTables returns a list of all tables in the database
func ListTables(conn *Connection) ([]string, error) {
	var query string
	switch conn.Config.Driver {
	case DriverMySQL:
		query = "SHOW TABLES"
	case DriverPostgres:
		query = `
			SELECT table_name 
			FROM information_schema.tables 
			WHERE table_schema = 'public'
		`
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedDriver, conn.Config.Driver)
	}

	rows, err := conn.DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, table)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tables: %w", err)
	}

	return tables, nil
}

// GetTableInfo retrieves information about a table
func GetTableInfo(conn *Connection, tableName string) (*TableInfo, error) {
	isView, err := checkTableIsView(conn.DB, tableName, conn.Config.Driver)
	if err != nil {
		return nil, fmt.Errorf("failed to check if table is view: %w", err)
	}

	rowCount, err := countTableRowCount(conn.DB, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get row count: %w", err)
	}

	deps, err := getTableDependencies(conn.DB, tableName, conn.Config.Driver)
	if err != nil {
		return nil, fmt.Errorf("failed to get dependencies: %w", err)
	}

	return &TableInfo{
		Name:         tableName,
		RowCount:     rowCount,
		IsView:       isView,
		Dependencies: deps,
	}, nil
}

// checkTableIsView checks if a table is actually a view
func checkTableIsView(db *sql.DB, tableName string, driver string) (bool, error) {
	var query string
	switch driver {
	case DriverMySQL:
		query = `
			SELECT COUNT(*)
			FROM information_schema.views
			WHERE table_schema = DATABASE()
			AND table_name = ?`
	case DriverPostgres:
		query = `
			SELECT COUNT(*)
			FROM information_schema.views
			WHERE table_name = $1
			AND table_schema = 'public'`
	default:
		return false, fmt.Errorf("%w: %s", ErrUnsupportedDriver, driver)
	}

	var count int
	err := db.QueryRow(query, tableName).Scan(&count)
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

// countTableRowCount returns the number of rows in a table
func countTableRowCount(db *sql.DB, tableName string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	var count int64
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// getTableDependencies returns a list of tables that the given table depends on
func getTableDependencies(db *sql.DB, tableName string, driver string) ([]string, error) {
	var query string
	switch driver {
	case DriverMySQL:
		query = `
			SELECT DISTINCT TABLE_NAME
			FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
			WHERE REFERENCED_TABLE_SCHEMA = DATABASE()
			AND REFERENCED_TABLE_NAME = ?
			AND REFERENCED_COLUMN_NAME IS NOT NULL`
	case DriverPostgres:
		query = `
			SELECT DISTINCT tc.table_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.constraint_column_usage ccu
				ON tc.constraint_name = ccu.constraint_name
			WHERE tc.constraint_type = 'FOREIGN KEY'
			AND ccu.table_name = $1`
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedDriver, driver)
	}

	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var deps []string
	for rows.Next() {
		var dep string
		if err := rows.Scan(&dep); err != nil {
			return nil, err
		}
		deps = append(deps, dep)
	}

	return deps, rows.Err()
}

// sortTablesByDependencies sorts tables based on their dependencies
func sortTablesByDependencies(tables []string, deps map[string][]string) []string {
	visited := make(map[string]bool)
	temp := make(map[string]bool)
	result := make([]string, 0, len(tables))

	var visit func(table string) error
	visit = func(table string) error {
		if temp[table] {
			return fmt.Errorf("circular dependency detected: %s", table)
		}
		if visited[table] {
			return nil
		}

		temp[table] = true
		for _, dep := range deps[table] {
			if err := visit(dep); err != nil {
				return err
			}
		}
		temp[table] = false
		visited[table] = true
		result = append(result, table)
		return nil
	}

	for _, table := range tables {
		if !visited[table] {
			if err := visit(table); err != nil {
				// If there's a circular dependency, return tables in original order
				return tables
			}
		}
	}

	return result
}

// TruncateTable removes all rows from a table
func TruncateTable(conn *Connection, tableName string) error {
	query := fmt.Sprintf("TRUNCATE TABLE %s", tableName)
	_, err := conn.DB.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to truncate table: %w", err)
	}
	return nil
}

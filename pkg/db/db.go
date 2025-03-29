package db

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// InitDB initializes a database connection
func InitDB(driver, host string, port int, username, password, dbName string) (*sql.DB, error) {
	var dsn string
	switch driver {
	case "mysql":
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", username, password, host, port, dbName)
	case "postgres":
		dsn = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			host, port, username, password, dbName)
	default:
		return nil, fmt.Errorf("unsupported database driver: %s", driver)
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %v", err)
	}

	return db, nil
}

// GetTables returns a list of all tables in the specified database
func GetTables(db *sql.DB, dbName string, driver string) ([]string, error) {
	var query string
	switch driver {
	case "mysql":
		query = "SHOW TABLES"
	case "postgres":
		query = `
			SELECT table_name 
			FROM information_schema.tables 
			WHERE table_schema = 'public'
		`
	default:
		return nil, fmt.Errorf("unsupported database driver: %s", driver)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %v", err)
		}
		tables = append(tables, table)
	}

	return tables, nil
}

// GetTableSchema returns the CREATE TABLE/VIEW statement for the specified table/view
func GetTableSchema(db *sql.DB, table string, driver string) (string, error) {
	// Check if it's a view
	isView, err := IsView(db, table, driver)
	if err != nil {
		return "", err
	}

	if isView {
		// Get view definition
		var viewDef string
		switch driver {
		case "mysql":
			err := db.QueryRow(`
				SELECT VIEW_DEFINITION 
				FROM information_schema.views 
				WHERE table_name = ?
			`, table).Scan(&viewDef)
			if err != nil {
				return "", fmt.Errorf("failed to get view definition: %v", err)
			}
			return fmt.Sprintf("CREATE VIEW %s AS %s", table, viewDef), nil
		case "postgres":
			err := db.QueryRow(`
				SELECT view_definition 
				FROM information_schema.views 
				WHERE table_name = $1 
				AND table_schema = 'public'
			`, table).Scan(&viewDef)
			if err != nil {
				return "", fmt.Errorf("failed to get view definition: %v", err)
			}
			return fmt.Sprintf("CREATE VIEW %s AS %s", table, viewDef), nil
		}
	}

	// If not a view, get table schema
	var query string
	switch driver {
	case "mysql":
		query = fmt.Sprintf("SHOW CREATE TABLE %s", table)
	case "postgres":
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
		return "", fmt.Errorf("unsupported database driver: %s", driver)
	}

	var schema string
	var dummy string // for MySQL's extra column in SHOW CREATE TABLE

	if driver == "mysql" {
		err := db.QueryRow(query).Scan(&dummy, &schema)
		if err != nil {
			return "", fmt.Errorf("failed to get schema: %v", err)
		}
	} else {
		err := db.QueryRow(query, table).Scan(&schema)
		if err != nil {
			return "", fmt.Errorf("failed to get schema: %v", err)
		}
	}

	return schema, nil
}

// GetNonVirtualColumns returns a list of non-virtual column names for the given table
func GetNonVirtualColumns(db *sql.DB, table string, driver string) ([]string, error) {
	var query string
	switch driver {
	case "mysql":
		query = `
			SELECT COLUMN_NAME 
			FROM INFORMATION_SCHEMA.COLUMNS 
			WHERE TABLE_NAME = ? 
			AND (EXTRA NOT LIKE '%VIRTUAL%' AND EXTRA NOT LIKE '%STORED%')
			ORDER BY ORDINAL_POSITION
		`
	case "postgres":
		query = `
			SELECT column_name 
			FROM information_schema.columns 
			WHERE table_name = $1 
			AND table_schema = 'public'
			AND generation_expression = ''
			ORDER BY ordinal_position
		`
	default:
		return nil, fmt.Errorf("unsupported database driver: %s", driver)
	}

	rows, err := db.Query(query, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query table columns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("failed to scan column name: %v", err)
		}
		columns = append(columns, column)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating table columns: %v", err)
	}

	return columns, nil
}

// ExportTableData exports all data from the specified table
func ExportTableData(db *sql.DB, table string, condition string, driver string) ([]map[string]interface{}, error) {
	// Get non-virtual column names
	columns, err := GetNonVirtualColumns(db, table, driver)
	if err != nil {
		return nil, fmt.Errorf("failed to get non-virtual columns: %v", err)
	}

	// Build query with specific columns instead of *
	var columnList string
	switch driver {
	case "mysql":
		// For MySQL, escape column names with backticks
		escapedColumns := make([]string, len(columns))
		for i, col := range columns {
			escapedColumns[i] = fmt.Sprintf("`%s`", col)
		}
		columnList = strings.Join(escapedColumns, ", ")
	case "postgres":
		// For Postgres, escape column names with double quotes
		escapedColumns := make([]string, len(columns))
		for i, col := range columns {
			escapedColumns[i] = fmt.Sprintf(`"%s"`, col)
		}
		columnList = strings.Join(escapedColumns, ", ")
	default:
		columnList = strings.Join(columns, ", ")
	}

	query := fmt.Sprintf("SELECT %s FROM %s", columnList, table)
	if condition != "" {
		query += " WHERE " + condition
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query data: %v", err)
	}
	defer rows.Close()

	// Prepare result slice
	var result []map[string]interface{}

	// Prepare value holders
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Iterate through rows
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		// Create a map for this row
		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}

		result = append(result, row)
	}

	return result, nil
}

// ImportTableData imports data into the specified table
func ImportTableData(db *sql.DB, table string, data []map[string]interface{}, upsert bool, driver string) error {
	if len(data) == 0 {
		return nil // Nothing to import
	}

	// Get column names from the first row
	var columns []string
	for column := range data[0] {
		columns = append(columns, column)
	}

	// Build the base SQL statement
	var sqlStr string
	var placeholders []string

	switch driver {
	case "mysql":
		// Build INSERT or INSERT ... ON DUPLICATE KEY UPDATE statement for MySQL
		sqlStr = fmt.Sprintf("INSERT INTO %s (%s) VALUES ",
			table,
			buildColumnList(columns, driver))

		// Create placeholders for each row
		placeholders = make([]string, len(columns))
		for i := range columns {
			placeholders[i] = "?"
		}
		sqlStr += "(" + strings.Join(placeholders, ", ") + ")"

		if upsert {
			var updates []string
			for _, col := range columns {
				updates = append(updates, fmt.Sprintf("%s = VALUES(%s)", col, col))
			}
			sqlStr += " ON DUPLICATE KEY UPDATE " + strings.Join(updates, ", ")
		}

	case "postgres":
		// Build INSERT or INSERT ... ON CONFLICT statement for PostgreSQL
		sqlStr = fmt.Sprintf("INSERT INTO %s (%s) VALUES ",
			table,
			buildColumnList(columns, driver))

		// Create placeholders for each row
		placeholders = make([]string, len(columns))
		for i := range columns {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}
		sqlStr += "(" + strings.Join(placeholders, ", ") + ")"

		if upsert {
			sqlStr += " ON CONFLICT ON CONSTRAINT " + table + "_pkey DO UPDATE SET "
			var updates []string
			for _, col := range columns {
				updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
			}
			sqlStr += strings.Join(updates, ", ")
		}

	default:
		return fmt.Errorf("unsupported database driver: %s", driver)
	}

	// Prepare the statement
	stmt, err := db.Prepare(sqlStr)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Import each row
	for _, row := range data {
		// Extract values in the same order as columns
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			values[i] = row[col]
		}

		// Execute the statement
		_, err = stmt.Exec(values...)
		if err != nil {
			return fmt.Errorf("failed to import row: %v", err)
		}
	}

	return nil
}

// Helper function to build column list based on driver
func buildColumnList(columns []string, driver string) string {
	switch driver {
	case "mysql":
		return "`" + strings.Join(columns, "`, `") + "`"
	case "postgres":
		return "\"" + strings.Join(columns, "\", \"") + "\""
	default:
		return strings.Join(columns, ", ")
	}
}

// TruncateTable truncates the specified table
func TruncateTable(db *sql.DB, table string) error {
	_, err := db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", table))
	if err != nil {
		return fmt.Errorf("failed to truncate table: %v", err)
	}
	return nil
}

// GetTableRowCount returns the number of rows in the specified table
func GetTableRowCount(db *sql.DB, table string) (int, error) {
	var count int
	err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %v", err)
	}
	return count, nil
}

// GetTableColumns returns an ordered list of column names for the given table
func GetTableColumns(db *sql.DB, table string, driver string) ([]string, error) {
	var query string
	switch driver {
	case "mysql":
		query = `
			SELECT COLUMN_NAME 
			FROM INFORMATION_SCHEMA.COLUMNS 
			WHERE TABLE_NAME = ? 
			ORDER BY ORDINAL_POSITION`
	case "postgres":
		query = `
			SELECT column_name 
			FROM information_schema.columns 
			WHERE table_name = $1 
			ORDER BY ordinal_position`
	default:
		return nil, fmt.Errorf("unsupported database driver: %s", driver)
	}

	rows, err := db.Query(query, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query table columns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("failed to scan column name: %v", err)
		}
		columns = append(columns, column)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating table columns: %v", err)
	}

	return columns, nil
}

// IsView checks if the given table name is actually a view
func IsView(db *sql.DB, table string, driver string) (bool, error) {
	var count int
	switch driver {
	case "mysql":
		err := db.QueryRow(`
			SELECT COUNT(*) 
			FROM information_schema.views 
			WHERE table_name = ?
		`, table).Scan(&count)
		if err != nil {
			return false, fmt.Errorf("failed to check if view exists: %v", err)
		}
	case "postgres":
		err := db.QueryRow(`
			SELECT COUNT(*) 
			FROM information_schema.views 
			WHERE table_name = $1 
			AND table_schema = 'public'
		`, table).Scan(&count)
		if err != nil {
			return false, fmt.Errorf("failed to check if view exists: %v", err)
		}
	default:
		return false, fmt.Errorf("unsupported database driver: %s", driver)
	}
	return count > 0, nil
}

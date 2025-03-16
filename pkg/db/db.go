package db

import (
	"database/sql"
	"fmt"

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

// GetTableSchema returns the CREATE TABLE statement for the specified table
func GetTableSchema(db *sql.DB, table string, driver string) (string, error) {
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

// ExportTableData exports all data from the specified table
func ExportTableData(db *sql.DB, table string, condition string) ([]map[string]interface{}, error) {
	// Get column names
	query := fmt.Sprintf("SELECT * FROM %s", table)
	if condition != "" {
		query += " WHERE " + condition
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query data: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
	}

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

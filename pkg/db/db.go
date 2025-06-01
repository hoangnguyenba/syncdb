package db

import (
	"database/sql"
	"fmt"
	"io"
	"sort"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// Database represents a database connection and its operations
type Database struct {
	Conn *Connection
}

// NewDatabase creates a new Database instance
func NewDatabase(conn *Connection) *Database {
	return &Database{
		Conn: conn,
	}
}

// ExportTable exports data from a table to a writer
func (db *Database) ExportTable(tableName string, writer io.Writer) error {
	return ExportTableData(db.Conn, tableName, writer)
}

// ImportTable imports data into a table from a reader
func (db *Database) ImportTable(tableName string, reader io.Reader, disableForeignKeyCheck bool) error {
	return ImportTableData(db.Conn, tableName, reader, disableForeignKeyCheck)
}

// GetTableInfo retrieves information about a table
func (db *Database) GetTableInfo(tableName string) (*TableInfo, error) {
	return GetTableInfo(db.Conn, tableName)
}

// GetSchema retrieves the schema information for a table or view
func (db *Database) GetSchema(tableName string) (*SchemaInfo, error) {
	return GetSchema(db.Conn, tableName)
}

// ListTables returns a list of tables in the database
func (db *Database) ListTables() ([]string, error) {
	return ListTables(db.Conn)
}

// TruncateTable truncates a table
func (db *Database) TruncateTable(tableName string) error {
	return TruncateTable(db.Conn, tableName)
}

// TableExport represents the exported data and schema of a table
type TableExport struct {
	Name    string
	Schema  *SchemaInfo
	Data    []map[string]interface{}
	Columns []string
}

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

// buildColumnList builds a comma-separated list of column names
func buildColumnList(columns []string, driver string) string {
	var quoted []string
	for _, col := range columns {
		switch driver {
		case DriverMySQL:
			quoted = append(quoted, fmt.Sprintf("`%s`", col))
		case DriverPostgres:
			quoted = append(quoted, fmt.Sprintf(`"%s"`, col))
		}
	}
	return strings.Join(quoted, ",")
}

// buildUpdateList builds the SET clause for upsert queries
func buildUpdateList(columns []string, driver string) string {
	var updates []string
	for _, col := range columns {
		switch driver {
		case DriverMySQL:
			updates = append(updates, fmt.Sprintf("`%s`=VALUES(`%s`)", col, col))
		case DriverPostgres:
			updates = append(updates, fmt.Sprintf(`"%s"=EXCLUDED."%s"`, col, col))
		}
	}
	return strings.Join(updates, ",")
}

// getPlaceholder returns the appropriate placeholder for the database driver
func getPlaceholder(driver string, position int) string {
	switch driver {
	case DriverMySQL:
		return "?"
	case DriverPostgres:
		return fmt.Sprintf("$%d", position)
	default:
		return "?"
	}
}

// countTableRows returns the number of rows in the specified table
func countTableRows(db *sql.DB, tableName string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	var count int64
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %w", err)
	}
	return count, nil
}

// getSchemaColumns returns an ordered list of column names for the given table
func getSchemaColumns(db *sql.DB, table string, driver string) ([]string, error) {
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

// checkIsView checks if the given table name is actually a view
func checkIsView(db *sql.DB, table string, driver string) (bool, error) {
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

// getTableDeps returns a list of tables that the given table depends on
func getTableDeps(db *sql.DB, table string, driver string) ([]string, error) {
	var query string
	switch driver {
	case "mysql":
		query = `
			SELECT DISTINCT referenced_table_name
			FROM information_schema.key_column_usage
			WHERE table_name = ?
			AND referenced_table_name IS NOT NULL
			AND table_schema = DATABASE()
		`
	case "postgres":
		query = `
			SELECT DISTINCT ccu.table_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.constraint_column_usage ccu
				ON tc.constraint_name = ccu.constraint_name
			WHERE tc.table_name = $1
			AND tc.constraint_type = 'FOREIGN KEY'
			AND tc.table_schema = 'public'
		`
	default:
		return nil, fmt.Errorf("unsupported database driver: %s", driver)
	}

	rows, err := db.Query(query, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query dependencies: %v", err)
	}
	defer rows.Close()

	var deps []string
	for rows.Next() {
		var dep string
		if err := rows.Scan(&dep); err != nil {
			return nil, fmt.Errorf("failed to scan dependency: %v", err)
		}
		deps = append(deps, dep)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating dependencies: %v", err)
	}

	return deps, nil
}

// sortTablesByDeps sorts tables based on their dependencies
func sortTablesByDeps(tables []string, deps map[string][]string) []string {
	visited := make(map[string]bool)
	temp := make(map[string]bool)
	var result []string

	var visit func(table string)
	visit = func(table string) {
		if temp[table] {
			// Circular dependency detected
			return
		}
		if visited[table] {
			return
		}

		temp[table] = true
		for _, dep := range deps[table] {
			visit(dep)
		}
		temp[table] = false
		visited[table] = true
		result = append(result, table)
	}

	for _, table := range tables {
		if !visited[table] {
			visit(table)
		}
	}

	// Reverse the result to get the correct order
	sort.Slice(result, func(i, j int) bool {
		return result[i] > result[j]
	})

	return result
}

// GetTables returns a list of tables in the database
func GetTables(conn *Connection) ([]string, error) {
	var query string
	switch conn.Config.Driver {
	case DriverMySQL:
		query = `
			SELECT TABLE_NAME
			FROM information_schema.tables
			WHERE table_schema = DATABASE()
			AND table_type = 'BASE TABLE'
			ORDER BY table_name`
	case DriverPostgres:
		query = `
			SELECT table_name
			FROM information_schema.tables
			WHERE table_schema = 'public'
			AND table_type = 'BASE TABLE'
			ORDER BY table_name`
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

	return tables, rows.Err()
}

// GetTableDependencies returns a list of tables that the given table depends on
func GetTableDependencies(conn *Connection, tableName string) ([]string, error) {
	return getTableDependencies(conn.DB, tableName, conn.Config.Driver)
}

// SortTablesByDependencies sorts tables based on their dependencies
func SortTablesByDependencies(tables []string, deps map[string][]string) []string {
	return sortTablesByDependencies(tables, deps)
}

// GetTableSchema returns the schema information for a table
func GetTableSchema(conn *Connection, tableName string) (*SchemaInfo, error) {
	return GetSchema(conn, tableName)
}

// IsView checks if a table is actually a view
func IsView(conn *Connection, tableName string) (bool, error) {
	return checkTableIsView(conn.DB, tableName, conn.Config.Driver)
}

// GetTableRowCount returns the number of rows in a table
func GetTableRowCount(conn *Connection, tableName string) (int64, error) {
	return countTableRowCount(conn.DB, tableName)
}

// DropDatabase drops an existing database
func DropDatabase(conn *Connection) error {
	// Get current database name
	dbName := conn.Config.Database

	// Create a temporary connection without database specified to allow dropping
	tempConn, err := InitDB(conn.Config.Driver, conn.Config.Host, conn.Config.Port,
		conn.Config.User, conn.Config.Password, "") // Empty database name
	if err != nil {
		return fmt.Errorf("failed to create temporary connection for dropping database: %v", err)
	}
	defer tempConn.Close()

	// Drop database
	query := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName)
	_, err = tempConn.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to drop database %s: %v", dbName, err)
	}

	fmt.Printf("Successfully dropped database: %s\n", dbName)
	return nil
}

// CreateDatabase creates a new database
func CreateDatabase(conn *Connection) error {
	// Get current database name
	dbName := conn.Config.Database

	// Create a temporary connection without database specified to allow creation
	tempConn, err := InitDB(conn.Config.Driver, conn.Config.Host, conn.Config.Port,
		conn.Config.User, conn.Config.Password, "") // Empty database name
	if err != nil {
		return fmt.Errorf("failed to create temporary connection for creating database: %v", err)
	}
	defer tempConn.Close()

	// Create database
	query := fmt.Sprintf("CREATE DATABASE `%s`", dbName)
	_, err = tempConn.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create database %s: %v", dbName, err)
	}

	return nil
}

// ExecuteSchema executes a schema definition SQL script
func ExecuteSchema(conn *Connection, schemaSQL string) error {
	// Split by double newlines to handle multiple statements
	statements := strings.Split(strings.TrimSpace(schemaSQL), "\n\n")

	// Start a transaction for schema changes
	tx, err := conn.DB.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %v", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		// Execute the schema statement
		_, err = tx.Exec(stmt)
		if err != nil {
			return fmt.Errorf("failed to execute schema statement: %v\nStatement: %s", err, stmt)
		}
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit schema changes: %v", err)
	}

	return nil
}

// ExecuteData executes data import SQL statements
func ExecuteData(conn *Connection, dataSQL string) error {
	separator := "\n--SYNCDB_QUERY_SEPARATOR--\n"
	statements := strings.Split(dataSQL, separator)

	// Configure MySQL settings for import
	if conn.Config.Driver == DriverMySQL {
		// Disable foreign key checks
		if _, err := conn.DB.Exec("SET FOREIGN_KEY_CHECKS = 0"); err != nil {
			return fmt.Errorf("failed to disable foreign key checks: %v", err)
		}
		// Allow zero datetime values
		if _, err := conn.DB.Exec("SET SESSION sql_mode = REPLACE(@@sql_mode, 'NO_ZERO_IN_DATE,NO_ZERO_DATE', '')"); err != nil {
			return fmt.Errorf("failed to configure sql_mode for zero dates: %v", err)
		}
		defer func() {
			// Restore default settings
			if _, err := conn.DB.Exec("SET FOREIGN_KEY_CHECKS = 1"); err != nil {
				fmt.Printf("Warning: failed to re-enable foreign key checks: %v\n", err)
			}
			if _, err := conn.DB.Exec("SET SESSION sql_mode = @@GLOBAL.sql_mode"); err != nil {
				fmt.Printf("Warning: failed to restore sql_mode: %v\n", err)
			}
		}()
	}

	// Start a transaction for data import
	tx, err := conn.DB.Begin()
	if err != nil {
		return fmt.Errorf("failed to start data import transaction: %v", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		// Execute the data statement
		_, err = tx.Exec(stmt)
		if err != nil {
			return fmt.Errorf("failed to execute data statement: %v\nStatement: %s", err, stmt)
		}
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit data import: %v", err)
	}

	return nil
}

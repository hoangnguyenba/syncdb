package db

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
)

// DataOperation represents a database operation (INSERT, UPDATE, DELETE)
type DataOperation struct {
	Type    string
	Table   string
	Data    map[string]interface{}
	Where   map[string]interface{}
	Columns []string
}

// ExportTableData exports data from a table to a writer
func ExportTableData(conn *Connection, tableName string, writer io.Writer) error {
	// Get non-virtual columns
	columns, err := getNonVirtualColumns(conn.DB, tableName, conn.Config.Driver)
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	// Build query
	escapedColumns := make([]string, len(columns))
	for i, col := range columns {
		escapedColumns[i] = EscapeIdentifier(conn.Config.Driver, col)
	}
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(escapedColumns, ", "), EscapeIdentifier(conn.Config.Driver, tableName))
	rows, err := conn.DB.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query data: %w", err)
	}
	defer rows.Close()

	// Get column names
	colNames, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get column names: %w", err)
	}

	// Create slice of pointers for scanning
	values := make([]interface{}, len(colNames))
	valuePtrs := make([]interface{}, len(colNames))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Process each row
	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert row to map
		rowData := make(map[string]interface{})
		for i, col := range colNames {
			val := values[i]
			if val != nil {
				// Check if the value is a string or []byte
				var strVal string
				switch v := val.(type) {
				case string:
					strVal = v
				case []byte:
					strVal = string(v)
				default:
					rowData[col] = val
					continue
				}

				// Attempt to decode base64 values
				decodedStr, err := tryBase64Decode(strVal)
				if err == nil && decodedStr != strVal {
					rowData[col] = decodedStr
				} else {
					rowData[col] = strVal
				}
			}
		}

		// Create operation
		op := DataOperation{
			Type:    "INSERT",
			Table:   tableName,
			Data:    rowData,
			Columns: columns,
		}

		// Write to output
		encoder := json.NewEncoder(writer)
		if err := encoder.Encode(op); err != nil {
			return fmt.Errorf("failed to encode operation: %w", err)
		}
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	return nil
}

// ImportTableData imports data into a table from a reader
func ImportTableData(conn *Connection, tableName string, reader io.Reader) error {
	decoder := json.NewDecoder(reader)
	for {
		var op DataOperation
		err := decoder.Decode(&op)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to decode operation: %w", err)
		}

		if op.Table != tableName {
			return fmt.Errorf("operation table name mismatch: expected %s, got %s", tableName, op.Table)
		}

		switch op.Type {
		case "INSERT":
			if err := executeInsertOperation(conn, op); err != nil {
				return fmt.Errorf("failed to execute insert: %w", err)
			}
		case "UPDATE":
			if err := executeUpdateOperation(conn, op); err != nil {
				return fmt.Errorf("failed to execute update: %w", err)
			}
		case "DELETE":
			if err := executeDeleteOperation(conn, op); err != nil {
				return fmt.Errorf("failed to execute delete: %w", err)
			}
		default:
			return fmt.Errorf("unsupported operation type: %s", op.Type)
		}
	}

	return nil
}

// executeInsertOperation executes an INSERT operation
func executeInsertOperation(conn *Connection, op DataOperation) error {
	columns := make([]string, 0, len(op.Data))
	values := make([]interface{}, 0, len(op.Data))
	placeholders := make([]string, 0, len(op.Data))

	for col, val := range op.Data {
		columns = append(columns, col)
		values = append(values, val)
		switch conn.Config.Driver {
		case DriverMySQL:
			placeholders = append(placeholders, "?")
		case DriverPostgres:
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(placeholders)+1))
		default:
			return fmt.Errorf("%w: %s", ErrUnsupportedDriver, conn.Config.Driver)
		}
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		op.Table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	_, err := conn.DB.Exec(query, values...)
	return err
}

// executeUpdateOperation executes an UPDATE operation
func executeUpdateOperation(conn *Connection, op DataOperation) error {
	if len(op.Where) == 0 {
		return fmt.Errorf("WHERE clause required for UPDATE operation")
	}

	// Build SET clause
	setClause := make([]string, 0, len(op.Data))
	values := make([]interface{}, 0, len(op.Data))
	placeholderCount := 1

	for col, val := range op.Data {
		setClause = append(setClause, fmt.Sprintf("%s = %s", col, getDataPlaceholder(conn.Config.Driver, placeholderCount)))
		values = append(values, val)
		placeholderCount++
	}

	// Build WHERE clause
	whereClause := make([]string, 0, len(op.Where))
	for col, val := range op.Where {
		whereClause = append(whereClause, fmt.Sprintf("%s = %s", col, getDataPlaceholder(conn.Config.Driver, placeholderCount)))
		values = append(values, val)
		placeholderCount++
	}

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		op.Table,
		strings.Join(setClause, ", "),
		strings.Join(whereClause, " AND "),
	)

	_, err := conn.DB.Exec(query, values...)
	return err
}

// executeDeleteOperation executes a DELETE operation
func executeDeleteOperation(conn *Connection, op DataOperation) error {
	if len(op.Where) == 0 {
		return fmt.Errorf("WHERE clause required for DELETE operation")
	}

	whereClause := make([]string, 0, len(op.Where))
	values := make([]interface{}, 0, len(op.Where))
	placeholderCount := 1

	for col, val := range op.Where {
		whereClause = append(whereClause, fmt.Sprintf("%s = %s", col, getDataPlaceholder(conn.Config.Driver, placeholderCount)))
		values = append(values, val)
		placeholderCount++
	}

	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		op.Table,
		strings.Join(whereClause, " AND "),
	)

	_, err := conn.DB.Exec(query, values...)
	return err
}

// getDataPlaceholder returns the appropriate placeholder for the given driver and position
func getDataPlaceholder(driver string, position int) string {
	switch driver {
	case DriverMySQL:
		return "?"
	case DriverPostgres:
		return fmt.Sprintf("$%d", position)
	default:
		return "?"
	}
}

// getNonVirtualColumns returns a list of non-virtual columns for the given table
func getNonVirtualColumns(db *sql.DB, tableName string, driver string) ([]string, error) {
	var query string
	switch driver {
	case DriverMySQL:
		query = `
			SELECT COLUMN_NAME 
			FROM INFORMATION_SCHEMA.COLUMNS 
			WHERE TABLE_SCHEMA = DATABASE() 
			AND TABLE_NAME = ? 
			AND GENERATION_EXPRESSION = ''
			ORDER BY ORDINAL_POSITION`
	case DriverPostgres:
		query = `
			SELECT column_name 
			FROM information_schema.columns 
			WHERE table_name = $1 
			AND is_generated = 'NEVER'
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

// tryBase64Decode attempts to decode a base64 string with multiple strategies
func tryBase64Decode(s string) (string, error) {
	// Remove any whitespace
	s = strings.TrimSpace(s)

	// Check if the string looks like a base64 encoded value
	base64Regex := regexp.MustCompile(`^[A-Za-z0-9+/]+={0,2}$`)
	if !base64Regex.MatchString(s) {
		return s, fmt.Errorf("not a base64 string")
	}

	// Try standard base64 decoding
	decodedBytes, err := base64.StdEncoding.DecodeString(s)
	if err == nil {
		// Check if decoded bytes form a valid string
		decodedStr := string(decodedBytes)
		if isValidDecodedString(decodedStr, s) {
			return decodedStr, nil
		}
	}

	// Try URL-safe base64 decoding
	decodedBytes, err = base64.URLEncoding.DecodeString(s)
	if err == nil {
		// Check if decoded bytes form a valid string
		decodedStr := string(decodedBytes)
		if isValidDecodedString(decodedStr, s) {
			return decodedStr, nil
		}
	}

	return s, fmt.Errorf("not a valid base64 string")
}

// isValidDecodedString checks if a decoded string is meaningful
func isValidDecodedString(decoded, original string) bool {
	// Skip if decoding didn't change the string
	if decoded == original {
		return false
	}

	// Check for timestamp-like pattern (YYYY-MM-DD HH:MM:SS)
	timestampRegex := regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`)
	if timestampRegex.MatchString(decoded) {
		return true
	}

	// Check for email-like pattern
	emailRegex := regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	if emailRegex.MatchString(decoded) {
		return true
	}

	// Check for printable ASCII characters
	for _, r := range decoded {
		if r < 32 || r > 126 {
			return false
		}
	}

	// Require minimum length and not just whitespace
	if len(strings.TrimSpace(decoded)) > 0 {
		return true
	}

	return false
}

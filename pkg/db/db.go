package db

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"

	"github.com/pkg/errors"

	"github.com/hoangnguyenba/syncdb/internal/config"
)

func Connect(config config.DatabaseConfig) (*sql.DB, error) {
	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", config.Username, config.Password, config.Host, config.Port, "yourdatabase")
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open database connection")
	}

	if err = db.Ping(); err != nil {
		return nil, errors.Wrap(err, "failed to ping database")
	}

	return db, nil
}

func Export(db *sql.DB, config config.ExportImportConfig, tables []string) ([]byte, error) {
	var result []byte
	query := "SELECT * FROM "

	if len(tables) == 0 {
		return nil, fmt.Errorf("no tables specified for export")
	}

	query += strings.Join(tables, ", ")

	if config.ExportCondition != nil {
		query += fmt.Sprintf(" WHERE %s", config.ExportCondition)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute export query")
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get columns")
	}

	var values []interface{}
	for rows.Next() {
		interfaceValues := make([]interface{}, len(columns))
		dest := make([]interface{}, len(columns))
		for i := range dest {
			dest[i] = &interfaceValues[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}

		values = append(values, dest...)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating rows")
	}

	// Convert values to JSON
	data, err := json.MarshalIndent(values, "", "  ")
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal data")
	}

	return data, nil
}

func Import(db *sql.DB, data []byte, config config.ExportImportConfig) error {
	var result []byte
	query := "SELECT * FROM "

	if len(tables) == 0 {
		return nil, fmt.Errorf("no tables specified for export")
	}

	query += strings.Join(tables, ", ")

	if config.ExportCondition != nil {
		query += fmt.Sprintf(" WHERE %s", config.ExportCondition)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute export query")
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get columns")
	}

	var values []interface{}
	for rows.Next() {
		interfaceValues := make([]interface{}, len(columns))
		dest := make([]interface{}, len(columns))
		for i := range dest {
			dest[i] = &interfaceValues[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}

		values = append(values, dest...)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating rows")
	}

	// Convert values to JSON
	data, err := json.MarshalIndent(values, "", "  ")
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal data")
	}

	return data, nil
}

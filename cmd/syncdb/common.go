package main

import (
	"database/sql"
)

var (
	includeSchema bool
)

// getTables returns a list of all tables in the specified database
func getTables(db *sql.DB, dbName string) ([]string, error) {
	// TODO: Implement getting tables from database
	return []string{}, nil
}

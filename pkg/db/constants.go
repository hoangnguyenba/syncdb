package db

import "errors"

// Database driver constants
const (
	DriverMySQL    = "mysql"
	DriverPostgres = "postgres"
)

// Error definitions
var (
	ErrUnsupportedDriver = errors.New("unsupported database driver")
	ErrTableNotFound     = errors.New("table not found")
	ErrInvalidTableName  = errors.New("invalid table name")
	ErrInvalidOperation  = errors.New("invalid operation")
	ErrInvalidQuery      = errors.New("invalid query")
)

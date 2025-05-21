package db

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// ConnectionConfig contains configuration for a database connection
type ConnectionConfig struct {
	Driver      string
	Host        string
	Port        int
	User        string
	Password    string
	Database    string
	Timeout     time.Duration
	RecordLimit int // Maximum number of records to export per table (0 means no limit)
}

// Connection represents a database connection
type Connection struct {
	DB     *sql.DB
	Config ConnectionConfig
}

// NewConnection creates a new database connection
func NewConnection(config ConnectionConfig) (*Connection, error) {
	dsn, err := buildDSN(config)
	if err != nil {
		return nil, fmt.Errorf("failed to build DSN: %w", err)
	}

	db, err := sql.Open(config.Driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Connection{
		DB:     db,
		Config: config,
	}, nil
}

// Close closes the database connection
func (c *Connection) Close() error {
	return c.DB.Close()
}

// buildDSN builds a database connection string
func buildDSN(config ConnectionConfig) (string, error) {
	switch config.Driver {
	case DriverMySQL:
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=%s",
			config.User,
			config.Password,
			config.Host,
			config.Port,
			config.Database,
			config.Timeout,
		), nil
	case DriverPostgres:
		return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable connect_timeout=%d",
			config.Host,
			config.Port,
			config.User,
			config.Password,
			config.Database,
			int(config.Timeout.Seconds()),
		), nil
	default:
		return "", fmt.Errorf("%w: %s", ErrUnsupportedDriver, config.Driver)
	}
}

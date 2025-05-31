package main

import (
	"archive/zip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/hoangnguyenba/syncdb/pkg/db"
	"github.com/hoangnguyenba/syncdb/pkg/storage"
	"github.com/spf13/cobra"
)

var sqlInsertRegex = regexp.MustCompile(`INSERT INTO (\w+) \((.*?)\) VALUES \((.*?)\);`)

func getLatestTimestampDir(basePath string, dbName string) (string, error) {
	// Check if base directory exists
	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		return "", fmt.Errorf("base directory not found: %s", basePath)
	}

	// Get all subdirectories in the base directory
	entries, err := os.ReadDir(basePath)
	if err != nil {
		return "", fmt.Errorf("failed to read directory: %v", err)
	}

	var latestTime time.Time
	var latestDir string
	prefix := dbName + "_"
	timestampLayout := "20060102_150405"

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		ts := strings.TrimPrefix(name, prefix)
		dirTime, err := time.Parse(timestampLayout, ts)
		if err != nil {
			continue
		}
		if latestDir == "" || dirTime.After(latestTime) {
			latestTime = dirTime
			latestDir = name
		}
	}

	if latestDir == "" {
		return "", fmt.Errorf("no valid timestamp directories found in %s with prefix %s", basePath, prefix)
	}

	return filepath.Join(basePath, latestDir), nil
}

func getLatestZipFile(basePath string, dbName string) (string, error) {
	// Check if base directory exists
	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		return "", fmt.Errorf("base directory not found: %s", basePath)
	}

	// Get all files in the base directory
	entries, err := os.ReadDir(basePath)
	if err != nil {
		return "", fmt.Errorf("failed to read directory: %v", err)
	}

	var latestTime time.Time
	var latestZip string
	prefix := dbName + "_"
	timestampLayout := "20060102_150405"

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".zip") {
			continue
		}
		ts := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".zip")
		fileTime, err := time.Parse(timestampLayout, ts)
		if err != nil {
			continue
		}
		if latestZip == "" || fileTime.After(latestTime) {
			latestTime = fileTime
			latestZip = name
		}
	}

	if latestZip == "" {
		return "", fmt.Errorf("no valid zip files found in %s with prefix %s", basePath, prefix)
	}

	return filepath.Join(basePath, latestZip), nil
}

func unzipFile(zipPath string, destPath string) error {
	fmt.Printf("Opening zip file: %s\n", zipPath)
	// Open the zip file
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return fmt.Errorf("failed to open zip file: %v", err)
	}
	defer reader.Close()

	fmt.Printf("Found %d files in zip archive\n", len(reader.File))

	// Extract each file
	extractedCount := 0
	for _, file := range reader.File {
		// Get file info
		fileInfo := file.FileInfo()

		// Skip directories
		if fileInfo.IsDir() {
			continue
		}

		// Open the file in the zip
		rc, err := file.Open()
		if err != nil {
			return fmt.Errorf("failed to open file in zip: %v", err)
		}

		// Create the file path, preserving timestamp directory structure
		path := filepath.Join(destPath, file.Name)

		// Create parent directory if it doesn't exist
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			rc.Close()
			return fmt.Errorf("failed to create directory: %v", err)
		}

		// Create the file
		outFile, err := os.Create(path)
		if err != nil {
			rc.Close()
			return fmt.Errorf("failed to create file: %v", err)
		}

		// Copy the contents
		written, err := io.Copy(outFile, rc)
		outFile.Close()
		rc.Close()
		if err != nil {
			return fmt.Errorf("failed to write file: %v", err)
		}

		extractedCount++
		fmt.Printf("Extracted: %s (%d bytes)\n", file.Name, written)
	}

	fmt.Printf("Successfully extracted %d files\n", extractedCount)
	return nil
}

func getImportPath(cmdArgs *CommonArgs) (string, error) {
	// If path is a directory and contains metadata file, use it directly
	if storage.IsExportPath(cmdArgs.Path) {
		fmt.Printf("Found metadata file in %s, using this path directly\n", cmdArgs.Path)
		return cmdArgs.Path, nil
	}

	// If path doesn't exist or is a directory without metadata, look for latest timestamp dir
	stat, err := os.Stat(cmdArgs.Path)
	if err == nil && stat.IsDir() {
		// Path exists and is a directory
		fmt.Printf("Looking for latest timestamp directory in: %s\n", cmdArgs.Path)
		importPath, err := getLatestTimestampDir(cmdArgs.Path, cmdArgs.Database)
		if err != nil {
			return "", fmt.Errorf("failed to get latest timestamp directory: %v", err)
		}
		fmt.Printf("Found latest timestamp directory: %s\n", importPath)
		return importPath, nil
	}

	// If path doesn't exist or is not a directory, assume it's a zip file
	if strings.HasSuffix(cmdArgs.Path, ".zip") {
		return cmdArgs.Path, nil
	}

	// Otherwise, try to find latest zip file in the directory
	dir := filepath.Dir(cmdArgs.Path)
	zipPath, err := getLatestZipFile(dir, cmdArgs.Database)
	if err != nil {
		return "", fmt.Errorf("failed to get latest zip file: %v", err)
	}
	return zipPath, nil
}

func newImportCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "import",
		Short: "Import database data",
		Long:  `Import database data from files previously exported by syncdb.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmdArgs, _, conn, err := loadAndValidateArgs(cmd)
			if err != nil {
				return err // Error already formatted by loadAndValidateArgs
			}
			defer conn.Close() // Ensure connection is closed

			if cmdArgs.Path == "" {
				return fmt.Errorf("path is required")
			}

			importPath, err := getImportPath(cmdArgs)
			if err != nil {
				return err
			}

			// If it's a zip file, extract it
			if strings.HasSuffix(importPath, ".zip") {
				importDir, err := os.MkdirTemp("", "syncdb_import_*")
				if err != nil {
					return fmt.Errorf("failed to create temporary directory: %v", err)
				}
				defer os.RemoveAll(importDir) // Clean up temp directory when done

				fmt.Printf("Unzipping file to: %s\n", importDir)
				if err := unzipFile(importPath, importDir); err != nil {
					return err
				}

				// Find the metadata file
				var metadataDir string
				err = filepath.Walk(importDir, func(path string, info os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					if !info.IsDir() && strings.HasSuffix(path, "0_metadata.json") {
						metadataDir = filepath.Dir(path)
						return filepath.SkipAll
					}
					return nil
				})
				if err != nil {
					return fmt.Errorf("failed to find metadata file: %v", err)
				}

				if metadataDir == "" {
					return fmt.Errorf("no metadata file found in zip file")
				}

				importPath = metadataDir
			}

			// At this point, importPath should point to a directory containing metadata.json
			if !storage.IsExportPath(importPath) {
				return fmt.Errorf("invalid import path: %s (no metadata file found)", importPath)
			}

			// Read metadata file
			metadataFile := filepath.Join(importPath, "0_metadata.json")
			metadataBytes, err := os.ReadFile(metadataFile)
			if err != nil {
				return fmt.Errorf("failed to read metadata file: %v", err)
			}

			// Parse metadata
			var metadata ExportData
			if err := json.Unmarshal(metadataBytes, &metadata.Metadata); err != nil {
				return fmt.Errorf("failed to parse metadata: %v", err)
			}

			// Import schema if included and requested
			if metadata.Metadata.Schema && cmdArgs.IncludeSchema {
				fmt.Println("Importing schema...")
				if cmdArgs.Drop {
					if err := db.DropDatabase(conn); err != nil {
						return fmt.Errorf("failed to drop database: %v", err)
					}
					if err := db.CreateDatabase(conn); err != nil {
						return fmt.Errorf("failed to create database: %v", err)
					}
				}

				schemaFile := filepath.Join(importPath, "0_schema.sql")
				schemaData, err := os.ReadFile(schemaFile)
				if err != nil {
					return fmt.Errorf("failed to read schema file: %v", err)
				}

				if err := db.ExecuteSchema(conn, string(schemaData)); err != nil {
					return fmt.Errorf("failed to execute schema: %v", err)
				}
			}

			// Skip data import if not included in export or not requested
			if !metadata.Metadata.IncludeData || !cmdArgs.IncludeData {
				fmt.Println("Skipping data import as requested")
				return nil
			}

			// Import data
			fmt.Println("Importing data...")

			// Get list of data files
			entries, err := os.ReadDir(importPath)
			if err != nil {
				return fmt.Errorf("failed to read import directory: %v", err)
			}

			// Import each data file in order
			startTable := 0
			if cmdArgs.FromTableIndex > 0 {
				startTable = cmdArgs.FromTableIndex - 1 // 1-based to 0-based
			}
			for i, entry := range entries {
				if i < startTable {
					continue
				}
				if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
					continue
				}
				if entry.Name() == "0_schema.sql" {
					continue // Skip schema file
				}

				filePath := filepath.Join(importPath, entry.Name())
				fmt.Printf("Importing %s...\n", entry.Name())

				fileData, err := os.ReadFile(filePath)
				if err != nil {
					return fmt.Errorf("failed to read data file %s: %v", filePath, err)
				}

				if cmdArgs.Truncate {
					tableName := strings.TrimSuffix(strings.TrimPrefix(entry.Name(), "0123456789_"), ".sql")
					if err := db.TruncateTable(conn, tableName); err != nil {
						return fmt.Errorf("failed to truncate table %s: %v", tableName, err)
					}
				}

				// Always split into chunks and import chunk by chunk
				separator := "\n--SYNCDB_QUERY_SEPARATOR--\n"
				if cmdArgs.QuerySeparator != "" {
					separator = cmdArgs.QuerySeparator
				}
				chunks := strings.Split(string(fileData), separator)
				startChunk := 0
				if i == startTable && cmdArgs.FromChunkIndex > 0 {
					startChunk = cmdArgs.FromChunkIndex - 1 // 1-based to 0-based
				}
				for chunkIdx, chunk := range chunks {
					if chunkIdx < startChunk {
						continue
					}
					if err := db.ExecuteData(conn, chunk); err != nil {
						logFile := filepath.Join(".", "import-error.log")
						errMsg := fmt.Sprintf("[ERROR] Failed to execute chunk %d from %s Error: %v\n", chunkIdx+1, entry.Name(), err)
						fullLog := time.Now().Format(time.RFC3339) + "\n" + errMsg + "SQL:\n" + chunk + "\n\n"
						f, ferr := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
						if ferr == nil {
							f.WriteString(fullLog)
							f.Close()
						} else {
							fmt.Printf("[ERROR] Could not write to import-error.log: %v\n", ferr)
						}
						fmt.Printf("[ERROR] Failed to execute chunk %d from %s: %v (see import-error.log)\n", chunkIdx+1, entry.Name(), err)
						return fmt.Errorf("failed to execute chunk %d from %s: %v (see import-error.log)", chunkIdx+1, entry.Name(), err)
					}
				}
			}

			fmt.Println("Import completed successfully")
			return nil
		},
	}

	// Add shared flags
	AddSharedFlags(cmd, true) // Pass true for import command

	// Add import-specific flags
	flags := cmd.Flags()
	flags.Bool("truncate", false, "Truncate tables before import")
	flags.Bool("drop", false, "Drop and recreate database before import")
	flags.String("query-separator", "\n--SYNCDB_QUERY_SEPARATOR--\n", "String used to separate SQL queries in import file")

	return cmd
}

// Helper function to extract table name from schema statement
func extractTableNameFromSchema(stmt string) string {
	// Common patterns for table creation and alteration
	createTableRegex := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[` + "`" + `]?(\w+)[` + "`" + `]?`)
	alterTableRegex := regexp.MustCompile(`(?i)ALTER\s+TABLE\s+[` + "`" + `]?(\w+)[` + "`" + `]?`)

	// Try to match CREATE TABLE
	if matches := createTableRegex.FindStringSubmatch(stmt); len(matches) > 1 {
		return matches[1]
	}

	// Try to match ALTER TABLE
	if matches := alterTableRegex.FindStringSubmatch(stmt); len(matches) > 1 {
		return matches[1]
	}

	return ""
}

// isBase64 checks if a string is Base64 encoded
func isBase64(s string) bool {
	_, err := base64.StdEncoding.DecodeString(s)
	return err == nil && len(s) > 8 && strings.TrimRight(s, "=") != s[:len(s)-1]
}

// decodeBase64Values decodes Base64 encoded values in a SQL statement
func decodeBase64Values(sql string) string {
	// Define regex to match SQL values
	valueRegex := regexp.MustCompile(`'([^']*)'`)

	// Find all values and try to decode them if they're Base64 encoded
	decoded := valueRegex.ReplaceAllStringFunc(sql, func(match string) string {
		// Extract value without quotes
		value := match[1 : len(match)-1]

		// Check if it's Base64 encoded
		if isBase64(value) {
			decodedBytes, err := base64.StdEncoding.DecodeString(value)
			if err == nil {
				// Replace with decoded value
				return "'" + string(decodedBytes) + "'"
			}
		}

		// Return original value if not Base64 or decoding failed
		return match
	})

	return decoded
}

func importSchema(conn *db.Connection, schemaContent []byte) error {
	// First pass: collect all CREATE TABLE statements
	createTableStatements := make(map[string]string)
	var currentStatement strings.Builder

	lines := strings.Split(string(schemaContent), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}

		currentStatement.WriteString(line)
		currentStatement.WriteString("\n")

		if strings.HasSuffix(line, ";") {
			stmt := currentStatement.String()
			if strings.Contains(strings.ToUpper(stmt), "CREATE TABLE") {
				// Extract table name and validate it exists
				tableName := extractTableNameFromSchema(stmt)
				if tableName != "" {
					createTableStatements[tableName] = stmt
					fmt.Printf("Found CREATE TABLE for %s\n", tableName)
				}
			}
			currentStatement.Reset()
		}
	}

	if len(createTableStatements) == 0 {
		return fmt.Errorf("no CREATE TABLE statements found in schema")
	}

	// Build dependency graph
	deps := make(map[string][]string)
	for tableName, stmt := range createTableStatements {
		if strings.Contains(stmt, "FOREIGN KEY") {
			// Look for all REFERENCES clauses
			refRegex := regexp.MustCompile(`(?i)FOREIGN\s+KEY\s*\([^)]+\)\s*REFERENCES\s+[\x60"']?(\w+)[\x60"']?\s*\([^)]+\)`)
			matches := refRegex.FindAllStringSubmatch(stmt, -1)
			for _, match := range matches {
				if len(match) > 1 {
					referencedTable := match[1]
					deps[tableName] = append(deps[tableName], referencedTable)
					fmt.Printf("Table %s depends on %s\n", tableName, referencedTable)
				}
			}
		}
	}

	// Sort tables by dependencies
	var tables []string
	for t := range createTableStatements {
		tables = append(tables, t)
	}
	sortedTables := db.SortTablesByDependencies(tables, deps)

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

	// Execute statements in dependency order with retry mechanism
	executedTables := make(map[string]bool)
	maxRetries := 5
	for attempt := 0; attempt < maxRetries; attempt++ {
		skippedTables := []string{}

		for _, tableName := range sortedTables {
			if executedTables[tableName] {
				continue
			}

			// Check if all dependencies are met
			canCreate := true
			for _, dep := range deps[tableName] {
				if !executedTables[dep] {
					canCreate = false
					break
				}
			}

			if !canCreate {
				skippedTables = append(skippedTables, tableName)
				continue
			}

			stmt := createTableStatements[tableName]
			fmt.Printf("Creating table %s... (attempt %d)\n", tableName, attempt+1)

			// Try to create the table
			_, err = tx.Exec(stmt)
			if err != nil {
				if strings.Contains(err.Error(), "Error 1824") ||
					strings.Contains(err.Error(), "errno 150") ||
					strings.Contains(strings.ToLower(err.Error()), "foreign key constraint fails") {
					skippedTables = append(skippedTables, tableName)
					fmt.Printf("Warning: Failed to create table %s (dependency issue), will retry\n", tableName)
					continue
				}
				return fmt.Errorf("failed to create table %s: %v", tableName, err)
			}

			executedTables[tableName] = true
			fmt.Printf("Successfully created table %s\n", tableName)
		}

		// If no tables were skipped or no progress was made, we're done
		if len(skippedTables) == 0 {
			break
		}

		// Check if we made any progress this iteration
		if attempt > 0 && len(skippedTables) == len(sortedTables) {
			return fmt.Errorf("failed to resolve table dependencies after %d attempts. Remaining tables: %v", attempt+1, skippedTables)
		}

		// Update sorted tables to only include remaining tables
		sortedTables = skippedTables
	}

	// Commit transaction if all is well
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit schema changes: %v", err)
	}

	fmt.Printf("Schema import completed successfully. Created %d tables.\n", len(executedTables))
	return nil
}

package main

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hoangnguyenba/syncdb/pkg/db"
	"github.com/hoangnguyenba/syncdb/pkg/storage"
	"github.com/spf13/cobra"
)

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
	// If using Google Drive storage, download the file first
	if cmdArgs.Storage == "gdrive" {
		// Initialize Google Drive storage
		gdriveStore, err := storage.NewGoogleDriveStorage(cmdArgs.GdriveCredentials, cmdArgs.GdriveFolder)
		if err != nil {
			return "", fmt.Errorf("failed to initialize Google Drive storage: %v", err)
		}

		// Extract file name from path
		fileName := filepath.Base(cmdArgs.Path)
		fmt.Printf("Downloading %s from Google Drive...\n", fileName)

		// Download file from Google Drive
		data, err := gdriveStore.Download(fileName)
		if err != nil {
			return "", fmt.Errorf("failed to download file from Google Drive: %v", err)
		}

		// Create a temporary file to store the downloaded content
		tempFile, err := os.CreateTemp("", "syncdb-gdrive-*"+filepath.Ext(fileName))
		if err != nil {
			return "", fmt.Errorf("failed to create temporary file: %v", err)
		}

		// Write the downloaded content to the temporary file
		if err := os.WriteFile(tempFile.Name(), data, 0644); err != nil {
			os.Remove(tempFile.Name())
			return "", fmt.Errorf("failed to write downloaded file: %v", err)
		}

		fmt.Printf("Successfully downloaded %s to %s\n", fileName, tempFile.Name())

		// Update the path to point to the downloaded file
		cmdArgs.Path = tempFile.Name()
	}

	// Continue with existing logic for local files
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
		Short: "Import database from files",
		Long: `Import database schema and/or data from files.
Examples:
  syncdb import --path ./backup/mydb_20240101 --host localhost --database targetdb
  syncdb import --path backup.zip --driver mysql --database targetdb --include-schema`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cmdArgs, _, conn, err := loadAndValidateArgs(cmd)
			if err != nil {
				return err // Error already formatted by loadAndValidateArgs
			}
			defer conn.Close() // Ensure connection is closed

			importPath, err := getImportPath(cmdArgs)
			if err != nil {
				return err
			}

			// If path is a zip file, extract it to a temp directory
			if strings.HasSuffix(importPath, ".zip") {
				// Create temp directory for import
				importDir := filepath.Join(os.TempDir(), "syncdb-import-"+time.Now().Format("20060102150405"))
				err := os.MkdirAll(importDir, 0755)
				if err != nil {
					return err
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

			// Filter tables based on --tables parameter
			var tablesToImport []string
			if len(cmdArgs.Tables) > 0 {
				availableTables := make(map[string]bool)
				for _, table := range metadata.Metadata.Tables {
					availableTables[table] = true
				}

				// Expand table patterns
				for _, pattern := range cmdArgs.Tables {
					pattern = strings.TrimSpace(pattern)
					for table := range availableTables {
						if db.TablePatternMatch(table, pattern) {
							tablesToImport = append(tablesToImport, table)
						}
					}
				}
				// Sort the tables for consistent order
				sort.Strings(tablesToImport)
			} else {
				tablesToImport = metadata.Metadata.Tables
			}

			if len(tablesToImport) == 0 {
				return fmt.Errorf("no tables to import after applying table filter")
			}

			fmt.Printf("Tables to import: %v\n", tablesToImport)

			// Read schema file first to get SQL mode if it exists
			var sqlMode string
			if metadata.Metadata.Schema && cmdArgs.IncludeSchema {
				schemaFile := filepath.Join(importPath, "0_schema.sql")
				schemaData, err := os.ReadFile(schemaFile)
				if err != nil {
					return fmt.Errorf("failed to read schema file: %v", err)
				}

				// Extract SQL mode from schema file if it exists
				lines := strings.Split(string(schemaData), "\n")
				for _, line := range lines {
					line = strings.TrimSpace(line)
					if strings.HasPrefix(line, "-- SQL_MODE=") {
						sqlMode = strings.TrimPrefix(line, "-- SQL_MODE=")
						break
					}
				}
			}

			// Handle drop and recreate database if requested
			if cmdArgs.Drop {
				fmt.Println("Dropping and recreating database...")
				if err := db.DropDatabase(conn); err != nil {
					return fmt.Errorf("failed to drop database: %v", err)
				}
				if err := db.CreateDatabase(conn); err != nil {
					return fmt.Errorf("failed to create database: %v", err)
				}

				// Set SQL mode if it was found in the schema file
				if sqlMode != "" && conn.Config.Driver == "mysql" {
					setModeSQL := fmt.Sprintf("SET GLOBAL sql_mode = '%s'", strings.TrimSpace(sqlMode))
					_, err := conn.DB.Exec(setModeSQL)
					if err != nil {
						return fmt.Errorf("failed to set global SQL mode to '%s': %v", sqlMode, err)
					}
					fmt.Printf("Set global SQL mode to: %s\n", sqlMode)
				}
			}

			// Import schema if included and requested
			if metadata.Metadata.Schema && cmdArgs.IncludeSchema {
				fmt.Println("Importing schema...")
				schemaFile := filepath.Join(importPath, "0_schema.sql")
				schemaData, err := os.ReadFile(schemaFile)
				if err != nil {
					return fmt.Errorf("failed to read schema file: %v", err)
				}

				// Filter schema content to only include selected tables
				if len(cmdArgs.Tables) > 0 {
					schemaData = filterSchemaContent(schemaData, tablesToImport)
				}

				if err := importSchema(conn, schemaData); err != nil {
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

			// Create a map of available tables from metadata
			availableTables := make(map[string]bool)
			for _, table := range metadata.Metadata.Tables {
				availableTables[table] = true
			}

			// Prepare file list based on metadata table order
			fileList := make([]string, 0)
			tableFileMap := make(map[string]string)
			skippedFiles := make([]string, 0)

			// Read directory entries
			entries, err := os.ReadDir(importPath)
			if err != nil {
				return fmt.Errorf("failed to read import directory: %v", err)
			}

			// Create file mapping
			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}

				fileName := entry.Name()
				if fileName == "0_schema.sql" || fileName == "0_metadata.json" {
					continue // Skip schema and metadata files
				}

				tableName := extractTableNameFromFile(fileName)
				if !validateTableName(tableName, availableTables) {
					skippedFiles = append(skippedFiles, fileName)
					continue
				}

				// Check if this table should be imported based on user-specified tables
				if len(tablesToImport) > 0 {
					found := false
					for _, t := range tablesToImport {
						if t == tableName {
							found = true
							break
						}
					}
					if !found {
						skippedFiles = append(skippedFiles, fileName)
						continue
					}
				}

				fmt.Printf("Found data file for table '%s': %s\n", tableName, fileName)
				tableFileMap[tableName] = fileName
			}

			if len(skippedFiles) > 0 {
				fmt.Printf("Skipped %d files:\n", len(skippedFiles))
				for _, file := range skippedFiles {
					fmt.Printf("  - %s\n", file)
				}
			}

			// Reorder fileList based on metadata table order
			for _, table := range tablesToImport {
				if fileName, exists := tableFileMap[table]; exists {
					fileList = append(fileList, fileName)
				}
			}

			if cmdArgs.FromTableIndex > 0 {
				fileList = fileList[cmdArgs.FromTableIndex-1:]
			}

			if len(fileList) == 0 {
				fmt.Println("No data files found to import from the specified table index")
				return nil
			}

			fmt.Printf("Found %d data files to import from table index %d\n", len(fileList), cmdArgs.FromTableIndex)

			for i, fileName := range fileList {
				fmt.Printf("Importing %s...\n", fileName)

				fileData, err := os.ReadFile(filepath.Join(importPath, fileName))
				if err != nil {
					return fmt.Errorf("failed to read data file %s: %v", fileName, err)
				}

				if cmdArgs.Truncate {
					tableName := extractTableNameFromFile(fileName)
					fmt.Printf("Truncating table '%s'...\n", tableName)
					if err := db.TruncateTable(conn, tableName); err != nil {
						return fmt.Errorf("failed to truncate table %s: %v", tableName, err)
					}
				}

				// Split into chunks and import chunk by chunk
				separator := "\n--SYNCDB_QUERY_SEPARATOR--\n"
				if cmdArgs.QuerySeparator != "" {
					separator = cmdArgs.QuerySeparator
				}
				chunks := strings.Split(string(fileData), separator)
				fmt.Printf("Processing %s: Found %d chunks to import\n", fileName, len(chunks))

				startChunk := 0
				if cmdArgs.FromChunkIndex > 0 && i == 0 {
					startChunk = cmdArgs.FromChunkIndex - 1 // 1-based to 0-based
				}

				processedRows := 0
				for chunkIdx, chunk := range chunks {
					if chunkIdx < startChunk {
						continue
					}

					// Skip empty chunks
					chunk = strings.TrimSpace(chunk)
					if chunk == "" {
						continue
					}

					currentTableName := extractTableNameFromFile(fileName)
					fmt.Printf("  Importing chunk %d/%d for %s (%d bytes)...\n",
						chunkIdx+1, len(chunks), currentTableName, len(chunk))

					err = db.ExecuteData(conn, chunk)
					if err != nil {
						// Log the failing chunk to a file for debugging
						logFile := fmt.Sprintf("%s_chunk_%d_error.sql", currentTableName, chunkIdx+1)
						logErr := os.WriteFile(logFile, []byte(chunk), 0644)
						if logErr != nil {
							fmt.Printf("Warning: Failed to write error log: %v\n", logErr)
						}
						return fmt.Errorf("failed to execute chunk %d in %s (chunk saved to %s): %v",
							chunkIdx+1, fileName, logFile, err)
					}
					processedRows++

					if processedRows%10 == 0 {
						fmt.Printf("    Progress: %d/%d chunks processed\n", processedRows, len(chunks))
					}
				}
				fmt.Printf("Completed importing %s: Processed %d chunks successfully\n", 
					extractTableNameFromFile(fileName), processedRows)
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

func importSchema(conn *db.Connection, schemaContent []byte) error {
	// First pass: collect SQL mode and CREATE TABLE statements
	createTableStatements := make(map[string]string)
	var currentStatement strings.Builder
	sqlMode := ""

	lines := strings.Split(string(schemaContent), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Check for SQL mode comment
		if strings.HasPrefix(line, "--") {
			if strings.HasPrefix(line, "-- SQL_MODE=") {
				sqlMode = strings.TrimPrefix(line, "-- SQL_MODE=")
			}
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

	// Set SQL mode if specified and this is MySQL
	if sqlMode != "" && conn.Config.Driver == "mysql" {
		setModeSQL := fmt.Sprintf("SET SESSION sql_mode = '%s'", strings.TrimSpace(sqlMode))
		_, err := conn.DB.Exec(setModeSQL)
		if err != nil {
			return fmt.Errorf("failed to set SQL mode to '%s': %v", sqlMode, err)
		}
		fmt.Printf("Set SQL mode to: %s\n", sqlMode)
	}

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

// filterSchemaContent filters SQL schema content to only include selected tables
// and their related objects (foreign keys, indexes, etc.)
func filterSchemaContent(schemaData []byte, tables []string) []byte {
	// Convert table list to a map for easier lookup
	tableMap := make(map[string]bool)
	for _, t := range tables {
		tableMap[t] = true
	}

	// Split schema content into statements
	stmts := strings.Split(string(schemaData), ";")
	var filteredStmts []string

	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		// Get table name from statement
		tableName := extractTableNameFromSchema(stmt)
		if tableName == "" {
			// If we can't determine table name, include the statement
			// This handles USE, SET, and other non-table-specific statements
			filteredStmts = append(filteredStmts, stmt)
			continue
		}

		// Include statement if it's for a selected table
		if tableMap[tableName] {
			filteredStmts = append(filteredStmts, stmt)
		}
	}

	// Rebuild schema content
	return []byte(strings.Join(filteredStmts, ";\n") + ";")
}

// extractTableNameFromFile extracts the table name from a data file name,
// handling numbered prefixes correctly (e.g., "79_postal_delivery_options.sql" -> "postal_delivery_options")
func extractTableNameFromFile(fileName string) string {
	// Skip files that don't have the .sql extension
	if !strings.HasSuffix(fileName, ".sql") {
		return ""
	}

	// Remove .sql extension
	baseName := strings.TrimSuffix(fileName, ".sql")

	// Split on underscore
	parts := strings.SplitN(baseName, "_", 2)
	if len(parts) != 2 {
		return ""
	}

	// Validate that the first part is a number
	if _, err := strconv.Atoi(parts[0]); err != nil {
		return ""
	}

	// Return everything after the first underscore
	return parts[1]
}

// validateTableName checks if a table name is valid and exists in the provided list
func validateTableName(tableName string, availableTables map[string]bool) bool {
	if tableName == "" {
		return false
	}

	// Check if it's in the available tables list
	if len(availableTables) > 0 {
		return availableTables[tableName]
	}

	// If no available tables list provided, perform basic validation
	// Table names should not contain special characters except underscore
	validTableName := regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
	return validTableName.MatchString(tableName)
}

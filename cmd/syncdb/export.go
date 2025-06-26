package main

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/hoangnguyenba/syncdb/pkg/config"
	"github.com/hoangnguyenba/syncdb/pkg/db"
	"github.com/hoangnguyenba/syncdb/pkg/profile"
	"github.com/hoangnguyenba/syncdb/pkg/storage"
)

type ExportData struct {
	Metadata struct {
		ExportedAt   time.Time `json:"exported_at"`
		DatabaseName string    `json:"database_name"`
		Tables       []string  `json:"tables"`
		Schema       bool      `json:"include_schema"`
		ViewData     bool      `json:"include_view_data"`
		IncludeData  bool      `json:"include_data"`
		Base64       bool      `json:"base64"`
	} `json:"metadata"`
	Schema map[string]string                   `json:"schema,omitempty"`
	Data   map[string][]map[string]interface{} `json:"data"` // Keep this for now, might remove if not needed later
}

var (
	exportConfig *config.Config
)

func init() {
	var err error
	exportConfig, err = config.LoadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
	}
}

func newExportCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export database data",
		Long:  `Export database data to a file.`,
		RunE:  runExport, // Use the named function
	}

	// Add shared flags
	AddSharedFlags(cmd, false) // Pass false for export command

	// Add export-specific flags
	flags := cmd.Flags()
	flags.Int("batch-size", 500, "Number of records to process in a batch")
	flags.Int("limit", 0, "Maximum number of records to export per table (0 means no limit)")

	return cmd
}

// loadAndValidateArgs loads configuration, merges flags, validates required fields,
// and establishes the initial database connection.
func loadAndValidateArgs(cmd *cobra.Command) (*CommonArgs, int, *db.Connection, error) {
	if exportConfig == nil {
		return nil, 0, nil, fmt.Errorf("configuration not loaded")
	}

	// Get profile name from flag
	profileName, _ := cmd.Flags().GetString("profile")

	// Populate arguments from flags, config, and profile
	cmdArgs, err := populateCommonArgsFromFlagsAndConfig(cmd, exportConfig.Export.CommonConfig, profileName)
	if err != nil {
		return nil, 0, nil, err // Return error from profile loading/parsing
	}

	// Get export-specific flags/config
	batchSize := getIntFlagWithConfigFallback(cmd, "batch-size", exportConfig.Export.BatchSize)
	cmdArgs.RecordLimit, _ = cmd.Flags().GetInt("limit") // Default is 0 (no limit)

	// Validate required values (Database name should now be resolved considering profile)
	if cmdArgs.Database == "" {
		return nil, 0, nil, fmt.Errorf("database name is required (set via --database flag, SYNCDB_EXPORT_DATABASE env, or profile)")
	}

	// Validate storage-specific arguments
	switch cmdArgs.Storage {
	case "s3":
		if cmdArgs.S3Bucket == "" {
			return nil, 0, nil, fmt.Errorf("s3-bucket is required when storage is set to s3")
		}
		if cmdArgs.S3Region == "" {
			return nil, 0, nil, fmt.Errorf("s3-region is required when storage is set to s3")
		}
	case "gdrive":
		creds, _ := cmd.Flags().GetString("gdrive-credentials")
		if creds == "" {
			syncDBDir, err := profile.GetSyncDBDir("")
			if err != nil {
				return nil, 0, nil, fmt.Errorf("failed to get syncdb directory: %w", err)
			}
			creds = filepath.Join(syncDBDir, "google-creds.json")
		}
		folder, _ := cmd.Flags().GetString("gdrive-folder")
		if creds == "" {
			return nil, 0, nil, fmt.Errorf("gdrive-credentials is required when storage is set to gdrive")
		}
		if folder == "" {
			return nil, 0, nil, fmt.Errorf("gdrive-folder is required when storage is set to gdrive")
		}
		cmdArgs.GdriveCredentials = creds
		cmdArgs.GdriveFolder = folder
	}

	// Initialize database connection
	database, err := db.InitDB(cmdArgs.Driver, cmdArgs.Host, cmdArgs.Port, cmdArgs.Username, cmdArgs.Password, cmdArgs.Database)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("failed to connect to database: %v", err)
	}
	// Note: The caller (runExport) will be responsible for closing the connection

	// Create a Connection instance
	conn := &db.Connection{
		DB: database,
		Config: db.ConnectionConfig{
			Driver:      cmdArgs.Driver,
			Host:        cmdArgs.Host,
			Port:        cmdArgs.Port,
			User:        cmdArgs.Username,
			Password:    cmdArgs.Password,
			Database:    cmdArgs.Database,
			RecordLimit: cmdArgs.RecordLimit,
		},
	}

	cmdArgs.FromTableIndex, _ = cmd.Flags().GetInt("from-table-index")
	cmdArgs.FromChunkIndex, _ = cmd.Flags().GetInt("from-chunk-index")

	return &cmdArgs, batchSize, conn, nil // Return address of cmdArgs
}

func expandTablePatterns(allTables, patterns []string) map[string]bool {
	result := make(map[string]bool)
	for _, pat := range patterns {
		for _, tbl := range allTables {
			if db.TablePatternMatch(tbl, strings.TrimSpace(pat)) {
				result[tbl] = true
			}
		}
	}
	return result
}

// getFinalTables determines the list of tables to be exported based on command arguments,
// database schema dependencies, and exclusion lists. It also returns maps indicating
// which tables should have their schema or data excluded.
func getFinalTables(conn *db.Connection, cmdArgs *CommonArgs) ([]string, map[string]bool, map[string]bool, error) {
	var err error
	currentTables := cmdArgs.Tables
	allTables := currentTables
	if len(currentTables) == 0 {
		allTables, err = db.GetTables(conn)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to get tables: %v", err)
		}
		currentTables = allTables
	}

	// Expand patterns for all table-related params
	expandedInclude := expandTablePatterns(allTables, cmdArgs.Tables)
	expandedExclude := expandTablePatterns(allTables, cmdArgs.ExcludeTable)
	expandedExcludeSchema := expandTablePatterns(allTables, cmdArgs.ExcludeTableSchema)
	expandedExcludeData := expandTablePatterns(allTables, cmdArgs.ExcludeTableData)

	// Get table dependencies and sort tables to ensure proper order during export
	deps := make(map[string][]string)
	for _, table := range currentTables {
		tableDeps, err := db.GetTableDependencies(conn, table)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to get dependencies for table %s: %v", table, err)
		}
		// Only include dependencies that are in our current table list
		var filteredDeps []string
		for _, dep := range tableDeps {
			for _, current := range currentTables {
				if dep == current {
					filteredDeps = append(filteredDeps, dep)
					break
				}
			}
		}
		deps[table] = filteredDeps
		fmt.Printf("Table %s depends on: %v\n", table, filteredDeps)
	}

	// Sort tables by dependencies to ensure parent tables are exported first
	sortedTables := db.SortTablesByDependencies(currentTables, deps)
	fmt.Printf("Tables sorted by dependencies: %v\n", sortedTables)

	// Create maps for faster lookup
	excludeTableMap := expandedExclude
	excludeSchemaMap := expandedExcludeSchema
	excludeDataMap := expandedExcludeData

	// If a table is fully excluded, also exclude schema and data
	for t := range excludeTableMap {
		excludeSchemaMap[t] = true
		excludeDataMap[t] = true
	}

	// Filter out only completely excluded tables while preserving dependency order
	// Tables in excludeDataMap will still be included in finalTables but their data won't be exported
	var finalTables []string
	for _, t := range sortedTables {
		if !excludeTableMap[t] && (len(expandedInclude) == 0 || expandedInclude[t]) {
			finalTables = append(finalTables, t)
		}
	}

	fmt.Printf("Final table order for export: %v\n", finalTables)
	return finalTables, excludeSchemaMap, excludeDataMap, nil
}

// writeMetadata creates and writes the 0_metadata.json file.
func writeMetadata(exportPath string, cmdArgs *CommonArgs, finalTables []string) error { // Changed commonArgs to CommonArgs
	metadata := struct {
		ExportedAt   time.Time `json:"exported_at"`
		DatabaseName string    `json:"database_name"`
		Tables       []string  `json:"tables"`
		Schema       bool      `json:"include_schema"`
		ViewData     bool      `json:"include_view_data"`
		IncludeData  bool      `json:"include_data"`
		Base64       bool      `json:"base64"`
	}{
		ExportedAt:   time.Now(),
		DatabaseName: cmdArgs.Database,
		Tables:       finalTables,
		Schema:       cmdArgs.IncludeSchema,
		ViewData:     cmdArgs.IncludeViewData,
		IncludeData:  cmdArgs.IncludeData,
		Base64:       cmdArgs.Base64,
	}

	metadataData, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}

	metadataFile := filepath.Join(exportPath, "0_metadata.json")
	if err = os.WriteFile(metadataFile, metadataData, 0644); err != nil {
		return fmt.Errorf("failed to write metadata file %s: %v", metadataFile, err)
	}
	fmt.Printf("Wrote metadata file: %s\n", metadataFile)
	return nil
}

// writeSchema fetches and writes the schema definitions to a file (SQL or JSON).
func writeSchema(conn *db.Connection, exportPath string, cmdArgs *CommonArgs, finalTables []string, excludeSchemaMap map[string]bool) error {
	schemaDefinitions := make(map[string]string)
	for _, table := range finalTables {
		if excludeSchemaMap[table] {
			continue // Skip excluded tables
		}

		schema, err := db.GetTableSchema(conn, table)
		if err != nil {
			return fmt.Errorf("failed to get schema for table %s: %v", table, err)
		}
		schemaDefinitions[table] = schema.Definition
	}

	// Get SQL mode for MySQL databases
	var sqlMode string
	if conn.Config.Driver == "mysql" {
		err := conn.DB.QueryRow("SELECT @@SESSION.sql_mode").Scan(&sqlMode)
		if err != nil {
			return fmt.Errorf("failed to get SQL mode: %v", err)
		}
		schemaDefinitions["__sql_mode"] = sqlMode
	}

	// Write schema based on format
	var schemaData []byte
	schemaFileName := ""
	var err error

	if cmdArgs.Format == "sql" {
		schemaFileName = "0_schema.sql"
		var schemaOutput []string

		// Add SQL mode as a comment at the top of the file for MySQL
		if sqlMode, ok := schemaDefinitions["__sql_mode"]; ok {
			schemaOutput = append(schemaOutput, fmt.Sprintf("-- SQL_MODE=%s", sqlMode))
			delete(schemaDefinitions, "__sql_mode") // Remove from the table definitions
		}

		// Ensure consistent order for SQL output (iterate over finalTables which is sorted)
		for _, table := range finalTables {
			if definition, ok := schemaDefinitions[table]; ok {
				schemaOutput = append(schemaOutput, fmt.Sprintf("-- Table structure for %s\n%s\n", table, definition))
			}
		}
		schemaData = []byte(strings.Join(schemaOutput, "\n\n"))
	} else { // Default to JSON
		schemaFileName = "0_schema.json"
		schemaData, err = json.MarshalIndent(schemaDefinitions, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal schema to JSON: %v", err)
		}
	}

	schemaFile := filepath.Join(exportPath, schemaFileName)
	if err = os.WriteFile(schemaFile, schemaData, 0644); err != nil {
		return fmt.Errorf("failed to write schema file %s: %v", schemaFile, err)
	}
	fmt.Printf("Wrote schema file: %s\n", schemaFile)
	return nil
}

// writeTableDataFile exports data for a single table, formats it as SQL INSERTs,
// and writes it to a .sql file. Returns the number of records written.
func writeTableDataFileWithResume(conn *db.Connection, exportPath string, table string, cmdArgs *CommonArgs, batchSize int, tableIndex int, fromChunk int) (int, error) {
	fmt.Printf("Exporting data for table '%s'...", table)

	isView, err := db.IsView(conn, table)
	if err != nil {
		return 0, fmt.Errorf("failed to check if %s is a view: %v", table, err)
	}
	if isView && !cmdArgs.IncludeViewData {
		fmt.Println(" skipping view.")
		return 0, nil // Not an error, just skipping
	}

	// Create a buffer to store the raw JSON data from db.ExportTableData
	var buf bytes.Buffer
	if err := db.ExportTableData(conn, table, &buf); err != nil {
		return 0, fmt.Errorf("failed to export raw data for table %s: %v", table, err)
	}

	// Decode the JSON data from the buffer
	var operations []db.DataOperation
	decoder := json.NewDecoder(&buf)
	for {
		var op db.DataOperation
		if err := decoder.Decode(&op); err == io.EOF {
			break
		} else if err != nil {
			// Handle potential empty buffer case gracefully
			if buf.Len() == 0 {
				break // No data was written to the buffer
			}
			return 0, fmt.Errorf("failed to decode operation for table %s: %v", table, err)
		}
		operations = append(operations, op)
	}

	// Convert operations to data map slice
	data := make([]map[string]interface{}, len(operations))
	for i, op := range operations {
		data[i] = op.Data
	}

	recordCount := len(data)
	if recordCount == 0 {
		fmt.Println(" done (0 records).")
		// Optionally write an empty file or skip writing? For now, skip.
		return 0, nil
	}

	// --- Convert data to SQL format ---
	var sqlStatements []string

	// Get columns from database schema to ensure consistency and order
	tableSchema, err := db.GetTableSchema(conn, table)
	if err != nil {
		return 0, fmt.Errorf("failed to get schema for table %s during data export: %v", table, err)
	}
	allColumns := tableSchema.Columns

	// Add backticks to column names
	backtickedColumns := make([]string, len(allColumns))
	for i, col := range allColumns {
		backtickedColumns[i] = fmt.Sprintf("`%s`", col)
	}
	columnList := strings.Join(backtickedColumns, ", ")

	// Process in batches for bulk insert
	for i := 0; i < recordCount; i += batchSize {
		end := i + batchSize
		if end > recordCount {
			end = recordCount
		}
		batch := data[i:end]
		if len(batch) == 0 {
			continue
		}

		// Start the INSERT statement
		insertStmt := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES\n", table, columnList)
		valueStrings := make([]string, 0, len(batch))

		// Generate value sets for each row in the batch
		for _, row := range batch {
			values := make([]string, len(allColumns))
			for j, col := range allColumns {
				val, exists := row[col]
				if !exists || val == nil {
					values[j] = "NULL"
				} else {
					switch v := val.(type) {
					case string:
						if cmdArgs.Base64 {
							encodedValue := base64.StdEncoding.EncodeToString([]byte(v))
							values[j] = fmt.Sprintf("'%s'", encodedValue)
						} else {
							// Escape single quotes
							escapedString := strings.ReplaceAll(v, "'", "''")
							// Escape control characters (including tab, newline, etc.)
							escapedString = escapeControlCharsForSQL(escapedString)
							values[j] = fmt.Sprintf("'%s'", escapedString)
						}
					case time.Time:
						// Format time consistently, handle potential zero time
						if v.IsZero() {
							values[j] = "NULL" // Or appropriate default like '0000-00-00 00:00:00'
						} else {
							values[j] = fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05"))
						}
					case []byte: // Handle byte slices (e.g., BLOBs)
						if cmdArgs.Base64 {
							encodedValue := base64.StdEncoding.EncodeToString(v)
							values[j] = fmt.Sprintf("'%s'", encodedValue)
						} else {
							// Representing raw bytes in SQL is tricky.
							// For simplicity, maybe return error or require base64 for blobs?
							// Or use a placeholder/warning.
							// For now, let's assume base64 is preferred for binary.
							// If not base64, maybe hex encode?
							// values[j] = fmt.Sprintf("X'%x'", v) // Example for hex (MySQL specific?)
							return 0, fmt.Errorf("binary data found in table %s column %s, use --base64 flag for export", table, col)
						}
					case bool:
						if v {
							values[j] = "1"
						} else {
							values[j] = "0"
						}
					default:
						// Handle numbers, etc.
						values[j] = fmt.Sprintf("%v", v) // Default representation
					}
				}
			}
			valueStrings = append(valueStrings, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
		}

		// Complete the statement for the batch
		// Make sure statement ends with semicolon if not already present
		stmt := insertStmt + strings.Join(valueStrings, ",\n")
		if !strings.HasSuffix(strings.TrimSpace(stmt), ";") {
			stmt += ";"
		}
		sqlStatements = append(sqlStatements, stmt)
	}

	// Write data to file
	// Use tableIndex directly since it's already 1-based
	dataFile := filepath.Join(exportPath, fmt.Sprintf("%d_%s.sql", tableIndex, table))

	// Use query separator for compatibility with import
	separator := "\n--SYNCDB_QUERY_SEPARATOR--\n"
	if cmdArgs.QuerySeparator != "" {
		separator = cmdArgs.QuerySeparator
	}

	// Join statements with separator, ensuring each statement has a semicolon
	finalContent := strings.Join(sqlStatements, separator)

	if err := os.WriteFile(dataFile, []byte(finalContent), 0644); err != nil {
		return 0, fmt.Errorf("failed to write data file for table %s (%s): %v", table, dataFile, err)
	}

	fmt.Printf(" done (%d records written to %s)\n", recordCount, dataFile)
	return recordCount, nil
}

// TableExportResult holds the result of exporting a single table
type TableExportResult struct {
	TableName      string
	RecordsWritten int
	Error          error
}

// writeDataFiles exports table data in parallel using goroutines.
// Returns the total number of records exported across all tables.
func writeDataFiles(conn *db.Connection, exportPath string, cmdArgs *CommonArgs, finalTables []string, excludeDataMap map[string]bool, batchSize int) (int, error) {
	// Determine number of workers (default to number of CPU cores, but allow override via environment variable)
	numWorkers := runtime.NumCPU() / 2
	if envWorkers := os.Getenv("SYNCDB_EXPORT_WORKERS"); envWorkers != "" {
		if n, err := strconv.Atoi(envWorkers); err == nil && n > 0 {
			numWorkers = n
		}
	}
	if numWorkers < 1 {
		numWorkers = 1
	}

	// Create channels for work distribution and results
	tableChan := make(chan tableWork, len(finalTables))
	resultChan := make(chan TableExportResult, len(finalTables))

	// Create all worker connections first
	workerConns := make([]*db.Connection, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workerConfig := conn.Config // This is a copy of the ConnectionConfig struct
		workerConn, err := db.NewConnection(workerConfig)
		if err != nil {
			// Clean up any connections we've created so far
			for j := 0; j < i; j++ {
				workerConns[j].Close()
			}
			close(tableChan)
			return 0, fmt.Errorf("failed to create database connection for worker %d: %v", i+1, err)
		}
		workerConns[i] = workerConn
	}

	// Make sure we close all connections when we're done
	defer func() {
		for _, conn := range workerConns {
			if conn != nil {
				conn.Close()
			}
		}
	}()

	// Start worker goroutines
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		// Each worker gets its own connection
		workerConn := workerConns[i]

		wg.Add(1)
		go func() {
			defer wg.Done()
			for work := range tableChan {
				recordsWritten, err := writeTableDataFileWithResume(workerConn, exportPath, work.Table, cmdArgs, batchSize, work.FileIndex, work.FromChunk)
				resultChan <- TableExportResult{
					TableName:      work.Table,
					RecordsWritten: recordsWritten,
					Error:          err,
				}
			}
		}()
	}

	// Send work to workers in a separate goroutine
	go func() {
		fileIndex := 1
		startTable := 0
		if cmdArgs.FromTableIndex > 0 {
			startTable = cmdArgs.FromTableIndex - 1 // 1-based to 0-based
		}

		for i, table := range finalTables {
			if i < startTable {
				fileIndex++
				continue
			}

			if excludeDataMap[table] {
				fmt.Printf("Skipping data export for table '%s' due to exclusion.\n", table)
				fileIndex++
				continue
			}

			fromChunk := 0
			if i == startTable && cmdArgs.FromChunkIndex > 0 {
				fromChunk = cmdArgs.FromChunkIndex
			}

			tableChan <- tableWork{
				Table:     table,
				FileIndex: fileIndex,
				FromChunk: fromChunk,
			}
			fileIndex++
		}
		close(tableChan)
	}()

	// Close result channel when all workers are done
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	var totalRecords int
	var errors []string

	for result := range resultChan {
		if result.Error != nil {
			errMsg := fmt.Sprintf("error exporting table %s: %v", result.TableName, result.Error)
			errors = append(errors, errMsg)
			// Continue processing other tables instead of failing immediately
			continue
		}
		totalRecords += result.RecordsWritten
		fmt.Printf("Exported %d records from table '%s'\n", result.RecordsWritten, result.TableName)
	}

	// If there were any errors, return them all
	if len(errors) > 0 {
		return totalRecords, fmt.Errorf("encountered %d errors during export:\n%s",
			len(errors), strings.Join(errors, "\n"))
	}

	return totalRecords, nil
}

// tableWork represents a unit of work for exporting a single table
type tableWork struct {
	Table     string
	FileIndex int
	FromChunk int
}

// createZipArchive creates a zip file containing the contents of the export directory.
func createZipArchive(exportPath string, zipFileName string) error {
	zipFile, err := os.Create(zipFileName)
	if err != nil {
		return fmt.Errorf("failed to create zip file %s: %v", zipFileName, err)
	}
	defer zipFile.Close() // Ensure file is closed even on error during walk

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close() // Ensure writer is closed

	// Walk through the export directory and add files to zip
	err = filepath.Walk(exportPath, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr // Propagate walk error
		}

		// Skip the root export directory itself
		if path == exportPath {
			return nil
		}

		// Skip directories (zip writer handles directory entries implicitly)
		if info.IsDir() {
			return nil
		}

		// Create a new file header
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return fmt.Errorf("failed to create zip header for %s: %v", path, err)
		}

		// Set the name in the archive relative to the export directory's parent
		// This ensures the timestamped directory is the root inside the zip
		relPath, err := filepath.Rel(filepath.Dir(exportPath), path)
		if err != nil {
			return fmt.Errorf("failed to get relative path for %s: %v", path, err)
		}
		header.Name = relPath
		header.Method = zip.Deflate // Use compression

		// Create writer for this file within zip
		writer, err := zipWriter.CreateHeader(header)
		if err != nil {
			return fmt.Errorf("failed to create zip entry for %s: %v", path, err)
		}

		// Open the file to be zipped
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open file %s for zipping: %v", path, err)
		}
		defer file.Close()

		// Copy the file content into the zip
		_, err = io.Copy(writer, file)
		if err != nil {
			return fmt.Errorf("failed to write file %s to zip: %v", path, err)
		}

		return nil
	})

	if err != nil {
		// Attempt to remove partially created zip file on error
		zipWriter.Close()
		zipFile.Close()
		os.Remove(zipFileName)
		return fmt.Errorf("failed during zip archive creation: %v", err)
	}

	// Explicitly close writer and file before returning success
	if err := zipWriter.Close(); err != nil {
		return fmt.Errorf("failed to finalize zip writer: %v", err)
	}
	if err := zipFile.Close(); err != nil {
		return fmt.Errorf("failed to close zip file handle: %v", err)
	}

	fmt.Printf("Successfully created zip archive: %s\n", zipFileName)
	return nil
}

// uploadToS3 uploads either a single file (zip) or the contents of a directory to S3.
func uploadToS3(localPath string, isDirectory bool, cmdArgs *CommonArgs, timestamp string) error { // Changed commonArgs to CommonArgs
	// Initialize S3 storage
	s3Store := storage.NewS3Storage(cmdArgs.S3Bucket, cmdArgs.S3Region)
	if s3Store == nil {
		return fmt.Errorf("failed to initialize S3 storage. Please ensure AWS credentials are set (e.g., AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)")
	}

	if isDirectory {
		// Upload individual files from the directory
		fmt.Printf("Uploading individual files from %s to s3://%s/%s/%s/...\n", localPath, cmdArgs.S3Bucket, cmdArgs.Path, timestamp)

		err := filepath.Walk(localPath, func(path string, info os.FileInfo, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if info.IsDir() {
				return nil // Skip directories
			}

			// Open file
			file, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("failed to open file %s for S3 upload: %v", path, err)
			}
			defer file.Close()

			// Create S3 key: folderPath / timestamp / filename
			relPath, err := filepath.Rel(localPath, path) // Path relative to the timestamp dir
			if err != nil {
				return fmt.Errorf("failed to get relative path for %s: %v", path, err)
			}
			s3Key := filepath.Join(cmdArgs.Path, timestamp, relPath) // Base folder + timestamp + relative file path

			// Read file data
			fileData, err := io.ReadAll(file)
			if err != nil {
				return fmt.Errorf("failed to read file %s for S3 upload: %v", path, err)
			}

			// Upload to S3
			if err := s3Store.Upload(fileData, s3Key); err != nil {
				return fmt.Errorf("failed to upload file %s to S3 key s3://%s/%s: %v", path, cmdArgs.S3Bucket, s3Key, err)
			}
			fmt.Printf("Uploaded %s to s3://%s/%s\n", filepath.Base(path), cmdArgs.S3Bucket, s3Key)
			return nil
		})

		if err != nil {
			return fmt.Errorf("failed during S3 directory upload: %v", err)
		}
		fmt.Printf("Successfully uploaded all files from %s to S3 bucket: %s, path prefix: %s/%s\n", localPath, cmdArgs.S3Bucket, cmdArgs.Path, timestamp)

	} else {
		// Upload a single file (the zip archive)
		zipFileName := localPath
		zipFileData, err := os.ReadFile(zipFileName)
		if err != nil {
			return fmt.Errorf("failed to read zip file %s for S3 upload: %v", zipFileName, err)
		}

		// S3 key: Path / zipfilename.zip
		s3Key := filepath.Join(cmdArgs.Path, filepath.Base(zipFileName))
		fmt.Printf("Uploading %s to s3://%s/%s...\n", zipFileName, cmdArgs.S3Bucket, s3Key)

		if err := s3Store.Upload(zipFileData, s3Key); err != nil {
			return fmt.Errorf("failed to upload zip file %s to S3: %v", zipFileName, err)
		}
		fmt.Printf("Successfully uploaded %s to s3://%s/%s\n", zipFileName, cmdArgs.S3Bucket, s3Key)
	}

	return nil
}

// uploadToGDrive uploads either a single file (zip) or the contents of a directory to Google Drive.
func uploadToGDrive(localPath string, isDirectory bool, cmdArgs *CommonArgs, timestamp string) error {
	// Initialize Google Drive storage
	gdriveStore, err := storage.NewGoogleDriveStorage(cmdArgs.GdriveCredentials, cmdArgs.GdriveFolder)
	if err != nil {
		return fmt.Errorf("failed to initialize Google Drive storage: %v", err)
	}

	if isDirectory {
		// Upload individual files from the directory
		fmt.Printf("\n=== Starting directory upload to Google Drive ===\n")
		fmt.Printf("Source directory: %s\n", localPath)
		fmt.Printf("Google Drive folder ID: %s\n", cmdArgs.GdriveFolder)
		fmt.Printf("Using credentials from: %s\n", cmdArgs.GdriveCredentials)

		var totalFiles int
		err := filepath.Walk(localPath, func(path string, info os.FileInfo, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if !info.IsDir() {
				totalFiles++
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to count files for upload: %v", err)
		}

		fmt.Printf("Found %d files to upload\n\n", totalFiles)
		uploaded := 0

		err = filepath.Walk(localPath, func(path string, info os.FileInfo, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if info.IsDir() {
				return nil // Skip directories
			}

			// Open file
			file, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("failed to open file %s for Google Drive upload: %v", path, err)
			}
			defer file.Close()

			// Read file data
			fileData, err := io.ReadAll(file)
			if err != nil {
				return fmt.Errorf("failed to read file %s for Google Drive upload: %v", path, err)
			}

			// Create filename: timestamp_directory/filename
			relPath, err := filepath.Rel(localPath, path) // Path relative to the timestamp dir
			if err != nil {
				return fmt.Errorf("failed to get relative path for %s: %v", path, err)
			}

			fileName := filepath.Join(timestamp, relPath)
			fmt.Printf("Uploading %s to Google Drive...\n", fileName)

			// Upload to Google Drive
			if err := gdriveStore.Upload(fileData, fileName); err != nil {
				return fmt.Errorf("failed to upload file %s to Google Drive: %v", fileName, err)
			}
			uploaded++
			fmt.Printf("Progress: [%d/%d] files uploaded\n", uploaded, totalFiles)
			return nil
		})

		if err != nil {
			return fmt.Errorf("failed during Google Drive directory upload: %v", err)
		}
		fmt.Printf("\n=== Directory upload completed successfully ===\n")
		fmt.Printf("Total files uploaded: %d\n", totalFiles)
		fmt.Printf("Source directory: %s\n", localPath)
		fmt.Printf("Google Drive folder ID: %s\n", cmdArgs.GdriveFolder)

	} else {
		// Upload a single file (the zip archive)
		zipFileName := localPath
		fmt.Printf("\n=== Starting zip file upload to Google Drive ===\n")
		fmt.Printf("Source file: %s\n", zipFileName)
		fmt.Printf("Google Drive folder ID: %s\n", cmdArgs.GdriveFolder)
		fmt.Printf("Using credentials from: %s\n", cmdArgs.GdriveCredentials)

		fileInfo, err := os.Stat(zipFileName)
		if err != nil {
			return fmt.Errorf("failed to get file info for %s: %v", zipFileName, err)
		}
		fmt.Printf("File size: %.2f MB\n\n", float64(fileInfo.Size())/(1024*1024))

		zipFileData, err := os.ReadFile(zipFileName)
		if err != nil {
			return fmt.Errorf("failed to read zip file %s for Google Drive upload: %v", zipFileName, err)
		}

		// Use the base name of the zip file as the target name
		fileName := filepath.Base(zipFileName)
		fmt.Printf("Starting upload of %s...\n", fileName)

		if err := gdriveStore.Upload(zipFileData, fileName); err != nil {
			return fmt.Errorf("failed to upload zip file %s to Google Drive: %v", fileName, err)
		}
		fmt.Printf("\n=== Upload completed successfully ===\n")
	}

	return nil
}

// cleanupLocalFiles removes the specified local files or directories, logging warnings on failure.
func cleanupLocalFiles(paths ...string) {
	for _, path := range paths {
		if path == "" {
			continue
		}
		fmt.Printf("Cleaning up local path: %s\n", path)
		// if err := os.RemoveAll(path); err != nil {
		// 	fmt.Printf("Warning: failed to clean up path %s: %v\n", path, err)
		// }
	}
}

// runExport is the main execution function for the export command.
func runExport(cmd *cobra.Command, cmdLineArgs []string) error {
	cmdArgs, batchSize, conn, err := loadAndValidateArgs(cmd)
	if err != nil {
		return err // Error already formatted by loadAndValidateArgs
	}
	defer conn.Close() // Ensure connection is closed

	// Get the final list of tables to export, considering dependencies and exclusions
	finalTables, excludeSchemaMap, excludeDataMap, err := getFinalTables(conn, cmdArgs)
	if err != nil {
		return err // Error already formatted by getFinalTables
	}

	// If the provided path exists and contains metadata file, use it directly
	exportPath := cmdArgs.Path
	if storage.IsExportPath(exportPath) {
		// Use the provided path as is since it already contains metadata
		fmt.Printf("Using existing export path: %s\n", exportPath)
	} else {
		// Create timestamp for folder
		timestamp := time.Now().Format("20060102_150405")
		fileName := cmdArgs.FileName
		if fileName == "" {
			fileName = fmt.Sprintf("%s_%s", cmdArgs.Database, timestamp)
		}
		exportPath = filepath.Join(cmdArgs.Path, fileName)
	}

	// Create directory structure if needed
	if err = os.MkdirAll(exportPath, 0755); err != nil {
		return fmt.Errorf("failed to create export directory %s: %v", exportPath, err)
	}

	// Write metadata first
	if err = writeMetadata(exportPath, cmdArgs, finalTables); err != nil {
		return err // Error already formatted by writeMetadata
	}

	// Export schema if requested
	if cmdArgs.IncludeSchema {
		if err = writeSchema(conn, exportPath, cmdArgs, finalTables, excludeSchemaMap); err != nil {
			return err // Error already formatted by writeSchema
		}
	}

	// Export table data
	if cmdArgs.IncludeData {
		recordsExported, err := writeDataFiles(conn, exportPath, cmdArgs, finalTables, excludeDataMap, batchSize)
		if err != nil {
			return err // Error already formatted by writeDataFiles
		}
		fmt.Printf("Total records exported: %d\n", recordsExported)
	}

	// Create zip file if requested
	var zipFileName string
	if cmdArgs.Zip {
		zipFileName = exportPath + ".zip"
		fmt.Printf("Creating zip archive: %s\n", zipFileName)
		if err = createZipArchive(exportPath, zipFileName); err != nil {
			return fmt.Errorf("failed to create zip archive: %v", err)
		}
		// Zip successful, remove original directory *unless* S3 upload fails later
		// We'll handle cleanup after potential S3 upload
	}

	// Handle uploads to remote storage
	switch cmdArgs.Storage {
	case "s3":
		uploadPath := exportPath // Default to uploading the directory
		isDirectory := true
		if cmdArgs.Zip {
			uploadPath = zipFileName // Upload the zip file instead
			isDirectory = false
		}

		if err = uploadToS3(uploadPath, isDirectory, cmdArgs, filepath.Base(exportPath)); err != nil {
			// S3 upload failed. Don't clean up local files automatically.
			// User might want to retry or keep the local copy.
			fmt.Printf("S3 Upload failed: %v\n", err)
			fmt.Println("Local files/zip kept due to S3 upload failure.")
			return err
		}

		// Clean up local files after successful S3 upload (unless --keep-local was specified)
		cleanupLocalFiles(exportPath)
		if cmdArgs.Zip {
			cleanupLocalFiles(zipFileName)
		}

	case "gdrive":
		uploadPath := exportPath // Default to uploading the directory
		isDirectory := true
		if cmdArgs.Zip {
			uploadPath = zipFileName // Upload the zip file instead
			isDirectory = false
		}

		if err = uploadToGDrive(uploadPath, isDirectory, cmdArgs, filepath.Base(exportPath)); err != nil {
			// Google Drive upload failed. Don't clean up local files automatically.
			fmt.Printf("Google Drive Upload failed: %v\n", err)
			fmt.Println("Local files/zip kept due to Google Drive upload failure.")
			return err
		}

		// Clean up local files after successful upload (unless --keep-local was specified)
		cleanupLocalFiles(exportPath)
		if cmdArgs.Zip {
			cleanupLocalFiles(zipFileName)
		}
	}

	return nil
}

// escapeControlCharsForSQL escapes control characters in a string for SQL/JSON compatibility
func escapeControlCharsForSQL(s string) string {
	replacer := strings.NewReplacer(
		"\\", "\\\\", // escape backslash first
		"\t", "\\t",
		"\n", "\\n",
		"\r", "\\r",
		"\b", "\\b",
		"\f", "\\f",
		"\v", "\\v",
		"\x00", "\\0",
	)
	// Replace ASCII control chars 0x01-0x1F (except tab, newline, carriage return) with escaped unicode
	var out strings.Builder
	for _, r := range s {
		if r < 0x20 && r != '\t' && r != '\n' && r != '\r' {
			out.WriteString(fmt.Sprintf("\\u%04x", r))
		} else {
			out.WriteRune(r)
		}
	}
	return replacer.Replace(out.String())
}

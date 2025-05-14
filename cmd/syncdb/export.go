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
	"strings"
	"time"

	"github.com/hoangnguyenba/syncdb/pkg/config"
	"github.com/hoangnguyenba/syncdb/pkg/db"
	"github.com/hoangnguyenba/syncdb/pkg/storage"
	"github.com/spf13/cobra"
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

	return cmd
}

// loadAndValidateArgs loads configuration, merges flags, validates required fields,
// and establishes the initial database connection.
func loadAndValidateArgs(cmd *cobra.Command) (*CommonArgs, int, *db.Connection, error) { // Changed commonArgs to CommonArgs
	// Load config from environment
	cfg, err := config.LoadConfig()
	if err != nil {
		return nil, 0, nil, fmt.Errorf("failed to load config: %v", err)
	}

	// Get profile name from flag
	profileName, _ := cmd.Flags().GetString("profile")

	// Populate arguments from flags, config, and profile
	cmdArgs, err := populateCommonArgsFromFlagsAndConfig(cmd, cfg.Export.CommonConfig, profileName)
	if err != nil {
		return nil, 0, nil, err // Return error from profile loading/parsing
	}

	// Get export-specific flags/config (batch-size is not part of profile)
	// Use the simpler helper here as profile doesn't affect batch size
	batchSize := getIntFlagWithConfigFallback(cmd, "batch-size", cfg.Export.BatchSize)

	// Validate required values (Database name should now be resolved considering profile)
	if cmdArgs.Database == "" {
		return nil, 0, nil, fmt.Errorf("database name is required (set via --database flag, SYNCDB_EXPORT_DATABASE env, or profile)")
	}
	// Validate S3 args if storage=s3 (Storage is not part of profile)
	if cmdArgs.Storage == "s3" {
		if cmdArgs.S3Bucket == "" {
			return nil, 0, nil, fmt.Errorf("s3-bucket is required when storage is set to s3")
		}
		if cmdArgs.S3Region == "" {
			return nil, 0, nil, fmt.Errorf("s3-region is required when storage is set to s3")
		}
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
			Driver:   cmdArgs.Driver,
			Host:     cmdArgs.Host,
			Port:     cmdArgs.Port,
			User:     cmdArgs.Username,
			Password: cmdArgs.Password,
			Database: cmdArgs.Database,
		},
	}

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

	// Filter out fully excluded tables while preserving dependency order
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
func writeSchema(conn *db.Connection, exportPath string, cmdArgs *CommonArgs, finalTables []string, excludeSchemaMap map[string]bool) error { // Changed commonArgs to CommonArgs
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

	// Write schema based on format
	var schemaData []byte
	var schemaFileName string
	var err error

	if cmdArgs.Format == "sql" {
		schemaFileName = "0_schema.sql"
		var schemaOutput []string
		// Ensure consistent order for SQL output (iterate over finalTables which is sorted)
		for _, table := range finalTables {
			if definition, ok := schemaDefinitions[table]; ok {
				schemaOutput = append(schemaOutput, fmt.Sprintf("-- Table structure for %s\n%s\n", table, definition))
			}
		}
		schemaData = []byte(strings.Join(schemaOutput, "\n"))
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
func writeTableDataFile(conn *db.Connection, exportPath string, table string, cmdArgs *CommonArgs, batchSize int, tableIndex int) (int, error) { // Changed commonArgs to CommonArgs
	fmt.Printf("Exporting data for table '%s'...", table)

	// Check if it's a view and if view data should be excluded
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
		// Check for "no rows in result set" specifically if needed, might not be a fatal error
		// if strings.Contains(err.Error(), "no rows in result set") {
		// 	fmt.Println(" done (0 records).")
		// 	return 0, nil // Table is empty, not an error
		// }
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
							values[j] = fmt.Sprintf("'%s'", encodedValue) // Assuming base64 strings are safe for SQL literals
						} else {
							escapedString := strings.ReplaceAll(v, "'", "''")
							// escapedString = strings.ReplaceAll(escapedString, "\\", "\\\\") // Consider if needed
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
							// Representing raw bytes in SQL is tricky. Hex is common.
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
		insertStmt += strings.Join(valueStrings, ",\n") + ";"
		sqlStatements = append(sqlStatements, insertStmt)
	}

	// Write data to file
	// Use tableIndex+1 for 1-based file numbering consistent with original logic
	dataFile := filepath.Join(exportPath, fmt.Sprintf("%d_%s.sql", tableIndex+1, table))
	if err := os.WriteFile(dataFile, []byte(strings.Join(sqlStatements, "\n\n")), 0644); err != nil {
		return 0, fmt.Errorf("failed to write data file for table %s (%s): %v", table, dataFile, err)
	}

	fmt.Printf(" done (%d records written to %s)\n", recordCount, dataFile)
	return recordCount, nil
}

// writeDataFiles iterates through tables and calls writeTableDataFile for each.
// Returns the total number of records exported across all tables.
func writeDataFiles(conn *db.Connection, exportPath string, cmdArgs *CommonArgs, finalTables []string, excludeDataMap map[string]bool, batchSize int) (int, error) { // Changed commonArgs to CommonArgs
	totalRecords := 0
	for i, table := range finalTables {
		if excludeDataMap[table] {
			fmt.Printf("Skipping data export for table '%s' due to exclusion.\n", table)
			continue
		}

		// Pass table index (i) for file naming
		recordsWritten, err := writeTableDataFile(conn, exportPath, table, cmdArgs, batchSize, i)
		if err != nil {
			// Decide if we should continue with other tables or stop on first error
			return totalRecords, fmt.Errorf("error exporting data for table %s: %v", table, err) // Stop on error
		}
		totalRecords += recordsWritten
	}
	return totalRecords, nil
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
		fmt.Printf("Uploading individual files from %s to s3://%s/%s/%s/...\n", localPath, cmdArgs.S3Bucket, cmdArgs.FolderPath, timestamp)

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
			s3Key := filepath.Join(cmdArgs.FolderPath, timestamp, relPath) // Base folder + timestamp + relative file path

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
		fmt.Printf("Successfully uploaded all files from %s to S3 bucket: %s, path prefix: %s/%s\n", localPath, cmdArgs.S3Bucket, cmdArgs.FolderPath, timestamp)

	} else {
		// Upload a single file (the zip archive)
		zipFileName := localPath
		zipFileData, err := os.ReadFile(zipFileName)
		if err != nil {
			return fmt.Errorf("failed to read zip file %s for S3 upload: %v", zipFileName, err)
		}

		// S3 key: folderPath / zipfilename.zip
		s3Key := filepath.Join(cmdArgs.FolderPath, filepath.Base(zipFileName))
		fmt.Printf("Uploading %s to s3://%s/%s...\n", zipFileName, cmdArgs.S3Bucket, s3Key)

		if err := s3Store.Upload(zipFileData, s3Key); err != nil {
			return fmt.Errorf("failed to upload zip file %s to S3: %v", zipFileName, err)
		}
		fmt.Printf("Successfully uploaded %s to s3://%s/%s\n", zipFileName, cmdArgs.S3Bucket, s3Key)
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

	// Create timestamp for folder
	timestamp := time.Now().Format("20060102_150405")
	exportPath := filepath.Join(cmdArgs.FolderPath, timestamp) // Use cmdArgs.FolderPath

	// Create directory structure
	if err = os.MkdirAll(exportPath, 0755); err != nil {
		return fmt.Errorf("failed to create directory structure: %v", err)
	}

	fmt.Printf("Starting export of %d tables from database '%s' to %s\n", len(finalTables), cmdArgs.Database, exportPath)

	// Write metadata file
	if err = writeMetadata(exportPath, cmdArgs, finalTables); err != nil {
		// Attempt cleanup before returning error
		cleanupLocalFiles(exportPath)
		return err
	}

	// Export and write schema if requested
	if cmdArgs.IncludeSchema {
		if err = writeSchema(conn, exportPath, cmdArgs, finalTables, excludeSchemaMap); err != nil {
			// cleanupLocalFiles(exportPath)
			return err
		}
	}

	// Export and write data if requested
	totalRecords := 0
	if cmdArgs.IncludeData {
		totalRecords, err = writeDataFiles(conn, exportPath, cmdArgs, finalTables, excludeDataMap, batchSize)
		if err != nil {
			cleanupLocalFiles(exportPath)
			return err
		}
		fmt.Printf("Finished exporting data. Total records: %d\n", totalRecords)
	} else {
		fmt.Println("Skipping data export as per --include-data=false.")
	}

	// --- Post-export processing (Zip, S3 Upload, Cleanup) ---
	zipFileName := "" // Store zip file name if created

	// Handle Zipping
	if cmdArgs.Zip {
		zipFileName = filepath.Join(cmdArgs.FolderPath, timestamp+".zip")
		if err = createZipArchive(exportPath, zipFileName); err != nil {
			cleanupLocalFiles(exportPath, zipFileName) // Clean up dir and potentially partial zip
			return err                                 // Error already formatted by createZipArchive
		}
		// Zip successful, remove original directory *unless* S3 upload fails later
		// We'll handle cleanup after potential S3 upload
	}

	// Handle S3 Upload
	if cmdArgs.Storage == "s3" {
		uploadPath := exportPath // Default to uploading the directory
		isDirectory := true
		if cmdArgs.Zip {
			uploadPath = zipFileName // Upload the zip file instead
			isDirectory = false
		}

		if err = uploadToS3(uploadPath, isDirectory, cmdArgs, timestamp); err != nil {
			// S3 upload failed. Don't clean up local files automatically.
			// User might want to retry or keep the local copy.
			fmt.Printf("S3 Upload failed: %v\n", err)
			fmt.Println("Local files/zip kept due to S3 upload failure.")
			// Return the S3 error, but maybe wrap it?
			return fmt.Errorf("S3 upload failed: %w", err)
		}

		// S3 Upload successful, clean up local files
		if cmdArgs.Zip {
			cleanupLocalFiles(exportPath, zipFileName) // Clean up original dir and the zip file
		} else {
			cleanupLocalFiles(exportPath) // Clean up the directory
		}
	} else {
		// Local storage scenario
		if cmdArgs.Zip {
			// Zip created locally, clean up original directory
			cleanupLocalFiles(exportPath)
			fmt.Printf("Successfully exported %d tables (%d records) to %s\n", len(finalTables), totalRecords, zipFileName)
		} else {
			// No zip, no S3 - files are in exportPath
			fmt.Printf("Successfully exported %d tables (%d records) to %s\n", len(finalTables), totalRecords, exportPath)
		}
	}

	return nil
}

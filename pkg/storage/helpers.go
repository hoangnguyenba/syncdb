package storage

import (
	"os"
	"path/filepath"
)

// IsExportPath checks if the given path contains the metadata file indicating it's a complete export path.
func IsExportPath(path string) bool {
	if path == "" {
		return false
	}

	metadataPath := filepath.Join(path, "0_metadata.json")
	if _, err := os.Stat(metadataPath); err == nil {
		return true
	}
	return false
}

package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type QueryDefinition struct {
	Database   string                 `json:"database" yaml:"database"`
	Collection string                 `json:"collection" yaml:"collection"`
	Operation  string                 `json:"operation" yaml:"operation"`
	Filter     map[string]interface{} `json:"filter" yaml:"filter"`
	Pipeline   []interface{}          `json:"pipeline,omitempty" yaml:"pipeline,omitempty"`
	Projection map[string]interface{} `json:"projection,omitempty" yaml:"projection,omitempty"`
	Limit      int64                  `json:"limit,omitempty" yaml:"limit,omitempty"`
	Update     map[string]interface{} `json:"update,omitempty" yaml:"update,omitempty"`
	Upsert     bool                   `json:"upsert,omitempty" yaml:"upsert,omitempty"`
}

type QueriesFile struct {
	Queries []QueryDefinition
}

// LoadQueries filters files based on the 'loadDefault' flag.
// - If loadDefault is TRUE: Load ONLY 'default.json'.
// - If loadDefault is FALSE: Load ALL files EXCEPT 'default.json'.
// - Single file paths are always loaded.
func LoadQueries(path string, loadDefault bool) (*QueriesFile, error) {
	if path == "" {
		return &QueriesFile{}, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat path %s: %w", path, err)
	}

	var allQueries []QueryDefinition

	if info.IsDir() {
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil, fmt.Errorf("read queries dir: %w", err)
		}

		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".json") {
				continue
			}

			isDefault := strings.EqualFold(entry.Name(), "default.json")

			if loadDefault {
				// Mode: Default Workload -> Load ONLY default.json
				if !isDefault {
					continue
				}
			} else {
				// Mode: Custom Workload -> Load EVERYTHING ELSE
				if isDefault {
					continue
				}
			}

			fullPath := filepath.Join(path, entry.Name())
			loaded, err := loadQueriesFromFile(fullPath)
			if err != nil {
				return nil, fmt.Errorf("error loading query file %s: %w", entry.Name(), err)
			}
			allQueries = append(allQueries, loaded.Queries...)
		}
	} else {
		// Single file: Always load it.
		loaded, err := loadQueriesFromFile(path)
		if err != nil {
			return nil, err
		}
		allQueries = append(allQueries, loaded.Queries...)
	}

	return &QueriesFile{Queries: allQueries}, nil
}

func loadQueriesFromFile(path string) (*QueriesFile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read queries file: %w", err)
	}

	var defs []QueryDefinition
	if err := json.Unmarshal(b, &defs); err != nil {
		return nil, fmt.Errorf("invalid JSON format for queries: %w", err)
	}

	return &QueriesFile{Queries: defs}, nil
}

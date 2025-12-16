package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type CollectionField struct {
	Type      string                     `json:"type"`
	Provider  string                     `json:"provider,omitempty"`
	MaxLength int                        `json:"maxLength,omitempty"`
	MinLength int                        `json:"minLength,omitempty"`
	Min       *int                       `json:"min,omitempty"`
	Max       *int                       `json:"max,omitempty"`
	Enum      []string                   `json:"enum,omitempty"`
	Items     *CollectionField           `json:"items,omitempty"`
	Fields    map[string]CollectionField `json:"fields,omitempty"`
	ArraySize int                        `json:"arraySize,omitempty"`
}

type IndexDefinition struct {
	Keys map[string]interface{} `json:"keys"`
}

// ShardConfig defines how a collection should be sharded.
type ShardConfig struct {
	Key    map[string]interface{} `json:"key"`
	Unique bool                   `json:"unique,omitempty"`
}

type CollectionDefinition struct {
	DatabaseName string                     `json:"database"`
	Name         string                     `json:"collection"`
	Fields       map[string]CollectionField `json:"fields"`
	Indexes      []IndexDefinition          `json:"indexes,omitempty"`
	ShardConfig  *ShardConfig               `json:"shardConfig,omitempty"`
}

type CollectionsFile struct {
	Collections []CollectionDefinition `json:"collections"`
}

// LoadCollections filters files based on the 'loadDefault' flag (from DefaultWorkload config).
func LoadCollections(path string, loadDefault bool) (*CollectionsFile, error) {
	if path == "" {
		return &CollectionsFile{}, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat path %s: %w", path, err)
	}

	var allCollections []CollectionDefinition

	if info.IsDir() {
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil, fmt.Errorf("read collections dir: %w", err)
		}

		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".json") {
				continue
			}

			isDefault := strings.EqualFold(entry.Name(), "default.json")
			if loadDefault && !isDefault {
				continue // Skip non-default files
			}
			if !loadDefault && isDefault {
				continue // Skip default file
			}

			fullPath := filepath.Join(path, entry.Name())
			loaded, err := loadCollectionsFromFile(fullPath)
			if err != nil {
				return nil, fmt.Errorf("error loading collection file %s: %w", entry.Name(), err)
			}
			allCollections = append(allCollections, loaded.Collections...)
		}
	} else {
		// Single file: apply filtering logic to the specific file
		filename := filepath.Base(path)
		isDefault := strings.EqualFold(filename, "default.json")

		shouldLoad := true
		if loadDefault && !isDefault {
			shouldLoad = false
		}
		if !loadDefault && isDefault {
			shouldLoad = false
		}

		if shouldLoad {
			loaded, err := loadCollectionsFromFile(path)
			if err != nil {
				return nil, err
			}
			allCollections = append(allCollections, loaded.Collections...)
		}
	}

	return &CollectionsFile{Collections: allCollections}, nil
}

func loadCollectionsFromFile(path string) (*CollectionsFile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read collections file: %w", err)
	}

	var wrapped CollectionsFile
	if err := json.Unmarshal(b, &wrapped); err == nil && len(wrapped.Collections) > 0 {
		return &wrapped, nil
	}

	var arr []CollectionDefinition
	if err := json.Unmarshal(b, &arr); err == nil && len(arr) > 0 {
		return &CollectionsFile{Collections: arr}, nil
	}

	return nil, fmt.Errorf("invalid collections format in %s", path)
}

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package api

// Factory is the base interface for all plugin factories
type Factory interface {
	// FactoryIdentifier returns the unique identifier of this factory
	FactoryIdentifier() string

	// OptionRules returns the configuration option rules
	OptionRules() []OptionRule
}

// TableSourceFactory creates Source instances
type TableSourceFactory interface {
	Factory

	// CreateSource creates a source from the given context
	CreateSource(ctx TableSourceFactoryContext) (Source, error)
}

// TableSinkFactory creates Sink instances
type TableSinkFactory interface {
	Factory

	// CreateSink creates a sink from the given context
	CreateSink(ctx TableSinkFactoryContext) (Sink, error)
}

// TableSourceFactoryContext provides context for creating sources
type TableSourceFactoryContext interface {
	// GetOptions returns the configuration options
	GetOptions() *Config

	// GetCatalogTable returns the catalog table if available
	GetCatalogTable() *CatalogTable

	// GetClassLoader returns nil (Go doesn't have ClassLoader)
	GetClassLoader() interface{}
}

// TableSinkFactoryContext provides context for creating sinks
type TableSinkFactoryContext interface {
	// GetOptions returns the configuration options
	GetOptions() *Config

	// GetCatalogTable returns the catalog table
	GetCatalogTable() *CatalogTable
}

// CatalogTable represents a table in the catalog
type CatalogTable struct {
	TableID     TableID
	TableSchema *SeaTunnelRowType
	Options     map[string]string
	PartitionKeys []string
	Comment     string
}

// TableID identifies a table
type TableID struct {
	Catalog  string
	Database string
	Schema   string
	Table    string
}

// String returns the full table identifier
func (t TableID) String() string {
	result := t.Table
	if t.Schema != "" {
		result = t.Schema + "." + result
	}
	if t.Database != "" {
		result = t.Database + "." + result
	}
	if t.Catalog != "" {
		result = t.Catalog + "." + result
	}
	return result
}

// OptionRule defines a configuration option rule
type OptionRule struct {
	Name         string
	Type         OptionType
	Required     bool
	DefaultValue interface{}
	Description  string
}

// OptionType represents the type of an option
type OptionType int

const (
	OptionTypeString OptionType = iota
	OptionTypeInt
	OptionTypeLong
	OptionTypeDouble
	OptionTypeBoolean
	OptionTypeEnum
	OptionTypeList
	OptionTypeMap
)

// Config represents configuration options
type Config struct {
	data map[string]interface{}
}

// NewConfig creates a new Config
func NewConfig() *Config {
	return &Config{data: make(map[string]interface{})}
}

// NewConfigFromMap creates a Config from a map
func NewConfigFromMap(data map[string]interface{}) *Config {
	return &Config{data: data}
}

// Get returns a value by key
func (c *Config) Get(key string) (interface{}, bool) {
	v, ok := c.data[key]
	return v, ok
}

// GetString returns a string value
func (c *Config) GetString(key string) string {
	if v, ok := c.data[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// GetStringDefault returns a string value with default
func (c *Config) GetStringDefault(key, defaultValue string) string {
	if v, ok := c.data[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return defaultValue
}

// GetInt returns an int value
func (c *Config) GetInt(key string) int {
	if v, ok := c.data[key]; ok {
		switch n := v.(type) {
		case int:
			return n
		case int64:
			return int(n)
		case float64:
			return int(n)
		}
	}
	return 0
}

// GetIntDefault returns an int value with default
func (c *Config) GetIntDefault(key string, defaultValue int) int {
	if v, ok := c.data[key]; ok {
		switch n := v.(type) {
		case int:
			return n
		case int64:
			return int(n)
		case float64:
			return int(n)
		}
	}
	return defaultValue
}

// GetInt64 returns an int64 value
func (c *Config) GetInt64(key string) int64 {
	if v, ok := c.data[key]; ok {
		switch n := v.(type) {
		case int64:
			return n
		case int:
			return int64(n)
		case float64:
			return int64(n)
		}
	}
	return 0
}

// GetFloat64 returns a float64 value
func (c *Config) GetFloat64(key string) float64 {
	if v, ok := c.data[key]; ok {
		switch n := v.(type) {
		case float64:
			return n
		case int:
			return float64(n)
		case int64:
			return float64(n)
		}
	}
	return 0
}

// GetBool returns a bool value
func (c *Config) GetBool(key string) bool {
	if v, ok := c.data[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

// GetBoolDefault returns a bool value with default
func (c *Config) GetBoolDefault(key string, defaultValue bool) bool {
	if v, ok := c.data[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return defaultValue
}

// GetStringList returns a string list value
func (c *Config) GetStringList(key string) []string {
	if v, ok := c.data[key]; ok {
		if list, ok := v.([]interface{}); ok {
			result := make([]string, 0, len(list))
			for _, item := range list {
				if s, ok := item.(string); ok {
					result = append(result, s)
				}
			}
			return result
		}
	}
	return nil
}

// GetConfig returns a nested config
func (c *Config) GetConfig(key string) *Config {
	if v, ok := c.data[key]; ok {
		if m, ok := v.(map[string]interface{}); ok {
			return NewConfigFromMap(m)
		}
	}
	return NewConfig()
}

// Set sets a value
func (c *Config) Set(key string, value interface{}) {
	c.data[key] = value
}

// Has checks if a key exists
func (c *Config) Has(key string) bool {
	_, ok := c.data[key]
	return ok
}

// Keys returns all keys
func (c *Config) Keys() []string {
	keys := make([]string, 0, len(c.data))
	for k := range c.data {
		keys = append(keys, k)
	}
	return keys
}

// ToMap returns the underlying map
func (c *Config) ToMap() map[string]interface{} {
	return c.data
}

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

package fake

import (
	"fmt"
	"strings"

	"github.com/apache/seatunnel-go/pkg/api"
)

// FakeSourceFactory creates FakeSource instances
type FakeSourceFactory struct{}

// NewFakeSourceFactory creates a new factory
func NewFakeSourceFactory() *FakeSourceFactory {
	return &FakeSourceFactory{}
}

// FactoryIdentifier returns the identifier
func (f *FakeSourceFactory) FactoryIdentifier() string {
	return PluginName
}

// OptionRules returns the configuration options
func (f *FakeSourceFactory) OptionRules() []api.OptionRule {
	return []api.OptionRule{
		{
			Name:         "row.num",
			Type:         api.OptionTypeLong,
			Required:     false,
			DefaultValue: int64(10),
			Description:  "Number of rows to generate",
		},
		{
			Name:         "split.num",
			Type:         api.OptionTypeInt,
			Required:     false,
			DefaultValue: 1,
			Description:  "Number of splits",
		},
		{
			Name:         "split.read-interval",
			Type:         api.OptionTypeLong,
			Required:     false,
			DefaultValue: int64(0),
			Description:  "Interval between reads in milliseconds",
		},
		{
			Name:         "schema",
			Type:         api.OptionTypeMap,
			Required:     false,
			Description:  "Schema definition",
		},
		{
			Name:         "string.length",
			Type:         api.OptionTypeInt,
			Required:     false,
			DefaultValue: 50,
			Description:  "Default string length",
		},
		{
			Name:         "string.fake.mode",
			Type:         api.OptionTypeEnum,
			Required:     false,
			DefaultValue: "range",
			Description:  "String generation mode: range, sentence, word, uuid, name, email, phone, address",
		},
		{
			Name:         "array.size",
			Type:         api.OptionTypeInt,
			Required:     false,
			DefaultValue: 3,
			Description:  "Default array size",
		},
		{
			Name:         "map.size",
			Type:         api.OptionTypeInt,
			Required:     false,
			DefaultValue: 3,
			Description:  "Default map size",
		},
		{
			Name:         "bytes.length",
			Type:         api.OptionTypeInt,
			Required:     false,
			DefaultValue: 16,
			Description:  "Default bytes length",
		},
	}
}

// CreateSource creates a fake source
func (f *FakeSourceFactory) CreateSource(ctx api.TableSourceFactoryContext) (api.Source, error) {
	config := ctx.GetOptions()

	// Parse options
	options := DefaultFakeOptions()
	options.RowCount = config.GetInt64("row.num")
	if options.RowCount <= 0 {
		options.RowCount = 10
	}

	options.SplitNum = config.GetIntDefault("split.num", 1)
	options.SplitReadInterval = config.GetInt64("split.read-interval")
	options.StringLength = config.GetIntDefault("string.length", 50)
	options.ArraySize = config.GetIntDefault("array.size", 3)
	options.MapSize = config.GetIntDefault("map.size", 3)
	options.BytesLength = config.GetIntDefault("bytes.length", 16)

	// Parse string fake mode
	if modeStr := config.GetString("string.fake.mode"); modeStr != "" {
		options.StringFakeMode = parseStringFakeMode(modeStr)
	}

	// Parse schema
	var schema *api.SeaTunnelRowType
	if ctx.GetCatalogTable() != nil && ctx.GetCatalogTable().TableSchema != nil {
		schema = ctx.GetCatalogTable().TableSchema
	} else {
		// Parse schema from config
		var err error
		schema, err = parseSchemaFromConfig(config)
		if err != nil {
			return nil, fmt.Errorf("failed to parse schema: %w", err)
		}
	}

	if schema == nil {
		// Create default schema
		schema = createDefaultSchema()
	}

	return NewFakeSource(schema, options), nil
}

// parseStringFakeMode parses string fake mode from string
func parseStringFakeMode(mode string) StringFakeMode {
	switch strings.ToLower(mode) {
	case "sentence":
		return StringFakeModeSentence
	case "word":
		return StringFakeModeWord
	case "uuid":
		return StringFakeModeUUID
	case "name":
		return StringFakeModeName
	case "email":
		return StringFakeModeEmail
	case "phone":
		return StringFakeModePhone
	case "address":
		return StringFakeModeAddress
	default:
		return StringFakeModeRange
	}
}

// parseSchemaFromConfig parses schema from configuration
func parseSchemaFromConfig(config *api.Config) (*api.SeaTunnelRowType, error) {
	schemaConfig := config.GetConfig("schema")
	if schemaConfig == nil {
		return nil, nil
	}

	fieldsConfig := schemaConfig.GetConfig("fields")
	if fieldsConfig == nil {
		return nil, nil
	}

	var fields []*api.SeaTunnelFieldType
	for _, key := range fieldsConfig.Keys() {
		typeStr := fieldsConfig.GetString(key)
		dataType, err := parseDataType(typeStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse type for field %s: %w", key, err)
		}
		fields = append(fields, api.NewField(key, dataType))
	}

	return api.NewRowSchema(fields...), nil
}

// parseDataType parses a data type from string
func parseDataType(typeStr string) (*api.SeaTunnelDataType, error) {
	typeStr = strings.TrimSpace(strings.ToUpper(typeStr))

	// Handle parameterized types
	if strings.HasPrefix(typeStr, "ARRAY<") && strings.HasSuffix(typeStr, ">") {
		inner := typeStr[6 : len(typeStr)-1]
		elementType, err := parseDataType(inner)
		if err != nil {
			return nil, err
		}
		return api.NewArrayType(elementType), nil
	}

	if strings.HasPrefix(typeStr, "MAP<") && strings.HasSuffix(typeStr, ">") {
		inner := typeStr[4 : len(typeStr)-1]
		parts := splitTypeParams(inner)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid map type: %s", typeStr)
		}
		keyType, err := parseDataType(parts[0])
		if err != nil {
			return nil, err
		}
		valueType, err := parseDataType(parts[1])
		if err != nil {
			return nil, err
		}
		return api.NewMapType(keyType, valueType), nil
	}

	if strings.HasPrefix(typeStr, "DECIMAL(") && strings.HasSuffix(typeStr, ")") {
		inner := typeStr[8 : len(typeStr)-1]
		parts := strings.Split(inner, ",")
		precision := 10
		scale := 0
		if len(parts) >= 1 {
			fmt.Sscanf(strings.TrimSpace(parts[0]), "%d", &precision)
		}
		if len(parts) >= 2 {
			fmt.Sscanf(strings.TrimSpace(parts[1]), "%d", &scale)
		}
		return api.NewDecimalType(precision, scale), nil
	}

	// Simple types
	switch typeStr {
	case "NULL":
		return api.NewBasicType(api.NULL), nil
	case "BOOLEAN", "BOOL":
		return api.NewBasicType(api.BOOLEAN), nil
	case "TINYINT":
		return api.NewBasicType(api.TINYINT), nil
	case "SMALLINT":
		return api.NewBasicType(api.SMALLINT), nil
	case "INT", "INTEGER":
		return api.NewBasicType(api.INT), nil
	case "BIGINT", "LONG":
		return api.NewBasicType(api.BIGINT), nil
	case "FLOAT":
		return api.NewBasicType(api.FLOAT), nil
	case "DOUBLE":
		return api.NewBasicType(api.DOUBLE), nil
	case "STRING", "VARCHAR", "TEXT":
		return api.NewBasicType(api.STRING), nil
	case "BYTES", "BINARY":
		return api.NewBasicType(api.BYTES), nil
	case "DATE":
		return api.NewBasicType(api.DATE), nil
	case "TIME":
		return api.NewBasicType(api.TIME), nil
	case "TIMESTAMP", "DATETIME":
		return api.NewBasicType(api.TIMESTAMP), nil
	default:
		return nil, fmt.Errorf("unknown type: %s", typeStr)
	}
}

// splitTypeParams splits type parameters respecting nesting
func splitTypeParams(s string) []string {
	var result []string
	var current strings.Builder
	depth := 0

	for _, c := range s {
		switch c {
		case '<':
			depth++
			current.WriteRune(c)
		case '>':
			depth--
			current.WriteRune(c)
		case ',':
			if depth == 0 {
				result = append(result, strings.TrimSpace(current.String()))
				current.Reset()
			} else {
				current.WriteRune(c)
			}
		default:
			current.WriteRune(c)
		}
	}

	if current.Len() > 0 {
		result = append(result, strings.TrimSpace(current.String()))
	}

	return result
}

// createDefaultSchema creates a default schema for testing
func createDefaultSchema() *api.SeaTunnelRowType {
	return api.NewRowSchema(
		api.NewField("id", api.NewBasicType(api.BIGINT)),
		api.NewField("name", api.NewBasicType(api.STRING)),
		api.NewField("age", api.NewBasicType(api.INT)),
		api.NewField("score", api.NewBasicType(api.DOUBLE)),
		api.NewField("active", api.NewBasicType(api.BOOLEAN)),
		api.NewField("created_at", api.NewBasicType(api.TIMESTAMP)),
	)
}

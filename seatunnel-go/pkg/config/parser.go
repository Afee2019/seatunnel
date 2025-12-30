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

package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/apache/seatunnel-go/pkg/api"
)

// JobConfig represents a SeaTunnel job configuration
type JobConfig struct {
	Env       *EnvConfig     `json:"env"`
	Source    []PluginConfig `json:"source"`
	Transform []PluginConfig `json:"transform,omitempty"`
	Sink      []PluginConfig `json:"sink"`
}

// EnvConfig represents the environment configuration
type EnvConfig struct {
	JobMode                 string `json:"job.mode,omitempty"`
	Parallelism             int    `json:"parallelism,omitempty"`
	JobName                 string `json:"job.name,omitempty"`
	CheckpointInterval      int    `json:"checkpoint.interval,omitempty"`
	ReadLimitRowsPerSecond  int64  `json:"read_limit.rows_per_second,omitempty"`
	ReadLimitBytesPerSecond int64  `json:"read_limit.bytes_per_second,omitempty"`
}

// PluginConfig represents a plugin configuration
type PluginConfig struct {
	PluginName      string                 `json:"plugin_name"`
	PluginOutput    string                 `json:"plugin_output,omitempty"`
	PluginInput     []string               `json:"plugin_input,omitempty"`
	ResultTableName string                 `json:"result_table_name,omitempty"`
	SourceTableName []string               `json:"source_table_name,omitempty"`
	Options         map[string]interface{} `json:"-"`
}

// ConfigParser parses SeaTunnel configuration files
type ConfigParser struct {
	variables map[string]string
}

// NewConfigParser creates a new config parser
func NewConfigParser() *ConfigParser {
	return &ConfigParser{
		variables: make(map[string]string),
	}
}

// SetVariable sets a variable for substitution
func (p *ConfigParser) SetVariable(key, value string) {
	p.variables[key] = value
}

// SetVariables sets multiple variables
func (p *ConfigParser) SetVariables(vars map[string]string) {
	for k, v := range vars {
		p.variables[k] = v
	}
}

// ParseFile parses a configuration file
func (p *ConfigParser) ParseFile(path string) (*JobConfig, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	return p.Parse(string(content))
}

// Parse parses configuration content
func (p *ConfigParser) Parse(content string) (*JobConfig, error) {
	// Substitute variables
	content = p.substituteVariables(content)

	// Parse HOCON
	parser := newHOCONParser(content)
	rawConfig, err := parser.parse()
	if err != nil {
		return nil, fmt.Errorf("failed to parse HOCON: %w", err)
	}

	return p.buildJobConfig(rawConfig)
}

// substituteVariables replaces ${var} placeholders
func (p *ConfigParser) substituteVariables(content string) string {
	re := regexp.MustCompile(`\$\{([^}]+)\}`)
	return re.ReplaceAllStringFunc(content, func(match string) string {
		varName := match[2 : len(match)-1] // Remove ${ and }
		if value, ok := p.variables[varName]; ok {
			return value
		}
		// Check environment variable
		if value := os.Getenv(varName); value != "" {
			return value
		}
		return match // Keep original if not found
	})
}

// hoconParser is a simple HOCON parser
type hoconParser struct {
	content string
	pos     int
	length  int
}

func newHOCONParser(content string) *hoconParser {
	return &hoconParser{
		content: content,
		pos:     0,
		length:  len(content),
	}
}

func (p *hoconParser) parse() (map[string]interface{}, error) {
	p.skipWhitespaceAndComments()
	return p.parseObject()
}

func (p *hoconParser) parseObject() (map[string]interface{}, error) {
	result := make(map[string]interface{})

	// Skip optional opening brace
	p.skipWhitespaceAndComments()
	if p.pos < p.length && p.content[p.pos] == '{' {
		p.pos++
	}

	for p.pos < p.length {
		p.skipWhitespaceAndComments()

		if p.pos >= p.length {
			break
		}

		// Check for closing brace
		if p.content[p.pos] == '}' {
			p.pos++
			break
		}

		// Parse key
		key, err := p.parseKey()
		if err != nil {
			return nil, err
		}
		if key == "" {
			break
		}

		p.skipWhitespaceAndComments()

		// Check for = or : or {
		if p.pos >= p.length {
			return nil, fmt.Errorf("unexpected end of input after key: %s", key)
		}

		var value interface{}
		if p.content[p.pos] == '=' || p.content[p.pos] == ':' {
			p.pos++
			p.skipWhitespaceAndComments()
			value, err = p.parseValue()
		} else if p.content[p.pos] == '{' {
			// Object value without separator
			value, err = p.parseObject()
		} else {
			return nil, fmt.Errorf("expected '=', ':' or '{' after key: %s, got '%c'", key, p.content[p.pos])
		}

		if err != nil {
			return nil, err
		}

		result[key] = value

		p.skipWhitespaceAndComments()

		// Skip optional comma or semicolon
		if p.pos < p.length && (p.content[p.pos] == ',' || p.content[p.pos] == ';') {
			p.pos++
		}
	}

	return result, nil
}

func (p *hoconParser) parseKey() (string, error) {
	p.skipWhitespaceAndComments()

	if p.pos >= p.length {
		return "", nil
	}

	// Check for closing brace
	if p.content[p.pos] == '}' {
		return "", nil
	}

	// Quoted key
	if p.content[p.pos] == '"' {
		return p.parseQuotedString()
	}

	// Unquoted key - can contain letters, digits, dots, underscores, hyphens
	start := p.pos
	for p.pos < p.length {
		c := p.content[p.pos]
		if unicode.IsLetter(rune(c)) || unicode.IsDigit(rune(c)) || c == '.' || c == '_' || c == '-' {
			p.pos++
		} else {
			break
		}
	}

	return p.content[start:p.pos], nil
}

func (p *hoconParser) parseValue() (interface{}, error) {
	p.skipWhitespaceAndComments()

	if p.pos >= p.length {
		return nil, fmt.Errorf("unexpected end of input")
	}

	c := p.content[p.pos]

	switch c {
	case '{':
		return p.parseObject()
	case '[':
		return p.parseArray()
	case '"':
		return p.parseQuotedString()
	default:
		return p.parseUnquotedValue()
	}
}

func (p *hoconParser) parseArray() ([]interface{}, error) {
	var result []interface{}

	// Skip opening bracket
	p.pos++

	for p.pos < p.length {
		p.skipWhitespaceAndComments()

		if p.pos >= p.length {
			return nil, fmt.Errorf("unexpected end of input in array")
		}

		// Check for closing bracket
		if p.content[p.pos] == ']' {
			p.pos++
			break
		}

		value, err := p.parseValue()
		if err != nil {
			return nil, err
		}

		result = append(result, value)

		p.skipWhitespaceAndComments()

		// Skip optional comma
		if p.pos < p.length && p.content[p.pos] == ',' {
			p.pos++
		}
	}

	return result, nil
}

func (p *hoconParser) parseQuotedString() (string, error) {
	// Skip opening quote
	p.pos++

	var sb strings.Builder
	for p.pos < p.length {
		c := p.content[p.pos]
		if c == '"' {
			p.pos++
			return sb.String(), nil
		}
		if c == '\\' && p.pos+1 < p.length {
			p.pos++
			next := p.content[p.pos]
			switch next {
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case 'r':
				sb.WriteByte('\r')
			case '"':
				sb.WriteByte('"')
			case '\\':
				sb.WriteByte('\\')
			default:
				sb.WriteByte(next)
			}
		} else {
			sb.WriteByte(c)
		}
		p.pos++
	}

	return "", fmt.Errorf("unterminated string")
}

func (p *hoconParser) parseUnquotedValue() (interface{}, error) {
	start := p.pos

	// Read until separator or end
	for p.pos < p.length {
		c := p.content[p.pos]
		if c == ',' || c == '}' || c == ']' || c == '\n' || c == '\r' || c == '#' {
			break
		}
		// Check for comment
		if c == '/' && p.pos+1 < p.length && p.content[p.pos+1] == '/' {
			break
		}
		p.pos++
	}

	value := strings.TrimSpace(p.content[start:p.pos])

	// Try to parse as different types
	if value == "true" {
		return true, nil
	}
	if value == "false" {
		return false, nil
	}
	if value == "null" {
		return nil, nil
	}

	// Try integer
	if i, err := strconv.ParseInt(value, 10, 64); err == nil {
		return i, nil
	}

	// Try float
	if f, err := strconv.ParseFloat(value, 64); err == nil {
		return f, nil
	}

	// Return as string
	return value, nil
}

func (p *hoconParser) skipWhitespaceAndComments() {
	for p.pos < p.length {
		c := p.content[p.pos]

		// Skip whitespace
		if unicode.IsSpace(rune(c)) {
			p.pos++
			continue
		}

		// Skip // comments
		if c == '/' && p.pos+1 < p.length && p.content[p.pos+1] == '/' {
			p.pos += 2
			for p.pos < p.length && p.content[p.pos] != '\n' {
				p.pos++
			}
			continue
		}

		// Skip # comments
		if c == '#' {
			for p.pos < p.length && p.content[p.pos] != '\n' {
				p.pos++
			}
			continue
		}

		break
	}
}

// buildJobConfig builds JobConfig from raw map
func (p *ConfigParser) buildJobConfig(raw map[string]interface{}) (*JobConfig, error) {
	config := &JobConfig{}

	// Parse env section
	if envRaw, ok := raw["env"].(map[string]interface{}); ok {
		config.Env = &EnvConfig{}
		if v, ok := envRaw["job.mode"].(string); ok {
			config.Env.JobMode = v
		}
		if v, ok := envRaw["parallelism"].(int64); ok {
			config.Env.Parallelism = int(v)
		} else if v, ok := envRaw["parallelism"].(float64); ok {
			config.Env.Parallelism = int(v)
		}
		if v, ok := envRaw["job.name"].(string); ok {
			config.Env.JobName = v
		}
	}

	// Parse source section
	if sourceRaw, ok := raw["source"].(map[string]interface{}); ok {
		for pluginName, pluginConfig := range sourceRaw {
			pc := p.parsePluginConfig(pluginName, pluginConfig)
			config.Source = append(config.Source, pc)
		}
	}

	// Parse transform section
	if transformRaw, ok := raw["transform"].(map[string]interface{}); ok {
		for pluginName, pluginConfig := range transformRaw {
			pc := p.parsePluginConfig(pluginName, pluginConfig)
			config.Transform = append(config.Transform, pc)
		}
	}

	// Parse sink section
	if sinkRaw, ok := raw["sink"].(map[string]interface{}); ok {
		for pluginName, pluginConfig := range sinkRaw {
			pc := p.parsePluginConfig(pluginName, pluginConfig)
			config.Sink = append(config.Sink, pc)
		}
	}

	return config, nil
}

// parsePluginConfig parses a plugin configuration
func (p *ConfigParser) parsePluginConfig(name string, config interface{}) PluginConfig {
	pc := PluginConfig{
		PluginName: name,
		Options:    make(map[string]interface{}),
	}

	if configMap, ok := config.(map[string]interface{}); ok {
		// Extract special fields
		if v, ok := configMap["plugin_output"].(string); ok {
			pc.PluginOutput = v
			delete(configMap, "plugin_output")
		}
		if v, ok := configMap["result_table_name"].(string); ok {
			pc.ResultTableName = v
			delete(configMap, "result_table_name")
		}
		if v, ok := configMap["plugin_input"].([]interface{}); ok {
			for _, item := range v {
				if s, ok := item.(string); ok {
					pc.PluginInput = append(pc.PluginInput, s)
				}
			}
			delete(configMap, "plugin_input")
		}
		if v, ok := configMap["source_table_name"].([]interface{}); ok {
			for _, item := range v {
				if s, ok := item.(string); ok {
					pc.SourceTableName = append(pc.SourceTableName, s)
				}
			}
			delete(configMap, "source_table_name")
		}

		// Remaining fields are options
		pc.Options = configMap
	}

	return pc
}

// ToAPIConfig converts PluginConfig to api.Config
func (pc *PluginConfig) ToAPIConfig() *api.Config {
	return api.NewConfigFromMap(pc.Options)
}

// Validate validates the job configuration
func (c *JobConfig) Validate() error {
	if len(c.Source) == 0 {
		return fmt.Errorf("at least one source is required")
	}
	if len(c.Sink) == 0 {
		return fmt.Errorf("at least one sink is required")
	}
	return nil
}

// GetJobMode returns the job mode (BATCH or STREAMING)
func (c *JobConfig) GetJobMode() string {
	if c.Env != nil && c.Env.JobMode != "" {
		return strings.ToUpper(c.Env.JobMode)
	}
	return "BATCH"
}

// GetParallelism returns the parallelism
func (c *JobConfig) GetParallelism() int {
	if c.Env != nil && c.Env.Parallelism > 0 {
		return c.Env.Parallelism
	}
	return 1
}

// String conversion helpers for config values

// ToString converts a value to string
func ToString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case int:
		return strconv.Itoa(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case bool:
		return strconv.FormatBool(val)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// ToInt converts a value to int
func ToInt(v interface{}) int {
	switch val := v.(type) {
	case float64:
		return int(val)
	case int:
		return val
	case int64:
		return int(val)
	case string:
		n, _ := strconv.Atoi(val)
		return n
	default:
		return 0
	}
}

// ToBool converts a value to bool
func ToBool(v interface{}) bool {
	switch val := v.(type) {
	case bool:
		return val
	case string:
		return strings.ToLower(val) == "true"
	default:
		return false
	}
}

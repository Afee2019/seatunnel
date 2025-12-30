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

package http

import (
	"time"

	"github.com/apache/seatunnel-go/pkg/api"
)

// HTTPMethod defines HTTP request method
type HTTPMethod string

const (
	MethodGET    HTTPMethod = "GET"
	MethodPOST   HTTPMethod = "POST"
	MethodPUT    HTTPMethod = "PUT"
	MethodDELETE HTTPMethod = "DELETE"
	MethodPATCH  HTTPMethod = "PATCH"
)

// ContentType defines content type
type ContentType string

const (
	ContentTypeJSON ContentType = "application/json"
	ContentTypeForm ContentType = "application/x-www-form-urlencoded"
	ContentTypeXML  ContentType = "application/xml"
	ContentTypeText ContentType = "text/plain"
)

// PaginationType defines pagination strategy
type PaginationType string

const (
	PaginationNone       PaginationType = "none"
	PaginationOffset     PaginationType = "offset"
	PaginationPageNum    PaginationType = "page_num"
	PaginationCursor     PaginationType = "cursor"
	PaginationNextURL    PaginationType = "next_url"
)

// HTTPConfig holds HTTP client configuration
type HTTPConfig struct {
	// Request settings
	URL         string
	Method      HTTPMethod
	Headers     map[string]string
	Params      map[string]string
	Body        string
	ContentType ContentType

	// Authentication
	AuthType     string // none, basic, bearer, api_key
	Username     string
	Password     string
	BearerToken  string
	APIKey       string
	APIKeyHeader string

	// Connection settings
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	RetryCount     int
	RetryInterval  time.Duration

	// TLS
	EnableTLS  bool
	CertFile   string
	KeyFile    string
	CAFile     string
	SkipVerify bool

	// Proxy
	ProxyURL string
}

// DefaultHTTPConfig returns default HTTP configuration
func DefaultHTTPConfig() *HTTPConfig {
	return &HTTPConfig{
		Method:         MethodGET,
		ContentType:    ContentTypeJSON,
		Headers:        make(map[string]string),
		Params:         make(map[string]string),
		AuthType:       "none",
		ConnectTimeout: 30 * time.Second,
		ReadTimeout:    60 * time.Second,
		RetryCount:     3,
		RetryInterval:  time.Second,
	}
}

// FromAPIConfig creates HTTPConfig from api.Config
func FromAPIConfig(config *api.Config) *HTTPConfig {
	hc := DefaultHTTPConfig()

	if v := config.GetString("url"); v != "" {
		hc.URL = v
	}
	if v := config.GetString("method"); v != "" {
		hc.Method = HTTPMethod(v)
	}
	if v := config.GetString("content_type"); v != "" {
		hc.ContentType = ContentType(v)
	}
	if v := config.GetString("body"); v != "" {
		hc.Body = v
	}

	// Headers
	if headers, ok := config.Get("headers"); ok && headers != nil {
		if headerMap, ok := headers.(map[string]interface{}); ok {
			for k, v := range headerMap {
				if s, ok := v.(string); ok {
					hc.Headers[k] = s
				}
			}
		}
	}

	// Params
	if params, ok := config.Get("params"); ok && params != nil {
		if paramMap, ok := params.(map[string]interface{}); ok {
			for k, v := range paramMap {
				if s, ok := v.(string); ok {
					hc.Params[k] = s
				}
			}
		}
	}

	// Authentication
	if v := config.GetString("auth_type"); v != "" {
		hc.AuthType = v
	}
	if v := config.GetString("username"); v != "" {
		hc.Username = v
	}
	if v := config.GetString("password"); v != "" {
		hc.Password = v
	}
	if v := config.GetString("bearer_token"); v != "" {
		hc.BearerToken = v
	}
	if v := config.GetString("api_key"); v != "" {
		hc.APIKey = v
	}
	if v := config.GetString("api_key_header"); v != "" {
		hc.APIKeyHeader = v
	}

	// Timeouts
	if v := config.GetInt("connect_timeout_ms"); v > 0 {
		hc.ConnectTimeout = time.Duration(v) * time.Millisecond
	}
	if v := config.GetInt("read_timeout_ms"); v > 0 {
		hc.ReadTimeout = time.Duration(v) * time.Millisecond
	}
	if v := config.GetInt("retry_count"); v > 0 {
		hc.RetryCount = v
	}

	// TLS
	hc.EnableTLS = config.GetBool("enable_tls")
	hc.SkipVerify = config.GetBool("skip_verify")
	if v := config.GetString("cert_file"); v != "" {
		hc.CertFile = v
	}
	if v := config.GetString("key_file"); v != "" {
		hc.KeyFile = v
	}
	if v := config.GetString("ca_file"); v != "" {
		hc.CAFile = v
	}

	// Proxy
	if v := config.GetString("proxy"); v != "" {
		hc.ProxyURL = v
	}

	return hc
}

// PaginationConfig holds pagination configuration
type PaginationConfig struct {
	Type PaginationType

	// For offset/page_num pagination
	OffsetField     string
	LimitField      string
	PageField       string
	PageSizeField   string
	PageSize        int
	TotalField      string // JSON path to total count

	// For cursor pagination
	CursorField     string
	CursorPath      string // JSON path to next cursor in response

	// For next_url pagination
	NextURLPath     string // JSON path to next URL in response

	// General
	DataPath        string // JSON path to data array
	MaxPages        int    // Maximum pages to fetch (0 = unlimited)
}

// DefaultPaginationConfig returns default pagination configuration
func DefaultPaginationConfig() *PaginationConfig {
	return &PaginationConfig{
		Type:          PaginationNone,
		PageSize:      100,
		OffsetField:   "offset",
		LimitField:    "limit",
		PageField:     "page",
		PageSizeField: "pageSize",
		CursorField:   "cursor",
		DataPath:      "data",
		MaxPages:      0,
	}
}

// PaginationConfigFromAPI creates PaginationConfig from api.Config
func PaginationConfigFromAPI(config *api.Config) *PaginationConfig {
	pc := DefaultPaginationConfig()

	if v := config.GetString("pagination_type"); v != "" {
		pc.Type = PaginationType(v)
	}
	if v := config.GetString("paginate_type"); v != "" {
		pc.Type = PaginationType(v)
	}

	// Offset/PageNum settings
	if v := config.GetString("offset_field"); v != "" {
		pc.OffsetField = v
	}
	if v := config.GetString("limit_field"); v != "" {
		pc.LimitField = v
	}
	if v := config.GetString("page_field"); v != "" {
		pc.PageField = v
	}
	if v := config.GetString("page_size_field"); v != "" {
		pc.PageSizeField = v
	}
	if v := config.GetInt("page_size"); v > 0 {
		pc.PageSize = v
	}
	if v := config.GetString("total_field"); v != "" {
		pc.TotalField = v
	}
	if v := config.GetString("total_page_size"); v != "" {
		pc.TotalField = v
	}

	// Cursor settings
	if v := config.GetString("cursor_field"); v != "" {
		pc.CursorField = v
	}
	if v := config.GetString("cursor_path"); v != "" {
		pc.CursorPath = v
	}

	// Next URL settings
	if v := config.GetString("next_url_path"); v != "" {
		pc.NextURLPath = v
	}

	// General
	if v := config.GetString("data_path"); v != "" {
		pc.DataPath = v
	}
	if v := config.GetString("content_json"); v != "" {
		pc.DataPath = v
	}
	if v := config.GetInt("max_pages"); v > 0 {
		pc.MaxPages = v
	}

	return pc
}

// SourceConfig holds HTTP source configuration
type SourceConfig struct {
	*HTTPConfig
	*PaginationConfig

	// Schema configuration
	SchemaFields []FieldConfig

	// Polling settings (for streaming)
	PollIntervalMs int64
	EnablePolling  bool
}

// FieldConfig defines a field mapping
type FieldConfig struct {
	Name     string
	Type     string
	JSONPath string
}

// DefaultSourceConfig returns default source configuration
func DefaultSourceConfig() *SourceConfig {
	return &SourceConfig{
		HTTPConfig:       DefaultHTTPConfig(),
		PaginationConfig: DefaultPaginationConfig(),
		PollIntervalMs:   1000,
	}
}

// SourceConfigFromAPI creates SourceConfig from api.Config
func SourceConfigFromAPI(config *api.Config) *SourceConfig {
	sc := &SourceConfig{
		HTTPConfig:       FromAPIConfig(config),
		PaginationConfig: PaginationConfigFromAPI(config),
		PollIntervalMs:   1000,
	}

	if v := config.GetInt64("poll_interval_ms"); v > 0 {
		sc.PollIntervalMs = v
	}
	sc.EnablePolling = config.GetBool("enable_polling")

	// Parse schema fields
	if schema, ok := config.Get("schema"); ok && schema != nil {
		if schemaMap, ok := schema.(map[string]interface{}); ok {
			if fields := schemaMap["fields"]; fields != nil {
				if fieldMap, ok := fields.(map[string]interface{}); ok {
					for name, fieldType := range fieldMap {
						fc := FieldConfig{Name: name}
						if typeStr, ok := fieldType.(string); ok {
							fc.Type = typeStr
						}
						sc.SchemaFields = append(sc.SchemaFields, fc)
					}
				}
			}
		}
	}

	return sc
}

// SinkConfig holds HTTP sink configuration
type SinkConfig struct {
	*HTTPConfig

	// Batching
	BatchSize     int
	BatchInterval time.Duration

	// Request formatting
	BodyTemplate string // Template for request body
	BatchMode    string // single, array, multipart
}

// DefaultSinkConfig returns default sink configuration
func DefaultSinkConfig() *SinkConfig {
	return &SinkConfig{
		HTTPConfig:    DefaultHTTPConfig(),
		BatchSize:     100,
		BatchInterval: time.Second,
		BatchMode:     "array",
	}
}

// SinkConfigFromAPI creates SinkConfig from api.Config
func SinkConfigFromAPI(config *api.Config) *SinkConfig {
	sc := &SinkConfig{
		HTTPConfig:    FromAPIConfig(config),
		BatchSize:     100,
		BatchInterval: time.Second,
		BatchMode:     "array",
	}

	if v := config.GetInt("batch_size"); v > 0 {
		sc.BatchSize = v
	}
	if v := config.GetInt("batch_interval_ms"); v > 0 {
		sc.BatchInterval = time.Duration(v) * time.Millisecond
	}
	if v := config.GetString("body_template"); v != "" {
		sc.BodyTemplate = v
	}
	if v := config.GetString("batch_mode"); v != "" {
		sc.BatchMode = v
	}

	// Override method to POST if not specified for sink
	if config.GetString("method") == "" {
		sc.Method = MethodPOST
	}

	return sc
}

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
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"
	"time"

	"github.com/apache/seatunnel-go/pkg/api"
)

// HTTPSink implements the Sink interface for HTTP
type HTTPSink struct {
	config *SinkConfig
	schema *api.SeaTunnelRowType
}

// NewHTTPSink creates a new HTTP sink
func NewHTTPSink(config *SinkConfig) *HTTPSink {
	return &HTTPSink{
		config: config,
	}
}

// GetPluginName returns the plugin name
func (s *HTTPSink) GetPluginName() string {
	return "Http"
}

// SetTypeInfo sets the input type information
func (s *HTTPSink) SetTypeInfo(rowType *api.SeaTunnelRowType) {
	s.schema = rowType
}

// GetConsumedType returns the row type consumed by this sink
func (s *HTTPSink) GetConsumedType() *api.SeaTunnelRowType {
	return s.schema
}

// CreateWriter creates a sink writer
func (s *HTTPSink) CreateWriter(ctx api.WriterContext) (api.SinkWriter, error) {
	client, err := NewHTTPClient(s.config.HTTPConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP client: %w", err)
	}

	var bodyTmpl *template.Template
	if s.config.BodyTemplate != "" {
		var err error
		bodyTmpl, err = template.New("body").Parse(s.config.BodyTemplate)
		if err != nil {
			return nil, fmt.Errorf("failed to parse body template: %w", err)
		}
	}

	return &HTTPSinkWriter{
		config:       s.config,
		client:       client,
		schema:       s.schema,
		buffer:       make([]*api.SeaTunnelRow, 0, s.config.BatchSize),
		bodyTemplate: bodyTmpl,
		lastFlush:    time.Now(),
	}, nil
}

// CreateCommitter creates a sink committer (not needed for HTTP)
func (s *HTTPSink) CreateCommitter() api.SinkCommitter {
	return nil
}

// CreateAggregatedCommitter creates an aggregated committer (not needed for HTTP)
func (s *HTTPSink) CreateAggregatedCommitter() api.SinkAggregatedCommitter {
	return nil
}

// HTTPSinkWriter writes data to HTTP endpoints
type HTTPSinkWriter struct {
	config       *SinkConfig
	client       *HTTPClient
	schema       *api.SeaTunnelRowType
	buffer       []*api.SeaTunnelRow
	bodyTemplate *template.Template
	lastFlush    time.Time
	rowCount     int64
}

// Open initializes the writer
func (w *HTTPSinkWriter) Open() error {
	return nil
}

// Write writes a row to the buffer
func (w *HTTPSinkWriter) Write(row *api.SeaTunnelRow) error {
	w.buffer = append(w.buffer, row)
	w.rowCount++

	// Flush when buffer is full or interval exceeded
	shouldFlush := len(w.buffer) >= w.config.BatchSize
	if !shouldFlush && w.config.BatchInterval > 0 {
		shouldFlush = time.Since(w.lastFlush) >= w.config.BatchInterval
	}

	if shouldFlush {
		return w.flush()
	}

	return nil
}

// flush sends the buffered data to the HTTP endpoint
func (w *HTTPSinkWriter) flush() error {
	if len(w.buffer) == 0 {
		return nil
	}

	defer func() {
		w.buffer = w.buffer[:0]
		w.lastFlush = time.Now()
	}()

	switch w.config.BatchMode {
	case "single":
		return w.flushSingle()
	case "array":
		return w.flushArray()
	default:
		return w.flushArray()
	}
}

// flushSingle sends each row as a separate request
func (w *HTTPSinkWriter) flushSingle() error {
	for _, row := range w.buffer {
		body, err := w.rowToBody(row)
		if err != nil {
			return err
		}

		resp, err := w.client.Do(&Request{
			URL:    w.config.URL,
			Method: w.config.Method,
			Body:   body,
		})
		if err != nil {
			return err
		}

		if resp.StatusCode >= 400 {
			return fmt.Errorf("HTTP error: %d, body: %s", resp.StatusCode, string(resp.Body))
		}
	}

	return nil
}

// flushArray sends all rows as an array in a single request
func (w *HTTPSinkWriter) flushArray() error {
	var body []byte
	var err error

	if w.bodyTemplate != nil {
		// Use template to generate body
		data := make([]map[string]interface{}, len(w.buffer))
		for i, row := range w.buffer {
			data[i] = w.rowToMap(row)
		}

		var buf bytes.Buffer
		if err := w.bodyTemplate.Execute(&buf, map[string]interface{}{"data": data}); err != nil {
			return fmt.Errorf("failed to execute body template: %w", err)
		}
		body = buf.Bytes()
	} else {
		// Convert rows to JSON array
		data := make([]map[string]interface{}, len(w.buffer))
		for i, row := range w.buffer {
			data[i] = w.rowToMap(row)
		}

		body, err = json.Marshal(data)
		if err != nil {
			return fmt.Errorf("failed to marshal rows: %w", err)
		}
	}

	resp, err := w.client.Do(&Request{
		URL:    w.config.URL,
		Method: w.config.Method,
		Body:   body,
	})
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP error: %d, body: %s", resp.StatusCode, string(resp.Body))
	}

	return nil
}

// rowToBody converts a row to request body
func (w *HTTPSinkWriter) rowToBody(row *api.SeaTunnelRow) ([]byte, error) {
	if w.bodyTemplate != nil {
		data := w.rowToMap(row)
		var buf bytes.Buffer
		if err := w.bodyTemplate.Execute(&buf, data); err != nil {
			return nil, fmt.Errorf("failed to execute body template: %w", err)
		}
		return buf.Bytes(), nil
	}

	// Default: convert to JSON object
	data := w.rowToMap(row)
	return json.Marshal(data)
}

// rowToMap converts a row to a map
func (w *HTTPSinkWriter) rowToMap(row *api.SeaTunnelRow) map[string]interface{} {
	data := make(map[string]interface{})
	for i, field := range w.schema.Fields {
		val := row.GetField(i)
		data[field.Name] = val
		// Also add with camelCase and snake_case variants
		data[toCamelCase(field.Name)] = val
		data[toSnakeCase(field.Name)] = val
	}
	return data
}

// toCamelCase converts a string to camelCase
func toCamelCase(s string) string {
	parts := strings.Split(s, "_")
	for i := 1; i < len(parts); i++ {
		if len(parts[i]) > 0 {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}
	return strings.Join(parts, "")
}

// toSnakeCase converts a string to snake_case
func toSnakeCase(s string) string {
	var result strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result.WriteByte('_')
		}
		result.WriteRune(r)
	}
	return strings.ToLower(result.String())
}

// PrepareCommit prepares for commit
func (w *HTTPSinkWriter) PrepareCommit() ([]api.CommitInfo, error) {
	if err := w.flush(); err != nil {
		return nil, err
	}
	return nil, nil
}

// AbortPrepare aborts prepared data
func (w *HTTPSinkWriter) AbortPrepare() {
	w.buffer = w.buffer[:0]
}

// SnapshotState snapshots the writer state
func (w *HTTPSinkWriter) SnapshotState(checkpointId int64) ([]byte, error) {
	// Flush before snapshot
	if err := w.flush(); err != nil {
		return nil, err
	}
	return json.Marshal(map[string]int64{"rowCount": w.rowCount})
}

// Close closes the writer
func (w *HTTPSinkWriter) Close() error {
	// Flush remaining data
	return w.flush()
}

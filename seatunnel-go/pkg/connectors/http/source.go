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
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"github.com/apache/seatunnel-go/pkg/api"
)

// HTTPSource implements the Source interface for HTTP
type HTTPSource struct {
	config *SourceConfig
	schema *api.SeaTunnelRowType
}

// NewHTTPSource creates a new HTTP source
func NewHTTPSource(config *SourceConfig) *HTTPSource {
	return &HTTPSource{
		config: config,
	}
}

// GetPluginName returns the plugin name
func (s *HTTPSource) GetPluginName() string {
	return "Http"
}

// GetBoundedness returns the boundedness of the source
func (s *HTTPSource) GetBoundedness() api.Boundedness {
	if s.config.EnablePolling {
		return api.Unbounded
	}
	return api.Bounded
}

// GetProducedType returns the produced row type
func (s *HTTPSource) GetProducedType() *api.SeaTunnelRowType {
	if s.schema != nil {
		return s.schema
	}

	// Build schema from config
	if len(s.config.SchemaFields) > 0 {
		fields := make([]*api.SeaTunnelFieldType, len(s.config.SchemaFields))
		for i, fc := range s.config.SchemaFields {
			fields[i] = api.NewField(fc.Name, parseFieldType(fc.Type))
		}
		return &api.SeaTunnelRowType{Fields: fields}
	}

	// Default: single JSON field
	return &api.SeaTunnelRowType{
		Fields: []*api.SeaTunnelFieldType{
			api.NewField("data", api.NewBasicType(api.STRING)),
		},
	}
}

// parseFieldType parses a type string to SeaTunnelDataType
func parseFieldType(typeStr string) *api.SeaTunnelDataType {
	switch typeStr {
	case "boolean", "bool":
		return api.NewBasicType(api.BOOLEAN)
	case "int", "integer", "int32":
		return api.NewBasicType(api.INT)
	case "long", "bigint", "int64":
		return api.NewBasicType(api.BIGINT)
	case "float", "float32":
		return api.NewBasicType(api.FLOAT)
	case "double", "float64":
		return api.NewBasicType(api.DOUBLE)
	default:
		return api.NewBasicType(api.STRING)
	}
}

// SetProducedType sets the produced row type
func (s *HTTPSource) SetProducedType(rowType *api.SeaTunnelRowType) {
	s.schema = rowType
}

// CreateEnumerator creates a split enumerator
func (s *HTTPSource) CreateEnumerator(ctx api.EnumeratorContext) (api.SplitEnumerator, error) {
	return &HTTPSplitEnumerator{
		config: s.config,
	}, nil
}

// CreateReader creates a source reader
func (s *HTTPSource) CreateReader(ctx api.ReaderContext) (api.SourceReader, error) {
	client, err := NewHTTPClient(s.config.HTTPConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP client: %w", err)
	}

	return &HTTPSourceReader{
		config: s.config,
		client: client,
		schema: s.GetProducedType(),
	}, nil
}

// RestoreEnumerator restores an enumerator from checkpoint
func (s *HTTPSource) RestoreEnumerator(ctx api.EnumeratorContext, state []byte) (api.SplitEnumerator, error) {
	return s.CreateEnumerator(ctx)
}

// HTTPSplit represents an HTTP split
type HTTPSplit struct {
	splitID string
	url     string
	params  map[string]string
}

// SplitID returns the split ID
func (s *HTTPSplit) SplitID() string {
	return s.splitID
}

// HTTPSplitEnumerator enumerates HTTP splits
type HTTPSplitEnumerator struct {
	config       *SourceConfig
	assignedOnce bool
}

// Open initializes the enumerator
func (e *HTTPSplitEnumerator) Open() error {
	return nil
}

// Run runs the enumerator
func (e *HTTPSplitEnumerator) Run() error {
	return nil
}

// AddSplitsBack adds splits back for re-processing
func (e *HTTPSplitEnumerator) AddSplitsBack(splits []api.SourceSplit, subtaskId int) {
	// Not needed for simple implementation
}

// RegisterReader registers a reader
func (e *HTTPSplitEnumerator) RegisterReader(subtaskId int) {
	// Not needed for simple implementation
}

// HandleSplitRequest handles a split request from reader
func (e *HTTPSplitEnumerator) HandleSplitRequest(subtaskId int) {
	// For simple implementation, splits are assigned during Run
}

// SnapshotState snapshots the enumerator state
func (e *HTTPSplitEnumerator) SnapshotState(checkpointId int64) ([]byte, error) {
	return nil, nil
}

// NotifyCheckpointComplete notifies that a checkpoint is complete
func (e *HTTPSplitEnumerator) NotifyCheckpointComplete(checkpointId int64) error {
	return nil
}

// Close closes the enumerator
func (e *HTTPSplitEnumerator) Close() error {
	return nil
}

// HTTPSourceReader reads data from HTTP endpoints
type HTTPSourceReader struct {
	config       *SourceConfig
	client       *HTTPClient
	schema       *api.SeaTunnelRowType
	splits       []api.SourceSplit
	noMoreSplits bool
	mu           sync.Mutex
}

// Open initializes the reader
func (r *HTTPSourceReader) Open() error {
	return nil
}

// PollNext polls for the next batch of data
func (r *HTTPSourceReader) PollNext(collector api.Collector) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.splits) == 0 {
		if r.noMoreSplits {
			collector.MarkEvent(api.EndOfSplitEvent{SplitID: "complete"})
		}
		return nil
	}

	// Process one split at a time
	split := r.splits[0].(*HTTPSplit)
	r.splits = r.splits[1:]

	// Read data with pagination
	return r.readWithPagination(split, collector)
}

// readWithPagination reads data using the configured pagination strategy
func (r *HTTPSourceReader) readWithPagination(split *HTTPSplit, collector api.Collector) error {
	switch r.config.Type {
	case PaginationOffset:
		return r.readWithOffsetPagination(split, collector)
	case PaginationPageNum:
		return r.readWithPageNumPagination(split, collector)
	case PaginationCursor:
		return r.readWithCursorPagination(split, collector)
	case PaginationNextURL:
		return r.readWithNextURLPagination(split, collector)
	default:
		return r.readSinglePage(split, collector)
	}
}

// readSinglePage reads a single page without pagination
func (r *HTTPSourceReader) readSinglePage(split *HTTPSplit, collector api.Collector) error {
	resp, err := r.client.Do(&Request{
		URL:    split.url,
		Method: r.config.Method,
		Params: split.params,
		Body:   []byte(r.config.Body),
	})
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}

	_, err = r.processResponse(resp.Body, collector)
	return err
}

// readWithOffsetPagination reads with offset-based pagination
func (r *HTTPSourceReader) readWithOffsetPagination(split *HTTPSplit, collector api.Collector) error {
	offset := 0
	pageCount := 0

	for {
		if r.config.MaxPages > 0 && pageCount >= r.config.MaxPages {
			break
		}

		params := make(map[string]string)
		for k, v := range split.params {
			params[k] = v
		}
		params[r.config.OffsetField] = strconv.Itoa(offset)
		params[r.config.LimitField] = strconv.Itoa(r.config.PageSize)

		resp, err := r.client.Do(&Request{
			URL:    split.url,
			Method: r.config.Method,
			Params: params,
			Body:   []byte(r.config.Body),
		})
		if err != nil {
			return err
		}

		if resp.StatusCode >= 400 {
			return fmt.Errorf("HTTP error: %d", resp.StatusCode)
		}

		count, err := r.processResponse(resp.Body, collector)
		if err != nil {
			return err
		}

		if count == 0 || count < r.config.PageSize {
			break
		}

		offset += r.config.PageSize
		pageCount++
	}

	return nil
}

// readWithPageNumPagination reads with page number pagination
func (r *HTTPSourceReader) readWithPageNumPagination(split *HTTPSplit, collector api.Collector) error {
	page := 1
	pageCount := 0

	for {
		if r.config.MaxPages > 0 && pageCount >= r.config.MaxPages {
			break
		}

		params := make(map[string]string)
		for k, v := range split.params {
			params[k] = v
		}
		params[r.config.PageField] = strconv.Itoa(page)
		params[r.config.PageSizeField] = strconv.Itoa(r.config.PageSize)

		resp, err := r.client.Do(&Request{
			URL:    split.url,
			Method: r.config.Method,
			Params: params,
			Body:   []byte(r.config.Body),
		})
		if err != nil {
			return err
		}

		if resp.StatusCode >= 400 {
			return fmt.Errorf("HTTP error: %d", resp.StatusCode)
		}

		count, err := r.processResponse(resp.Body, collector)
		if err != nil {
			return err
		}

		if count == 0 || count < r.config.PageSize {
			break
		}

		page++
		pageCount++
	}

	return nil
}

// readWithCursorPagination reads with cursor-based pagination
func (r *HTTPSourceReader) readWithCursorPagination(split *HTTPSplit, collector api.Collector) error {
	cursor := ""
	pageCount := 0

	for {
		if r.config.MaxPages > 0 && pageCount >= r.config.MaxPages {
			break
		}

		params := make(map[string]string)
		for k, v := range split.params {
			params[k] = v
		}
		if cursor != "" {
			params[r.config.CursorField] = cursor
		}

		resp, err := r.client.Do(&Request{
			URL:    split.url,
			Method: r.config.Method,
			Params: params,
			Body:   []byte(r.config.Body),
		})
		if err != nil {
			return err
		}

		if resp.StatusCode >= 400 {
			return fmt.Errorf("HTTP error: %d", resp.StatusCode)
		}

		if _, err := r.processResponse(resp.Body, collector); err != nil {
			return err
		}

		// Extract next cursor
		nextCursor, err := JSONPathString(resp.Body, r.config.CursorPath)
		if err != nil || nextCursor == "" {
			break
		}

		cursor = nextCursor
		pageCount++
	}

	return nil
}

// readWithNextURLPagination reads with next URL pagination
func (r *HTTPSourceReader) readWithNextURLPagination(split *HTTPSplit, collector api.Collector) error {
	nextURL := split.url
	pageCount := 0

	for nextURL != "" {
		if r.config.MaxPages > 0 && pageCount >= r.config.MaxPages {
			break
		}

		resp, err := r.client.Do(&Request{
			URL:    nextURL,
			Method: r.config.Method,
			Params: split.params,
			Body:   []byte(r.config.Body),
		})
		if err != nil {
			return err
		}

		if resp.StatusCode >= 400 {
			return fmt.Errorf("HTTP error: %d", resp.StatusCode)
		}

		if _, err := r.processResponse(resp.Body, collector); err != nil {
			return err
		}

		// Extract next URL
		nextURL, err = JSONPathString(resp.Body, r.config.NextURLPath)
		if err != nil {
			nextURL = ""
		}

		pageCount++
	}

	return nil
}

// processResponse processes an HTTP response and returns the number of records
func (r *HTTPSourceReader) processResponse(body []byte, collector api.Collector) (int, error) {
	// Extract data array using JSON path
	data, err := JSONPathArray(body, r.config.DataPath)
	if err != nil {
		// Try to process as single object
		var obj map[string]interface{}
		if err := json.Unmarshal(body, &obj); err == nil {
			row := r.mapToRow(obj)
			collector.Collect(row)
			return 1, nil
		}
		return 0, fmt.Errorf("failed to extract data: %w", err)
	}

	if data == nil {
		return 0, nil
	}

	count := 0
	for _, item := range data {
		if itemMap, ok := item.(map[string]interface{}); ok {
			row := r.mapToRow(itemMap)
			collector.Collect(row)
			count++
		}
	}

	return count, nil
}

// mapToRow converts a map to a SeaTunnelRow
func (r *HTTPSourceReader) mapToRow(data map[string]interface{}) *api.SeaTunnelRow {
	fields := make([]interface{}, len(r.schema.Fields))

	for i, field := range r.schema.Fields {
		val, ok := data[field.Name]
		if !ok {
			// Try to find using JSON path from config
			for _, fc := range r.config.SchemaFields {
				if fc.Name == field.Name && fc.JSONPath != "" {
					bytes, _ := json.Marshal(data)
					val, _ = JSONPath(bytes, fc.JSONPath)
					break
				}
			}
		}
		fields[i] = convertValue(val, field.Type)
	}

	return api.NewSeaTunnelRow(fields)
}

// convertValue converts a value to the appropriate type
func convertValue(val interface{}, dataType *api.SeaTunnelDataType) interface{} {
	if val == nil {
		return nil
	}

	switch dataType.SqlType {
	case api.BOOLEAN:
		switch v := val.(type) {
		case bool:
			return v
		case string:
			return v == "true" || v == "1"
		}
	case api.INT:
		switch v := val.(type) {
		case float64:
			return int32(v)
		case int:
			return int32(v)
		}
	case api.BIGINT:
		switch v := val.(type) {
		case float64:
			return int64(v)
		case int:
			return int64(v)
		}
	case api.FLOAT:
		switch v := val.(type) {
		case float64:
			return float32(v)
		}
	case api.DOUBLE:
		switch v := val.(type) {
		case float64:
			return v
		}
	case api.STRING:
		return fmt.Sprintf("%v", val)
	}

	return val
}

// AddSplits adds splits to the reader
func (r *HTTPSourceReader) AddSplits(splits []api.SourceSplit) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.splits = append(r.splits, splits...)
	return nil
}

// HandleNoMoreSplits signals that no more splits will be added
func (r *HTTPSourceReader) HandleNoMoreSplits() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.noMoreSplits = true
}

// SnapshotState snapshots the reader state
func (r *HTTPSourceReader) SnapshotState(checkpointId int64) ([]api.SourceSplit, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	result := make([]api.SourceSplit, len(r.splits))
	copy(result, r.splits)
	return result, nil
}

// NotifyCheckpointComplete notifies that a checkpoint is complete
func (r *HTTPSourceReader) NotifyCheckpointComplete(checkpointId int64) error {
	return nil
}

// Close closes the reader
func (r *HTTPSourceReader) Close() error {
	return nil
}

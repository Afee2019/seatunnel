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

package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/apache/seatunnel-go/pkg/api"
)

// RedisSource implements the Source interface for Redis
type RedisSource struct {
	config *SourceConfig
	schema *api.SeaTunnelRowType
}

// NewRedisSource creates a new Redis source
func NewRedisSource(config *SourceConfig) *RedisSource {
	return &RedisSource{
		config: config,
	}
}

// GetPluginName returns the plugin name
func (s *RedisSource) GetPluginName() string {
	return "Redis"
}

// GetBoundedness returns the boundedness of the source
func (s *RedisSource) GetBoundedness() api.Boundedness {
	return api.Bounded
}

// GetProducedType returns the produced row type
func (s *RedisSource) GetProducedType() *api.SeaTunnelRowType {
	if s.schema != nil {
		return s.schema
	}

	// Default schema based on data type
	switch s.config.DataType {
	case RedisDataTypeHash:
		return &api.SeaTunnelRowType{
			Fields: []*api.SeaTunnelFieldType{
				api.NewField("key", api.NewBasicType(api.STRING)),
				api.NewField("field", api.NewBasicType(api.STRING)),
				api.NewField("value", api.NewBasicType(api.STRING)),
			},
		}
	case RedisDataTypeList, RedisDataTypeSet:
		return &api.SeaTunnelRowType{
			Fields: []*api.SeaTunnelFieldType{
				api.NewField("key", api.NewBasicType(api.STRING)),
				api.NewField("value", api.NewBasicType(api.STRING)),
			},
		}
	case RedisDataTypeZSet:
		return &api.SeaTunnelRowType{
			Fields: []*api.SeaTunnelFieldType{
				api.NewField("key", api.NewBasicType(api.STRING)),
				api.NewField("member", api.NewBasicType(api.STRING)),
				api.NewField("score", api.NewBasicType(api.DOUBLE)),
			},
		}
	default: // String
		return &api.SeaTunnelRowType{
			Fields: []*api.SeaTunnelFieldType{
				api.NewField("key", api.NewBasicType(api.STRING)),
				api.NewField("value", api.NewBasicType(api.STRING)),
			},
		}
	}
}

// SetProducedType sets the produced row type
func (s *RedisSource) SetProducedType(rowType *api.SeaTunnelRowType) {
	s.schema = rowType
}

// CreateEnumerator creates a split enumerator
func (s *RedisSource) CreateEnumerator(ctx api.EnumeratorContext) (api.SplitEnumerator, error) {
	return &RedisSplitEnumerator{
		config: s.config,
	}, nil
}

// CreateReader creates a source reader
func (s *RedisSource) CreateReader(ctx api.ReaderContext) (api.SourceReader, error) {
	client, err := NewRedisClient(s.config.RedisConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis client: %w", err)
	}

	return &RedisSourceReader{
		config: s.config,
		client: client,
		schema: s.GetProducedType(),
	}, nil
}

// RestoreEnumerator restores an enumerator from checkpoint
func (s *RedisSource) RestoreEnumerator(ctx api.EnumeratorContext, state []byte) (api.SplitEnumerator, error) {
	return s.CreateEnumerator(ctx)
}

// RedisSplit represents a Redis split for parallel reading
type RedisSplit struct {
	splitID    string
	keyPattern string
	cursor     uint64
	keys       []string
}

// SplitID returns the split ID
func (s *RedisSplit) SplitID() string {
	return s.splitID
}

// RedisSplitEnumerator enumerates Redis splits
type RedisSplitEnumerator struct {
	config       *SourceConfig
	assignedOnce bool
}

// Open initializes the enumerator
func (e *RedisSplitEnumerator) Open() error {
	return nil
}

// Run runs the enumerator
func (e *RedisSplitEnumerator) Run() error {
	return nil
}

// AddSplitsBack adds splits back for re-processing
func (e *RedisSplitEnumerator) AddSplitsBack(splits []api.SourceSplit, subtaskId int) {
	// Not needed for bounded source
}

// RegisterReader registers a reader
func (e *RedisSplitEnumerator) RegisterReader(subtaskId int) {
	// Not needed for simple implementation
}

// HandleSplitRequest handles a split request from reader
func (e *RedisSplitEnumerator) HandleSplitRequest(subtaskId int) {
	// For simple implementation, splits are assigned during Run
}

// SnapshotState snapshots the enumerator state
func (e *RedisSplitEnumerator) SnapshotState(checkpointId int64) ([]byte, error) {
	return nil, nil
}

// NotifyCheckpointComplete notifies that a checkpoint is complete
func (e *RedisSplitEnumerator) NotifyCheckpointComplete(checkpointId int64) error {
	return nil
}

// Close closes the enumerator
func (e *RedisSplitEnumerator) Close() error {
	return nil
}

// RedisSourceReader reads data from Redis
type RedisSourceReader struct {
	config    *SourceConfig
	client    *RedisClient
	schema    *api.SeaTunnelRowType
	splits    []api.SourceSplit
	noMoreSplits bool
	mu        sync.Mutex
}

// Open initializes the reader
func (r *RedisSourceReader) Open() error {
	return r.client.Ping(context.Background())
}

// PollNext polls for the next batch of data
func (r *RedisSourceReader) PollNext(collector api.Collector) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.splits) == 0 {
		if r.noMoreSplits {
			collector.MarkEvent(api.EndOfSplitEvent{SplitID: "complete"})
		}
		return nil
	}

	// Process one split at a time
	split := r.splits[0].(*RedisSplit)
	r.splits = r.splits[1:]

	ctx := context.Background()

	// Scan keys matching pattern
	cursor := split.cursor
	for {
		keys, nextCursor, err := r.client.Scan(ctx, cursor, split.keyPattern, r.config.BatchSize)
		if err != nil {
			return fmt.Errorf("failed to scan keys: %w", err)
		}

		// Read values for each key
		for _, key := range keys {
			if err := r.readKey(ctx, key, collector); err != nil {
				return fmt.Errorf("failed to read key %s: %w", key, err)
			}

			// Delete after read if configured
			if r.config.DeleteAfterRead {
				if err := r.client.Del(ctx, key); err != nil {
					return fmt.Errorf("failed to delete key %s: %w", key, err)
				}
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return nil
}

// readKey reads a single key based on data type
func (r *RedisSourceReader) readKey(ctx context.Context, key string, collector api.Collector) error {
	switch r.config.DataType {
	case RedisDataTypeString:
		return r.readString(ctx, key, collector)
	case RedisDataTypeHash:
		return r.readHash(ctx, key, collector)
	case RedisDataTypeList:
		return r.readList(ctx, key, collector)
	case RedisDataTypeSet:
		return r.readSet(ctx, key, collector)
	case RedisDataTypeZSet:
		return r.readZSet(ctx, key, collector)
	default:
		return r.readString(ctx, key, collector)
	}
}

func (r *RedisSourceReader) readString(ctx context.Context, key string, collector api.Collector) error {
	value, err := r.client.Get(ctx, key)
	if err != nil {
		return err
	}

	row := api.NewSeaTunnelRow([]interface{}{key, value})
	collector.Collect(row)
	return nil
}

func (r *RedisSourceReader) readHash(ctx context.Context, key string, collector api.Collector) error {
	if len(r.config.HashFields) > 0 {
		// Read specific fields
		for _, field := range r.config.HashFields {
			value, err := r.client.HGet(ctx, key, field)
			if err != nil {
				continue // Skip missing fields
			}
			row := api.NewSeaTunnelRow([]interface{}{key, field, value})
			collector.Collect(row)
		}
	} else {
		// Read all fields
		fields, err := r.client.HGetAll(ctx, key)
		if err != nil {
			return err
		}
		for field, value := range fields {
			row := api.NewSeaTunnelRow([]interface{}{key, field, value})
			collector.Collect(row)
		}
	}
	return nil
}

func (r *RedisSourceReader) readList(ctx context.Context, key string, collector api.Collector) error {
	values, err := r.client.LRange(ctx, key, 0, -1)
	if err != nil {
		return err
	}
	for _, value := range values {
		row := api.NewSeaTunnelRow([]interface{}{key, value})
		collector.Collect(row)
	}
	return nil
}

func (r *RedisSourceReader) readSet(ctx context.Context, key string, collector api.Collector) error {
	members, err := r.client.SMembers(ctx, key)
	if err != nil {
		return err
	}
	for _, member := range members {
		row := api.NewSeaTunnelRow([]interface{}{key, member})
		collector.Collect(row)
	}
	return nil
}

func (r *RedisSourceReader) readZSet(ctx context.Context, key string, collector api.Collector) error {
	members, err := r.client.ZRangeWithScores(ctx, key, 0, -1)
	if err != nil {
		return err
	}
	for _, z := range members {
		row := api.NewSeaTunnelRow([]interface{}{key, z.Member, z.Score})
		collector.Collect(row)
	}
	return nil
}

// AddSplits adds splits to the reader
func (r *RedisSourceReader) AddSplits(splits []api.SourceSplit) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.splits = append(r.splits, splits...)
	return nil
}

// HandleNoMoreSplits signals that no more splits will be added
func (r *RedisSourceReader) HandleNoMoreSplits() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.noMoreSplits = true
}

// SnapshotState snapshots the reader state
func (r *RedisSourceReader) SnapshotState(checkpointId int64) ([]api.SourceSplit, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Return remaining splits
	result := make([]api.SourceSplit, len(r.splits))
	copy(result, r.splits)
	return result, nil
}

// NotifyCheckpointComplete notifies that a checkpoint is complete
func (r *RedisSourceReader) NotifyCheckpointComplete(checkpointId int64) error {
	return nil
}

// Close closes the reader
func (r *RedisSourceReader) Close() error {
	return r.client.Close()
}

// rowToJSON converts a row to JSON string
func rowToJSON(row *api.SeaTunnelRow, schema *api.SeaTunnelRowType) (string, error) {
	data := make(map[string]interface{})
	for i, field := range schema.Fields {
		data[field.Name] = row.GetField(i)
	}
	bytes, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

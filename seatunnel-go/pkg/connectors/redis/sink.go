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
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/apache/seatunnel-go/pkg/api"
)

// RedisSink implements the Sink interface for Redis
type RedisSink struct {
	config *SinkConfig
	schema *api.SeaTunnelRowType
}

// NewRedisSink creates a new Redis sink
func NewRedisSink(config *SinkConfig) *RedisSink {
	return &RedisSink{
		config: config,
	}
}

// GetPluginName returns the plugin name
func (s *RedisSink) GetPluginName() string {
	return "Redis"
}

// SetTypeInfo sets the input type information
func (s *RedisSink) SetTypeInfo(rowType *api.SeaTunnelRowType) {
	s.schema = rowType
}

// GetConsumedType returns the row type consumed by this sink
func (s *RedisSink) GetConsumedType() *api.SeaTunnelRowType {
	return s.schema
}

// CreateWriter creates a sink writer
func (s *RedisSink) CreateWriter(ctx api.WriterContext) (api.SinkWriter, error) {
	client, err := NewRedisClient(s.config.RedisConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis client: %w", err)
	}

	return &RedisSinkWriter{
		config:    s.config,
		client:    client,
		schema:    s.schema,
		buffer:    make([]*api.SeaTunnelRow, 0, s.config.BatchSize),
		rowCount:  0,
	}, nil
}

// CreateCommitter creates a sink committer (not needed for Redis)
func (s *RedisSink) CreateCommitter() api.SinkCommitter {
	return nil
}

// CreateAggregatedCommitter creates an aggregated committer (not needed for Redis)
func (s *RedisSink) CreateAggregatedCommitter() api.SinkAggregatedCommitter {
	return nil
}

// RedisSinkWriter writes data to Redis
type RedisSinkWriter struct {
	config    *SinkConfig
	client    *RedisClient
	schema    *api.SeaTunnelRowType
	buffer    []*api.SeaTunnelRow
	rowCount  int64
}

// Open initializes the writer
func (w *RedisSinkWriter) Open() error {
	return w.client.Ping(context.Background())
}

// Write writes a row to the buffer
func (w *RedisSinkWriter) Write(row *api.SeaTunnelRow) error {
	w.buffer = append(w.buffer, row)
	w.rowCount++

	// Flush when buffer is full
	if len(w.buffer) >= w.config.BatchSize {
		return w.flush()
	}

	return nil
}

// flush writes the buffer to Redis
func (w *RedisSinkWriter) flush() error {
	if len(w.buffer) == 0 {
		return nil
	}

	ctx := context.Background()
	pipe := w.client.Pipeline()

	for _, row := range w.buffer {
		if err := w.writeRow(ctx, pipe, row); err != nil {
			return err
		}
	}

	// Execute pipeline
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute pipeline: %w", err)
	}

	w.buffer = w.buffer[:0]
	return nil
}

// writeRow writes a single row to Redis pipeline
func (w *RedisSinkWriter) writeRow(ctx context.Context, pipe goredis.Pipeliner, row *api.SeaTunnelRow) error {
	// Determine the key
	key := w.getKey(row)
	if key == "" {
		return fmt.Errorf("could not determine key for row")
	}

	// Write based on data type
	switch w.config.DataType {
	case RedisDataTypeString:
		return w.writeString(ctx, pipe, key, row)
	case RedisDataTypeHash:
		return w.writeHash(ctx, pipe, key, row)
	case RedisDataTypeList:
		return w.writeList(ctx, pipe, key, row)
	case RedisDataTypeSet:
		return w.writeSet(ctx, pipe, key, row)
	case RedisDataTypeZSet:
		return w.writeZSet(ctx, pipe, key, row)
	default:
		return w.writeString(ctx, pipe, key, row)
	}
}

// getKey extracts the key from the row
func (w *RedisSinkWriter) getKey(row *api.SeaTunnelRow) string {
	// Try to use key field from row
	if w.config.KeyField != "" {
		for i, field := range w.schema.Fields {
			if field.Name == w.config.KeyField {
				val := row.GetField(i)
				if val != nil {
					return fmt.Sprintf("%v", val)
				}
			}
		}
	}

	// Use key pattern with row number
	if w.config.KeyPattern != "" {
		// Replace placeholders in pattern
		key := w.config.KeyPattern
		for i, field := range w.schema.Fields {
			placeholder := fmt.Sprintf("${%s}", field.Name)
			if strings.Contains(key, placeholder) {
				val := row.GetField(i)
				if val != nil {
					key = strings.ReplaceAll(key, placeholder, fmt.Sprintf("%v", val))
				}
			}
		}
		// Replace row count placeholder
		key = strings.ReplaceAll(key, "${__row_num__}", strconv.FormatInt(w.rowCount, 10))
		return key
	}

	// Default: use row count as key
	return fmt.Sprintf("row_%d", w.rowCount)
}

// getValue extracts the value from the row
func (w *RedisSinkWriter) getValue(row *api.SeaTunnelRow) (string, error) {
	// Use specific value field
	if w.config.ValueField != "" {
		for i, field := range w.schema.Fields {
			if field.Name == w.config.ValueField {
				val := row.GetField(i)
				if val != nil {
					return fmt.Sprintf("%v", val), nil
				}
			}
		}
	}

	// Serialize entire row based on format
	switch w.config.Format {
	case "json":
		return rowToJSON(row, w.schema)
	case "text":
		var parts []string
		for i := range w.schema.Fields {
			val := row.GetField(i)
			if val != nil {
				parts = append(parts, fmt.Sprintf("%v", val))
			}
		}
		return strings.Join(parts, ","), nil
	default:
		return rowToJSON(row, w.schema)
	}
}

func (w *RedisSinkWriter) writeString(ctx context.Context, pipe goredis.Pipeliner, key string, row *api.SeaTunnelRow) error {
	value, err := w.getValue(row)
	if err != nil {
		return err
	}

	if w.config.ExpireSeconds > 0 {
		pipe.SetEx(ctx, key, value, time.Duration(w.config.ExpireSeconds)*time.Second)
	} else {
		pipe.Set(ctx, key, value, 0)
	}
	return nil
}

func (w *RedisSinkWriter) writeHash(ctx context.Context, pipe goredis.Pipeliner, key string, row *api.SeaTunnelRow) error {
	// Use hash_key_field and hash_value_field if specified
	if w.config.HashKeyField != "" && w.config.HashValueField != "" {
		var hashKey, hashValue string
		for i, field := range w.schema.Fields {
			if field.Name == w.config.HashKeyField {
				val := row.GetField(i)
				if val != nil {
					hashKey = fmt.Sprintf("%v", val)
				}
			}
			if field.Name == w.config.HashValueField {
				val := row.GetField(i)
				if val != nil {
					hashValue = fmt.Sprintf("%v", val)
				}
			}
		}
		if hashKey != "" {
			pipe.HSet(ctx, key, hashKey, hashValue)
		}
	} else {
		// Write all fields as hash fields
		args := make([]interface{}, 0, len(w.schema.Fields)*2)
		for i, field := range w.schema.Fields {
			val := row.GetField(i)
			if val != nil {
				args = append(args, field.Name, fmt.Sprintf("%v", val))
			}
		}
		if len(args) > 0 {
			pipe.HSet(ctx, key, args...)
		}
	}

	if w.config.ExpireSeconds > 0 {
		pipe.Expire(ctx, key, time.Duration(w.config.ExpireSeconds)*time.Second)
	}
	return nil
}

func (w *RedisSinkWriter) writeList(ctx context.Context, pipe goredis.Pipeliner, key string, row *api.SeaTunnelRow) error {
	value, err := w.getValue(row)
	if err != nil {
		return err
	}

	pipe.RPush(ctx, key, value)
	if w.config.ExpireSeconds > 0 {
		pipe.Expire(ctx, key, time.Duration(w.config.ExpireSeconds)*time.Second)
	}
	return nil
}

func (w *RedisSinkWriter) writeSet(ctx context.Context, pipe goredis.Pipeliner, key string, row *api.SeaTunnelRow) error {
	value, err := w.getValue(row)
	if err != nil {
		return err
	}

	pipe.SAdd(ctx, key, value)
	if w.config.ExpireSeconds > 0 {
		pipe.Expire(ctx, key, time.Duration(w.config.ExpireSeconds)*time.Second)
	}
	return nil
}

func (w *RedisSinkWriter) writeZSet(ctx context.Context, pipe goredis.Pipeliner, key string, row *api.SeaTunnelRow) error {
	// Look for member and score fields
	var member string
	var score float64

	for i, field := range w.schema.Fields {
		val := row.GetField(i)
		if val == nil {
			continue
		}

		if field.Name == "member" || field.Name == w.config.ValueField {
			member = fmt.Sprintf("%v", val)
		}
		if field.Name == "score" {
			switch v := val.(type) {
			case float64:
				score = v
			case float32:
				score = float64(v)
			case int:
				score = float64(v)
			case int64:
				score = float64(v)
			}
		}
	}

	if member == "" {
		value, err := w.getValue(row)
		if err != nil {
			return err
		}
		member = value
	}

	pipe.ZAdd(ctx, key, goredis.Z{Score: score, Member: member})
	if w.config.ExpireSeconds > 0 {
		pipe.Expire(ctx, key, time.Duration(w.config.ExpireSeconds)*time.Second)
	}
	return nil
}

// PrepareCommit prepares for commit
func (w *RedisSinkWriter) PrepareCommit() ([]api.CommitInfo, error) {
	if err := w.flush(); err != nil {
		return nil, err
	}
	return nil, nil
}

// AbortPrepare aborts prepared data
func (w *RedisSinkWriter) AbortPrepare() {
	w.buffer = w.buffer[:0]
}

// SnapshotState snapshots the writer state
func (w *RedisSinkWriter) SnapshotState(checkpointId int64) ([]byte, error) {
	// Flush before snapshot
	if err := w.flush(); err != nil {
		return nil, err
	}
	return json.Marshal(map[string]int64{"rowCount": w.rowCount})
}

// Close closes the writer
func (w *RedisSinkWriter) Close() error {
	// Flush remaining data
	if err := w.flush(); err != nil {
		return err
	}
	return w.client.Close()
}

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

import (
	"context"
)

// Sink represents a data sink in SeaTunnel
type Sink interface {
	// GetPluginName returns the name of this sink plugin
	GetPluginName() string

	// SetTypeInfo sets the input row type
	SetTypeInfo(rowType *SeaTunnelRowType)

	// GetConsumedType returns the row type consumed by this sink
	GetConsumedType() *SeaTunnelRowType

	// CreateWriter creates a sink writer
	CreateWriter(ctx WriterContext) (SinkWriter, error)

	// CreateCommitter creates a sink committer (optional, returns nil if not needed)
	CreateCommitter() SinkCommitter

	// CreateAggregatedCommitter creates an aggregated committer (optional)
	CreateAggregatedCommitter() SinkAggregatedCommitter
}

// SinkWriter writes data to the sink
type SinkWriter interface {
	// Write writes a single row
	Write(row *SeaTunnelRow) error

	// PrepareCommit prepares for commit and returns commit info
	PrepareCommit() ([]CommitInfo, error)

	// AbortPrepare aborts a prepared commit
	AbortPrepare()

	// SnapshotState creates a checkpoint of the current state
	SnapshotState(checkpointID int64) ([]byte, error)

	// Close closes the writer
	Close() error
}

// SinkCommitter commits data written by SinkWriter
type SinkCommitter interface {
	// Commit commits the given commit info
	Commit(commitInfo []CommitInfo) ([]CommitInfo, error)

	// Abort aborts the given commit info
	Abort(commitInfo []CommitInfo) error

	// Close closes the committer
	Close() error
}

// SinkAggregatedCommitter aggregates and commits data from multiple writers
type SinkAggregatedCommitter interface {
	// Commit commits aggregated commit info from all writers
	Commit(aggregatedCommitInfo [][]CommitInfo) ([]CommitInfo, error)

	// Abort aborts aggregated commit info
	Abort(aggregatedCommitInfo [][]CommitInfo) error

	// Close closes the aggregated committer
	Close() error
}

// CommitInfo contains information needed for commit
type CommitInfo interface {
	// Serialize serializes the commit info
	Serialize() ([]byte, error)
}

// WriterContext provides context for sink writers
type WriterContext interface {
	// GetIndexOfSubtask returns the index of this subtask
	GetIndexOfSubtask() int

	// GetNumberOfParallelSubtasks returns the total number of parallel subtasks
	GetNumberOfParallelSubtasks() int

	// GetContext returns the context
	GetContext() context.Context

	// GetMetricsContext returns the metrics context
	GetMetricsContext() MetricsContext
}

// MultiTableResourceManager manages resources for multi-table sinks
type MultiTableResourceManager interface {
	// GetOrCreateResource gets or creates a shared resource
	GetOrCreateResource(tableID string, creator func() (interface{}, error)) (interface{}, error)

	// Close closes all resources
	Close() error
}

// SupportMultiTableSink indicates a sink supports multiple tables
type SupportMultiTableSink interface {
	Sink

	// CreateMultiTableResourceManager creates a resource manager
	CreateMultiTableResourceManager(parallelism int) MultiTableResourceManager
}

// SupportSchemaEvolution indicates a sink supports schema evolution
type SupportSchemaEvolution interface {
	// ApplySchemaChange applies a schema change
	ApplySchemaChange(change SchemaChange) error
}

// SchemaChange represents a schema change event
type SchemaChange interface {
	isSchemaChange()
}

// AddColumnChange represents adding a column
type AddColumnChange struct {
	TableID    string
	Column     *SeaTunnelFieldType
	AfterField string // Name of the field after which to add, empty for last
}

func (AddColumnChange) isSchemaChange() {}

// DropColumnChange represents dropping a column
type DropColumnChange struct {
	TableID    string
	ColumnName string
}

func (DropColumnChange) isSchemaChange() {}

// RenameColumnChange represents renaming a column
type RenameColumnChange struct {
	TableID string
	OldName string
	NewName string
}

func (RenameColumnChange) isSchemaChange() {}

// ModifyColumnChange represents modifying a column type
type ModifyColumnChange struct {
	TableID string
	Column  *SeaTunnelFieldType
}

func (ModifyColumnChange) isSchemaChange() {}

// SimpleCommitInfo is a basic implementation of CommitInfo
type SimpleCommitInfo struct {
	Data []byte
}

// Serialize serializes the commit info
func (c *SimpleCommitInfo) Serialize() ([]byte, error) {
	return c.Data, nil
}

// NewSimpleCommitInfo creates a new simple commit info
func NewSimpleCommitInfo(data []byte) *SimpleCommitInfo {
	return &SimpleCommitInfo{Data: data}
}

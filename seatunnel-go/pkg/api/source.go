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

// Source represents a data source in SeaTunnel
type Source interface {
	// GetPluginName returns the name of this source plugin
	GetPluginName() string

	// GetBoundedness returns the boundedness of this source
	GetBoundedness() Boundedness

	// GetProducedType returns the row type produced by this source
	GetProducedType() *SeaTunnelRowType

	// CreateEnumerator creates a split enumerator
	CreateEnumerator(ctx EnumeratorContext) (SplitEnumerator, error)

	// CreateReader creates a source reader
	CreateReader(ctx ReaderContext) (SourceReader, error)

	// RestoreEnumerator restores a split enumerator from checkpoint state
	RestoreEnumerator(ctx EnumeratorContext, state []byte) (SplitEnumerator, error)
}

// SourceSplit represents a portion of data to be read
type SourceSplit interface {
	// SplitID returns the unique identifier of this split
	SplitID() string
}

// SplitEnumerator discovers and assigns splits to readers
type SplitEnumerator interface {
	// Open initializes the enumerator
	Open() error

	// Run starts the split discovery and assignment
	Run() error

	// AddSplitsBack adds splits back for reassignment (e.g., after reader failure)
	AddSplitsBack(splits []SourceSplit, subtaskID int)

	// RegisterReader registers a reader
	RegisterReader(subtaskID int)

	// HandleSplitRequest handles a split request from a reader
	HandleSplitRequest(subtaskID int)

	// SnapshotState creates a checkpoint of the current state
	SnapshotState(checkpointID int64) ([]byte, error)

	// NotifyCheckpointComplete notifies that a checkpoint is complete
	NotifyCheckpointComplete(checkpointID int64) error

	// Close closes the enumerator
	Close() error
}

// SourceReader reads data from assigned splits
type SourceReader interface {
	// Open initializes the reader
	Open() error

	// PollNext reads the next batch of records
	PollNext(collector Collector) error

	// AddSplits adds splits to be read
	AddSplits(splits []SourceSplit) error

	// HandleNoMoreSplits notifies that no more splits will be assigned
	HandleNoMoreSplits()

	// SnapshotState creates a checkpoint of the current state
	SnapshotState(checkpointID int64) ([]SourceSplit, error)

	// NotifyCheckpointComplete notifies that a checkpoint is complete
	NotifyCheckpointComplete(checkpointID int64) error

	// Close closes the reader
	Close() error
}

// Collector collects rows produced by a source
type Collector interface {
	// Collect collects a single row
	Collect(row *SeaTunnelRow) error

	// CollectWithTimestamp collects a row with an event time timestamp
	CollectWithTimestamp(row *SeaTunnelRow, timestamp int64) error

	// MarkEvent marks an event (e.g., end of split)
	MarkEvent(event Event) error
}

// Event represents special events in the data stream
type Event interface {
	isEvent()
}

// EndOfSplitEvent indicates a split has been fully read
type EndOfSplitEvent struct {
	SplitID string
}

func (EndOfSplitEvent) isEvent() {}

// EnumeratorContext provides context for split enumerators
type EnumeratorContext interface {
	// GetParallelism returns the parallelism
	GetParallelism() int

	// GetCurrentParallelism returns the current parallelism
	GetCurrentParallelism() int

	// GetRegisteredReaders returns registered reader IDs
	GetRegisteredReaders() []int

	// AssignSplit assigns a split to a reader
	AssignSplit(subtaskID int, split SourceSplit) error

	// SignalNoMoreSplits signals that no more splits will be available for a reader
	SignalNoMoreSplits(subtaskID int) error

	// GetContext returns the context
	GetContext() context.Context

	// GetMetricsContext returns the metrics context
	GetMetricsContext() MetricsContext
}

// ReaderContext provides context for source readers
type ReaderContext interface {
	// GetIndexOfSubtask returns the index of this subtask
	GetIndexOfSubtask() int

	// GetBoundedness returns the boundedness of the source
	GetBoundedness() Boundedness

	// GetContext returns the context
	GetContext() context.Context

	// GetMetricsContext returns the metrics context
	GetMetricsContext() MetricsContext

	// SendSplitRequest requests more splits from the enumerator
	SendSplitRequest() error

	// SendSourceEvent sends an event to the coordinator
	SendSourceEvent(event Event) error
}

// MetricsContext provides access to metrics
type MetricsContext interface {
	// Counter returns or creates a counter metric
	Counter(name string) Counter

	// Gauge returns or creates a gauge metric
	Gauge(name string) Gauge

	// Meter returns or creates a meter metric
	Meter(name string) Meter
}

// Counter is a metric that can only increase
type Counter interface {
	Inc()
	Add(delta int64)
	Value() int64
}

// Gauge is a metric that can increase or decrease
type Gauge interface {
	Set(value int64)
	Inc()
	Dec()
	Value() int64
}

// Meter measures the rate of events
type Meter interface {
	Mark()
	MarkN(n int64)
	Rate() float64
}

// SimpleSplit is a basic implementation of SourceSplit
type SimpleSplit struct {
	ID string
}

// SplitID returns the split ID
func (s *SimpleSplit) SplitID() string {
	return s.ID
}

// NewSimpleSplit creates a new simple split
func NewSimpleSplit(id string) *SimpleSplit {
	return &SimpleSplit{ID: id}
}

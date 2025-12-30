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
	"sync"
	"time"

	"github.com/apache/seatunnel-go/pkg/api"
)

const (
	// PluginName is the name of the fake source plugin
	PluginName = "FakeSource"
)

// FakeSource is a source that generates fake data
type FakeSource struct {
	schema  *api.SeaTunnelRowType
	options *FakeOptions
}

// NewFakeSource creates a new fake source
func NewFakeSource(schema *api.SeaTunnelRowType, options *FakeOptions) *FakeSource {
	return &FakeSource{
		schema:  schema,
		options: options,
	}
}

// GetPluginName returns the plugin name
func (s *FakeSource) GetPluginName() string {
	return PluginName
}

// GetBoundedness returns the boundedness
func (s *FakeSource) GetBoundedness() api.Boundedness {
	return api.Bounded
}

// GetProducedType returns the produced row type
func (s *FakeSource) GetProducedType() *api.SeaTunnelRowType {
	return s.schema
}

// CreateEnumerator creates a split enumerator
func (s *FakeSource) CreateEnumerator(ctx api.EnumeratorContext) (api.SplitEnumerator, error) {
	return NewFakeSplitEnumerator(ctx, s.options)
}

// CreateReader creates a source reader
func (s *FakeSource) CreateReader(ctx api.ReaderContext) (api.SourceReader, error) {
	return NewFakeSourceReader(ctx, s.schema, s.options)
}

// RestoreEnumerator restores from checkpoint
func (s *FakeSource) RestoreEnumerator(ctx api.EnumeratorContext, state []byte) (api.SplitEnumerator, error) {
	// For simplicity, just create a new enumerator
	return s.CreateEnumerator(ctx)
}

// FakeSplit represents a portion of data to generate
type FakeSplit struct {
	ID       string
	RowStart int64
	RowEnd   int64
}

// SplitID returns the split ID
func (s *FakeSplit) SplitID() string {
	return s.ID
}

// FakeSplitEnumerator discovers splits
type FakeSplitEnumerator struct {
	ctx       api.EnumeratorContext
	options   *FakeOptions
	splits    []*FakeSplit
	assigned  map[int]bool
	mu        sync.Mutex
}

// NewFakeSplitEnumerator creates a new split enumerator
func NewFakeSplitEnumerator(ctx api.EnumeratorContext, options *FakeOptions) (*FakeSplitEnumerator, error) {
	return &FakeSplitEnumerator{
		ctx:      ctx,
		options:  options,
		assigned: make(map[int]bool),
	}, nil
}

// Open initializes the enumerator
func (e *FakeSplitEnumerator) Open() error {
	// Create splits
	rowsPerSplit := e.options.RowCount / int64(e.options.SplitNum)
	if rowsPerSplit < 1 {
		rowsPerSplit = 1
	}

	var rowStart int64 = 0
	for i := 0; i < e.options.SplitNum && rowStart < e.options.RowCount; i++ {
		rowEnd := rowStart + rowsPerSplit
		if rowEnd > e.options.RowCount {
			rowEnd = e.options.RowCount
		}

		split := &FakeSplit{
			ID:       fmt.Sprintf("fake-split-%d", i),
			RowStart: rowStart,
			RowEnd:   rowEnd,
		}
		e.splits = append(e.splits, split)
		rowStart = rowEnd
	}

	return nil
}

// Run starts split discovery
func (e *FakeSplitEnumerator) Run() error {
	// Assign splits to registered readers
	e.mu.Lock()
	defer e.mu.Unlock()

	readers := e.ctx.GetRegisteredReaders()
	if len(readers) == 0 {
		return nil
	}

	for i, split := range e.splits {
		readerIdx := readers[i%len(readers)]
		if err := e.ctx.AssignSplit(readerIdx, split); err != nil {
			return err
		}
	}

	// Signal no more splits
	for _, reader := range readers {
		if err := e.ctx.SignalNoMoreSplits(reader); err != nil {
			return err
		}
	}

	return nil
}

// AddSplitsBack adds splits back for reassignment
func (e *FakeSplitEnumerator) AddSplitsBack(splits []api.SourceSplit, subtaskID int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, split := range splits {
		if fs, ok := split.(*FakeSplit); ok {
			e.splits = append(e.splits, fs)
		}
	}
}

// RegisterReader registers a reader
func (e *FakeSplitEnumerator) RegisterReader(subtaskID int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.assigned[subtaskID] = false
}

// HandleSplitRequest handles a split request
func (e *FakeSplitEnumerator) HandleSplitRequest(subtaskID int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Assign an unassigned split
	for i, split := range e.splits {
		if err := e.ctx.AssignSplit(subtaskID, split); err == nil {
			// Remove from list
			e.splits = append(e.splits[:i], e.splits[i+1:]...)
			return
		}
	}
}

// SnapshotState creates a checkpoint
func (e *FakeSplitEnumerator) SnapshotState(checkpointID int64) ([]byte, error) {
	return nil, nil
}

// NotifyCheckpointComplete notifies checkpoint complete
func (e *FakeSplitEnumerator) NotifyCheckpointComplete(checkpointID int64) error {
	return nil
}

// Close closes the enumerator
func (e *FakeSplitEnumerator) Close() error {
	return nil
}

// FakeSourceReader reads from splits
type FakeSourceReader struct {
	ctx        api.ReaderContext
	schema     *api.SeaTunnelRowType
	options    *FakeOptions
	splits     []*FakeSplit
	generator  *FakeDataGenerator
	noMoreSplits bool
	currentRow int64
	mu         sync.Mutex
}

// NewFakeSourceReader creates a new source reader
func NewFakeSourceReader(ctx api.ReaderContext, schema *api.SeaTunnelRowType, options *FakeOptions) (*FakeSourceReader, error) {
	return &FakeSourceReader{
		ctx:       ctx,
		schema:    schema,
		options:   options,
		generator: NewFakeDataGenerator(schema, options),
	}, nil
}

// Open initializes the reader
func (r *FakeSourceReader) Open() error {
	return nil
}

// PollNext reads the next batch of records
func (r *FakeSourceReader) PollNext(collector api.Collector) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.splits) == 0 {
		if r.noMoreSplits {
			return nil // Done
		}
		// Request more splits
		return r.ctx.SendSplitRequest()
	}

	// Process current split
	split := r.splits[0]
	for r.currentRow < split.RowEnd-split.RowStart {
		row := r.generator.Generate()
		if err := collector.Collect(row); err != nil {
			return err
		}
		r.currentRow++

		// Respect read interval
		if r.options.SplitReadInterval > 0 {
			time.Sleep(time.Duration(r.options.SplitReadInterval) * time.Millisecond)
		}
	}

	// Mark split as complete
	if err := collector.MarkEvent(api.EndOfSplitEvent{SplitID: split.ID}); err != nil {
		return err
	}

	// Move to next split
	r.splits = r.splits[1:]
	r.currentRow = 0

	return nil
}

// AddSplits adds splits to read
func (r *FakeSourceReader) AddSplits(splits []api.SourceSplit) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, split := range splits {
		if fs, ok := split.(*FakeSplit); ok {
			r.splits = append(r.splits, fs)
		}
	}
	return nil
}

// HandleNoMoreSplits notifies no more splits
func (r *FakeSourceReader) HandleNoMoreSplits() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.noMoreSplits = true
}

// SnapshotState creates a checkpoint
func (r *FakeSourceReader) SnapshotState(checkpointID int64) ([]api.SourceSplit, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	result := make([]api.SourceSplit, len(r.splits))
	for i, s := range r.splits {
		result[i] = s
	}
	return result, nil
}

// NotifyCheckpointComplete notifies checkpoint complete
func (r *FakeSourceReader) NotifyCheckpointComplete(checkpointID int64) error {
	return nil
}

// Close closes the reader
func (r *FakeSourceReader) Close() error {
	return nil
}

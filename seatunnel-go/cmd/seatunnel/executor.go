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

package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/apache/seatunnel-go/pkg/api"
	"github.com/apache/seatunnel-go/pkg/config"
	"github.com/apache/seatunnel-go/pkg/registry"
	"go.uber.org/zap"
)

// LocalExecutor executes a job locally
type LocalExecutor struct {
	config   *config.JobConfig
	logger   *zap.Logger
	registry *registry.ConnectorRegistry
}

// NewLocalExecutor creates a new local executor
func NewLocalExecutor(config *config.JobConfig, logger *zap.Logger) *LocalExecutor {
	return &LocalExecutor{
		config:   config,
		logger:   logger,
		registry: registry.GetRegistry(),
	}
}

// Execute executes the job
func (e *LocalExecutor) Execute() error {
	ctx := context.Background()

	// Create sources
	sources := make([]api.Source, 0, len(e.config.Source))
	for _, sourceConfig := range e.config.Source {
		source, err := e.registry.CreateSource(
			sourceConfig.PluginName,
			sourceConfig.ToAPIConfig(),
			nil,
		)
		if err != nil {
			return fmt.Errorf("failed to create source %s: %w", sourceConfig.PluginName, err)
		}
		sources = append(sources, source)
	}

	// Create sinks
	sinks := make([]api.Sink, 0, len(e.config.Sink))
	for _, sinkConfig := range e.config.Sink {
		sink, err := e.registry.CreateSink(
			sinkConfig.PluginName,
			sinkConfig.ToAPIConfig(),
			nil,
		)
		if err != nil {
			return fmt.Errorf("failed to create sink %s: %w", sinkConfig.PluginName, err)
		}
		sinks = append(sinks, sink)
	}

	// Set type info for sinks
	for i, source := range sources {
		rowType := source.GetProducedType()
		for _, sink := range sinks {
			sink.SetTypeInfo(rowType)
		}
		e.logger.Info("Source schema",
			zap.Int("sourceIndex", i),
			zap.String("sourceName", source.GetPluginName()),
			zap.Int("fieldCount", rowType.GetFieldCount()),
		)
	}

	// Create a simple pipeline: source -> sink
	// For now, single-threaded execution
	for sourceIdx, source := range sources {
		e.logger.Info("Processing source",
			zap.Int("index", sourceIdx),
			zap.String("name", source.GetPluginName()),
		)

		// Create reader context
		readerCtx := &localReaderContext{
			ctx:         ctx,
			subtaskID:   0,
			boundedness: source.GetBoundedness(),
		}

		// Create reader
		reader, err := source.CreateReader(readerCtx)
		if err != nil {
			return fmt.Errorf("failed to create reader: %w", err)
		}
		defer reader.Close()

		if err := reader.Open(); err != nil {
			return fmt.Errorf("failed to open reader: %w", err)
		}

		// Create writer contexts and writers
		writers := make([]api.SinkWriter, len(sinks))
		for i, sink := range sinks {
			writerCtx := &localWriterContext{
				ctx:       ctx,
				subtaskID: 0,
				numTasks:  1,
			}
			writer, err := sink.CreateWriter(writerCtx)
			if err != nil {
				return fmt.Errorf("failed to create writer: %w", err)
			}
			writers[i] = writer
			defer writer.Close()
		}

		// Create enumerator and assign splits
		enumCtx := &localEnumeratorContext{
			ctx:         ctx,
			parallelism: 1,
			readers:     []int{0},
			splitsChan:  make(chan api.SourceSplit, 100),
		}

		enumerator, err := source.CreateEnumerator(enumCtx)
		if err != nil {
			return fmt.Errorf("failed to create enumerator: %w", err)
		}
		defer enumerator.Close()

		if err := enumerator.Open(); err != nil {
			return fmt.Errorf("failed to open enumerator: %w", err)
		}

		// Run enumerator in background
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := enumerator.Run(); err != nil {
				e.logger.Error("Enumerator error", zap.Error(err))
			}
		}()

		// Assign splits to reader
		go func() {
			for split := range enumCtx.splitsChan {
				reader.AddSplits([]api.SourceSplit{split})
			}
			reader.HandleNoMoreSplits()
		}()

		// Create collector
		collector := &localCollector{
			writers: writers,
			logger:  e.logger,
		}

		// Poll and write
		for {
			if err := reader.PollNext(collector); err != nil {
				return fmt.Errorf("poll error: %w", err)
			}

			// Check if done (simple check for bounded sources)
			if source.GetBoundedness() == api.Bounded && collector.finished {
				break
			}
		}

		// Commit writes
		for _, writer := range writers {
			if _, err := writer.PrepareCommit(); err != nil {
				e.logger.Warn("Commit error", zap.Error(err))
			}
		}

		wg.Wait()

		e.logger.Info("Source completed",
			zap.Int("index", sourceIdx),
			zap.Int64("rowsRead", collector.rowCount.Load()),
		)
	}

	return nil
}

// localReaderContext implements ReaderContext
type localReaderContext struct {
	ctx         context.Context
	subtaskID   int
	boundedness api.Boundedness
	metrics     *localMetricsContext
}

func (c *localReaderContext) GetIndexOfSubtask() int {
	return c.subtaskID
}

func (c *localReaderContext) GetBoundedness() api.Boundedness {
	return c.boundedness
}

func (c *localReaderContext) GetContext() context.Context {
	return c.ctx
}

func (c *localReaderContext) GetMetricsContext() api.MetricsContext {
	if c.metrics == nil {
		c.metrics = &localMetricsContext{}
	}
	return c.metrics
}

func (c *localReaderContext) SendSplitRequest() error {
	return nil
}

func (c *localReaderContext) SendSourceEvent(event api.Event) error {
	return nil
}

// localWriterContext implements WriterContext
type localWriterContext struct {
	ctx       context.Context
	subtaskID int
	numTasks  int
	metrics   *localMetricsContext
}

func (c *localWriterContext) GetIndexOfSubtask() int {
	return c.subtaskID
}

func (c *localWriterContext) GetNumberOfParallelSubtasks() int {
	return c.numTasks
}

func (c *localWriterContext) GetContext() context.Context {
	return c.ctx
}

func (c *localWriterContext) GetMetricsContext() api.MetricsContext {
	if c.metrics == nil {
		c.metrics = &localMetricsContext{}
	}
	return c.metrics
}

// localEnumeratorContext implements EnumeratorContext
type localEnumeratorContext struct {
	ctx         context.Context
	parallelism int
	readers     []int
	splitsChan  chan api.SourceSplit
	metrics     *localMetricsContext
}

func (c *localEnumeratorContext) GetParallelism() int {
	return c.parallelism
}

func (c *localEnumeratorContext) GetCurrentParallelism() int {
	return c.parallelism
}

func (c *localEnumeratorContext) GetRegisteredReaders() []int {
	return c.readers
}

func (c *localEnumeratorContext) AssignSplit(subtaskID int, split api.SourceSplit) error {
	c.splitsChan <- split
	return nil
}

func (c *localEnumeratorContext) SignalNoMoreSplits(subtaskID int) error {
	close(c.splitsChan)
	return nil
}

func (c *localEnumeratorContext) GetContext() context.Context {
	return c.ctx
}

func (c *localEnumeratorContext) GetMetricsContext() api.MetricsContext {
	if c.metrics == nil {
		c.metrics = &localMetricsContext{}
	}
	return c.metrics
}

// localCollector implements Collector
type localCollector struct {
	writers  []api.SinkWriter
	logger   *zap.Logger
	rowCount atomic.Int64
	finished bool
}

func (c *localCollector) Collect(row *api.SeaTunnelRow) error {
	c.rowCount.Add(1)
	for _, writer := range c.writers {
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	return nil
}

func (c *localCollector) CollectWithTimestamp(row *api.SeaTunnelRow, timestamp int64) error {
	return c.Collect(row)
}

func (c *localCollector) MarkEvent(event api.Event) error {
	if _, ok := event.(api.EndOfSplitEvent); ok {
		c.finished = true
	}
	return nil
}

// localMetricsContext implements MetricsContext
type localMetricsContext struct {
	counters map[string]*localCounter
	gauges   map[string]*localGauge
	meters   map[string]*localMeter
	mu       sync.Mutex
}

func (c *localMetricsContext) Counter(name string) api.Counter {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.counters == nil {
		c.counters = make(map[string]*localCounter)
	}
	if counter, ok := c.counters[name]; ok {
		return counter
	}
	counter := &localCounter{}
	c.counters[name] = counter
	return counter
}

func (c *localMetricsContext) Gauge(name string) api.Gauge {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gauges == nil {
		c.gauges = make(map[string]*localGauge)
	}
	if gauge, ok := c.gauges[name]; ok {
		return gauge
	}
	gauge := &localGauge{}
	c.gauges[name] = gauge
	return gauge
}

func (c *localMetricsContext) Meter(name string) api.Meter {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.meters == nil {
		c.meters = make(map[string]*localMeter)
	}
	if meter, ok := c.meters[name]; ok {
		return meter
	}
	meter := &localMeter{}
	c.meters[name] = meter
	return meter
}

type localCounter struct {
	value atomic.Int64
}

func (c *localCounter) Inc()            { c.value.Add(1) }
func (c *localCounter) Add(delta int64) { c.value.Add(delta) }
func (c *localCounter) Value() int64    { return c.value.Load() }

type localGauge struct {
	value atomic.Int64
}

func (g *localGauge) Set(value int64) { g.value.Store(value) }
func (g *localGauge) Inc()            { g.value.Add(1) }
func (g *localGauge) Dec()            { g.value.Add(-1) }
func (g *localGauge) Value() int64    { return g.value.Load() }

type localMeter struct {
	count atomic.Int64
}

func (m *localMeter) Mark()         { m.count.Add(1) }
func (m *localMeter) MarkN(n int64) { m.count.Add(n) }
func (m *localMeter) Rate() float64 { return float64(m.count.Load()) }

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

package console

import (
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/apache/seatunnel-go/pkg/api"
	"go.uber.org/zap"
)

const (
	// PluginName is the name of the console sink plugin
	PluginName = "Console"

	// DefaultLogPrintData controls whether to print data
	DefaultLogPrintData = true

	// DefaultLogPrintDelay controls the delay between prints
	DefaultLogPrintDelay = 0
)

// ConsoleSink is a sink that outputs data to console/log
type ConsoleSink struct {
	rowType      *api.SeaTunnelRowType
	logPrintData bool
	logPrintDelay int
}

// NewConsoleSink creates a new console sink
func NewConsoleSink(config *api.Config) *ConsoleSink {
	return &ConsoleSink{
		logPrintData:  config.GetBoolDefault("log.print.data", DefaultLogPrintData),
		logPrintDelay: config.GetIntDefault("log.print.delay.ms", DefaultLogPrintDelay),
	}
}

// GetPluginName returns the plugin name
func (s *ConsoleSink) GetPluginName() string {
	return PluginName
}

// SetTypeInfo sets the input row type
func (s *ConsoleSink) SetTypeInfo(rowType *api.SeaTunnelRowType) {
	s.rowType = rowType
}

// GetConsumedType returns the consumed row type
func (s *ConsoleSink) GetConsumedType() *api.SeaTunnelRowType {
	return s.rowType
}

// CreateWriter creates a console sink writer
func (s *ConsoleSink) CreateWriter(ctx api.WriterContext) (api.SinkWriter, error) {
	return NewConsoleSinkWriter(s.rowType, ctx.GetIndexOfSubtask(), s.logPrintData, s.logPrintDelay)
}

// CreateCommitter returns nil as console sink doesn't need commit
func (s *ConsoleSink) CreateCommitter() api.SinkCommitter {
	return nil
}

// CreateAggregatedCommitter returns nil
func (s *ConsoleSink) CreateAggregatedCommitter() api.SinkAggregatedCommitter {
	return nil
}

// ConsoleSinkWriter writes rows to console
type ConsoleSinkWriter struct {
	rowType      *api.SeaTunnelRowType
	subtaskIndex int
	rowCounter   atomic.Int64
	logPrintData bool
	logPrintDelay int
	logger       *zap.Logger
}

// NewConsoleSinkWriter creates a new console sink writer
func NewConsoleSinkWriter(rowType *api.SeaTunnelRowType, subtaskIndex int, logPrintData bool, logPrintDelay int) (*ConsoleSinkWriter, error) {
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}

	return &ConsoleSinkWriter{
		rowType:      rowType,
		subtaskIndex: subtaskIndex,
		logPrintData: logPrintData,
		logPrintDelay: logPrintDelay,
		logger:       logger,
	}, nil
}

// Write writes a row to console
func (w *ConsoleSinkWriter) Write(row *api.SeaTunnelRow) error {
	if !w.logPrintData {
		return nil
	}

	rowIndex := w.rowCounter.Add(1)

	// Format the row
	var fields []string
	for i := 0; i < row.GetArity(); i++ {
		field := row.GetField(i)
		var fieldType *api.SeaTunnelDataType
		if w.rowType != nil && i < len(w.rowType.Fields) {
			fieldType = w.rowType.Fields[i].Type
		}
		fields = append(fields, api.FormatField(fieldType, field))
	}

	// Build output message
	var tableInfo string
	if tableID := row.GetTableID(); tableID != "" {
		tableInfo = fmt.Sprintf(" table=%s", tableID)
	}

	output := strings.Join(fields, ", ")

	w.logger.Info("Console output",
		zap.Int("subtaskIndex", w.subtaskIndex),
		zap.Int64("rowIndex", rowIndex),
		zap.String("kind", row.GetKind().String()),
		zap.String("tableId", tableInfo),
		zap.String("data", output),
	)

	return nil
}

// PrepareCommit prepares for commit
func (w *ConsoleSinkWriter) PrepareCommit() ([]api.CommitInfo, error) {
	return nil, nil
}

// AbortPrepare aborts a prepared commit
func (w *ConsoleSinkWriter) AbortPrepare() {
	// Nothing to abort for console sink
}

// SnapshotState creates a checkpoint
func (w *ConsoleSinkWriter) SnapshotState(checkpointID int64) ([]byte, error) {
	return nil, nil
}

// Close closes the writer
func (w *ConsoleSinkWriter) Close() error {
	return w.logger.Sync()
}

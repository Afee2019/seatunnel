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
	"github.com/apache/seatunnel-go/pkg/api"
)

// ConsoleSinkFactory creates ConsoleSink instances
type ConsoleSinkFactory struct{}

// NewConsoleSinkFactory creates a new factory
func NewConsoleSinkFactory() *ConsoleSinkFactory {
	return &ConsoleSinkFactory{}
}

// FactoryIdentifier returns the identifier
func (f *ConsoleSinkFactory) FactoryIdentifier() string {
	return PluginName
}

// OptionRules returns the configuration options
func (f *ConsoleSinkFactory) OptionRules() []api.OptionRule {
	return []api.OptionRule{
		{
			Name:         "log.print.data",
			Type:         api.OptionTypeBoolean,
			Required:     false,
			DefaultValue: true,
			Description:  "Whether to print data to log",
		},
		{
			Name:         "log.print.delay.ms",
			Type:         api.OptionTypeInt,
			Required:     false,
			DefaultValue: 0,
			Description:  "Delay in milliseconds between prints",
		},
	}
}

// CreateSink creates a console sink
func (f *ConsoleSinkFactory) CreateSink(ctx api.TableSinkFactoryContext) (api.Sink, error) {
	sink := NewConsoleSink(ctx.GetOptions())
	if ctx.GetCatalogTable() != nil {
		sink.SetTypeInfo(ctx.GetCatalogTable().TableSchema)
	}
	return sink, nil
}

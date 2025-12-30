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
	"github.com/apache/seatunnel-go/pkg/api"
)

// HTTPSourceFactory creates HTTP sources
type HTTPSourceFactory struct{}

// NewHTTPSourceFactory creates a new factory
func NewHTTPSourceFactory() *HTTPSourceFactory {
	return &HTTPSourceFactory{}
}

// FactoryIdentifier returns the factory identifier
func (f *HTTPSourceFactory) FactoryIdentifier() string {
	return "Http"
}

// OptionRules returns the option rules
func (f *HTTPSourceFactory) OptionRules() []api.OptionRule {
	return []api.OptionRule{
		{Name: "url", Type: api.OptionTypeString, Required: true, Description: "HTTP URL"},
		{Name: "method", Type: api.OptionTypeString, Required: false, DefaultValue: "GET", Description: "HTTP method"},
		{Name: "headers", Type: api.OptionTypeMap, Required: false, Description: "HTTP headers"},
		{Name: "params", Type: api.OptionTypeMap, Required: false, Description: "Query parameters"},
		{Name: "body", Type: api.OptionTypeString, Required: false, Description: "Request body"},
		{Name: "content_type", Type: api.OptionTypeString, Required: false, DefaultValue: "application/json", Description: "Content type"},
		{Name: "pagination_type", Type: api.OptionTypeString, Required: false, DefaultValue: "none", Description: "Pagination type: none, offset, page_num, cursor, next_url"},
		{Name: "page_size", Type: api.OptionTypeInt, Required: false, DefaultValue: 100, Description: "Page size"},
		{Name: "data_path", Type: api.OptionTypeString, Required: false, DefaultValue: "data", Description: "JSON path to data array"},
		{Name: "schema", Type: api.OptionTypeMap, Required: false, Description: "Schema definition"},
	}
}

// CreateSource creates an HTTP source
func (f *HTTPSourceFactory) CreateSource(ctx api.TableSourceFactoryContext) (api.Source, error) {
	config := ctx.GetOptions()
	sourceConfig := SourceConfigFromAPI(config)
	return NewHTTPSource(sourceConfig), nil
}

// HTTPSinkFactory creates HTTP sinks
type HTTPSinkFactory struct{}

// NewHTTPSinkFactory creates a new factory
func NewHTTPSinkFactory() *HTTPSinkFactory {
	return &HTTPSinkFactory{}
}

// FactoryIdentifier returns the factory identifier
func (f *HTTPSinkFactory) FactoryIdentifier() string {
	return "Http"
}

// OptionRules returns the option rules
func (f *HTTPSinkFactory) OptionRules() []api.OptionRule {
	return []api.OptionRule{
		{Name: "url", Type: api.OptionTypeString, Required: true, Description: "HTTP URL"},
		{Name: "method", Type: api.OptionTypeString, Required: false, DefaultValue: "POST", Description: "HTTP method"},
		{Name: "headers", Type: api.OptionTypeMap, Required: false, Description: "HTTP headers"},
		{Name: "content_type", Type: api.OptionTypeString, Required: false, DefaultValue: "application/json", Description: "Content type"},
		{Name: "batch_size", Type: api.OptionTypeInt, Required: false, DefaultValue: 100, Description: "Batch size"},
		{Name: "batch_mode", Type: api.OptionTypeString, Required: false, DefaultValue: "array", Description: "Batch mode: single, array"},
		{Name: "body_template", Type: api.OptionTypeString, Required: false, Description: "Body template"},
	}
}

// CreateSink creates an HTTP sink
func (f *HTTPSinkFactory) CreateSink(ctx api.TableSinkFactoryContext) (api.Sink, error) {
	config := ctx.GetOptions()
	sinkConfig := SinkConfigFromAPI(config)
	return NewHTTPSink(sinkConfig), nil
}

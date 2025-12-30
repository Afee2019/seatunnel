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
	"github.com/apache/seatunnel-go/pkg/api"
)

// RedisSourceFactory creates Redis sources
type RedisSourceFactory struct{}

// NewRedisSourceFactory creates a new factory
func NewRedisSourceFactory() *RedisSourceFactory {
	return &RedisSourceFactory{}
}

// FactoryIdentifier returns the factory identifier
func (f *RedisSourceFactory) FactoryIdentifier() string {
	return "Redis"
}

// OptionRules returns the option rules
func (f *RedisSourceFactory) OptionRules() []api.OptionRule {
	return []api.OptionRule{
		{Name: "host", Type: api.OptionTypeString, Required: true, Description: "Redis host"},
		{Name: "port", Type: api.OptionTypeInt, Required: false, DefaultValue: 6379, Description: "Redis port"},
		{Name: "password", Type: api.OptionTypeString, Required: false, Description: "Redis password"},
		{Name: "database", Type: api.OptionTypeInt, Required: false, DefaultValue: 0, Description: "Redis database number"},
		{Name: "mode", Type: api.OptionTypeString, Required: false, DefaultValue: "standalone", Description: "Redis mode: standalone, cluster, sentinel"},
		{Name: "key_pattern", Type: api.OptionTypeString, Required: false, DefaultValue: "*", Description: "Key pattern for SCAN"},
		{Name: "data_type", Type: api.OptionTypeString, Required: false, DefaultValue: "string", Description: "Redis data type: string, hash, list, set, zset"},
		{Name: "batch_size", Type: api.OptionTypeLong, Required: false, DefaultValue: int64(1000), Description: "Batch size for SCAN"},
	}
}

// CreateSource creates a Redis source
func (f *RedisSourceFactory) CreateSource(ctx api.TableSourceFactoryContext) (api.Source, error) {
	config := ctx.GetOptions()
	sourceConfig := SourceConfigFromAPI(config)
	return NewRedisSource(sourceConfig), nil
}

// RedisSinkFactory creates Redis sinks
type RedisSinkFactory struct{}

// NewRedisSinkFactory creates a new factory
func NewRedisSinkFactory() *RedisSinkFactory {
	return &RedisSinkFactory{}
}

// FactoryIdentifier returns the factory identifier
func (f *RedisSinkFactory) FactoryIdentifier() string {
	return "Redis"
}

// OptionRules returns the option rules
func (f *RedisSinkFactory) OptionRules() []api.OptionRule {
	return []api.OptionRule{
		{Name: "host", Type: api.OptionTypeString, Required: true, Description: "Redis host"},
		{Name: "port", Type: api.OptionTypeInt, Required: false, DefaultValue: 6379, Description: "Redis port"},
		{Name: "password", Type: api.OptionTypeString, Required: false, Description: "Redis password"},
		{Name: "database", Type: api.OptionTypeInt, Required: false, DefaultValue: 0, Description: "Redis database number"},
		{Name: "mode", Type: api.OptionTypeString, Required: false, DefaultValue: "standalone", Description: "Redis mode: standalone, cluster, sentinel"},
		{Name: "key_pattern", Type: api.OptionTypeString, Required: false, Description: "Key pattern template"},
		{Name: "key_field", Type: api.OptionTypeString, Required: false, Description: "Field to use as key"},
		{Name: "data_type", Type: api.OptionTypeString, Required: false, DefaultValue: "string", Description: "Redis data type: string, hash, list, set, zset"},
		{Name: "expire", Type: api.OptionTypeLong, Required: false, DefaultValue: int64(0), Description: "Expiration time in seconds"},
		{Name: "batch_size", Type: api.OptionTypeInt, Required: false, DefaultValue: 1000, Description: "Batch size for pipeline"},
	}
}

// CreateSink creates a Redis sink
func (f *RedisSinkFactory) CreateSink(ctx api.TableSinkFactoryContext) (api.Sink, error) {
	config := ctx.GetOptions()
	sinkConfig := SinkConfigFromAPI(config)
	return NewRedisSink(sinkConfig), nil
}

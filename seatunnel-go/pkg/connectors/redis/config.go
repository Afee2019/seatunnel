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
	"fmt"
	"time"

	"github.com/apache/seatunnel-go/pkg/api"
)

// RedisMode defines the Redis deployment mode
type RedisMode string

const (
	// RedisModeStandalone is for single Redis instance
	RedisModeStandalone RedisMode = "standalone"
	// RedisModeCluster is for Redis Cluster
	RedisModeCluster RedisMode = "cluster"
	// RedisModeSentinel is for Redis Sentinel
	RedisModeSentinel RedisMode = "sentinel"
)

// RedisDataType defines the Redis data structure type
type RedisDataType string

const (
	RedisDataTypeString RedisDataType = "string"
	RedisDataTypeHash   RedisDataType = "hash"
	RedisDataTypeList   RedisDataType = "list"
	RedisDataTypeSet    RedisDataType = "set"
	RedisDataTypeZSet   RedisDataType = "zset"
)

// RedisConfig holds Redis connection configuration
type RedisConfig struct {
	// Connection settings
	Host     string
	Port     int
	Mode     RedisMode
	Password string
	Database int

	// Cluster settings
	Nodes []string // For cluster mode

	// Sentinel settings
	MasterName     string   // For sentinel mode
	SentinelNodes  []string // For sentinel mode

	// Connection pool
	MaxRetries      int
	MinIdleConns    int
	MaxConnAge      time.Duration
	PoolTimeout     time.Duration
	IdleTimeout     time.Duration
	ConnectTimeout  time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration

	// TLS
	EnableTLS  bool
	CertFile   string
	KeyFile    string
	CAFile     string
	SkipVerify bool
}

// DefaultRedisConfig returns default Redis configuration
func DefaultRedisConfig() *RedisConfig {
	return &RedisConfig{
		Host:           "localhost",
		Port:           6379,
		Mode:           RedisModeStandalone,
		Database:       0,
		MaxRetries:     3,
		MinIdleConns:   2,
		MaxConnAge:     0,
		PoolTimeout:    4 * time.Second,
		IdleTimeout:    5 * time.Minute,
		ConnectTimeout: 5 * time.Second,
		ReadTimeout:    3 * time.Second,
		WriteTimeout:   3 * time.Second,
	}
}

// FromAPIConfig creates RedisConfig from api.Config
func FromAPIConfig(config *api.Config) *RedisConfig {
	rc := DefaultRedisConfig()

	if v := config.GetString("host"); v != "" {
		rc.Host = v
	}
	if v := config.GetInt("port"); v > 0 {
		rc.Port = v
	}
	if v := config.GetString("mode"); v != "" {
		rc.Mode = RedisMode(v)
	}
	if v := config.GetString("password"); v != "" {
		rc.Password = v
	}
	if v := config.GetInt("database"); v > 0 {
		rc.Database = v
	}
	if v := config.GetInt("db_num"); v > 0 {
		rc.Database = v
	}

	// Cluster nodes
	if nodes, ok := config.Get("nodes"); ok && nodes != nil {
		if nodeList, ok := nodes.([]interface{}); ok {
			for _, n := range nodeList {
				if s, ok := n.(string); ok {
					rc.Nodes = append(rc.Nodes, s)
				}
			}
		}
	}

	// Sentinel settings
	if v := config.GetString("master_name"); v != "" {
		rc.MasterName = v
	}
	if v := config.GetString("master.name"); v != "" {
		rc.MasterName = v
	}

	// TLS
	rc.EnableTLS = config.GetBool("enable_tls")
	if v := config.GetString("cert_file"); v != "" {
		rc.CertFile = v
	}
	if v := config.GetString("key_file"); v != "" {
		rc.KeyFile = v
	}
	if v := config.GetString("ca_file"); v != "" {
		rc.CAFile = v
	}
	rc.SkipVerify = config.GetBool("skip_verify")

	return rc
}

// Validate validates the Redis configuration
func (c *RedisConfig) Validate() error {
	if c.Mode == RedisModeCluster && len(c.Nodes) == 0 {
		return fmt.Errorf("cluster mode requires at least one node address")
	}
	if c.Mode == RedisModeSentinel && c.MasterName == "" {
		return fmt.Errorf("sentinel mode requires master name")
	}
	if c.Mode == RedisModeSentinel && len(c.SentinelNodes) == 0 {
		return fmt.Errorf("sentinel mode requires at least one sentinel node")
	}
	return nil
}

// Address returns the Redis address for standalone mode
func (c *RedisConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// SourceConfig holds Redis source-specific configuration
type SourceConfig struct {
	*RedisConfig

	// Key pattern for SCAN
	KeyPattern string
	// Data type
	DataType RedisDataType
	// Hash field to retrieve (for hash type)
	HashFields []string
	// Batch size for SCAN
	BatchSize int64
	// Whether to delete keys after reading
	DeleteAfterRead bool
}

// DefaultSourceConfig returns default source configuration
func DefaultSourceConfig() *SourceConfig {
	return &SourceConfig{
		RedisConfig: DefaultRedisConfig(),
		KeyPattern:  "*",
		DataType:    RedisDataTypeString,
		BatchSize:   1000,
	}
}

// SourceConfigFromAPI creates SourceConfig from api.Config
func SourceConfigFromAPI(config *api.Config) *SourceConfig {
	sc := &SourceConfig{
		RedisConfig:     FromAPIConfig(config),
		KeyPattern:      "*",
		DataType:        RedisDataTypeString,
		BatchSize:       1000,
		DeleteAfterRead: false,
	}

	if v := config.GetString("key_pattern"); v != "" {
		sc.KeyPattern = v
	}
	if v := config.GetString("keys"); v != "" {
		sc.KeyPattern = v
	}
	if v := config.GetString("data_type"); v != "" {
		sc.DataType = RedisDataType(v)
	}
	if v := config.GetInt64("batch_size"); v > 0 {
		sc.BatchSize = v
	}
	sc.DeleteAfterRead = config.GetBool("delete_after_read")

	// Hash fields
	if fields, ok := config.Get("hash_fields"); ok && fields != nil {
		if fieldList, ok := fields.([]interface{}); ok {
			for _, f := range fieldList {
				if s, ok := f.(string); ok {
					sc.HashFields = append(sc.HashFields, s)
				}
			}
		}
	}

	return sc
}

// SinkConfig holds Redis sink-specific configuration
type SinkConfig struct {
	*RedisConfig

	// Key pattern/prefix
	KeyPattern string
	// Key field from row (if not using pattern)
	KeyField string
	// Value field from row (for simple types)
	ValueField string
	// Data type
	DataType RedisDataType
	// Hash key field (for hash type)
	HashKeyField string
	// Hash value field (for hash type)
	HashValueField string
	// Expire time in seconds (0 = no expiry)
	ExpireSeconds int64
	// Batch size for pipeline
	BatchSize int
	// Custom format for value serialization
	Format string // json, text
}

// DefaultSinkConfig returns default sink configuration
func DefaultSinkConfig() *SinkConfig {
	return &SinkConfig{
		RedisConfig:   DefaultRedisConfig(),
		DataType:      RedisDataTypeString,
		BatchSize:     1000,
		Format:        "json",
		ExpireSeconds: 0,
	}
}

// SinkConfigFromAPI creates SinkConfig from api.Config
func SinkConfigFromAPI(config *api.Config) *SinkConfig {
	sc := &SinkConfig{
		RedisConfig:   FromAPIConfig(config),
		DataType:      RedisDataTypeString,
		BatchSize:     1000,
		Format:        "json",
		ExpireSeconds: 0,
	}

	if v := config.GetString("key_pattern"); v != "" {
		sc.KeyPattern = v
	}
	if v := config.GetString("key"); v != "" {
		sc.KeyPattern = v
	}
	if v := config.GetString("key_field"); v != "" {
		sc.KeyField = v
	}
	if v := config.GetString("value_field"); v != "" {
		sc.ValueField = v
	}
	if v := config.GetString("data_type"); v != "" {
		sc.DataType = RedisDataType(v)
	}
	if v := config.GetString("hash_key_field"); v != "" {
		sc.HashKeyField = v
	}
	if v := config.GetString("hash_value_field"); v != "" {
		sc.HashValueField = v
	}
	if v := config.GetInt64("expire"); v > 0 {
		sc.ExpireSeconds = v
	}
	if v := config.GetInt("batch_size"); v > 0 {
		sc.BatchSize = v
	}
	if v := config.GetString("format"); v != "" {
		sc.Format = v
	}

	return sc
}

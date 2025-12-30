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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
)

// RedisClient is a wrapper around redis client that supports different modes
type RedisClient struct {
	config *RedisConfig
	client redis.UniversalClient
}

// NewRedisClient creates a new Redis client based on configuration
func NewRedisClient(config *RedisConfig) (*RedisClient, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	var client redis.UniversalClient

	// Build TLS config if enabled
	var tlsConfig *tls.Config
	if config.EnableTLS {
		var err error
		tlsConfig, err = buildTLSConfig(config)
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
	}

	switch config.Mode {
	case RedisModeCluster:
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:          config.Nodes,
			Password:       config.Password,
			MaxRetries:     config.MaxRetries,
			MinIdleConns:   config.MinIdleConns,
			ConnMaxLifetime: config.MaxConnAge,
			PoolTimeout:    config.PoolTimeout,
			DialTimeout:    config.ConnectTimeout,
			ReadTimeout:    config.ReadTimeout,
			WriteTimeout:   config.WriteTimeout,
			TLSConfig:      tlsConfig,
		})

	case RedisModeSentinel:
		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:       config.MasterName,
			SentinelAddrs:    config.SentinelNodes,
			Password:         config.Password,
			DB:               config.Database,
			MaxRetries:       config.MaxRetries,
			MinIdleConns:     config.MinIdleConns,
			ConnMaxLifetime:  config.MaxConnAge,
			PoolTimeout:      config.PoolTimeout,
			DialTimeout:      config.ConnectTimeout,
			ReadTimeout:      config.ReadTimeout,
			WriteTimeout:     config.WriteTimeout,
			TLSConfig:        tlsConfig,
		})

	default: // Standalone
		client = redis.NewClient(&redis.Options{
			Addr:            config.Address(),
			Password:        config.Password,
			DB:              config.Database,
			MaxRetries:      config.MaxRetries,
			MinIdleConns:    config.MinIdleConns,
			ConnMaxLifetime: config.MaxConnAge,
			PoolTimeout:     config.PoolTimeout,
			DialTimeout:     config.ConnectTimeout,
			ReadTimeout:     config.ReadTimeout,
			WriteTimeout:    config.WriteTimeout,
			TLSConfig:       tlsConfig,
		})
	}

	return &RedisClient{
		config: config,
		client: client,
	}, nil
}

// buildTLSConfig builds TLS configuration
func buildTLSConfig(config *RedisConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: config.SkipVerify,
	}

	// Load CA cert if specified
	if config.CAFile != "" {
		caCert, err := os.ReadFile(config.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsConfig.RootCAs = caCertPool
	}

	// Load client cert if specified
	if config.CertFile != "" && config.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// Ping tests the connection
func (c *RedisClient) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// Close closes the connection
func (c *RedisClient) Close() error {
	return c.client.Close()
}

// Get returns the value for a key
func (c *RedisClient) Get(ctx context.Context, key string) (string, error) {
	return c.client.Get(ctx, key).Result()
}

// Set sets a key-value pair with optional expiration
func (c *RedisClient) Set(ctx context.Context, key, value string, expireSeconds int64) error {
	if expireSeconds > 0 {
		return c.client.SetEx(ctx, key, value, 0).Err()
	}
	return c.client.Set(ctx, key, value, 0).Err()
}

// Del deletes keys
func (c *RedisClient) Del(ctx context.Context, keys ...string) error {
	return c.client.Del(ctx, keys...).Err()
}

// Scan scans keys matching pattern
func (c *RedisClient) Scan(ctx context.Context, cursor uint64, pattern string, count int64) ([]string, uint64, error) {
	return c.client.Scan(ctx, cursor, pattern, count).Result()
}

// HGet gets a hash field value
func (c *RedisClient) HGet(ctx context.Context, key, field string) (string, error) {
	return c.client.HGet(ctx, key, field).Result()
}

// HGetAll gets all hash fields
func (c *RedisClient) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return c.client.HGetAll(ctx, key).Result()
}

// HSet sets hash field values
func (c *RedisClient) HSet(ctx context.Context, key string, values ...interface{}) error {
	return c.client.HSet(ctx, key, values...).Err()
}

// LPush pushes values to a list
func (c *RedisClient) LPush(ctx context.Context, key string, values ...interface{}) error {
	return c.client.LPush(ctx, key, values...).Err()
}

// RPush pushes values to the end of a list
func (c *RedisClient) RPush(ctx context.Context, key string, values ...interface{}) error {
	return c.client.RPush(ctx, key, values...).Err()
}

// LRange gets a range of list elements
func (c *RedisClient) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return c.client.LRange(ctx, key, start, stop).Result()
}

// SAdd adds members to a set
func (c *RedisClient) SAdd(ctx context.Context, key string, members ...interface{}) error {
	return c.client.SAdd(ctx, key, members...).Err()
}

// SMembers gets all set members
func (c *RedisClient) SMembers(ctx context.Context, key string) ([]string, error) {
	return c.client.SMembers(ctx, key).Result()
}

// ZAdd adds members to a sorted set
func (c *RedisClient) ZAdd(ctx context.Context, key string, members ...redis.Z) error {
	return c.client.ZAdd(ctx, key, members...).Err()
}

// ZRange gets a range of sorted set members
func (c *RedisClient) ZRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return c.client.ZRange(ctx, key, start, stop).Result()
}

// ZRangeWithScores gets a range of sorted set members with scores
func (c *RedisClient) ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]redis.Z, error) {
	return c.client.ZRangeWithScores(ctx, key, start, stop).Result()
}

// Pipeline returns a pipeline for batch operations
func (c *RedisClient) Pipeline() redis.Pipeliner {
	return c.client.Pipeline()
}

// Type returns the type of a key
func (c *RedisClient) Type(ctx context.Context, key string) (string, error) {
	return c.client.Type(ctx, key).Result()
}

// Expire sets expiration on a key
func (c *RedisClient) Expire(ctx context.Context, key string, seconds int64) error {
	return c.client.Expire(ctx, key, 0).Err()
}

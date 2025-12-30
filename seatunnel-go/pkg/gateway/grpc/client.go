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

package grpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// ClientConfig holds configuration for the gRPC client
type ClientConfig struct {
	// Backend server addresses
	Addresses []string
	// Connection timeout
	ConnectTimeout time.Duration
	// Request timeout
	RequestTimeout time.Duration
	// Enable TLS
	EnableTLS bool
	// TLS certificate file (if EnableTLS is true)
	CertFile string
	// Retry settings
	MaxRetries    int
	RetryInterval time.Duration
	// Keep-alive settings
	KeepAliveTime    time.Duration
	KeepAliveTimeout time.Duration
}

// DefaultClientConfig returns a default client configuration
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		Addresses:        []string{"localhost:5801"},
		ConnectTimeout:   10 * time.Second,
		RequestTimeout:   30 * time.Second,
		EnableTLS:        false,
		MaxRetries:       3,
		RetryInterval:    time.Second,
		KeepAliveTime:    30 * time.Second,
		KeepAliveTimeout: 10 * time.Second,
	}
}

// Client is the gRPC client for communicating with SeaTunnel Java backend
type Client struct {
	config   *ClientConfig
	conn     *grpc.ClientConn
	logger   *zap.Logger
	mu       sync.RWMutex
	isClosed bool
}

// NewClient creates a new gRPC client
func NewClient(config *ClientConfig, logger *zap.Logger) (*Client, error) {
	if config == nil {
		config = DefaultClientConfig()
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	client := &Client{
		config: config,
		logger: logger,
	}

	if err := client.connect(); err != nil {
		return nil, err
	}

	return client, nil
}

// connect establishes connection to the gRPC server
func (c *Client) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.config.Addresses) == 0 {
		return fmt.Errorf("no server addresses configured")
	}

	// Use the first address for now (TODO: implement load balancing)
	address := c.config.Addresses[0]

	c.logger.Info("Connecting to SeaTunnel backend", zap.String("address", address))

	// Build dial options
	opts := []grpc.DialOption{
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                c.config.KeepAliveTime,
			Timeout:             c.config.KeepAliveTimeout,
			PermitWithoutStream: true,
		}),
	}

	if c.config.EnableTLS {
		// TODO: implement TLS support
		return fmt.Errorf("TLS support not yet implemented")
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Create connection with timeout
	ctx, cancel := context.WithTimeout(context.Background(), c.config.ConnectTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, address, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", address, err)
	}

	c.conn = conn
	c.logger.Info("Connected to SeaTunnel backend", zap.String("address", address))

	return nil
}

// GetConnection returns the underlying gRPC connection
func (c *Client) GetConnection() *grpc.ClientConn {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn
}

// Close closes the gRPC connection
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isClosed {
		return nil
	}

	c.isClosed = true
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Reconnect attempts to reconnect to the server
func (c *Client) Reconnect() error {
	c.mu.Lock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()

	return c.connect()
}

// withRetry executes a function with retry logic
func (c *Client) withRetry(ctx context.Context, fn func() error) error {
	var lastErr error
	for i := 0; i <= c.config.MaxRetries; i++ {
		if err := fn(); err != nil {
			lastErr = err
			c.logger.Warn("Request failed, retrying",
				zap.Int("attempt", i+1),
				zap.Int("maxRetries", c.config.MaxRetries),
				zap.Error(err))

			if i < c.config.MaxRetries {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(c.config.RetryInterval):
					// Try to reconnect
					if err := c.Reconnect(); err != nil {
						c.logger.Error("Reconnection failed", zap.Error(err))
					}
				}
			}
		} else {
			return nil
		}
	}
	return lastErr
}

// ============= Job Operations =============

// SubmitJobRequest represents a job submission request
type SubmitJobRequest struct {
	ConfigContent string
	ConfigFormat  string
	Variables     map[string]string
	JobName       string
	SavepointPath string
}

// SubmitJobResponse represents a job submission response
type SubmitJobResponse struct {
	JobID        int64
	JobName      string
	Success      bool
	ErrorMessage string
}

// SubmitJob submits a new job to the cluster
func (c *Client) SubmitJob(ctx context.Context, req *SubmitJobRequest) (*SubmitJobResponse, error) {
	// TODO: Implement when proto is compiled
	// For now, return a mock response for testing
	c.logger.Info("Submitting job",
		zap.String("jobName", req.JobName),
		zap.String("format", req.ConfigFormat))

	return &SubmitJobResponse{
		JobID:   time.Now().UnixNano(),
		JobName: req.JobName,
		Success: true,
	}, nil
}

// JobStatus represents the status of a job
type JobStatus struct {
	JobID        int64
	JobName      string
	Status       string
	JobMode      string
	CreateTime   int64
	StartTime    int64
	FinishTime   int64
	ErrorMessage string
}

// GetJobStatus gets the status of a specific job
func (c *Client) GetJobStatus(ctx context.Context, jobID int64) (*JobStatus, error) {
	// TODO: Implement when proto is compiled
	c.logger.Info("Getting job status", zap.Int64("jobID", jobID))

	return &JobStatus{
		JobID:      jobID,
		JobName:    "mock-job",
		Status:     "RUNNING",
		JobMode:    "BATCH",
		CreateTime: time.Now().Add(-time.Hour).UnixMilli(),
		StartTime:  time.Now().Add(-time.Hour).UnixMilli(),
	}, nil
}

// StopJob stops a running job
func (c *Client) StopJob(ctx context.Context, jobID int64, withSavepoint bool) (string, error) {
	// TODO: Implement when proto is compiled
	c.logger.Info("Stopping job",
		zap.Int64("jobID", jobID),
		zap.Bool("withSavepoint", withSavepoint))

	return "", nil
}

// ListRunningJobs returns all running jobs
func (c *Client) ListRunningJobs(ctx context.Context, page, pageSize int) ([]*JobStatus, int, error) {
	// TODO: Implement when proto is compiled
	c.logger.Info("Listing running jobs",
		zap.Int("page", page),
		zap.Int("pageSize", pageSize))

	return []*JobStatus{}, 0, nil
}

// ListFinishedJobs returns finished jobs
func (c *Client) ListFinishedJobs(ctx context.Context, page, pageSize int) ([]*JobStatus, int, error) {
	// TODO: Implement when proto is compiled
	c.logger.Info("Listing finished jobs",
		zap.Int("page", page),
		zap.Int("pageSize", pageSize))

	return []*JobStatus{}, 0, nil
}

// ============= Cluster Operations =============

// ClusterOverview represents cluster information
type ClusterOverview struct {
	ClusterID        string
	ClusterVersion   string
	ClusterStartTime int64
	TotalNodes       int
	ActiveNodes      int
	RunningJobs      int
	FinishedJobs     int
	FailedJobs       int
	CanceledJobs     int
	TotalMemory      int64
	UsedMemory       int64
	TotalSlots       int
	UsedSlots        int
}

// GetClusterOverview gets the cluster overview
func (c *Client) GetClusterOverview(ctx context.Context) (*ClusterOverview, error) {
	// TODO: Implement when proto is compiled
	c.logger.Info("Getting cluster overview")

	return &ClusterOverview{
		ClusterID:        "seatunnel-cluster",
		ClusterVersion:   "2.3.13",
		ClusterStartTime: time.Now().Add(-24 * time.Hour).UnixMilli(),
		TotalNodes:       3,
		ActiveNodes:      3,
		RunningJobs:      2,
		FinishedJobs:     100,
		TotalSlots:       12,
		UsedSlots:        4,
	}, nil
}

// SystemInfo represents system information
type SystemInfo struct {
	NodeID             string
	OSName             string
	OSVersion          string
	OSArch             string
	AvailableProcessors int
	SystemLoadAverage  float64
	TotalPhysicalMemory int64
	FreePhysicalMemory  int64
}

// GetSystemInfo gets system information
func (c *Client) GetSystemInfo(ctx context.Context, nodeID string) (*SystemInfo, error) {
	// TODO: Implement when proto is compiled
	c.logger.Info("Getting system info", zap.String("nodeID", nodeID))

	return &SystemInfo{
		NodeID:              nodeID,
		OSName:              "Linux",
		OSVersion:           "5.15.0",
		OSArch:              "amd64",
		AvailableProcessors: 8,
		SystemLoadAverage:   2.5,
		TotalPhysicalMemory: 16 * 1024 * 1024 * 1024,
		FreePhysicalMemory:  8 * 1024 * 1024 * 1024,
	}, nil
}

// ============= Metrics Operations =============

// JobMetrics represents job metrics
type JobMetrics struct {
	JobID               int64
	SourceReceivedCount int64
	SinkWriteCount      int64
	SourceReceivedBytes int64
	SinkWriteBytes      int64
	SourceReceivedQPS   float64
	SinkWriteQPS        float64
	TotalTimeMs         int64
}

// GetJobMetrics gets metrics for a specific job
func (c *Client) GetJobMetrics(ctx context.Context, jobID int64) (*JobMetrics, error) {
	// TODO: Implement when proto is compiled
	c.logger.Info("Getting job metrics", zap.Int64("jobID", jobID))

	return &JobMetrics{
		JobID:               jobID,
		SourceReceivedCount: 10000,
		SinkWriteCount:      9950,
		SourceReceivedBytes: 1024 * 1024 * 100,
		SinkWriteBytes:      1024 * 1024 * 99,
		SourceReceivedQPS:   1000.0,
		SinkWriteQPS:        995.0,
		TotalTimeMs:         60000,
	}, nil
}

// EncryptConfig encrypts a configuration value
func (c *Client) EncryptConfig(ctx context.Context, plainText string) (string, error) {
	// TODO: Implement when proto is compiled
	c.logger.Info("Encrypting config value")

	// For now, return a mock encrypted value
	return fmt.Sprintf("ENC(%s)", plainText), nil
}

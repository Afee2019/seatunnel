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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Java REST API 端点常量
const (
	ContextPath                = "/hazelcast/rest/maps"
	RestURLOverview            = "/overview"
	RestURLRunningJobs         = "/running-jobs"
	RestURLJobInfo             = "/job-info"
	RestURLFinishedJobs        = "/finished-jobs"
	RestURLSystemMonitoringInfo = "/system-monitoring-information"
	RestURLSubmitJob           = "/submit-job"
	RestURLStopJob             = "/stop-job"
	RestURLEncryptConfig       = "/encrypt-config"
	RestURLLogs                = "/logs"
	RestURLMetrics             = "/metrics"
)

// ClientConfig holds configuration for the REST client
type ClientConfig struct {
	// Backend server addresses (http://host:port)
	Addresses []string
	// Connection timeout
	ConnectTimeout time.Duration
	// Request timeout
	RequestTimeout time.Duration
	// Enable TLS
	EnableTLS bool
	// Skip TLS verification (for development)
	SkipTLSVerify bool
	// Retry settings
	MaxRetries    int
	RetryInterval time.Duration
}

// DefaultClientConfig returns a default client configuration
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		Addresses:      []string{"http://localhost:8216"},
		ConnectTimeout: 10 * time.Second,
		RequestTimeout: 30 * time.Second,
		EnableTLS:      false,
		MaxRetries:     3,
		RetryInterval:  time.Second,
	}
}

// Client is the HTTP REST client for communicating with SeaTunnel Java backend
type Client struct {
	config     *ClientConfig
	httpClient *http.Client
	baseURL    string
	logger     *zap.Logger
	mu         sync.RWMutex
	isClosed   bool
}

// NewClient creates a new REST client
func NewClient(config *ClientConfig, logger *zap.Logger) (*Client, error) {
	if config == nil {
		config = DefaultClientConfig()
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	// Create HTTP client with custom transport
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   config.ConnectTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	if config.EnableTLS && config.SkipTLSVerify {
		transport.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   config.RequestTimeout,
	}

	// Use first address for now (TODO: implement load balancing)
	baseURL := config.Addresses[0]
	logger.Info("正在连接 SeaTunnel 后端", zap.String("地址", baseURL))

	return &Client{
		config:     config,
		httpClient: httpClient,
		baseURL:    baseURL,
		logger:     logger,
	}, nil
}

// Close closes the client
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isClosed {
		return nil
	}

	c.isClosed = true
	c.httpClient.CloseIdleConnections()
	return nil
}

// doRequest performs an HTTP request with retry logic
func (c *Client) doRequest(ctx context.Context, method, path string, body interface{}) ([]byte, error) {
	url := c.baseURL + ContextPath + path

	var reqBody io.Reader
	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("序列化请求体失败: %w", err)
		}
		reqBody = bytes.NewReader(jsonData)
	}

	var lastErr error
	for i := 0; i <= c.config.MaxRetries; i++ {
		req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
		if err != nil {
			return nil, fmt.Errorf("创建请求失败: %w", err)
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			c.logger.Warn("请求失败，正在重试",
				zap.Int("尝试次数", i+1),
				zap.Int("最大重试次数", c.config.MaxRetries),
				zap.Error(err))

			if i < c.config.MaxRetries {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(c.config.RetryInterval):
					continue
				}
			}
			continue
		}

		defer resp.Body.Close()
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("读取响应失败: %w", err)
		}

		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("HTTP 错误 %d: %s", resp.StatusCode, string(respBody))
		}

		return respBody, nil
	}

	return nil, fmt.Errorf("请求失败，已重试 %d 次: %w", c.config.MaxRetries, lastErr)
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

// javaSubmitJobRequest is the Java REST API request format
type javaSubmitJobRequest struct {
	Env    map[string]interface{} `json:"env,omitempty"`
	Source []map[string]interface{} `json:"source,omitempty"`
	Sink   []map[string]interface{} `json:"sink,omitempty"`
	Transform []map[string]interface{} `json:"transform,omitempty"`
	JobName string `json:"jobName,omitempty"`
	IsStartWithSavePoint bool `json:"isStartWithSavePoint,omitempty"`
}

// SubmitJob submits a new job to the cluster
func (c *Client) SubmitJob(ctx context.Context, req *SubmitJobRequest) (*SubmitJobResponse, error) {
	c.logger.Info("正在提交作业",
		zap.String("作业名称", req.JobName),
		zap.String("格式", req.ConfigFormat))

	// Build request body - send config content directly
	// Java API accepts the raw config content
	body := map[string]interface{}{
		"env": map[string]interface{}{
			"job.name": req.JobName,
		},
	}

	// If config content is provided, we need to parse it or send it differently
	// For now, we'll send a simple format
	if req.ConfigFormat == "json" {
		// Try to parse JSON config
		var configObj map[string]interface{}
		if err := json.Unmarshal([]byte(req.ConfigContent), &configObj); err == nil {
			body = configObj
		}
	}

	respBody, err := c.doRequest(ctx, http.MethodPost, RestURLSubmitJob, body)
	if err != nil {
		return nil, fmt.Errorf("提交作业失败: %w", err)
	}

	// Parse response
	var resp struct {
		JobID   int64  `json:"jobId"`
		JobName string `json:"jobName"`
	}
	if err := json.Unmarshal(respBody, &resp); err != nil {
		// Response might just be the job ID as string
		return &SubmitJobResponse{
			Success: true,
			JobName: req.JobName,
		}, nil
	}

	return &SubmitJobResponse{
		JobID:   resp.JobID,
		JobName: resp.JobName,
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

// javaJobStatus is the Java REST API response format
type javaJobStatus struct {
	JobID      int64  `json:"jobId"`
	JobName    string `json:"jobName"`
	JobStatus  string `json:"jobStatus"`
	CreateTime int64  `json:"createTime"`
	FinishTime int64  `json:"finishTime"`
	ErrorMsg   string `json:"errorMsg"`
}

// GetJobStatus gets the status of a specific job
func (c *Client) GetJobStatus(ctx context.Context, jobID int64) (*JobStatus, error) {
	c.logger.Info("正在获取作业状态", zap.Int64("作业ID", jobID))

	path := fmt.Sprintf("%s/%d", RestURLJobInfo, jobID)
	respBody, err := c.doRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("获取作业状态失败: %w", err)
	}

	var resp javaJobStatus
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, fmt.Errorf("解析响应失败: %w", err)
	}

	return &JobStatus{
		JobID:        resp.JobID,
		JobName:      resp.JobName,
		Status:       resp.JobStatus,
		CreateTime:   resp.CreateTime,
		FinishTime:   resp.FinishTime,
		ErrorMessage: resp.ErrorMsg,
	}, nil
}

// StopJob stops a running job
func (c *Client) StopJob(ctx context.Context, jobID int64, withSavepoint bool) (string, error) {
	c.logger.Info("正在停止作业",
		zap.Int64("作业ID", jobID),
		zap.Bool("创建保存点", withSavepoint))

	body := map[string]interface{}{
		"jobId":              jobID,
		"isStopWithSavePoint": withSavepoint,
	}

	_, err := c.doRequest(ctx, http.MethodPost, RestURLStopJob, body)
	if err != nil {
		return "", fmt.Errorf("停止作业失败: %w", err)
	}

	return "", nil
}

// ListRunningJobs returns all running jobs
func (c *Client) ListRunningJobs(ctx context.Context, page, pageSize int) ([]*JobStatus, int, error) {
	c.logger.Info("正在列出运行中的作业",
		zap.Int("页码", page),
		zap.Int("每页数量", pageSize))

	respBody, err := c.doRequest(ctx, http.MethodGet, RestURLRunningJobs, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("列出运行中作业失败: %w", err)
	}

	var jobs []javaJobStatus
	if err := json.Unmarshal(respBody, &jobs); err != nil {
		return nil, 0, fmt.Errorf("解析响应失败: %w", err)
	}

	result := make([]*JobStatus, len(jobs))
	for i, j := range jobs {
		result[i] = &JobStatus{
			JobID:        j.JobID,
			JobName:      j.JobName,
			Status:       j.JobStatus,
			CreateTime:   j.CreateTime,
			FinishTime:   j.FinishTime,
			ErrorMessage: j.ErrorMsg,
		}
	}

	return result, len(result), nil
}

// ListFinishedJobs returns finished jobs
func (c *Client) ListFinishedJobs(ctx context.Context, page, pageSize int) ([]*JobStatus, int, error) {
	c.logger.Info("正在列出已完成的作业",
		zap.Int("页码", page),
		zap.Int("每页数量", pageSize))

	respBody, err := c.doRequest(ctx, http.MethodGet, RestURLFinishedJobs, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("列出已完成作业失败: %w", err)
	}

	var jobs []javaJobStatus
	if err := json.Unmarshal(respBody, &jobs); err != nil {
		return nil, 0, fmt.Errorf("解析响应失败: %w", err)
	}

	result := make([]*JobStatus, len(jobs))
	for i, j := range jobs {
		result[i] = &JobStatus{
			JobID:        j.JobID,
			JobName:      j.JobName,
			Status:       j.JobStatus,
			CreateTime:   j.CreateTime,
			FinishTime:   j.FinishTime,
			ErrorMessage: j.ErrorMsg,
		}
	}

	return result, len(result), nil
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

// javaClusterOverview is the Java REST API response format
// Note: Java API returns numbers as strings
type javaClusterOverview struct {
	ProjectVersion  string `json:"projectVersion"`
	GitCommitAbbrev string `json:"gitCommitAbbrev"`
	TotalSlot       string `json:"totalSlot"`
	UnassignedSlot  string `json:"unassignedSlot"`
	Workers         string `json:"workers"`
	RunningJobs     string `json:"runningJobs"`
	FinishedJobs    string `json:"finishedJobs"`
	FailedJobs      string `json:"failedJobs"`
	CancelledJobs   string `json:"cancelledJobs"`
	PendingJobs     string `json:"pendingJobs"`
}

// atoi safely converts string to int, returns 0 on error
func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// GetClusterOverview gets the cluster overview
func (c *Client) GetClusterOverview(ctx context.Context) (*ClusterOverview, error) {
	c.logger.Info("正在获取集群概览")

	respBody, err := c.doRequest(ctx, http.MethodGet, RestURLOverview, nil)
	if err != nil {
		return nil, fmt.Errorf("获取集群概览失败: %w", err)
	}

	var resp javaClusterOverview
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, fmt.Errorf("解析响应失败: %w", err)
	}

	totalSlot := atoi(resp.TotalSlot)
	unassignedSlot := atoi(resp.UnassignedSlot)
	workers := atoi(resp.Workers)

	return &ClusterOverview{
		ClusterVersion: resp.ProjectVersion,
		TotalNodes:     workers,
		ActiveNodes:    workers,
		RunningJobs:    atoi(resp.RunningJobs),
		FinishedJobs:   atoi(resp.FinishedJobs),
		FailedJobs:     atoi(resp.FailedJobs),
		CanceledJobs:   atoi(resp.CancelledJobs),
		TotalSlots:     totalSlot,
		UsedSlots:      totalSlot - unassignedSlot,
	}, nil
}

// SystemInfo represents system information
type SystemInfo struct {
	NodeID              string
	OSName              string
	OSVersion           string
	OSArch              string
	AvailableProcessors int
	SystemLoadAverage   float64
	TotalPhysicalMemory int64
	FreePhysicalMemory  int64
}

// javaSystemInfo is the Java REST API response format
type javaSystemInfo struct {
	Processors    int     `json:"processors"`
	PhysicalMemory int64  `json:"physical.memory"`
	FreeMemory    int64   `json:"free.memory"`
	OSName        string  `json:"osName"`
	OSArch        string  `json:"osArch"`
	OSVersion     string  `json:"osVersion"`
}

// GetSystemInfo gets system information
func (c *Client) GetSystemInfo(ctx context.Context, nodeID string) (*SystemInfo, error) {
	c.logger.Info("正在获取系统信息", zap.String("节点ID", nodeID))

	respBody, err := c.doRequest(ctx, http.MethodGet, RestURLSystemMonitoringInfo, nil)
	if err != nil {
		return nil, fmt.Errorf("获取系统信息失败: %w", err)
	}

	// Response is a list of system info for all nodes
	var nodes []javaSystemInfo
	if err := json.Unmarshal(respBody, &nodes); err != nil {
		return nil, fmt.Errorf("解析响应失败: %w", err)
	}

	if len(nodes) == 0 {
		return nil, fmt.Errorf("没有节点信息")
	}

	// Return first node info
	node := nodes[0]
	return &SystemInfo{
		NodeID:              nodeID,
		OSName:              node.OSName,
		OSVersion:           node.OSVersion,
		OSArch:              node.OSArch,
		AvailableProcessors: node.Processors,
		TotalPhysicalMemory: node.PhysicalMemory,
		FreePhysicalMemory:  node.FreeMemory,
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
	c.logger.Info("正在获取作业指标", zap.Int64("作业ID", jobID))

	// First get job info which includes metrics
	path := fmt.Sprintf("%s/%d", RestURLJobInfo, jobID)
	respBody, err := c.doRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("获取作业指标失败: %w", err)
	}

	var resp struct {
		JobID   int64 `json:"jobId"`
		Metrics struct {
			SourceReceivedCount int64   `json:"TableSourceReceivedCount"`
			SinkWriteCount      int64   `json:"TableSinkWriteCount"`
			SourceReceivedQPS   float64 `json:"TableSourceReceivedQPS"`
			SinkWriteQPS        float64 `json:"TableSinkWriteQPS"`
			SourceReceivedBytes int64   `json:"TableSourceReceivedBytes"`
			SinkWriteBytes      int64   `json:"TableSinkWriteBytes"`
		} `json:"metrics"`
	}
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, fmt.Errorf("解析响应失败: %w", err)
	}

	return &JobMetrics{
		JobID:               jobID,
		SourceReceivedCount: resp.Metrics.SourceReceivedCount,
		SinkWriteCount:      resp.Metrics.SinkWriteCount,
		SourceReceivedBytes: resp.Metrics.SourceReceivedBytes,
		SinkWriteBytes:      resp.Metrics.SinkWriteBytes,
		SourceReceivedQPS:   resp.Metrics.SourceReceivedQPS,
		SinkWriteQPS:        resp.Metrics.SinkWriteQPS,
	}, nil
}

// EncryptConfig encrypts a configuration value
func (c *Client) EncryptConfig(ctx context.Context, plainText string) (string, error) {
	c.logger.Info("正在加密配置值")

	body := map[string]string{
		"data": plainText,
	}

	respBody, err := c.doRequest(ctx, http.MethodPost, RestURLEncryptConfig, body)
	if err != nil {
		return "", fmt.Errorf("加密配置失败: %w", err)
	}

	// Response should be the encrypted value
	return string(respBody), nil
}

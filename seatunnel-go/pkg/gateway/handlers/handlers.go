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

package handlers

import (
	"net/http"
	"strconv"
	"time"

	"github.com/apache/seatunnel-go/pkg/gateway/grpc"
	"github.com/apache/seatunnel-go/pkg/gateway/metrics"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// Handlers holds all HTTP handlers
type Handlers struct {
	client  *grpc.Client
	metrics *metrics.GatewayMetrics
	logger  *zap.Logger
}

// NewHandlers creates a new Handlers instance
func NewHandlers(client *grpc.Client, metrics *metrics.GatewayMetrics, logger *zap.Logger) *Handlers {
	return &Handlers{
		client:  client,
		metrics: metrics,
		logger:  logger,
	}
}

// ============= Response Types =============

// APIResponse is a standard API response
type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// OverviewResponse represents the cluster overview response
type OverviewResponse struct {
	ClusterID        string `json:"clusterId"`
	ClusterVersion   string `json:"clusterVersion"`
	ClusterStartTime int64  `json:"clusterStartTime"`
	TotalNodes       int    `json:"totalNodes"`
	ActiveNodes      int    `json:"activeNodes"`
	RunningJobs      int    `json:"runningJobs"`
	FinishedJobs     int    `json:"finishedJobs"`
	FailedJobs       int    `json:"failedJobs"`
	CanceledJobs     int    `json:"canceledJobs"`
	TotalSlots       int    `json:"totalSlots"`
	UsedSlots        int    `json:"usedSlots"`
	TotalMemory      int64  `json:"totalMemory"`
	UsedMemory       int64  `json:"usedMemory"`
}

// JobResponse represents a job status response
type JobResponse struct {
	JobID        int64  `json:"jobId"`
	JobName      string `json:"jobName"`
	Status       string `json:"status"`
	JobMode      string `json:"jobMode"`
	CreateTime   int64  `json:"createTime"`
	StartTime    int64  `json:"startTime"`
	FinishTime   int64  `json:"finishTime,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

// JobListResponse represents a list of jobs
type JobListResponse struct {
	Jobs       []JobResponse `json:"jobs"`
	TotalCount int           `json:"totalCount"`
	Page       int           `json:"page"`
	PageSize   int           `json:"pageSize"`
}

// SubmitJobRequest represents a job submission request
type SubmitJobRequest struct {
	Config        string            `json:"config" binding:"required"`
	ConfigFormat  string            `json:"configFormat"`
	Variables     map[string]string `json:"variables"`
	JobName       string            `json:"jobName"`
	SavepointPath string            `json:"savepointPath"`
}

// SubmitJobResponse represents a job submission response
type SubmitJobResponse struct {
	JobID   int64  `json:"jobId"`
	JobName string `json:"jobName"`
}

// StopJobRequest represents a job stop request
type StopJobRequest struct {
	JobID         int64 `json:"jobId" binding:"required"`
	WithSavepoint bool  `json:"withSavepoint"`
}

// JobMetricsResponse represents job metrics
type JobMetricsResponse struct {
	JobID               int64   `json:"jobId"`
	SourceReceivedCount int64   `json:"sourceReceivedCount"`
	SinkWriteCount      int64   `json:"sinkWriteCount"`
	SourceReceivedBytes int64   `json:"sourceReceivedBytes"`
	SinkWriteBytes      int64   `json:"sinkWriteBytes"`
	SourceReceivedQPS   float64 `json:"sourceReceivedQps"`
	SinkWriteQPS        float64 `json:"sinkWriteQps"`
	TotalTimeMs         int64   `json:"totalTimeMs"`
}

// SystemInfoResponse represents system information
type SystemInfoResponse struct {
	NodeID              string  `json:"nodeId"`
	OSName              string  `json:"osName"`
	OSVersion           string  `json:"osVersion"`
	OSArch              string  `json:"osArch"`
	AvailableProcessors int     `json:"availableProcessors"`
	SystemLoadAverage   float64 `json:"systemLoadAverage"`
	TotalPhysicalMemory int64   `json:"totalPhysicalMemory"`
	FreePhysicalMemory  int64   `json:"freePhysicalMemory"`
}

// ============= Handlers =============

// GetOverview handles GET /api/overview
func (h *Handlers) GetOverview(c *gin.Context) {
	start := time.Now()

	overview, err := h.client.GetClusterOverview(c.Request.Context())
	if err != nil {
		h.logger.Error("Failed to get cluster overview", zap.Error(err))
		c.JSON(http.StatusInternalServerError, APIResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	// Update Prometheus metrics
	h.metrics.UpdateClusterMetrics(
		overview.TotalNodes, overview.ActiveNodes,
		overview.TotalSlots, overview.UsedSlots,
		overview.TotalMemory, overview.UsedMemory,
		overview.RunningJobs,
	)

	response := OverviewResponse{
		ClusterID:        overview.ClusterID,
		ClusterVersion:   overview.ClusterVersion,
		ClusterStartTime: overview.ClusterStartTime,
		TotalNodes:       overview.TotalNodes,
		ActiveNodes:      overview.ActiveNodes,
		RunningJobs:      overview.RunningJobs,
		FinishedJobs:     overview.FinishedJobs,
		FailedJobs:       overview.FailedJobs,
		CanceledJobs:     overview.CanceledJobs,
		TotalSlots:       overview.TotalSlots,
		UsedSlots:        overview.UsedSlots,
		TotalMemory:      overview.TotalMemory,
		UsedMemory:       overview.UsedMemory,
	}

	h.metrics.RecordHTTPRequest(c.Request.Method, "/api/overview", "200", time.Since(start).Seconds())

	c.JSON(http.StatusOK, APIResponse{
		Success: true,
		Data:    response,
	})
}

// GetRunningJobs handles GET /api/running-jobs
func (h *Handlers) GetRunningJobs(c *gin.Context) {
	start := time.Now()

	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("pageSize", "20"))

	jobs, total, err := h.client.ListRunningJobs(c.Request.Context(), page, pageSize)
	if err != nil {
		h.logger.Error("Failed to list running jobs", zap.Error(err))
		c.JSON(http.StatusInternalServerError, APIResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	jobResponses := make([]JobResponse, len(jobs))
	for i, job := range jobs {
		jobResponses[i] = JobResponse{
			JobID:        job.JobID,
			JobName:      job.JobName,
			Status:       job.Status,
			JobMode:      job.JobMode,
			CreateTime:   job.CreateTime,
			StartTime:    job.StartTime,
			FinishTime:   job.FinishTime,
			ErrorMessage: job.ErrorMessage,
		}
	}

	h.metrics.RecordHTTPRequest(c.Request.Method, "/api/running-jobs", "200", time.Since(start).Seconds())

	c.JSON(http.StatusOK, APIResponse{
		Success: true,
		Data: JobListResponse{
			Jobs:       jobResponses,
			TotalCount: total,
			Page:       page,
			PageSize:   pageSize,
		},
	})
}

// GetFinishedJobs handles GET /api/finished-jobs
func (h *Handlers) GetFinishedJobs(c *gin.Context) {
	start := time.Now()

	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("pageSize", "20"))

	jobs, total, err := h.client.ListFinishedJobs(c.Request.Context(), page, pageSize)
	if err != nil {
		h.logger.Error("Failed to list finished jobs", zap.Error(err))
		c.JSON(http.StatusInternalServerError, APIResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	jobResponses := make([]JobResponse, len(jobs))
	for i, job := range jobs {
		jobResponses[i] = JobResponse{
			JobID:        job.JobID,
			JobName:      job.JobName,
			Status:       job.Status,
			JobMode:      job.JobMode,
			CreateTime:   job.CreateTime,
			StartTime:    job.StartTime,
			FinishTime:   job.FinishTime,
			ErrorMessage: job.ErrorMessage,
		}
	}

	h.metrics.RecordHTTPRequest(c.Request.Method, "/api/finished-jobs", "200", time.Since(start).Seconds())

	c.JSON(http.StatusOK, APIResponse{
		Success: true,
		Data: JobListResponse{
			Jobs:       jobResponses,
			TotalCount: total,
			Page:       page,
			PageSize:   pageSize,
		},
	})
}

// GetJobInfo handles GET /api/job-info/:jobId
func (h *Handlers) GetJobInfo(c *gin.Context) {
	start := time.Now()

	jobIDStr := c.Param("jobId")
	jobID, err := strconv.ParseInt(jobIDStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, APIResponse{
			Success: false,
			Error:   "Invalid job ID",
		})
		return
	}

	job, err := h.client.GetJobStatus(c.Request.Context(), jobID)
	if err != nil {
		h.logger.Error("Failed to get job info", zap.Error(err), zap.Int64("jobId", jobID))
		c.JSON(http.StatusInternalServerError, APIResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	h.metrics.RecordHTTPRequest(c.Request.Method, "/api/job-info", "200", time.Since(start).Seconds())

	c.JSON(http.StatusOK, APIResponse{
		Success: true,
		Data: JobResponse{
			JobID:        job.JobID,
			JobName:      job.JobName,
			Status:       job.Status,
			JobMode:      job.JobMode,
			CreateTime:   job.CreateTime,
			StartTime:    job.StartTime,
			FinishTime:   job.FinishTime,
			ErrorMessage: job.ErrorMessage,
		},
	})
}

// SubmitJob handles POST /api/submit-job
func (h *Handlers) SubmitJob(c *gin.Context) {
	start := time.Now()

	var req SubmitJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, APIResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	if req.ConfigFormat == "" {
		req.ConfigFormat = "hocon"
	}

	resp, err := h.client.SubmitJob(c.Request.Context(), &grpc.SubmitJobRequest{
		ConfigContent: req.Config,
		ConfigFormat:  req.ConfigFormat,
		Variables:     req.Variables,
		JobName:       req.JobName,
		SavepointPath: req.SavepointPath,
	})

	duration := time.Since(start).Seconds()

	if err != nil || !resp.Success {
		h.logger.Error("Failed to submit job", zap.Error(err))
		h.metrics.RecordJobSubmit(false, duration)
		errMsg := ""
		if err != nil {
			errMsg = err.Error()
		} else {
			errMsg = resp.ErrorMessage
		}
		c.JSON(http.StatusInternalServerError, APIResponse{
			Success: false,
			Error:   errMsg,
		})
		return
	}

	h.metrics.RecordJobSubmit(true, duration)
	h.metrics.RecordHTTPRequest(c.Request.Method, "/api/submit-job", "200", duration)

	c.JSON(http.StatusOK, APIResponse{
		Success: true,
		Data: SubmitJobResponse{
			JobID:   resp.JobID,
			JobName: resp.JobName,
		},
	})
}

// StopJob handles POST /api/stop-job
func (h *Handlers) StopJob(c *gin.Context) {
	start := time.Now()

	var req StopJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, APIResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	savepointPath, err := h.client.StopJob(c.Request.Context(), req.JobID, req.WithSavepoint)
	if err != nil {
		h.logger.Error("Failed to stop job", zap.Error(err), zap.Int64("jobId", req.JobID))
		c.JSON(http.StatusInternalServerError, APIResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	h.metrics.RecordHTTPRequest(c.Request.Method, "/api/stop-job", "200", time.Since(start).Seconds())

	response := map[string]interface{}{
		"jobId": req.JobID,
	}
	if savepointPath != "" {
		response["savepointPath"] = savepointPath
	}

	c.JSON(http.StatusOK, APIResponse{
		Success: true,
		Data:    response,
	})
}

// GetJobMetrics handles GET /api/job-metrics/:jobId
func (h *Handlers) GetJobMetrics(c *gin.Context) {
	start := time.Now()

	jobIDStr := c.Param("jobId")
	jobID, err := strconv.ParseInt(jobIDStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, APIResponse{
			Success: false,
			Error:   "Invalid job ID",
		})
		return
	}

	metrics, err := h.client.GetJobMetrics(c.Request.Context(), jobID)
	if err != nil {
		h.logger.Error("Failed to get job metrics", zap.Error(err), zap.Int64("jobId", jobID))
		c.JSON(http.StatusInternalServerError, APIResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	h.metrics.RecordHTTPRequest(c.Request.Method, "/api/job-metrics", "200", time.Since(start).Seconds())

	c.JSON(http.StatusOK, APIResponse{
		Success: true,
		Data: JobMetricsResponse{
			JobID:               metrics.JobID,
			SourceReceivedCount: metrics.SourceReceivedCount,
			SinkWriteCount:      metrics.SinkWriteCount,
			SourceReceivedBytes: metrics.SourceReceivedBytes,
			SinkWriteBytes:      metrics.SinkWriteBytes,
			SourceReceivedQPS:   metrics.SourceReceivedQPS,
			SinkWriteQPS:        metrics.SinkWriteQPS,
			TotalTimeMs:         metrics.TotalTimeMs,
		},
	})
}

// GetSystemInfo handles GET /api/system-monitoring-information
func (h *Handlers) GetSystemInfo(c *gin.Context) {
	start := time.Now()

	nodeID := c.Query("nodeId")

	info, err := h.client.GetSystemInfo(c.Request.Context(), nodeID)
	if err != nil {
		h.logger.Error("Failed to get system info", zap.Error(err))
		c.JSON(http.StatusInternalServerError, APIResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	h.metrics.RecordHTTPRequest(c.Request.Method, "/api/system-monitoring-information", "200", time.Since(start).Seconds())

	c.JSON(http.StatusOK, APIResponse{
		Success: true,
		Data: SystemInfoResponse{
			NodeID:              info.NodeID,
			OSName:              info.OSName,
			OSVersion:           info.OSVersion,
			OSArch:              info.OSArch,
			AvailableProcessors: info.AvailableProcessors,
			SystemLoadAverage:   info.SystemLoadAverage,
			TotalPhysicalMemory: info.TotalPhysicalMemory,
			FreePhysicalMemory:  info.FreePhysicalMemory,
		},
	})
}

// EncryptConfig handles POST /api/encrypt-config
func (h *Handlers) EncryptConfig(c *gin.Context) {
	start := time.Now()

	var req struct {
		PlainText string `json:"plainText" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, APIResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	encrypted, err := h.client.EncryptConfig(c.Request.Context(), req.PlainText)
	if err != nil {
		h.logger.Error("Failed to encrypt config", zap.Error(err))
		c.JSON(http.StatusInternalServerError, APIResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	h.metrics.RecordHTTPRequest(c.Request.Method, "/api/encrypt-config", "200", time.Since(start).Seconds())

	c.JSON(http.StatusOK, APIResponse{
		Success: true,
		Data: map[string]string{
			"encryptedText": encrypted,
		},
	})
}

// HealthCheck handles GET /health
func (h *Handlers) HealthCheck(c *gin.Context) {
	// Simple health check - just return OK
	// In a real implementation, we would check gRPC connection status
	c.JSON(http.StatusOK, map[string]interface{}{
		"status": "UP",
		"time":   time.Now().Unix(),
	})
}

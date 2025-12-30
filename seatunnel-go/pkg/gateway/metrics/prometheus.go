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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	namespace = "seatunnel"
	subsystem = "gateway"
)

// GatewayMetrics holds all Prometheus metrics for the gateway
type GatewayMetrics struct {
	// HTTP metrics
	HTTPRequestsTotal   *prometheus.CounterVec
	HTTPRequestDuration *prometheus.HistogramVec
	HTTPRequestsInFlight prometheus.Gauge

	// gRPC client metrics
	GRPCRequestsTotal   *prometheus.CounterVec
	GRPCRequestDuration *prometheus.HistogramVec
	GRPCConnectionState prometheus.Gauge

	// Job metrics (collected from backend)
	JobsRunning      prometheus.Gauge
	JobsFinished     prometheus.Counter
	JobsFailed       prometheus.Counter
	JobsCanceled     prometheus.Counter
	JobSubmitTotal   *prometheus.CounterVec
	JobSubmitLatency prometheus.Histogram

	// Cluster metrics
	ClusterNodesTotal  prometheus.Gauge
	ClusterNodesActive prometheus.Gauge
	ClusterSlotsTotal  prometheus.Gauge
	ClusterSlotsUsed   prometheus.Gauge
	ClusterMemoryTotal prometheus.Gauge
	ClusterMemoryUsed  prometheus.Gauge

	// Per-job metrics (collected from backend)
	SourceReceivedRows  *prometheus.GaugeVec
	SourceReceivedBytes *prometheus.GaugeVec
	SourceReceivedQPS   *prometheus.GaugeVec
	SinkWriteRows       *prometheus.GaugeVec
	SinkWriteBytes      *prometheus.GaugeVec
	SinkWriteQPS        *prometheus.GaugeVec
}

// NewGatewayMetrics creates and registers all gateway metrics
func NewGatewayMetrics(registry prometheus.Registerer) *GatewayMetrics {
	if registry == nil {
		registry = prometheus.DefaultRegisterer
	}

	factory := promauto.With(registry)

	m := &GatewayMetrics{
		// HTTP metrics
		HTTPRequestsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "http_requests_total",
				Help:      "Total number of HTTP requests",
			},
			[]string{"method", "path", "status"},
		),
		HTTPRequestDuration: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "http_request_duration_seconds",
				Help:      "HTTP request duration in seconds",
				Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			},
			[]string{"method", "path"},
		),
		HTTPRequestsInFlight: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "http_requests_in_flight",
				Help:      "Number of HTTP requests currently being processed",
			},
		),

		// gRPC metrics
		GRPCRequestsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "grpc_requests_total",
				Help:      "Total number of gRPC requests to backend",
			},
			[]string{"method", "status"},
		),
		GRPCRequestDuration: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "grpc_request_duration_seconds",
				Help:      "gRPC request duration in seconds",
				Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			},
			[]string{"method"},
		),
		GRPCConnectionState: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      "grpc_connection_state",
				Help:      "gRPC connection state (0=disconnected, 1=connected)",
			},
		),

		// Job metrics
		JobsRunning: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "jobs_running",
				Help:      "Number of currently running jobs",
			},
		),
		JobsFinished: factory.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "jobs_finished_total",
				Help:      "Total number of finished jobs",
			},
		),
		JobsFailed: factory.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "jobs_failed_total",
				Help:      "Total number of failed jobs",
			},
		),
		JobsCanceled: factory.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "jobs_canceled_total",
				Help:      "Total number of canceled jobs",
			},
		),
		JobSubmitTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "job_submit_total",
				Help:      "Total number of job submissions",
			},
			[]string{"result"},
		),
		JobSubmitLatency: factory.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "job_submit_latency_seconds",
				Help:      "Job submission latency in seconds",
				Buckets:   []float64{.1, .25, .5, 1, 2.5, 5, 10, 30, 60},
			},
		),

		// Cluster metrics
		ClusterNodesTotal: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "cluster_nodes_total",
				Help:      "Total number of nodes in the cluster",
			},
		),
		ClusterNodesActive: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "cluster_nodes_active",
				Help:      "Number of active nodes in the cluster",
			},
		),
		ClusterSlotsTotal: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "cluster_slots_total",
				Help:      "Total number of task slots in the cluster",
			},
		),
		ClusterSlotsUsed: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "cluster_slots_used",
				Help:      "Number of used task slots in the cluster",
			},
		),
		ClusterMemoryTotal: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "cluster_memory_bytes_total",
				Help:      "Total memory in the cluster in bytes",
			},
		),
		ClusterMemoryUsed: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "cluster_memory_bytes_used",
				Help:      "Used memory in the cluster in bytes",
			},
		),

		// Per-job metrics
		SourceReceivedRows: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "job_source_received_rows",
				Help:      "Total rows received from source",
			},
			[]string{"job_id", "job_name"},
		),
		SourceReceivedBytes: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "job_source_received_bytes",
				Help:      "Total bytes received from source",
			},
			[]string{"job_id", "job_name"},
		),
		SourceReceivedQPS: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "job_source_received_qps",
				Help:      "Source read QPS",
			},
			[]string{"job_id", "job_name"},
		),
		SinkWriteRows: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "job_sink_write_rows",
				Help:      "Total rows written to sink",
			},
			[]string{"job_id", "job_name"},
		),
		SinkWriteBytes: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "job_sink_write_bytes",
				Help:      "Total bytes written to sink",
			},
			[]string{"job_id", "job_name"},
		),
		SinkWriteQPS: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "job_sink_write_qps",
				Help:      "Sink write QPS",
			},
			[]string{"job_id", "job_name"},
		),
	}

	return m
}

// UpdateClusterMetrics updates cluster-wide metrics
func (m *GatewayMetrics) UpdateClusterMetrics(
	nodesTotal, nodesActive int,
	slotsTotal, slotsUsed int,
	memoryTotal, memoryUsed int64,
	runningJobs int,
) {
	m.ClusterNodesTotal.Set(float64(nodesTotal))
	m.ClusterNodesActive.Set(float64(nodesActive))
	m.ClusterSlotsTotal.Set(float64(slotsTotal))
	m.ClusterSlotsUsed.Set(float64(slotsUsed))
	m.ClusterMemoryTotal.Set(float64(memoryTotal))
	m.ClusterMemoryUsed.Set(float64(memoryUsed))
	m.JobsRunning.Set(float64(runningJobs))
}

// UpdateJobMetrics updates per-job metrics
func (m *GatewayMetrics) UpdateJobMetrics(
	jobID, jobName string,
	sourceRows, sourceBytes int64,
	sourceQPS float64,
	sinkRows, sinkBytes int64,
	sinkQPS float64,
) {
	m.SourceReceivedRows.WithLabelValues(jobID, jobName).Set(float64(sourceRows))
	m.SourceReceivedBytes.WithLabelValues(jobID, jobName).Set(float64(sourceBytes))
	m.SourceReceivedQPS.WithLabelValues(jobID, jobName).Set(sourceQPS)
	m.SinkWriteRows.WithLabelValues(jobID, jobName).Set(float64(sinkRows))
	m.SinkWriteBytes.WithLabelValues(jobID, jobName).Set(float64(sinkBytes))
	m.SinkWriteQPS.WithLabelValues(jobID, jobName).Set(sinkQPS)
}

// RemoveJobMetrics removes metrics for a completed job
func (m *GatewayMetrics) RemoveJobMetrics(jobID, jobName string) {
	m.SourceReceivedRows.DeleteLabelValues(jobID, jobName)
	m.SourceReceivedBytes.DeleteLabelValues(jobID, jobName)
	m.SourceReceivedQPS.DeleteLabelValues(jobID, jobName)
	m.SinkWriteRows.DeleteLabelValues(jobID, jobName)
	m.SinkWriteBytes.DeleteLabelValues(jobID, jobName)
	m.SinkWriteQPS.DeleteLabelValues(jobID, jobName)
}

// RecordHTTPRequest records an HTTP request
func (m *GatewayMetrics) RecordHTTPRequest(method, path, status string, duration float64) {
	m.HTTPRequestsTotal.WithLabelValues(method, path, status).Inc()
	m.HTTPRequestDuration.WithLabelValues(method, path).Observe(duration)
}

// RecordGRPCRequest records a gRPC request
func (m *GatewayMetrics) RecordGRPCRequest(method, status string, duration float64) {
	m.GRPCRequestsTotal.WithLabelValues(method, status).Inc()
	m.GRPCRequestDuration.WithLabelValues(method).Observe(duration)
}

// RecordJobSubmit records a job submission
func (m *GatewayMetrics) RecordJobSubmit(success bool, duration float64) {
	result := "success"
	if !success {
		result = "failure"
	}
	m.JobSubmitTotal.WithLabelValues(result).Inc()
	m.JobSubmitLatency.Observe(duration)
}

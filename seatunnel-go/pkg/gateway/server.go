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

package gateway

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/apache/seatunnel-go/pkg/gateway/grpc"
	"github.com/apache/seatunnel-go/pkg/gateway/handlers"
	"github.com/apache/seatunnel-go/pkg/gateway/metrics"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// ServerConfig holds configuration for the gateway server
type ServerConfig struct {
	// HTTP server settings
	Host            string
	Port            int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration

	// Backend settings
	BackendAddresses []string
	BackendTimeout   time.Duration

	// CORS settings
	AllowOrigins     []string
	AllowMethods     []string
	AllowHeaders     []string
	AllowCredentials bool

	// Enable features
	EnableMetrics bool
	EnableSwagger bool
	DebugMode     bool
}

// DefaultServerConfig returns default server configuration
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Host:             "0.0.0.0",
		Port:             8215,
		ReadTimeout:      30 * time.Second,
		WriteTimeout:     30 * time.Second,
		ShutdownTimeout:  10 * time.Second,
		BackendAddresses: []string{"http://localhost:8216"},
		BackendTimeout:   30 * time.Second,
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization"},
		AllowCredentials: true,
		EnableMetrics:    true,
		EnableSwagger:    false,
		DebugMode:        false,
	}
}

// Server is the REST API gateway server
type Server struct {
	config     *ServerConfig
	router     *gin.Engine
	httpServer *http.Server
	grpcClient *grpc.Client
	metrics    *metrics.GatewayMetrics
	logger     *zap.Logger
}

// NewServer creates a new gateway server
func NewServer(config *ServerConfig, logger *zap.Logger) (*Server, error) {
	if config == nil {
		config = DefaultServerConfig()
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	// Set Gin mode
	if config.DebugMode {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	// Create gRPC client
	grpcConfig := &grpc.ClientConfig{
		Addresses:      config.BackendAddresses,
		RequestTimeout: config.BackendTimeout,
	}
	grpcClient, err := grpc.NewClient(grpcConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("创建 gRPC 客户端失败: %w", err)
	}

	// Create metrics
	registry := prometheus.DefaultRegisterer
	gatewayMetrics := metrics.NewGatewayMetrics(registry)

	// Create handlers
	h := handlers.NewHandlers(grpcClient, gatewayMetrics, logger)

	// Create router
	router := gin.New()

	// Add middleware
	router.Use(gin.Recovery())
	router.Use(requestLogger(logger))

	// CORS
	router.Use(cors.New(cors.Config{
		AllowOrigins:     config.AllowOrigins,
		AllowMethods:     config.AllowMethods,
		AllowHeaders:     config.AllowHeaders,
		AllowCredentials: config.AllowCredentials,
		MaxAge:           12 * time.Hour,
	}))

	// Metrics middleware
	if config.EnableMetrics {
		router.Use(metricsMiddleware(gatewayMetrics))
	}

	// Register routes
	registerRoutes(router, h, config.EnableMetrics)

	// Create HTTP server
	httpServer := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", config.Host, config.Port),
		Handler:      router,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
	}

	return &Server{
		config:     config,
		router:     router,
		httpServer: httpServer,
		grpcClient: grpcClient,
		metrics:    gatewayMetrics,
		logger:     logger,
	}, nil
}

// registerRoutes registers all API routes
func registerRoutes(router *gin.Engine, h *handlers.Handlers, enableMetrics bool) {
	// Health check
	router.GET("/health", h.HealthCheck)

	// Prometheus metrics endpoint
	if enableMetrics {
		router.GET("/metrics", gin.WrapH(promhttp.Handler()))
	}

	// API routes
	api := router.Group("/api")
	{
		// Cluster info
		api.GET("/overview", h.GetOverview)
		api.GET("/system-monitoring-information", h.GetSystemInfo)

		// Job management
		api.GET("/running-jobs", h.GetRunningJobs)
		api.GET("/finished-jobs", h.GetFinishedJobs)
		api.GET("/job-info/:jobId", h.GetJobInfo)
		api.GET("/job-metrics/:jobId", h.GetJobMetrics)
		api.POST("/submit-job", h.SubmitJob)
		api.POST("/stop-job", h.StopJob)

		// Config
		api.POST("/encrypt-config", h.EncryptConfig)
	}

	// Hazelcast REST API compatibility (same endpoints, different path)
	hazelcast := router.Group("/hazelcast/rest/maps")
	{
		hazelcast.GET("/overview", h.GetOverview)
		hazelcast.GET("/running-jobs", h.GetRunningJobs)
		hazelcast.GET("/finished-jobs", h.GetFinishedJobs)
		hazelcast.GET("/job-info/:jobId", h.GetJobInfo)
		hazelcast.POST("/submit-job", h.SubmitJob)
		hazelcast.POST("/stop-job", h.StopJob)
	}
}

// requestLogger is a middleware that logs HTTP requests
func requestLogger(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		c.Next()

		latency := time.Since(start)
		status := c.Writer.Status()

		if query != "" {
			path = path + "?" + query
		}

		logger.Info("HTTP 请求",
			zap.String("方法", c.Request.Method),
			zap.String("路径", path),
			zap.Int("状态码", status),
			zap.Duration("耗时", latency),
			zap.String("客户端IP", c.ClientIP()),
		)
	}
}

// metricsMiddleware is a middleware that records HTTP metrics
func metricsMiddleware(m *metrics.GatewayMetrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		m.HTTPRequestsInFlight.Inc()
		defer m.HTTPRequestsInFlight.Dec()

		c.Next()
	}
}

// Start starts the gateway server
func (s *Server) Start() error {
	s.logger.Info("正在启动 SeaTunnel Gateway",
		zap.String("地址", s.httpServer.Addr),
		zap.Bool("指标已启用", s.config.EnableMetrics),
	)

	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("启动服务器失败: %w", err)
	}

	return nil
}

// Stop gracefully stops the gateway server
func (s *Server) Stop() error {
	s.logger.Info("正在关闭 SeaTunnel Gateway")

	ctx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
	defer cancel()

	if err := s.httpServer.Shutdown(ctx); err != nil {
		s.logger.Error("服务器关闭失败", zap.Error(err))
		return err
	}

	if err := s.grpcClient.Close(); err != nil {
		s.logger.Error("gRPC 客户端关闭失败", zap.Error(err))
		return err
	}

	s.logger.Info("SeaTunnel Gateway 已停止")
	return nil
}

// GetRouter returns the Gin router for testing
func (s *Server) GetRouter() *gin.Engine {
	return s.router
}

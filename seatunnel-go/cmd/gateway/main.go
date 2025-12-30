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

package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/apache/seatunnel-go/pkg/gateway"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	version   = "2.3.13-SNAPSHOT-go"
	buildTime = "unknown"
	gitCommit = "unknown"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "seatunnel-gateway",
		Short: "SeaTunnel REST API 网关",
		Long: `SeaTunnel Gateway 是一个轻量级的 REST API 网关，
提供 HTTP 端点用于与 SeaTunnel 集群交互。

它通过 gRPC 桥接前端与 Java 后端，并导出 Prometheus 指标用于监控。`,
		Run: runGateway,
	}

	// Server flags
	rootCmd.Flags().StringP("host", "H", "0.0.0.0", "绑定的主机地址")
	rootCmd.Flags().IntP("port", "p", 8215, "监听端口")

	// Backend flags
	rootCmd.Flags().StringSlice("backend", []string{"http://localhost:8216"}, "后端 REST API 地址 (格式: http://host:port)")
	rootCmd.Flags().Duration("backend-timeout", 30*time.Second, "后端请求超时时间")

	// Feature flags
	rootCmd.Flags().Bool("enable-metrics", true, "启用 Prometheus 指标端点")
	rootCmd.Flags().Bool("debug", false, "启用调试模式")

	// CORS flags
	rootCmd.Flags().StringSlice("cors-origins", []string{"*"}, "CORS 允许的来源")

	// Logging flags
	rootCmd.Flags().String("log-level", "info", "日志级别 (debug, info, warn, error)")
	rootCmd.Flags().String("log-format", "json", "日志格式 (json, console)")

	// Version command
	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "打印版本信息",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("SeaTunnel Gateway 网关\n")
			fmt.Printf("  版本:     %s\n", version)
			fmt.Printf("  构建时间: %s\n", buildTime)
			fmt.Printf("  Git提交:  %s\n", gitCommit)
		},
	})

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runGateway(cmd *cobra.Command, args []string) {
	// Parse flags
	host, _ := cmd.Flags().GetString("host")
	port, _ := cmd.Flags().GetInt("port")
	backendAddrs, _ := cmd.Flags().GetStringSlice("backend")
	backendTimeout, _ := cmd.Flags().GetDuration("backend-timeout")
	enableMetrics, _ := cmd.Flags().GetBool("enable-metrics")
	debug, _ := cmd.Flags().GetBool("debug")
	corsOrigins, _ := cmd.Flags().GetStringSlice("cors-origins")
	logLevel, _ := cmd.Flags().GetString("log-level")
	logFormat, _ := cmd.Flags().GetString("log-format")

	// Initialize logger
	logger := initLogger(logLevel, logFormat)
	defer logger.Sync()

	// Create server config
	config := &gateway.ServerConfig{
		Host:             host,
		Port:             port,
		ReadTimeout:      30 * time.Second,
		WriteTimeout:     30 * time.Second,
		ShutdownTimeout:  10 * time.Second,
		BackendAddresses: backendAddrs,
		BackendTimeout:   backendTimeout,
		AllowOrigins:     corsOrigins,
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization"},
		AllowCredentials: true,
		EnableMetrics:    enableMetrics,
		DebugMode:        debug,
	}

	// Create server
	server, err := gateway.NewServer(config, logger)
	if err != nil {
		logger.Fatal("创建服务器失败", zap.Error(err))
	}

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Info("收到信号，正在关闭", zap.String("信号", sig.String()))
		if err := server.Stop(); err != nil {
			logger.Error("关闭时出错", zap.Error(err))
		}
	}()

	// Start server
	logger.Info("正在启动 SeaTunnel Gateway",
		zap.String("版本", version),
		zap.String("地址", fmt.Sprintf("%s:%d", host, port)),
		zap.Strings("后端", backendAddrs),
	)

	if err := server.Start(); err != nil {
		logger.Fatal("服务器启动失败", zap.Error(err))
	}
}

func initLogger(level, format string) *zap.Logger {
	// Parse log level
	var zapLevel zapcore.Level
	switch strings.ToLower(level) {
	case "debug":
		zapLevel = zapcore.DebugLevel
	case "info":
		zapLevel = zapcore.InfoLevel
	case "warn", "warning":
		zapLevel = zapcore.WarnLevel
	case "error":
		zapLevel = zapcore.ErrorLevel
	default:
		zapLevel = zapcore.InfoLevel
	}

	// Create config
	var config zap.Config
	if format == "console" {
		config = zap.NewDevelopmentConfig()
	} else {
		config = zap.NewProductionConfig()
	}
	config.Level = zap.NewAtomicLevelAt(zapLevel)

	// Build logger
	logger, err := config.Build()
	if err != nil {
		// Fallback to nop logger
		return zap.NewNop()
	}

	return logger
}

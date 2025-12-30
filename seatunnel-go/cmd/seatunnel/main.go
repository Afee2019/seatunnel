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
	"strings"

	"github.com/apache/seatunnel-go/pkg/config"
	"github.com/apache/seatunnel-go/pkg/registry"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	version   = "2.3.13-SNAPSHOT-go"
	buildTime = "unknown"
	gitCommit = "unknown"
)

// Command line flags
var (
	configPath   string
	variables    []string
	checkConfig  bool
	listPlugins  bool
	jobName      string
	parallelism  int
	masterAddr   string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "seatunnel",
		Short: "SeaTunnel Go 命令行工具 - 高性能数据集成工具",
		Long: `SeaTunnel Go 命令行工具是 SeaTunnel 的轻量级命令行界面。
支持本地运行作业和向 SeaTunnel 集群提交作业。`,
		Version: version,
		Run:     runCommand,
	}

	// Add flags
	rootCmd.Flags().StringVarP(&configPath, "config", "c", "", "配置文件路径 (必填)")
	rootCmd.Flags().StringSliceVarP(&variables, "variable", "i", nil, "变量替换，格式: key=value")
	rootCmd.Flags().BoolVar(&checkConfig, "check", false, "检查配置文件语法")
	rootCmd.Flags().BoolVar(&listPlugins, "list-plugins", false, "列出可用插件")
	rootCmd.Flags().StringVarP(&jobName, "name", "n", "", "作业名称")
	rootCmd.Flags().IntVarP(&parallelism, "parallelism", "p", 1, "并行度")
	rootCmd.Flags().StringVarP(&masterAddr, "master", "m", "", "集群模式的主节点地址")

	// Add subcommands
	rootCmd.AddCommand(newVersionCmd())
	rootCmd.AddCommand(newCheckCmd())
	rootCmd.AddCommand(newLocalCmd())
	rootCmd.AddCommand(newListCmd())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runCommand(cmd *cobra.Command, args []string) {
	if listPlugins {
		listAvailablePlugins()
		return
	}

	if configPath == "" {
		cmd.Help()
		return
	}

	if checkConfig {
		checkConfigFile()
		return
	}

	runLocalJob()
}

// newVersionCmd creates the version command
func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "打印版本信息",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("SeaTunnel Go 命令行工具\n")
			fmt.Printf("  版本:     %s\n", version)
			fmt.Printf("  构建时间: %s\n", buildTime)
			fmt.Printf("  Git提交:  %s\n", gitCommit)
		},
	}
}

// newCheckCmd creates the check command
func newCheckCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check",
		Short: "检查配置文件语法",
		Run: func(cmd *cobra.Command, args []string) {
			if configPath == "" {
				fmt.Fprintln(os.Stderr, "错误: 必须指定 --config 参数")
				os.Exit(1)
			}
			checkConfigFile()
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "配置文件路径 (必填)")
	return cmd
}

// newLocalCmd creates the local run command
func newLocalCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "local",
		Short: "以本地模式运行作业",
		Run: func(cmd *cobra.Command, args []string) {
			if configPath == "" {
				fmt.Fprintln(os.Stderr, "错误: 必须指定 --config 参数")
				os.Exit(1)
			}
			runLocalJob()
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "配置文件路径 (必填)")
	cmd.Flags().StringSliceVarP(&variables, "variable", "i", nil, "变量替换，格式: key=value")
	cmd.Flags().StringVarP(&jobName, "name", "n", "", "作业名称")
	cmd.Flags().IntVarP(&parallelism, "parallelism", "p", 1, "并行度")
	return cmd
}

// newListCmd creates the list command
func newListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "列出可用插件",
		Run: func(cmd *cobra.Command, args []string) {
			listAvailablePlugins()
		},
	}
}

// checkConfigFile checks the configuration file syntax
func checkConfigFile() {
	parser := config.NewConfigParser()

	// Set variables
	for _, v := range variables {
		parts := strings.SplitN(v, "=", 2)
		if len(parts) == 2 {
			parser.SetVariable(parts[0], parts[1])
		}
	}

	jobConfig, err := parser.ParseFile(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "配置检查失败: %v\n", err)
		os.Exit(1)
	}

	if err := jobConfig.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "配置验证失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("配置检查通过")
	fmt.Printf("  作业模式: %s\n", jobConfig.GetJobMode())
	fmt.Printf("  并行度:   %d\n", jobConfig.GetParallelism())
	fmt.Printf("  数据源:   %d 个\n", len(jobConfig.Source))
	fmt.Printf("  转换器:   %d 个\n", len(jobConfig.Transform))
	fmt.Printf("  数据汇:   %d 个\n", len(jobConfig.Sink))
}

// listAvailablePlugins lists all available plugins
func listAvailablePlugins() {
	reg := registry.GetRegistry()

	fmt.Println("可用的数据源插件:")
	for _, name := range reg.ListSourceFactories() {
		fmt.Printf("  - %s\n", name)
	}

	fmt.Println("\n可用的数据汇插件:")
	for _, name := range reg.ListSinkFactories() {
		fmt.Printf("  - %s\n", name)
	}
}

// runLocalJob runs the job in local mode
func runLocalJob() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// Parse config
	parser := config.NewConfigParser()
	for _, v := range variables {
		parts := strings.SplitN(v, "=", 2)
		if len(parts) == 2 {
			parser.SetVariable(parts[0], parts[1])
		}
	}

	jobConfig, err := parser.ParseFile(configPath)
	if err != nil {
		logger.Fatal("解析配置文件失败", zap.Error(err))
	}

	if err := jobConfig.Validate(); err != nil {
		logger.Fatal("配置文件无效", zap.Error(err))
	}

	// Create executor
	executor := NewLocalExecutor(jobConfig, logger)

	// Run job
	logger.Info("正在启动 SeaTunnel 作业",
		zap.String("配置文件", configPath),
		zap.String("作业模式", jobConfig.GetJobMode()),
		zap.Int("并行度", jobConfig.GetParallelism()),
	)

	if err := executor.Execute(); err != nil {
		logger.Fatal("作业执行失败", zap.Error(err))
	}

	logger.Info("作业执行成功")
}

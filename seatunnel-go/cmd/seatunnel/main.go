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
		Short: "SeaTunnel Go CLI - A high-performance data integration tool",
		Long: `SeaTunnel Go CLI is a lightweight command-line interface for SeaTunnel.
It supports running local jobs and submitting jobs to SeaTunnel clusters.`,
		Version: version,
		Run:     runCommand,
	}

	// Add flags
	rootCmd.Flags().StringVarP(&configPath, "config", "c", "", "Config file path (required)")
	rootCmd.Flags().StringSliceVarP(&variables, "variable", "i", nil, "Variable substitution in format key=value")
	rootCmd.Flags().BoolVar(&checkConfig, "check", false, "Check config file syntax")
	rootCmd.Flags().BoolVar(&listPlugins, "list-plugins", false, "List available plugins")
	rootCmd.Flags().StringVarP(&jobName, "name", "n", "", "Job name")
	rootCmd.Flags().IntVarP(&parallelism, "parallelism", "p", 1, "Parallelism level")
	rootCmd.Flags().StringVarP(&masterAddr, "master", "m", "", "Master address for cluster mode")

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
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("SeaTunnel Go CLI\n")
			fmt.Printf("  Version:    %s\n", version)
			fmt.Printf("  Build Time: %s\n", buildTime)
			fmt.Printf("  Git Commit: %s\n", gitCommit)
		},
	}
}

// newCheckCmd creates the check command
func newCheckCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check",
		Short: "Check configuration file syntax",
		Run: func(cmd *cobra.Command, args []string) {
			if configPath == "" {
				fmt.Fprintln(os.Stderr, "Error: --config is required")
				os.Exit(1)
			}
			checkConfigFile()
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Config file path (required)")
	return cmd
}

// newLocalCmd creates the local run command
func newLocalCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "local",
		Short: "Run job in local mode",
		Run: func(cmd *cobra.Command, args []string) {
			if configPath == "" {
				fmt.Fprintln(os.Stderr, "Error: --config is required")
				os.Exit(1)
			}
			runLocalJob()
		},
	}
	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Config file path (required)")
	cmd.Flags().StringSliceVarP(&variables, "variable", "i", nil, "Variable substitution in format key=value")
	cmd.Flags().StringVarP(&jobName, "name", "n", "", "Job name")
	cmd.Flags().IntVarP(&parallelism, "parallelism", "p", 1, "Parallelism level")
	return cmd
}

// newListCmd creates the list command
func newListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List available plugins",
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
		fmt.Fprintf(os.Stderr, "Config check FAILED: %v\n", err)
		os.Exit(1)
	}

	if err := jobConfig.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Config validation FAILED: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Config check PASSED")
	fmt.Printf("  Job Mode:    %s\n", jobConfig.GetJobMode())
	fmt.Printf("  Parallelism: %d\n", jobConfig.GetParallelism())
	fmt.Printf("  Sources:     %d\n", len(jobConfig.Source))
	fmt.Printf("  Transforms:  %d\n", len(jobConfig.Transform))
	fmt.Printf("  Sinks:       %d\n", len(jobConfig.Sink))
}

// listAvailablePlugins lists all available plugins
func listAvailablePlugins() {
	reg := registry.GetRegistry()

	fmt.Println("Available Source Plugins:")
	for _, name := range reg.ListSourceFactories() {
		fmt.Printf("  - %s\n", name)
	}

	fmt.Println("\nAvailable Sink Plugins:")
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
		logger.Fatal("Failed to parse config", zap.Error(err))
	}

	if err := jobConfig.Validate(); err != nil {
		logger.Fatal("Invalid config", zap.Error(err))
	}

	// Create executor
	executor := NewLocalExecutor(jobConfig, logger)

	// Run job
	logger.Info("Starting SeaTunnel job",
		zap.String("config", configPath),
		zap.String("mode", jobConfig.GetJobMode()),
		zap.Int("parallelism", jobConfig.GetParallelism()),
	)

	if err := executor.Execute(); err != nil {
		logger.Fatal("Job execution failed", zap.Error(err))
	}

	logger.Info("Job completed successfully")
}

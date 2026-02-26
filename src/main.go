// Copyright 2025 kg.sai. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"dragonfly5/server/cluster"
	"dragonfly5/server/global"
	"dragonfly5/server/rdb"

	gonanoid "github.com/matoous/go-nanoid/v2"
	"gopkg.in/natefinch/lumberjack.v2"
	"gopkg.in/yaml.v3"
)

// initSlog sets the default slog handler with the given level and log file under logDir.
// Level: debug, info, warn, error (default info). Creates logDir if missing; uses lumberjack for rotation.
func initSlog(lg global.LoggerConfig) {
	var l slog.Level
	switch strings.ToLower(strings.TrimSpace(lg.Level)) {
	case "debug":
		l = slog.LevelDebug
	case "info", "":
		l = slog.LevelInfo
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}

	logDir := strings.TrimSpace(lg.Dir)
	if logDir == "" {
		logDir = "logs"
	}
	if err := os.MkdirAll(logDir, 0755); err != nil {
		slog.Error("Failed to create log directory", "dir", logDir, "err", err)
		os.Exit(1)
	}
	logPath := filepath.Join(logDir, "server.log")

	maxSize := lg.MaxSizeMB
	if maxSize <= 0 {
		maxSize = 100
	}
	maxBackups := lg.MaxBackups
	if maxBackups < 0 {
		maxBackups = 3
	}
	maxAge := lg.MaxAgeDays
	if maxAge < 0 {
		maxAge = 30
	}

	w := &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    maxSize,
		MaxBackups: maxBackups,
		MaxAge:     maxAge,
		Compress:   lg.Compress,
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: l})))
}

// runServer starts the HTTP server with the given configuration.
// It initializes cluster health info (one DatasourceInfo per MyDatasources entry),
// normalizes connection limits (MaxConns, MaxWriteConns, MinWriteConns, timeouts),
// builds the datasource manager, creates the balancer and router, then starts
// the server and a health ticker. It blocks until SIGINT/SIGTERM, then performs
// graceful shutdown (node status STOPPING, server shutdown, DsManager shutdown).
func runServer(config global.Config) {
	// Initialize cluster health info
	datasourceInfo := make([]cluster.DatasourceInfo, len(config.MyDatasources))
	for i := range config.MyDatasources {
		dsConfig := &config.MyDatasources[i]

		if dsConfig.MaxWriteConns == 1 {
			dsConfig.MaxWriteConns = 2
		}
		dsConfig.MaxConns = max(3, dsConfig.MaxConns)
		dsConfig.MaxWriteConns = min(dsConfig.MaxWriteConns, dsConfig.MaxConns-1)
		dsConfig.MinWriteConns = min(dsConfig.MinWriteConns, dsConfig.MaxWriteConns)
		if dsConfig.MaxConnLifetimeSec < 1 {
			dsConfig.MaxConnLifetimeSec = 1800
		}
		if dsConfig.MaxTxIdleTimeoutSec < 1 {
			dsConfig.MaxTxIdleTimeoutSec = 15
		}
		if dsConfig.DefaultQueryTimeoutSec < 1 {
			dsConfig.DefaultQueryTimeoutSec = 30
		}
		datasourceInfo[i] = *cluster.NewDatasourceInfo(*dsConfig)
	}

	// Initialize DsManager
	dsManager := rdb.NewDsManager(config.MyDatasources)

	// Set maxHttpQueue for HTTP connection limiting
	maxHttpQueue := config.MaxHttpQueue
	if maxHttpQueue <= 0 {
		maxHttpQueue = 1000 // default
	}

	nodeId, _ := gonanoid.New(9)
	thisNode := &cluster.NodeInfo{
		NodeID:       fmt.Sprintf("%s-%s", config.NodeName, nodeId),
		Status:       cluster.STARTING,
		BaseURL:      "-",
		SecretKey:    config.SecretKey,
		MaxHttpQueue: maxHttpQueue,
		Datasources:  datasourceInfo,
	}

	fmt.Printf("### [Config] Node ID: %s\n", thisNode.NodeID)
	fmt.Printf("### [Config] Node Name: %s\n", config.NodeName)
	fmt.Printf("### [Config] Max Client HTTP Queue: %d\n", maxHttpQueue)
	fmt.Printf("### [Config] Datasources: %v\n", datasourceInfo)

	// collect cluster health information
	clusterNodes := make([]*cluster.NodeInfo, 0, len(config.ClusterNodes))
	for _, nodeURL := range config.ClusterNodes {
		clusterNodes = append(clusterNodes, &cluster.NodeInfo{
			Status:  cluster.STARTING,
			BaseURL: nodeURL,
		})
	}
	balancer := cluster.NewBalancer(thisNode, clusterNodes)

	// Create router
	router := NewRouter(balancer, dsManager)

	// Create HTTP server
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", config.NodePort),
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Start server in a goroutine
	go func() {
		fmt.Printf("### [Server] Starting on port: %d\n", config.NodePort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("### [Server] Server failed to start: %v\n", err)
			os.Exit(1)
		}
	}()

	router.StartHealthTicker()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	fmt.Println("### [Server] Shutting down server...")
	thisNode.Mu.Lock()
	thisNode.Status = cluster.STOPPING
	thisNode.Mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		fmt.Printf("### [Server] Shutdown error: %v\n", err)
	}

	// Shutdown DsManager
	dsManager.Shutdown()

	fmt.Println("### [Server] Exited successfully.")
}

// main is the entry point. It loads the config from YAML (default path "config.yaml",
// overridable by first CLI arg), validates that at least one datasource is present,
// then calls runServer. On config read or parse error it exits after logging.
func main() {
	// Load configuration
	configPath := "config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		fmt.Printf("### [Error] Failed to read config file: %v\n", err)
		os.Exit(1)
		return
	}

	var config global.Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		fmt.Printf("### [Error] Failed to parse config file: %v\n", err)
		os.Exit(1)
		return
	}

	if len(config.MyDatasources) < 1 {
		fmt.Printf("### [Error] Wrong configuration file: %s, no datasources\n", configPath)
		os.Exit(1)
		return
	}

	initSlog(config.Logger)

	runServer(config)
}

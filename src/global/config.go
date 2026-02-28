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

package global

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	gonanoid "github.com/matoous/go-nanoid/v2"
)

type Config struct {
	NodeName      string             `yaml:"nodeName"`
	NodePort      int                `yaml:"nodePort"`
	SecretKey     string             `yaml:"secretKey"`
	MaxHttpQueue  int                `yaml:"maxHttpQueue"`
	Logger        LoggerConfig       `yaml:"logger"`
	MyDatasources []DatasourceConfig `yaml:"myDatasources"`
	ClusterNodes  []string           `yaml:"clusterNodes"`
}

// LoggerConfig holds logging and rotation settings.
type LoggerConfig struct {
	Level      string `yaml:"level"`      // debug, info, warn, error
	Dir        string `yaml:"dir"`        // ログ出力先（カレントからの相対パス）
	MaxSizeMB  int    `yaml:"maxSizeMB"`  // 1ファイル最大サイズ（MB）
	MaxBackups int    `yaml:"maxBackups"` // 保持する世代数
	MaxAgeDays int    `yaml:"maxAgeDays"` // 日数超過で削除（0=無制限）
	Compress   bool   `yaml:"compress"`   // ローテート済みをgzip圧縮
}

type DatasourceConfig struct {
	DatasourceID       string `yaml:"datasourceId"`
	DatabaseName       string `yaml:"databaseName"`
	Driver             string `yaml:"driver"`
	DSN                string `yaml:"dsn"`
	MaxConns           int    `yaml:"maxConns"`
	MaxConnLifetimeSec int    `yaml:"maxConnLifetimeSec"`

	MaxWriteConns          int `yaml:"maxWriteConns"`
	MinWriteConns          int `yaml:"minWriteConns"`
	MaxTxIdleTimeoutSec    int `yaml:"maxTxIdleTimeoutSec"`
	DefaultQueryTimeoutSec int `yaml:"defaultQueryTimeoutSec"`
}

const HEADER_SECRET_KEY = "_cy_SecretKey"
const HEADER_DB_NAME = "_cy_DbName"
const HEADER_TX_ID = "_cy_TxID"
const HEADER_REDIRECT_COUNT = "_cy_RdCount"
const HEADER_TIMEOUT_SEC = "_cy_TimeoutSec"

const EP_PATH_QUERY = "/query"
const EP_PATH_EXECUTE = "/execute"
const EP_PATH_TX_BEGIN = "/begin"
const EP_PATH_TX_COMMIT = "/commit"
const EP_PATH_TX_ROLLBACK = "/rollback"
const EP_PATH_TX_CLOSE = "/close"

// ENDPOINT_TYPE identifies the kind of API endpoint from the request path.
// It is used by the balancer to choose the right scoring and routing logic.
type ENDPOINT_TYPE int

const (
	EP_Query ENDPOINT_TYPE = iota
	EP_Execute
	EP_BeginTx
	EP_Other
)

// GetEndpointType returns the endpoint type for the given path by suffix match:
// EP_Query for paths ending with EP_PATH_QUERY, EP_Execute for EP_PATH_EXECUTE,
// EP_BeginTx for EP_PATH_TX_BEGIN; otherwise EP_Other.
func GetEndpointType(path string) ENDPOINT_TYPE {

	if strings.HasSuffix(path, EP_PATH_QUERY) {
		return EP_Query
	}
	if strings.HasSuffix(path, EP_PATH_EXECUTE) {
		return EP_Execute
	}
	if strings.HasSuffix(path, EP_PATH_TX_BEGIN) {
		return EP_BeginTx
	}

	return EP_Other
}

const CTX_DS_IDX = "$S_IDX"
const CTX_LOGGER = "$LOGGER"

// GetCtxDsIdx returns the datasource index stored in the request context.
// The index is set by the balancer middleware (CTX_DS_IDX) after node selection.
// The second return value is false if the key is missing or not an int.
func GetCtxDsIdx(r *http.Request) (int, bool) {
	value, ok := r.Context().Value(CTX_DS_IDX).(int)
	return value, ok
}

// Logger is a wrapper for slog.Logger.
type Logger struct {
	requestId string
}

func (l *Logger) Debug(msg string, args ...any) {
	slog.Debug(l.requestId, append([]any{"spot", msg}, args...)...)
}
func (l *Logger) Info(msg string, args ...any) {
	slog.Info(l.requestId, append([]any{"spot", msg}, args...)...)
}
func (l *Logger) Warn(msg string, args ...any) {
	slog.Warn(l.requestId, append([]any{"spot", msg}, args...)...)
}
func (l *Logger) Error(msg string, args ...any) {
	slog.Error(l.requestId, append([]any{"spot", msg}, args...)...)
}

// NewLogger creates a new Logger.
func NewLogger(traceId string) *Logger {
	if traceId == "" {
		traceId, _ = gonanoid.New(9)
		traceId = "D5" + traceId
	}
	return &Logger{requestId: traceId}
}

// GetCtxLogger returns the Logger stored in the request context.
// If not found, create a new Logger with an empty requestId.
func GetCtxLogger(ctx context.Context) *Logger {
	logger, ok := ctx.Value(CTX_LOGGER).(*Logger)
	if ok {
		return logger
	}
	return NewLogger("")
}

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

package rdb

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"dragonfly5/server/global"
	. "dragonfly5/server/global"

	"github.com/paulbellamy/ratecounter"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type RequestParams struct {
	SQL        string       `json:"sql"`
	Params     []ParamValue `json:"params,omitempty"`
	TimeoutSec int          `json:"timeoutSec,omitempty"`
	OffsetRows int          `json:"offsetRows,omitempty"`
	LimitRows  int          `json:"limitRows,omitempty"`
}

// INT, LONG, DOUBLE, DECIMAL, BOOL
// DATE, DATETIME
// STRING, BINARY
type ValueType string

const (
	NULL     ValueType = "NULL"
	INT      ValueType = "INT"
	LONG     ValueType = "LONG"
	FLOAT    ValueType = "FLOAT"
	DOUBLE   ValueType = "DOUBLE"
	BOOL     ValueType = "BOOL"
	DATE     ValueType = "DATE"
	DATETIME ValueType = "DATETIME"
	STRING   ValueType = "STRING"
	BYTES    ValueType = "BYTES"
)

// ParamValue represents a parameter value
type ParamValue struct {
	Type  ValueType `json:"type"`
	Value any       `json:"value,omitempty"`
}

type ExecuteResponse struct {
	EffectedRows  int64 `json:"effectedRows"`
	ElapsedTimeUs int64 `json:"elapsedTimeUs"`
}

const STAT_WINDOW_INTERVAL = 5 * time.Minute

type StatsInfo struct {
	mu sync.Mutex

	statLatency  *prometheus.SummaryVec
	statTotal    *ratecounter.RateCounter
	statErrors   *ratecounter.RateCounter
	statTimeouts *ratecounter.RateCounter
}

// ExecuteHandler handles /v1/rdb/execute requests
type RequestHandler struct {
	dsManager        *DsManager
	statsInfos       []*StatsInfo
	slimResponseMode bool
}

// NewRequestHandler constructs a RequestHandler that uses the given DsManager and
// allocates one StatsInfo per datasource. Each StatsInfo has a Prometheus
// summary for p95 latency and rate counters (STAT_WINDOW_INTERVAL) for
// total requests, errors, and timeouts, used for health and balancer scoring.
func NewRequestHandler(dsManager *DsManager, slimResponseMode bool) *RequestHandler {
	statsInfos := make([]*StatsInfo, len(dsManager.dss))
	for i := range statsInfos {
		var statLatency = prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Objectives: map[float64]float64{0.95: 0.01},
			MaxAge:     STAT_WINDOW_INTERVAL,
		}, []string{"latency"})
		statsInfos[i] = &StatsInfo{
			statLatency:  statLatency,
			statTotal:    ratecounter.NewRateCounter(STAT_WINDOW_INTERVAL),
			statErrors:   ratecounter.NewRateCounter(STAT_WINDOW_INTERVAL),
			statTimeouts: ratecounter.NewRateCounter(STAT_WINDOW_INTERVAL),
		}
	}
	return &RequestHandler{
		dsManager:        dsManager,
		statsInfos:       statsInfos,
		slimResponseMode: slimResponseMode,
	}
}

// Query handles POST /rdb/query. The datasource index is taken from context
// (set by balancer). It parses the JSON body (SQL, params, timeoutSec,
// limitRows), runs a read-only query via DsManager.Query, then streams the
// result as JSON (meta, rows, totalCount, elapsedTimeUs). On timeout or
// error it updates stats and returns an appropriate HTTP error.
func (rh *RequestHandler) Query(w http.ResponseWriter, r *http.Request) {
	dsIDX, ok := GetCtxDsIdx(r)
	if !ok {
		ResponseError(w, r, RP_BAD_REQUEST, "Datasource INDEX hasn't decided by Balancer")
		return
	}

	startTime := time.Now()
	req, parameters, err := parseRequest(r)
	if err != nil {
		ResponseError(w, r, RP_BAD_REQUEST, err.Error())
		return
	}

	global.GetCtxLogger(r.Context()).Debug("RequestHandler", "detail", "Executing Query", "dsIDX", dsIDX, "sql", req.SQL, "params", parameters)

	// Execute query without transaction
	rows, releaseResource, err := rh.dsManager.Query(r.Context(), req.TimeoutSec, dsIDX, req.SQL, parameters...)
	if releaseResource != nil {
		defer releaseResource()
	}
	if err != nil {
		if err == ErrDsNotFound {
			ResponseError(w, r, RP_DATASOURCE_NOT_FOUND, "Datasource not found for Query")
			return
		}
		if err == context.DeadlineExceeded {
			rh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), false, true)
			ResponseError(w, r, RP_CLIENT_CANCELLED, "Request timeout for Query")
			return
		}
		rh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
		ResponseError(w, r, RP_DATASOURCE_EXCEPTION, err.Error())
		return
	}
	defer rows.Close()

	if rh.slimResponseMode {
		// In slim response mode, we skip meta and totalCount to reduce overhead.
		if err := responseQueryResultSlim(w, rows, req.OffsetRows, req.LimitRows, startTime); err != nil {
			ResponseError(w, r, RP_SERVER_EXCEPTION, err.Error())
			rh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
			return
		}
	} else {
		if err := responseQueryResultJson(w, rows, req.OffsetRows, req.LimitRows, startTime); err != nil {
			ResponseError(w, r, RP_SERVER_EXCEPTION, err.Error())
			rh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
			return
		}
	}

	rh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), false, false)
}

// QueryTx handles POST /rdb/tx/query. The transaction ID is taken from the
// request header. It parses the body, runs the query in that transaction
// via DsManager.QueryTx, then streams the result as JSON. On timeout or
// error it updates stats and returns an appropriate HTTP error.
func (rh *RequestHandler) QueryTx(w http.ResponseWriter, r *http.Request) {
	txID := r.Header.Get(HEADER_TX_ID)
	if txID == "" {
		ResponseError(w, r, RP_BAD_REQUEST, "TxID is required")
		return
	}

	startTime := time.Now()
	req, parameters, err := parseRequest(r)
	if err != nil {
		ResponseError(w, r, RP_BAD_REQUEST, err.Error())
		return
	}

	global.GetCtxLogger(r.Context()).Debug("RequestHandler", "detail", "Executing QueryTx", "txID", txID, "sql", req.SQL, "params", parameters)

	// Execute query in transaction
	rows, releaseResource, dsIDX, err := rh.dsManager.QueryTx(r.Context(), req.TimeoutSec, txID, req.SQL, parameters...)
	if releaseResource != nil {
		defer releaseResource()
	}
	if err != nil {
		if err == ErrDsNotFound {
			ResponseError(w, r, RP_DATASOURCE_NOT_FOUND, "Datasource not found for QueryTx")
			return
		}
		if err == ErrTxNotFound {
			ResponseError(w, r, RP_DATASOURCE_TX_NOT_FOUND, "Transaction not found for QueryTx")
			return
		}
		if err == context.DeadlineExceeded {
			rh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), false, true)
			ResponseError(w, r, RP_CLIENT_CANCELLED, "Request timeout for QueryTx")
			return
		}
		rh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
		global.ResponseError(w, r, RP_DATASOURCE_EXCEPTION, err.Error())
		return
	}
	defer rows.Close()

	if rh.slimResponseMode {
		// In slim response mode, we skip meta and totalCount to reduce overhead.
		if err := responseQueryResultSlim(w, rows, req.OffsetRows, req.LimitRows, startTime); err != nil {
			ResponseError(w, r, RP_SERVER_EXCEPTION, err.Error())
			rh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
			return
		}
	} else {
		if err := responseQueryResultJson(w, rows, req.OffsetRows, req.LimitRows, startTime); err != nil {
			ResponseError(w, r, RP_SERVER_EXCEPTION, err.Error())
			rh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
			return
		}
	}

	rh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), false, false)
}

// Execute handles POST /rdb/execute. The datasource index is taken from
// context. It parses the body, runs the statement via DsManager.Execute,
// then responds with effectedRows and elapsedTimeUs. On timeout or error
// it updates stats and returns an appropriate HTTP error.
func (rh *RequestHandler) Execute(w http.ResponseWriter, r *http.Request) {
	dsIDX, ok := GetCtxDsIdx(r)
	if !ok {
		ResponseError(w, r, RP_BAD_REQUEST, "Datasource INDEX hasn't decided by Balancer")
		return
	}

	startTime := time.Now()
	req, parameters, err := parseRequest(r)
	if err != nil {
		ResponseError(w, r, RP_BAD_REQUEST, err.Error())
		return
	}
	global.GetCtxLogger(r.Context()).Debug("RequestHandler", "detail", "Executing Execute", "dsIDX", dsIDX, "sql", req.SQL, "params", parameters)

	// Get database
	result, releaseResource, err := rh.dsManager.Execute(r.Context(), req.TimeoutSec, dsIDX, req.SQL, parameters...)
	if releaseResource != nil {
		defer releaseResource()
	}
	if err != nil {
		if err == ErrDsNotFound {
			ResponseError(w, r, RP_DATASOURCE_NOT_FOUND, "Datasource not found for Execute")
			return
		}
		if err == context.DeadlineExceeded {
			rh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), false, true)
			ResponseError(w, r, RP_CLIENT_CANCELLED, "Request timeout for Execute")
			return
		}
		rh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
		ResponseError(w, r, RP_DATASOURCE_EXCEPTION, err.Error())
		return
	}

	// Get affected rows
	affectedRows, err := result.RowsAffected()
	if err != nil {
		ResponseError(w, r, RP_SERVER_EXCEPTION, err.Error())
		return
	}

	// Update health info: record successful execution
	rh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), false, false)

	// Write response
	response := ExecuteResponse{
		EffectedRows:  affectedRows,
		ElapsedTimeUs: time.Since(startTime).Microseconds(),
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// ExecuteTx handles POST /rdb/tx/execute. The transaction ID is taken from
// the request header. It parses the body, runs the statement in that
// transaction via DsManager.ExecuteTx, then responds with effectedRows
// and elapsedTimeUs. On timeout or error it updates stats and returns an
// appropriate HTTP error.
func (rh *RequestHandler) ExecuteTx(w http.ResponseWriter, r *http.Request) {
	txID := r.Header.Get(HEADER_TX_ID)
	if txID == "" {
		ResponseError(w, r, RP_BAD_REQUEST, "TxID is required")
		return
	}

	startTime := time.Now()

	req, parameters, err := parseRequest(r)
	if err != nil {
		ResponseError(w, r, RP_BAD_REQUEST, err.Error())
		return
	}
	global.GetCtxLogger(r.Context()).Debug("RequestHandler", "detail", "Executing ExecuteTx", "txID", txID, "sql", req.SQL, "params", parameters)

	// Execute in transaction
	result, releaseResource, dsIDX, err := rh.dsManager.ExecuteTx(r.Context(), req.TimeoutSec, txID, req.SQL, parameters...)
	if releaseResource != nil {
		defer releaseResource()
	}
	if err != nil {
		if err == ErrDsNotFound {
			ResponseError(w, r, RP_DATASOURCE_NOT_FOUND, "Datasource not found for ExecuteTx")
			return
		}
		if err == ErrTxNotFound {
			ResponseError(w, r, RP_DATASOURCE_TX_NOT_FOUND, "Transaction not found for ExecuteTx")
			return
		}
		if err == context.DeadlineExceeded {
			rh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), false, true)
			ResponseError(w, r, RP_CLIENT_CANCELLED, "Request timeout for ExecuteTx")
			return
		}
		rh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
		ResponseError(w, r, RP_DATASOURCE_EXCEPTION, err.Error())
		return
	}

	// Get affected rows
	affectedRows, err := result.RowsAffected()
	if err != nil {
		ResponseError(w, r, RP_SERVER_EXCEPTION, err.Error())
		return
	}

	// Update health info: record successful execution
	rh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), false, false)

	// Write response
	response := ExecuteResponse{
		EffectedRows:  affectedRows,
		ElapsedTimeUs: time.Since(startTime).Microseconds(),
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// statsSetResult updates the per-datasource stats for the given index.
// It always increments the total counter. If isError or isTimeout it
// increments the corresponding rate counter; otherwise it records latencyMs
// in the p95 summary. Ignores invalid datasourceIdx (< 0).
func (rh *RequestHandler) statsSetResult(datasourceIdx int, latencyMs int64, isError bool, isTimeout bool) {
	if datasourceIdx < 0 {
		return // When invalid TxID
	}

	stats := rh.statsInfos[datasourceIdx]
	stats.mu.Lock()
	defer stats.mu.Unlock()

	stats.statTotal.Incr(1)

	if isError {
		stats.statErrors.Incr(1)
		return
	}
	if isTimeout {
		stats.statTimeouts.Incr(1)
		return
	}
	stats.statLatency.WithLabelValues("p95").Observe(float64(latencyMs))
}

// StatsGet returns the current p95 latency (ms), 1-minute error rate, and
// 1-minute timeout rate for the given datasource index. Used by the
// router to fill the self node's DatasourceInfo for health/balancer. If
// the summary has no samples, p95 is reported as 16.
func (rh *RequestHandler) StatsGet(datasourceIdx int) (runningRead int, runningWrite int, runningTx int, latencyP95Ms int, errorRate1m float64, timeoutRate1m float64) {
	stats := rh.statsInfos[datasourceIdx]

	stats.mu.Lock()
	latency := &dto.Metric{}
	stats.statLatency.WithLabelValues("p95").(prometheus.Metric).Write(latency)
	p95 := latency.GetSummary().GetQuantile()[0].GetValue()
	if math.IsNaN(p95) {
		p95 = 16.0
	}

	total := float64(stats.statTotal.Rate())
	errorRate1m = 0.0
	timeoutRate1m = 0.0
	if total > 0 {
		errorRate1m = float64(stats.statErrors.Rate()) / total
		timeoutRate1m = float64(stats.statTimeouts.Rate()) / total
	}
	stats.mu.Unlock()

	runningRead, runningWrite, runningTx = rh.dsManager.StatsGet(datasourceIdx)

	return runningRead, runningWrite, runningTx, int(p95), errorRate1m, timeoutRate1m
}

// parseRequest reads the JSON body into RequestParams (SQL, Params,
// TimeoutSec, LimitRows), converts Params to driver values via
// convertParams, and overrides TimeoutSec from HEADER_TIMEOUT_SEC if
// present. The body is consumed and closed. Returns an error if JSON
// decode fails, SQL is empty, or param conversion fails.
func parseRequest(r *http.Request) (request *RequestParams, params []any, err error) {
	if r.Body != nil {
		defer func() {
			io.Copy(io.Discard, r.Body)
			r.Body.Close()
		}()
	}

	var req RequestParams
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return nil, nil, fmt.Errorf("Failed to parse request: %w", err)
	}

	if req.SQL == "" {
		return nil, nil, fmt.Errorf("SQL is required")
	}

	// Convert params
	parameters, err := convertParams(req.Params)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to convert params: %w", err)
	}

	timeoutSec := r.Header.Get(HEADER_TIMEOUT_SEC)
	if timeoutSec != "" {
		timeoutSecInt, err := strconv.Atoi(timeoutSec)
		if err == nil {
			req.TimeoutSec = timeoutSecInt
		}
	}

	return &req, parameters, nil
}

// convertParams converts each ParamValue in the slice to a value suitable
// for database/sql (e.g. int32, int64, string, time.Time, []byte) by
// calling convertParam. Returns an error on the first conversion failure.
func convertParams(params []ParamValue) ([]any, error) {
	args := make([]any, len(params))
	for i, p := range params {
		arg, err := convertParam(p)
		if err != nil {
			return nil, fmt.Errorf("param[%d]: %w", i, err)
		}
		args[i] = arg
	}
	return args, nil
}

// convertParam converts a single ParamValue to a driver-compatible value
// based on its Type (NULL, INT, LONG, DOUBLE, DECIMAL, BOOL, STRING, BINARY,
// DATE, DATETIME). Accepts various JSON number/string representations and
// returns an error for invalid or unknown types.
func convertParam(p ParamValue) (any, error) {
	if p.Value == nil {
		return nil, nil
	}

	switch p.Type {
	case NULL:
		return nil, nil
	case INT:
		if val, ok := p.Value.(int32); ok {
			return val, nil
		}
		if val, ok := p.Value.(float64); ok {
			return int32(val), nil
		}
		if val, ok := p.Value.(int); ok {
			return int32(val), nil
		}
		if val, ok := p.Value.(string); ok {
			var intVal int32
			_, err := fmt.Sscanf(val, "%d", &intVal)
			if err != nil {
				return nil, fmt.Errorf("Invalid int32 string: %v", err)
			}
			return intVal, nil
		}
		return nil, fmt.Errorf("Invalid int32 value: %v", p.Value)
	case LONG:
		if val, ok := p.Value.(int64); ok {
			return val, nil
		}
		if val, ok := p.Value.(float64); ok {
			return int64(val), nil
		}
		if val, ok := p.Value.(int); ok {
			return int64(val), nil
		}
		if val, ok := p.Value.(string); ok {
			var intVal int64
			_, err := fmt.Sscanf(val, "%d", &intVal)
			if err != nil {
				return nil, fmt.Errorf("Invalid int64 string: %v", err)
			}
			return intVal, nil
		}
		return nil, fmt.Errorf("Invalid int64 value: %v", p.Value)
	case FLOAT:
		if val, ok := p.Value.(float32); ok {
			return val, nil
		}
		if val, ok := p.Value.(float64); ok {
			return float32(val), nil
		}
		if val, ok := p.Value.(int); ok {
			return float32(val), nil
		}
		if val, ok := p.Value.(string); ok {
			var floatVal float32
			_, err := fmt.Sscanf(val, "%f", &floatVal)
			if err != nil {
				return nil, fmt.Errorf("Invalid float32 string: %v", err)
			}
			return floatVal, nil
		}
		return nil, fmt.Errorf("Invalid float32 value: %v", p.Value)
	case DOUBLE:
		if val, ok := p.Value.(float64); ok {
			return val, nil
		}
		if val, ok := p.Value.(float32); ok {
			return float64(val), nil
		}
		if val, ok := p.Value.(int); ok {
			return float64(val), nil
		}
		if val, ok := p.Value.(string); ok {
			var floatVal float64
			_, err := fmt.Sscanf(val, "%f", &floatVal)
			if err != nil {
				return nil, fmt.Errorf("Invalid float64 string: %v", err)
			}
			return floatVal, nil
		}
		return nil, fmt.Errorf("Invalid float64 value: %v", p.Value)
	case BOOL:
		if val, ok := p.Value.(bool); ok {
			return val, nil
		}
		if val, ok := p.Value.(string); ok {
			switch val {
			case "true", "True", "TRUE", "1":
				return true, nil
			case "false", "False", "FALSE", "0":
				return false, nil
			}
		}
		return nil, fmt.Errorf("Invalid bool value: %v", p.Value)
	case STRING:
		if val, ok := p.Value.(string); ok {
			return val, nil
		}
		return fmt.Sprintf("%v", p.Value), nil
	case BYTES:
		if val, ok := p.Value.([]byte); ok {
			return val, nil
		}
		if val, ok := p.Value.(string); ok {
			decoded, err := base64.StdEncoding.DecodeString(val)
			if err != nil {
				return nil, fmt.Errorf("Invalid base64: %w", err)
			}
			return decoded, nil
		}
		return nil, fmt.Errorf("Invalid bytes_base64 value: %v", p.Value)
	case DATE, DATETIME:
		if val, ok := p.Value.(time.Time); ok {
			return val, nil
		}
		if val, ok := p.Value.(string); ok {
			t, err := time.Parse(time.RFC3339, val)
			if err != nil {
				return nil, fmt.Errorf("Invalid RFC3339 timestamp: %w", err)
			}
			return t, nil
		}
		return nil, fmt.Errorf("Invalid timestamp_rfc3339 value: %v", p.Value)
	default:
		return nil, fmt.Errorf("Unknown param type: %s", p.Type)
	}
}

// BeginTxRequest represents the request body for /v1/rdb/tx/begin
type TxRequestParams struct {
	IsolationLevel  *string `json:"isolationLevel,omitempty"`
	MaxTxTimeoutSec *int    `json:"maxTxTimeoutSec,omitempty"`
}

// BeginTxResponse represents the response for /v1/rdb/tx/begin
type BeginTxResponse struct {
	TxID      string    `json:"txId"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// OkResponse represents a simple OK response
type OkResponse struct {
	OK bool `json:"ok"`
}

// BeginTx handles POST /rdb/tx/begin.
// It parses the body (optional isolationLevel, maxTxTimeoutSec) and the
// datasource index from context (set by balancer). It starts a transaction
// on that datasource and responds with txId and expiresAt. On timeout it
// returns 408; on parse or begin failure it returns 400 or 5xx with an
// error code.
func (rh *RequestHandler) BeginTx(w http.ResponseWriter, r *http.Request) {
	dsIDX, req, isolationLevel, err := parseTxRequest(r)
	if err != nil {
		ResponseError(w, r, RP_BAD_REQUEST, err.Error())
		return
	}

	global.GetCtxLogger(r.Context()).Debug("TxHandler", "detail", "BeginTx", "dsIDX", dsIDX, "isolationLevel", isolationLevel, "maxTxTimeoutSec", req.MaxTxTimeoutSec)

	// Begin transaction (default isolation level: ReadCommitted)
	txEntry, err := rh.dsManager.BeginTx(r.Context(), dsIDX, isolationLevel, req.MaxTxTimeoutSec)
	if err != nil {
		if err == context.DeadlineExceeded {
			ResponseError(w, r, RP_CLIENT_CANCELLED, "Request timeout for BeginTx")
			return
		}
		ResponseError(w, r, RP_DATASOURCE_EXCEPTION, err.Error())
		return
	}

	// Write response
	response := BeginTxResponse{
		TxID:      txEntry.TxID,
		ExpiresAt: txEntry.ExpiresAt,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// CommitTx handles PUT /rdb/tx/commit.
// The transaction ID is taken from the request header. It commits the
// transaction in DsManager. Returns 409 if the transaction is not found,
// or 5xx on DB error. On success responds with {"ok": true}.
func (rh *RequestHandler) CommitTx(w http.ResponseWriter, r *http.Request) {
	txID := getTxID(r)

	if txID == "" {
		ResponseError(w, r, RP_BAD_REQUEST, "TxId is required")
		return
	}

	global.GetCtxLogger(r.Context()).Debug("TxHandler", "detail", "CommitTx", "txID", txID)

	// Commit transaction
	err := rh.dsManager.CommitTx(txID)
	if err != nil {
		if err == ErrDsNotFound {
			ResponseError(w, r, RP_DATASOURCE_NOT_FOUND, "Datasource not found for CommitTx")
			return
		}
		if err == ErrTxNotFound {
			ResponseError(w, r, RP_DATASOURCE_TX_NOT_FOUND, "Transaction not found for CommitTx")
			return
		}
		if err == context.DeadlineExceeded {
			ResponseError(w, r, RP_CLIENT_CANCELLED, "Request timeout for CommitTx")
			return
		}
		ResponseError(w, r, RP_DATASOURCE_EXCEPTION, err.Error())
		return
	}

	// Write response
	response := OkResponse{
		OK: true,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// RollbackTx handles PUT /rdb/tx/rollback.
// The transaction ID is taken from the request header. It rolls back the
// transaction in DsManager. Returns 409 if not found, 5xx on DB error.
// On success responds with {"ok": true}.
func (rh *RequestHandler) RollbackTx(w http.ResponseWriter, r *http.Request) {
	txID := getTxID(r)

	if txID == "" {
		ResponseError(w, r, RP_BAD_REQUEST, "TxId is required")
		return
	}

	global.GetCtxLogger(r.Context()).Debug("TxHandler", "detail", "RollbackTx", "txID", txID)

	// Rollback transaction
	err := rh.dsManager.RollbackTx(txID)
	if err != nil {
		if err == ErrDsNotFound {
			ResponseError(w, r, RP_DATASOURCE_NOT_FOUND, "Datasource not found for RollbackTx")
			return
		}
		if err == ErrTxNotFound {
			ResponseError(w, r, RP_DATASOURCE_TX_NOT_FOUND, "Transaction not found for RollbackTx")
			return
		}
		if err == context.DeadlineExceeded {
			ResponseError(w, r, RP_CLIENT_CANCELLED, "Request timeout for RollbackTx")
			return
		}
		ResponseError(w, r, RP_DATASOURCE_EXCEPTION, err.Error())
		return
	}

	// Write response
	response := OkResponse{
		OK: true,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// CloseTx handles PUT /rdb/tx/close.
// The transaction ID is taken from the request header. It removes the
// transaction from the manager, releases semaphores, and closes the
// connection (no commit or rollback). Returns 409 if not found. On
// success responds with {"ok": true}.
func (rh *RequestHandler) CloseTx(w http.ResponseWriter, r *http.Request) {
	txID := getTxID(r)

	if txID == "" {
		ResponseError(w, r, RP_BAD_REQUEST, "TxId is required")
		return
	}

	global.GetCtxLogger(r.Context()).Debug("TxHandler", "detail", "CloseTx", "txID", txID)

	// Close transaction
	err := rh.dsManager.CloseTx(txID)
	if err != nil {
		if err == ErrDsNotFound {
			ResponseError(w, r, RP_DATASOURCE_NOT_FOUND, "Datasource not found for CloseTx")
			return
		}
		if err == ErrTxNotFound {
			ResponseError(w, r, RP_DATASOURCE_TX_NOT_FOUND, "Transaction not found for CloseTx")
			return
		}
		if err == context.DeadlineExceeded {
			ResponseError(w, r, RP_CLIENT_CANCELLED, "Request timeout for CloseTx")
			return
		}
		ResponseError(w, r, RP_DATASOURCE_EXCEPTION, err.Error())
		return
	}

	// Write response
	response := OkResponse{
		OK: true,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// parseTxRequest reads the JSON body into TxRequestParams, gets the
// datasource index from the request context (set by balancer), and
// optionally maps isolationLevel string to sql.IsolationLevel. Empty body
// is allowed. Returns error on decode failure, missing ds index, or
// invalid isolation level.
func parseTxRequest(r *http.Request) (int, *TxRequestParams, *sql.IsolationLevel, error) {
	if r.Body != nil {
		defer func() {
			io.Copy(io.Discard, r.Body)
			r.Body.Close()
		}()
	}

	var req TxRequestParams
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return -1, nil, nil, fmt.Errorf("Failed to parse request: %w", err)
	}

	// get datasourceId from context
	dsIDX, ok := GetCtxDsIdx(r)
	if !ok {
		return -1, nil, nil, fmt.Errorf("Datasource INDEX hasn't decided by Balancer")
	}

	if req.IsolationLevel == nil {
		return dsIDX, &req, nil, nil
	}

	isolationLevel := sql.LevelDefault
	switch *req.IsolationLevel {
	case "READ_UNCOMMITTED":
		isolationLevel = sql.LevelReadUncommitted
	case "READ_COMMITTED":
		isolationLevel = sql.LevelReadCommitted
	case "REPEATABLE_READ":
		isolationLevel = sql.LevelRepeatableRead
	case "SERIALIZABLE":
		isolationLevel = sql.LevelSerializable
	default:
		return -1, nil, nil, fmt.Errorf("Invalid isolation level: %s. (expected: READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE)", *req.IsolationLevel)
	}

	return dsIDX, &req, &isolationLevel, nil
}

// getTxID returns the value of the transaction ID header (HEADER_TX_ID).
func getTxID(r *http.Request) string {
	return r.Header.Get(HEADER_TX_ID)
}

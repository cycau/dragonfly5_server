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
	"github.com/shopspring/decimal"
)

type RequestParams struct {
	SQL        string       `json:"sql"`
	Params     []ParamValue `json:"params,omitempty"`
	TimeoutSec int          `json:"timeoutSec,omitempty"`
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
	DOUBLE   ValueType = "DOUBLE"
	DECIMAL  ValueType = "DECIMAL"
	BOOL     ValueType = "BOOL"
	DATE     ValueType = "DATE"
	DATETIME ValueType = "DATETIME"
	STRING   ValueType = "STRING"
	BINARY   ValueType = "BINARY"
)

// ParamValue represents a parameter value
type ParamValue struct {
	Type  ValueType `json:"type"`
	Value any       `json:"value,omitempty"`
}

// ExecuteResponse represents the response for /v1/rdb/execute
type QueryResponse struct {
	Meta          []ColumnMeta `json:"meta,omitempty"`
	Rows          []any        `json:"rows"`
	TotalCount    int          `json:"totalCount"`
	ElapsedTimeUs int64        `json:"elapsedTimeUs"`
}

type ExecuteResponse struct {
	EffectedRows  int64 `json:"effectedRows"`
	ElapsedTimeUs int64 `json:"elapsedTimeUs"`
}

// ColumnMeta contains metadata about a column
type ColumnMeta struct {
	Name     string `json:"name"`
	DBType   string `json:"dbType"`
	Nullable bool   `json:"nullable"`
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
type DmlHandler struct {
	dsManager  *DsManager
	statsInfos []*StatsInfo
}

// NewDmlHandler constructs a DmlHandler that uses the given DsManager and
// allocates one StatsInfo per datasource. Each StatsInfo has a Prometheus
// summary for p95 latency and rate counters (STAT_WINDOW_INTERVAL) for
// total requests, errors, and timeouts, used for health and balancer scoring.
func NewDmlHandler(dsManager *DsManager) *DmlHandler {
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
	return &DmlHandler{
		dsManager:  dsManager,
		statsInfos: statsInfos,
	}
}

// Query handles POST /rdb/query. The datasource index is taken from context
// (set by balancer). It parses the JSON body (SQL, params, timeoutSec,
// limitRows), runs a read-only query via DsManager.Query, then streams the
// result as JSON (meta, rows, totalCount, elapsedTimeUs). On timeout or
// error it updates stats and returns an appropriate HTTP error.
func (dh *DmlHandler) Query(w http.ResponseWriter, r *http.Request) {
	dsIDX, ok := GetCtxDsIdx(r)
	if !ok {
		ResponseError(w, r, RP_BAD_REQUEST, "Datasource INDEX hasn't decided by Balancer")
		return
	}

	startTime := time.Now()
	req, parameters, err := dh.parseRequest(r)
	if err != nil {
		ResponseError(w, r, RP_BAD_REQUEST, err.Error())
		return
	}

	global.GetCtxLogger(r.Context()).Debug("DmlHandler", "detail", "Executing Query", "dsIDX", dsIDX, "sql", req.SQL, "params", parameters)

	// Execute query without transaction
	rows, releaseResource, err := dh.dsManager.Query(r.Context(), req.TimeoutSec, dsIDX, req.SQL, parameters...)
	if releaseResource != nil {
		defer releaseResource()
	}
	if err != nil {
		if err == ErrDsNotFound {
			ResponseError(w, r, RP_DATASOURCE_NOT_FOUND, "Datasource not found for Query")
			return
		}
		if err == context.DeadlineExceeded {
			dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), false, true)
			ResponseError(w, r, RP_CLIENT_CANCELLED, "Request timeout for Query")
			return
		}
		dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
		ResponseError(w, r, RP_DATASOURCE_EXCEPTION, err.Error())
		return
	}
	defer rows.Close()

	if err := dh.responseQueryResult(w, rows, req.LimitRows, startTime); err != nil {
		ResponseError(w, r, RP_SERVER_EXCEPTION, err.Error())
		dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
		return
	}

	dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), false, false)
}

// QueryTx handles POST /rdb/tx/query. The transaction ID is taken from the
// request header. It parses the body, runs the query in that transaction
// via DsManager.QueryTx, then streams the result as JSON. On timeout or
// error it updates stats and returns an appropriate HTTP error.
func (dh *DmlHandler) QueryTx(w http.ResponseWriter, r *http.Request) {
	txID := r.Header.Get(HEADER_TX_ID)
	if txID == "" {
		ResponseError(w, r, RP_BAD_REQUEST, "TxID is required")
		return
	}

	startTime := time.Now()
	req, parameters, err := dh.parseRequest(r)
	if err != nil {
		ResponseError(w, r, RP_BAD_REQUEST, err.Error())
		return
	}

	global.GetCtxLogger(r.Context()).Debug("DmlHandler", "detail", "Executing QueryTx", "txID", txID, "sql", req.SQL, "params", parameters)

	// Execute query in transaction
	rows, releaseResource, dsIDX, err := dh.dsManager.QueryTx(r.Context(), req.TimeoutSec, txID, req.SQL, parameters...)
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
			dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), false, true)
			ResponseError(w, r, RP_CLIENT_CANCELLED, "Request timeout for QueryTx")
			return
		}
		dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
		global.ResponseError(w, r, RP_DATASOURCE_EXCEPTION, err.Error())
		return
	}
	defer rows.Close()

	if err := dh.responseQueryResult(w, rows, req.LimitRows, startTime); err != nil {
		ResponseError(w, r, RP_SERVER_EXCEPTION, err.Error())
		dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
		return
	}

	dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), false, false)
}

// responseQueryResult reads all rows (up to limitRows), builds column
// metadata and JSON-serializable row values ([]byte→base64, time→RFC3339,
// decimal→string), and writes a QueryResponse (meta, rows, totalCount,
// elapsedTimeUs) as JSON. Returns an error on column/scan/iteration failure.
func (dh *DmlHandler) responseQueryResult(w http.ResponseWriter, rows *sql.Rows, limitRows int, startTime time.Time) error {

	// Get column information
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("Failed to get columns: %w", err)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("Failed to get column types: %w", err)
	}

	// Build column metadata
	columnMeta := make([]ColumnMeta, len(columns))
	for i, colType := range columnTypes {
		nullable, _ := colType.Nullable()
		columnMeta[i] = ColumnMeta{
			Name:     columns[i],
			DBType:   colType.DatabaseTypeName(),
			Nullable: nullable,
		}
	}

	limit := math.MaxInt32
	if limitRows > 0 {
		limit = limitRows
	}

	// Read rows
	var resultRows []any
	rowCount := 0
	for rows.Next() {
		if rowCount >= limit {
			rowCount++
			continue
		}

		// Create slice for scanning
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("Failed to scan row: %w", err)
		}

		for i, val := range values {
			if val == nil {
				continue
			}

			switch v := val.(type) {
			case []byte:
				values[i] = base64.StdEncoding.EncodeToString(v)
			case time.Time:
				values[i] = v.Format(time.RFC3339)
			case decimal.Decimal:
				values[i] = v.String()
			default:
				values[i] = v
			}
		}

		resultRows = append(resultRows, values)
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("Rows iteration error: %w", err)
	}

	// Write response
	response := QueryResponse{
		Meta:          columnMeta,
		Rows:          resultRows,
		TotalCount:    rowCount,
		ElapsedTimeUs: time.Since(startTime).Microseconds(),
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
	return nil
}

// Execute handles POST /rdb/execute. The datasource index is taken from
// context. It parses the body, runs the statement via DsManager.Execute,
// then responds with effectedRows and elapsedTimeUs. On timeout or error
// it updates stats and returns an appropriate HTTP error.
func (dh *DmlHandler) Execute(w http.ResponseWriter, r *http.Request) {
	dsIDX, ok := GetCtxDsIdx(r)
	if !ok {
		ResponseError(w, r, RP_BAD_REQUEST, "Datasource INDEX hasn't decided by Balancer")
		return
	}

	startTime := time.Now()
	req, parameters, err := dh.parseRequest(r)
	if err != nil {
		ResponseError(w, r, RP_BAD_REQUEST, err.Error())
		return
	}
	global.GetCtxLogger(r.Context()).Debug("DmlHandler", "detail", "Executing Execute", "dsIDX", dsIDX, "sql", req.SQL, "params", parameters)

	// Get database
	result, releaseResource, err := dh.dsManager.Execute(r.Context(), req.TimeoutSec, dsIDX, req.SQL, parameters...)
	if releaseResource != nil {
		defer releaseResource()
	}
	if err != nil {
		if err == ErrDsNotFound {
			ResponseError(w, r, RP_DATASOURCE_NOT_FOUND, "Datasource not found for Execute")
			return
		}
		if err == context.DeadlineExceeded {
			dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), false, true)
			ResponseError(w, r, RP_CLIENT_CANCELLED, "Request timeout for Execute")
			return
		}
		dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
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
	dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), false, false)

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
func (dh *DmlHandler) ExecuteTx(w http.ResponseWriter, r *http.Request) {
	txID := r.Header.Get(HEADER_TX_ID)
	if txID == "" {
		ResponseError(w, r, RP_BAD_REQUEST, "TxID is required")
		return
	}

	startTime := time.Now()

	req, parameters, err := dh.parseRequest(r)
	if err != nil {
		ResponseError(w, r, RP_BAD_REQUEST, err.Error())
		return
	}
	global.GetCtxLogger(r.Context()).Debug("DmlHandler", "detail", "Executing ExecuteTx", "txID", txID, "sql", req.SQL, "params", parameters)

	// Execute in transaction
	result, releaseResource, dsIDX, err := dh.dsManager.ExecuteTx(r.Context(), req.TimeoutSec, txID, req.SQL, parameters...)
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
			dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), false, true)
			ResponseError(w, r, RP_CLIENT_CANCELLED, "Request timeout for ExecuteTx")
			return
		}
		dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), true, false)
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
	dh.statsSetResult(dsIDX, time.Since(startTime).Milliseconds(), false, false)

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
func (dh *DmlHandler) statsSetResult(datasourceIdx int, latencyMs int64, isError bool, isTimeout bool) {
	if datasourceIdx < 0 {
		return // When invalid TxID
	}

	stats := dh.statsInfos[datasourceIdx]
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
func (dh *DmlHandler) StatsGet(datasourceIdx int) (latencyP95Ms int, errorRate1m float64, timeoutRate1m float64) {
	stats := dh.statsInfos[datasourceIdx]
	stats.mu.Lock()
	defer stats.mu.Unlock()

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

	return int(p95), errorRate1m, timeoutRate1m
}

// parseRequest and param conversion helpers (used by DmlHandler).

// parseRequest reads the JSON body into RequestParams (SQL, Params,
// TimeoutSec, LimitRows), converts Params to driver values via
// convertParams, and overrides TimeoutSec from HEADER_TIMEOUT_SEC if
// present. The body is consumed and closed. Returns an error if JSON
// decode fails, SQL is empty, or param conversion fails.
func (dh *DmlHandler) parseRequest(r *http.Request) (request *RequestParams, params []any, err error) {
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
		if val, ok := p.Value.(int64); ok {
			return int32(val), nil
		}
		if val, ok := p.Value.(string); ok {
			var intVal int
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
		if val, ok := p.Value.(int32); ok {
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
	case DOUBLE:
		if val, ok := p.Value.(float32); ok {
			return val, nil
		}
		if val, ok := p.Value.(float64); ok {
			return val, nil
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
	case DECIMAL:
		if val, ok := p.Value.(decimal.Decimal); ok {
			return val, nil
		}
		if val, ok := p.Value.(string); ok {
			dec, err := decimal.NewFromString(val)
			if err != nil {
				return nil, fmt.Errorf("Invalid decimal string: %w", err)
			}
			return dec, nil
		}
		return nil, fmt.Errorf("Invalid decimal value: %v", p.Value)
	case BOOL:
		if val, ok := p.Value.(bool); ok {
			return val, nil
		}
		if val, ok := p.Value.(string); ok {
			switch val {
			case "true":
				return true, nil
			case "false":
				return false, nil
			}
		}
		return nil, fmt.Errorf("Invalid bool value: %v", p.Value)
	case STRING:
		if val, ok := p.Value.(string); ok {
			return val, nil
		}
		return fmt.Sprintf("%v", p.Value), nil
	case BINARY:
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

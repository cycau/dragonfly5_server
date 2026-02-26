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
	. "dragonfly5/server/global"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

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

// TxHandler handles transaction API requests
type TxHandler struct {
	dsManager *DsManager
}

// NewTxHandler creates a new TxHandler
func NewTxHandler(dsManager *DsManager) *TxHandler {
	return &TxHandler{
		dsManager: dsManager,
	}
}

// BeginTx handles POST /rdb/tx/begin.
// It parses the body (optional isolationLevel, maxTxTimeoutSec) and the
// datasource index from context (set by balancer). It starts a transaction
// on that datasource and responds with txId and expiresAt. On timeout it
// returns 408; on parse or begin failure it returns 400 or 5xx with an
// error code.
func (th *TxHandler) BeginTx(w http.ResponseWriter, r *http.Request) {
	dsIDX, req, isolationLevel, err := th.parseBeginRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Sprintf("Failed to parse request: %v", err))
		return
	}

	// Begin transaction (default isolation level: ReadCommitted)
	txEntry, err := th.dsManager.BeginTx(r.Context(), dsIDX, isolationLevel, req.MaxTxTimeoutSec)
	if err != nil {
		if err == context.DeadlineExceeded {
			writeError(w, http.StatusRequestTimeout, "TIMEOUT", "Request timeout")
			return
		}
		writeError(w, statusCodeForDbError(err), "BEGIN_ERROR", fmt.Sprintf("Failed to begin transaction: %v", err))
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
func (th *TxHandler) CommitTx(w http.ResponseWriter, r *http.Request) {
	txID := getTxID(r)

	if txID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "txId is required")
		return
	}

	// Commit transaction
	err := th.dsManager.CommitTx(txID)
	if err != nil {
		if err == ErrTxNotFound {
			writeError(w, http.StatusConflict, "TX_NOT_FOUND", "Transaction not found")
			return
		}
		writeError(w, statusCodeForDbError(err), "COMMIT_ERROR", fmt.Sprintf("Failed to commit transaction: %v", err))
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
func (th *TxHandler) RollbackTx(w http.ResponseWriter, r *http.Request) {
	txID := getTxID(r)

	if txID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "txId is required")
		return
	}

	// Rollback transaction
	err := th.dsManager.RollbackTx(txID)
	if err != nil {
		if err == ErrTxNotFound {
			writeError(w, http.StatusConflict, "TX_NOT_FOUND", "Transaction not found")
			return
		}
		writeError(w, statusCodeForDbError(err), "ROLLBACK_ERROR", fmt.Sprintf("Failed to rollback transaction: %v", err))
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
func (th *TxHandler) CloseTx(w http.ResponseWriter, r *http.Request) {
	txID := getTxID(r)

	if txID == "" {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "txId is required")
		return
	}

	// Close transaction
	err := th.dsManager.CloseTx(txID)
	if err != nil {
		if err == ErrTxNotFound {
			writeError(w, http.StatusConflict, "TX_NOT_FOUND", "Transaction not found")
			return
		}
		writeError(w, statusCodeForDbError(err), "CLOSE_ERROR", fmt.Sprintf("Failed to close transaction: %v", err))
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

// parseBeginRequest reads the JSON body into TxRequestParams, gets the
// datasource index from the request context (set by balancer), and
// optionally maps isolationLevel string to sql.IsolationLevel. Empty body
// is allowed. Returns error on decode failure, missing ds index, or
// invalid isolation level.
func (th *TxHandler) parseBeginRequest(r *http.Request) (int, *TxRequestParams, *sql.IsolationLevel, error) {
	if r.Body != nil {
		defer func() {
			io.Copy(io.Discard, r.Body)
			r.Body.Close()
		}()
	}

	var req TxRequestParams
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return -1, nil, nil, fmt.Errorf("failed to parse request: %w", err)
	}

	// get datasourceId from context
	dsIDX, ok := GetCtxDsIdx(r)
	if !ok {
		return -1, nil, nil, fmt.Errorf("datasource INDEX is required")
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
		return -1, nil, nil, fmt.Errorf("invalid isolation level: %s", *req.IsolationLevel)
	}

	return dsIDX, &req, &isolationLevel, nil
}

// getTxID returns the value of the transaction ID header (HEADER_TX_ID).
func getTxID(r *http.Request) string {
	return r.Header.Get(HEADER_TX_ID)
}

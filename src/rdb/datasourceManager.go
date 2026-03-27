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
	"dragonfly5/server/global"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

var (
	ErrDsNotFound  = errors.New("datasource not found")
	ErrTxNotFound  = errors.New("transaction not found")
	ErrTxException = errors.New("transaction exception")
)

// TxEntry represents one active transaction: a dedicated connection and
// sql.Tx, with TxID, expiration time, and optional idle timeout for
// extending expiry on activity. executing is used by cleanup to skip
// in-use transactions.
type TxEntry struct {
	TxID        string
	executing   bool
	ExpiresAt   time.Time
	idleTimeout *time.Duration
	conn        *sql.Conn
	tx          *sql.Tx
}

// QueryContext runs a read-only query in this transaction's sql.Tx.
func (e *TxEntry) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	rows, err := e.tx.QueryContext(ctx, query, args...)
	return rows, err
}

// ExecContext executes a statement in this transaction's sql.Tx.
func (e *TxEntry) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	result, err := e.tx.ExecContext(ctx, query, args...)
	return result, err
}

// commit calls Commit on the underlying Tx and closes the connection.
// On Commit failure, the connection is still closed to prevent leaks.
func (e *TxEntry) commit() error {
	err := e.tx.Commit()
	closeErr := e.conn.Close()
	if err != nil {
		return err
	}
	return closeErr
}

// rollback calls Rollback on the underlying Tx and closes the connection.
// On Rollback failure, the connection is still closed to prevent leaks.
func (e *TxEntry) rollback() error {
	err := e.tx.Rollback()
	closeErr := e.conn.Close()
	if err != nil {
		return err
	}
	return closeErr
}

// TxDatasource extends Datasource with per-datasource concurrency limits:
// semaphores for read (MaxConns - MinWriteConns), write (MaxWriteConns),
// and tx (70% of MaxWriteConns), plus a map of active TxEntry by txID and
// running counters for health/stats.
type TxDatasource struct {
	Datasource
	MaxTxIdleTimeout time.Duration

	idx      int
	mu       sync.Mutex
	capRead  int64
	capWrite int64
	capTx    int64
	semRead  *semaphore.Weighted
	semWrite *semaphore.Weighted
	semTx    *semaphore.Weighted
	entries  map[string]*TxEntry

	runningRead  atomic.Int32
	runningWrite atomic.Int32
	runningTx    atomic.Int32
}

// ResourceType identifies which semaphore and running counter to use when
// allocating capacity: read (SELECT), write (INSERT/UPDATE/DELETE), or
// transaction (BeginTx; uses both tx and write semaphores).
type ResourceType int

const (
	RESOURCE_TYPE_READ ResourceType = iota
	RESOURCE_TYPE_WRITE
	RESOURCE_TYPE_TX
)

// ReleaseResourceFunc is a callback invoked when a read, write, or
// transaction operation finishes. It must release the semaphore and
// decrement the corresponding running count (and for tx, optionally
// refresh the entry's expiry).
type ReleaseResourceFunc func()

// DsManager holds all TxDatasources, the transaction ID generator, and
// the background cleanup ticker. It provides Query, Execute, BeginTx,
// getTx, CommitTx, RollbackTx, CloseTx, and stats.
type DsManager struct {
	dss               []*TxDatasource
	txIDGen           *global.TxIDGenerator
	stopCleanupTicker chan struct{}
	wg                sync.WaitGroup
}

// NewDsManager builds a DsManager from the given datasource configs.
// For each config it creates a Datasource and wraps it in a TxDatasource
// with semaphores and entry map, then starts the background cleanup
// goroutine (startCleanupTicker). Panics if any datasource fails to init.
func NewDsManager(configs []global.DatasourceConfig) *DsManager {
	dss := make([]*TxDatasource, len(configs))

	for i, config := range configs {
		ds, err := NewDatasource(config)
		if err != nil {
			panic(fmt.Sprintf("failed to initialize datasource %s: %v", config.DatasourceID, err))
		}

		capRead := int64(config.MaxConns - config.MinWriteConns)
		capWrite := int64(config.MaxWriteConns)
		capTx := int64(config.MaxWriteConns * 7 / 10) // 70%

		txDs := &TxDatasource{
			Datasource:       *ds,
			MaxTxIdleTimeout: time.Duration(config.MaxTxIdleTimeoutSec) * time.Second,

			idx:      i,
			capRead:  capRead,
			capWrite: capWrite,
			capTx:    capTx,
			semRead:  semaphore.NewWeighted(capRead),
			semWrite: semaphore.NewWeighted(capWrite),
			semTx:    semaphore.NewWeighted(capTx),
			entries:  make(map[string]*TxEntry),
		}
		dss[i] = txDs

		fmt.Printf("### [Datasource] %d\n", i)
		fmt.Printf("     DatasourceID: %s\n", config.DatasourceID)
		fmt.Printf("     DatabaseName: %s\n", config.DatabaseName)
		fmt.Printf("     Driver: %s\n", config.Driver)
		fmt.Printf("     MaxConns: %d\n", config.MaxConns)
		fmt.Printf("     MaxWriteConns: %d\n", config.MaxWriteConns)
		fmt.Printf("     MinWriteConns: %d\n", config.MinWriteConns)
		fmt.Printf("     MaxConnLifetimeSec: %d\n", config.MaxConnLifetimeSec)
		fmt.Printf("     MaxTxIdleTimeoutSec: %d\n", config.MaxTxIdleTimeoutSec)
		fmt.Printf("     DefaultQueryTimeoutSec: %d\n", config.DefaultQueryTimeoutSec)
	}
	dm := &DsManager{
		dss:               dss,
		txIDGen:           global.NewTxIDGenerator(),
		stopCleanupTicker: make(chan struct{}),
	}

	// Start background cleanup goroutine
	dm.wg.Add(1)
	go dm.startCleanupTicker()

	return dm
}

// allocateSemaphore acquires one unit of the given resource type for the
// datasource at datasourceIdx: for READ it acquires semRead and increments
// runningRead; for WRITE, semWrite and runningWrite; for TX it acquires
// both semTx and semWrite and increments runningTx. On Acquire failure it
// decrements the count and returns an error. Returns the TxDatasource so
// the caller can run the operation and then call the release callback.
func (dm *DsManager) allocateSemaphore(ctx context.Context, datasourceIdx int, resourceType ResourceType) (*TxDatasource, error) {
	ds := dm.dss[datasourceIdx]
	if ds == nil {
		global.GetCtxLogger(ctx).Error("DatasourceManager", "detail", "Datasource not found", "datasourceIdx", datasourceIdx)
		return nil, ErrDsNotFound
	}

	switch resourceType {
	case RESOURCE_TYPE_READ:
		if ds.capRead < 1 {
			return nil, ErrDsException
		}
		ds.runningRead.Add(1)
	case RESOURCE_TYPE_WRITE:
		if ds.capWrite < 1 {
			return nil, ErrDsException
		}
		ds.runningWrite.Add(1)
	case RESOURCE_TYPE_TX:
		if ds.capTx < 1 {
			return nil, ErrDsException
		}
		ds.runningTx.Add(1)
	}

	switch resourceType {
	case RESOURCE_TYPE_READ:
		err := ds.semRead.Acquire(ctx, 1)
		if err != nil {
			ds.runningRead.Add(-1)
			global.GetCtxLogger(ctx).Error("DatasourceManager", "detail", "Failed to acquire read semaphore", "err", err)
			return nil, err
		}
	case RESOURCE_TYPE_WRITE:
		err := ds.semWrite.Acquire(ctx, 1)
		if err != nil {
			ds.runningWrite.Add(-1)
			global.GetCtxLogger(ctx).Error("DatasourceManager", "detail", "Failed to acquire write semaphore", "err", err)
			return nil, err
		}
	case RESOURCE_TYPE_TX:
		err := ds.semTx.Acquire(ctx, 1)
		if err != nil {
			ds.runningTx.Add(-1)
			global.GetCtxLogger(ctx).Error("DatasourceManager", "detail", "Failed to acquire transaction semaphore", "err", err)
			return nil, err
		}
		err = ds.semWrite.Acquire(ctx, 1)
		if err != nil {
			ds.runningTx.Add(-1)
			ds.semTx.Release(1)
			global.GetCtxLogger(ctx).Error("DatasourceManager", "detail", "Failed to acquire transaction semaphore", "err", err)
			return nil, err
		}
	}

	return ds, nil
}

// StatsGet returns the current running read, write, and transaction counts
// for the datasource at the given index. Used by health/balancer. If the
// index is invalid it returns 999999 for all three to avoid being chosen.
func (dm *DsManager) StatsGet(datasourceIdx int) (runningRead int32, runningWrite int32, runningTx int32) {
	ds := dm.dss[datasourceIdx]
	if ds == nil {
		return 999999, 999999, 999999
	}

	return ds.runningRead.Load(), ds.runningWrite.Load(), ds.runningTx.Load()
}

// BeginTx starts a new transaction on the datasource at datasourceIdx.
// It generates a txID, acquires tx (and write) semaphores, creates a
// connection and Tx with the given isolation level, then builds a TxEntry
// with optional maxTxTimeoutSec (if set, expiry is fixed; otherwise
// idleTimeout is used to extend on activity). The entry is registered and
// returned. Caller must use CommitTx/RollbackTx and CloseTx to release.
func (dm *DsManager) BeginTx(ctx context.Context, datasourceIdx int, isolationLevel *sql.IsolationLevel, maxTxTimeoutSec *int) (*TxEntry, error) {
	txID, err := dm.txIDGen.Generate(datasourceIdx)
	if err != nil {
		return nil, err
	}

	ds, err := dm.allocateSemaphore(ctx, datasourceIdx, RESOURCE_TYPE_TX)
	if err != nil {
		return nil, err
	}

	conn, tx, err := ds.newTx(isolationLevel)
	if err != nil {
		ds.runningTx.Add(-1)

		ds.semTx.Release(1)
		ds.semWrite.Release(1)
		global.GetCtxLogger(ctx).Error("DatasourceManager", "detail", "Failed to start transaction", "err", err)
		return nil, ErrTxException
	}

	idleTimeout := &ds.MaxTxIdleTimeout
	expiresAt := time.Now().Add(ds.MaxTxIdleTimeout)
	if maxTxTimeoutSec != nil && *maxTxTimeoutSec > 0 {
		expiresAt = time.Now().Add(time.Duration(*maxTxTimeoutSec) * time.Second)
		idleTimeout = nil
	}
	// Create entry
	entry := &TxEntry{
		TxID:        txID,
		executing:   false,
		idleTimeout: idleTimeout,
		ExpiresAt:   expiresAt,
		conn:        conn,
		tx:          tx,
	}

	// Register entry
	ds.mu.Lock()
	ds.entries[entry.TxID] = entry
	ds.mu.Unlock()

	global.GetCtxLogger(ctx).Debug("DatasourceManager", "detail", "Registered transaction", "txID", txID, "total entries", len(ds.entries))

	return entry, nil
}

// getTx resolves the datasource index from the txID (GetDsIdxFromTxID),
// looks up the TxDatasource, and retrieves the entry from the entries map.
// Marks the entry as executing. Returns the entry, TxDatasource, and error
// (ErrTxNotFound or ErrDsNotFound).
func (dm *DsManager) getTx(txID string) (entry *TxEntry, datasource *TxDatasource, err error) {
	dsIdx, err := global.GetDsIdxFromTxID(txID)
	if err != nil {
		return nil, nil, err
	}

	ds := dm.dss[dsIdx]
	if ds == nil {
		return nil, nil, ErrDsNotFound
	}

	ds.mu.Lock()
	defer ds.mu.Unlock()
	entry, ok := ds.entries[txID]
	if !ok {
		return nil, ds, ErrTxNotFound
	}

	entry.executing = true
	return entry, ds, nil
}

// CommitTx looks up the transaction by txID, calls commit() on the entry,
// removes the entry, and releases semaphores. Returns ErrTxNotFound or
// ErrTxException on commit failure.
func (dm *DsManager) CommitTx(txID string) error {
	entry, ds, err := dm.getTx(txID)
	if err != nil {
		return err
	}

	commitErr := entry.commit()

	ds.mu.Lock()
	delete(ds.entries, entry.TxID)
	ds.mu.Unlock()
	ds.runningTx.Add(-1)

	ds.semTx.Release(1)
	ds.semWrite.Release(1)

	if commitErr != nil {
		return ErrTxException
	}
	return nil
}

// RollbackTx looks up the transaction by txID, calls rollback() on the entry,
// removes the entry, and releases semaphores. Returns ErrTxNotFound or
// ErrTxException on rollback failure.
func (dm *DsManager) RollbackTx(txID string) error {
	entry, ds, err := dm.getTx(txID)
	if err != nil {
		return err
	}

	rollbackErr := entry.rollback()

	ds.mu.Lock()
	delete(ds.entries, entry.TxID)
	ds.mu.Unlock()
	ds.runningTx.Add(-1)

	ds.semTx.Release(1)
	ds.semWrite.Release(1)

	if rollbackErr != nil {
		return ErrTxException
	}
	return nil
}

// CloseTx looks up the transaction by txID, removes it from the datasource's
// entries map, decrements runningTx, releases semTx and semWrite, then
// rolls back the transaction and closes the connection. Returns
// ErrTxNotFound or ErrTxException on rollback/close failure.
func (dm *DsManager) CloseTx(txID string) error {
	entry, ds, err := dm.getTx(txID)
	if err != nil {
		return err
	}

	ds.mu.Lock()
	delete(ds.entries, entry.TxID)
	ds.mu.Unlock()
	ds.runningTx.Add(-1)

	ds.semTx.Release(1)
	ds.semWrite.Release(1)

	err = entry.rollback()
	if err != nil {
		return ErrTxException
	}

	return nil
}

// Query runs a read-only query on the datasource at datasourceIdx.
// It acquires a read semaphore, runs QueryContext on the TxDatasource,
// and returns the rows plus a ReleaseResourceFunc that decrements
// runningRead and releases the semaphore. timeoutSec is currently unused
// (timeout is applied at the HTTP layer). Caller must call release when
// done reading rows.
func (dm *DsManager) Query(ctx context.Context, timeoutSec int, datasourceIdx int, sql string, parameters ...any) (*sql.Rows, ReleaseResourceFunc, error) {
	ds, err := dm.allocateSemaphore(ctx, datasourceIdx, RESOURCE_TYPE_READ)
	if err != nil {
		return nil, nil, err
	}

	rows, err := ds.QueryContext(ctx, sql, parameters...)

	releaseResourceFunc := func() {
		// cancelCtx()

		ds.runningRead.Add(-1)
		ds.semRead.Release(1)
	}

	return rows, releaseResourceFunc, err
}

// QueryTx runs a read-only query inside the transaction identified by txID.
// It resolves the entry via getTx(txID), runs QueryContext on the
// entry, and returns rows plus a release callback that refreshes the
// entry's expiry (if idleTimeout is set) and clears executing. Also
// returns the datasource index for stats. Caller must call release when
// done reading rows.
func (dm *DsManager) QueryTx(ctx context.Context, timeoutSec int, txID string, sql string, parameters ...any) (*sql.Rows, ReleaseResourceFunc, int, error) {
	entry, ds, err := dm.getTx(txID)
	if err != nil {
		return nil, nil, -1, err
	}

	rows, err := entry.QueryContext(ctx, sql, parameters...)

	releaseResourceFunc := func() {
		ds.mu.Lock()
		if entry.idleTimeout != nil {
			entry.ExpiresAt = time.Now().Add(*entry.idleTimeout)
		}
		entry.executing = false
		ds.mu.Unlock()
	}

	return rows, releaseResourceFunc, ds.idx, err
}

// Execute runs a write statement on the datasource at datasourceIdx.
// It acquires a write semaphore, runs ExecContext on the TxDatasource,
// and returns the result plus a ReleaseResourceFunc that decrements
// runningWrite and releases the semaphore. Caller must call release when
// done.
func (dm *DsManager) Execute(ctx context.Context, timeoutSec int, datasourceIdx int, sql string, parameters ...any) (sql.Result, ReleaseResourceFunc, error) {
	ds, err := dm.allocateSemaphore(ctx, datasourceIdx, RESOURCE_TYPE_WRITE)
	if err != nil {
		return nil, nil, err
	}

	result, err := ds.ExecContext(ctx, sql, parameters...)

	releaseResourceFunc := func() {
		ds.runningWrite.Add(-1)
		ds.semWrite.Release(1)
	}

	return result, releaseResourceFunc, err
}

// ExecuteTx runs a write statement inside the transaction identified by txID.
// It resolves the entry via getTx(txId), runs ExecContext on the
// entry, and returns the result plus a release callback that refreshes
// expiry and clears executing. Also returns the datasource index. Caller
// must call release when done.
func (dm *DsManager) ExecuteTx(ctx context.Context, timeoutSec int, txId string, sql string, parameters ...any) (sql.Result, ReleaseResourceFunc, int, error) {
	entry, ds, err := dm.getTx(txId)
	if err != nil {
		return nil, nil, -1, err
	}

	result, err := entry.ExecContext(ctx, sql, parameters...)

	releaseResourceFunc := func() {
		ds.mu.Lock()
		if entry.idleTimeout != nil {
			entry.ExpiresAt = time.Now().Add(*entry.idleTimeout)
		}
		entry.executing = false
		ds.mu.Unlock()
	}

	return result, releaseResourceFunc, ds.idx, err
}

// startCleanupTicker runs in a goroutine and periodically (every 567ms)
// scans all TxDatasources for entries that are not executing and past
// ExpiresAt. For each such entry it removes from the map, decrements
// runningTx, releases semTx and semWrite, and runs Rollback and
// Conn.Close in a new goroutine. Exits when stopCleanupTicker is closed.
func (dm *DsManager) startCleanupTicker() {
	defer dm.wg.Done()

	ticker := time.NewTicker(567 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-dm.stopCleanupTicker: // stop cleanup
			return
		case <-ticker.C: // cleanup expired transactions
			now := time.Now()

			for _, ds := range dm.dss {
				expiredEntries := make([]*TxEntry, 0)
				ds.mu.Lock()

				for _, entry := range ds.entries {
					if entry == nil {
						continue
					}
					if entry.executing {
						continue
					}
					if now.Before(entry.ExpiresAt) {
						continue
					}
					expiredEntries = append(expiredEntries, entry)
				}

				for _, entry := range expiredEntries {
					delete(ds.entries, entry.TxID)
					ds.runningTx.Add(-1)
					ds.semTx.Release(1)
					ds.semWrite.Release(1)

					go entry.rollback()
					global.GetCtxLogger(context.TODO()).Debug("DatasourceManager", "detail", "Cleaned up expired transaction", "txID", entry.TxID, "runningTx", ds.runningTx.Load())
				}

				ds.mu.Unlock()
			}
		}
	}
}

// Shutdown stops the cleanup ticker (closes stopCleanupTicker and waits
// for the goroutine), then for each TxDatasource rolls back and closes
// all remaining entries and closes the underlying Datasource.
func (dm *DsManager) Shutdown() {
	close(dm.stopCleanupTicker)
	dm.wg.Wait()

	logger := global.GetCtxLogger(context.TODO())
	// Rollback all remaining transactions
	for _, ds := range dm.dss {
		ds.mu.Lock()

		fmt.Printf("Rollback transaction due to shutdown: %d unclosed on %s\n", len(ds.entries), ds.DatasourceID)
		logger.Info("Shutdown", "detail", "Rollback transaction due to shutdown", "unclosed", len(ds.entries), "datasourceID", ds.DatasourceID)
		for _, entry := range ds.entries {
			err := entry.rollback()
			if err != nil {
				fmt.Printf("Failed to rollback transaction: %s\n", entry.TxID)
				logger.Error("Shutdown", "detail", "Failed to rollback transaction", "txID", entry.TxID, "err", err)
			}
		}

		ds.Close()
		ds.mu.Unlock()
	}
}

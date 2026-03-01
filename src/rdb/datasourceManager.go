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
	"time"

	"golang.org/x/sync/semaphore"
)

var (
	ErrDsNotFound  = errors.New("datasource not found")
	ErrTxNotFound  = errors.New("transaction not found")
	ErrTxExpired   = errors.New("transaction expired")
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

// commit calls Commit on the underlying Tx. Used by DsManager.CommitTx;
// does not remove the entry or release semaphores (caller uses CloseTx).
func (e *TxEntry) commit() error {
	err := e.tx.Commit()
	if err != nil {
		return err
	}
	err = e.conn.Close()
	if err != nil {
		return err
	}
	return nil
}

// rollback calls Rollback on the underlying Tx. Used by DsManager.RollbackTx
// and by cleanup for expired entries; does not remove entry or release
// semaphores (caller uses CloseTx).
func (e *TxEntry) rollback() error {
	err := e.tx.Rollback()
	if err != nil {
		return err
	}
	err = e.conn.Close()
	if err != nil {
		return err
	}
	return nil
}

// TxDatasource extends Datasource with per-datasource concurrency limits:
// semaphores for read (MaxConns - MinWriteConns), write (MaxWriteConns),
// and tx (80% of MaxWriteConns), plus a map of active TxEntry by txID and
// running counters for health/stats.
type TxDatasource struct {
	Datasource
	MaxTxIdleTimeout time.Duration

	mu       sync.Mutex
	semRead  *semaphore.Weighted
	semWrite *semaphore.Weighted
	semTx    *semaphore.Weighted
	entries  map[string]*TxEntry

	runningRead  int
	runningWrite int
	runningTx    int
}

// registerEntry adds the given TxEntry to the datasource's entries map
// under entry.TxID. Must be called with the entry already created and
// semaphores already acquired.
func (ds *TxDatasource) registerEntry(entry *TxEntry) {
	ds.mu.Lock()

	ds.entries[entry.TxID] = entry

	ds.mu.Unlock()
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

// getEntry looks up the TxEntry by txID. Returns ErrTxNotFound if missing,
// ErrTxExpired if past ExpiresAt (and removes the entry, releases semaphores,
// and rolls back in a goroutine). Otherwise marks executing if requested,
// refreshes ExpiresAt when idleTimeout is set, and returns the entry.
func (ds *TxDatasource) getEntry(txID string, executing bool) (*TxEntry, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	entry, ok := ds.entries[txID]
	if !ok {
		return nil, ErrTxNotFound
	}

	// Check if expired
	if time.Now().After(entry.ExpiresAt) {
		// Remove expired entry
		delete(ds.entries, entry.TxID)
		ds.runningTx--
		ds.semTx.Release(1)
		ds.semWrite.Release(1)

		go func() {
			entry.rollback()
		}()
		return nil, ErrTxExpired
	}

	entry.executing = executing
	if entry.idleTimeout != nil {
		entry.ExpiresAt = time.Now().Add(*entry.idleTimeout)
	}

	return entry, nil
}

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

		txDs := &TxDatasource{
			Datasource:       *ds,
			MaxTxIdleTimeout: time.Duration(config.MaxTxIdleTimeoutSec) * time.Second,

			semRead:  semaphore.NewWeighted(int64(config.MaxConns - config.MinWriteConns)),
			semWrite: semaphore.NewWeighted(int64(config.MaxWriteConns)),
			semTx:    semaphore.NewWeighted(int64(max(1, config.MaxWriteConns*8/10))), // 80%
			entries:  make(map[string]*TxEntry),

			runningRead:  0,
			runningWrite: 0,
			runningTx:    0,
		}
		dss[i] = txDs
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

	ds.mu.Lock()
	switch resourceType {
	case RESOURCE_TYPE_READ:
		ds.runningRead++
	case RESOURCE_TYPE_WRITE:
		ds.runningWrite++
	case RESOURCE_TYPE_TX:
		ds.runningTx++
	}
	ds.mu.Unlock()

	switch resourceType {
	case RESOURCE_TYPE_READ:
		err := ds.semRead.Acquire(ctx, 1)
		if err != nil {
			ds.mu.Lock()
			ds.runningRead--
			ds.mu.Unlock()
			global.GetCtxLogger(ctx).Error("DatasourceManager", "detail", "Failed to acquire read semaphore", "err", err)
			return nil, err
		}
	case RESOURCE_TYPE_WRITE:
		err := ds.semWrite.Acquire(ctx, 1)
		if err != nil {
			ds.mu.Lock()
			ds.runningWrite--
			ds.mu.Unlock()
			global.GetCtxLogger(ctx).Error("DatasourceManager", "detail", "Failed to acquire write semaphore", "err", err)
			return nil, err
		}
	case RESOURCE_TYPE_TX:
		err := ds.semTx.Acquire(ctx, 1)
		if err != nil {
			ds.mu.Lock()
			ds.runningTx--
			ds.mu.Unlock()
			global.GetCtxLogger(ctx).Error("DatasourceManager", "detail", "Failed to acquire transaction semaphore", "err", err)
			return nil, err
		}
		err = ds.semWrite.Acquire(ctx, 1)
		if err != nil {
			ds.mu.Lock()
			ds.runningTx--
			ds.mu.Unlock()
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
func (dm *DsManager) StatsGet(datasourceIdx int) (runningRead int, runningWrite int, runningTx int) {
	ds := dm.dss[datasourceIdx]
	if ds == nil {
		return 999999, 999999, 999999
	}

	ds.mu.Lock()
	runningRead = ds.runningRead
	runningWrite = ds.runningWrite
	runningTx = ds.runningTx
	ds.mu.Unlock()

	return runningRead, runningWrite, runningTx
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
		ds.mu.Lock()
		ds.runningTx--
		ds.mu.Unlock()

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
	ds.registerEntry(entry)
	global.GetCtxLogger(ctx).Debug("DatasourceManager", "detail", "Registered transaction", "txID", txID, "total entries", len(ds.entries))

	return entry, nil
}

// getTx resolves the datasource index from the txID (GetDsIdxFromTxID),
// looks up the TxDatasource, then calls getEntry(txID, executing). Returns
// the entry, datasource index, and error (ErrTxNotFound, ErrTxExpired, or
// datasource not found).
func (dm *DsManager) getTx(txID string, executing bool) (entry *TxEntry, dsIdx int, err error) {
	dsIdx, err = global.GetDsIdxFromTxID(txID)
	if err != nil {
		return nil, -1, err
	}

	ds := dm.dss[dsIdx]
	if ds == nil {
		return nil, -1, ErrDsNotFound
	}

	entry, err = ds.getEntry(txID, executing)
	if err != nil {
		return nil, dsIdx, err
	}

	return entry, dsIdx, nil
}

// CommitTx looks up the transaction by txID (without marking executing),
// then calls commit() on the entry. Does not remove the entry or release
// semaphores; the client should call CloseTx afterward. Returns ErrTxNotFound
// or DB error.
func (dm *DsManager) CommitTx(txID string) error {
	entry, dsIdx, err := dm.getTx(txID, false)
	if err != nil {
		return err
	}

	err = entry.commit()
	if err != nil {
		return ErrTxException
	}

	ds := dm.dss[dsIdx]
	ds.mu.Lock()
	delete(ds.entries, entry.TxID)
	ds.runningTx--
	ds.mu.Unlock()

	ds.semTx.Release(1)
	ds.semWrite.Release(1)

	return nil
}

// RollbackTx looks up the transaction by txID (without marking executing),
// then calls rollback() on the entry. Does not remove the entry or release
// semaphores; the client should call CloseTx afterward. Returns ErrTxNotFound
// or DB error.
func (dm *DsManager) RollbackTx(txID string) error {
	entry, dsIdx, err := dm.getTx(txID, false)
	if err != nil {
		return err
	}

	err = entry.rollback()
	if err != nil {
		return ErrTxException
	}

	ds := dm.dss[dsIdx]
	ds.mu.Lock()
	delete(ds.entries, entry.TxID)
	ds.runningTx--
	ds.mu.Unlock()

	ds.semTx.Release(1)
	ds.semWrite.Release(1)

	return nil
}

// CloseTx looks up the transaction by txID, removes it from the datasource's
// entries map, decrements runningTx, releases semTx and semWrite, and
// closes the connection. No commit or rollback is performed. Returns
// ErrTxNotFound or error from closing the connection.
func (dm *DsManager) CloseTx(txID string) error {
	entry, dsIdx, err := dm.getTx(txID, false)
	if err != nil {
		return nil
	}

	ds := dm.dss[dsIdx]
	ds.mu.Lock()
	delete(ds.entries, entry.TxID)
	ds.runningTx--
	ds.mu.Unlock()

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
	//time.Sleep(10 * time.Second) // TODO:for test

	rows, err := ds.QueryContext(ctx, sql, parameters...)

	releaseResourceFunc := func() {
		// cancelCtx()

		ds.mu.Lock()
		ds.runningRead--
		ds.mu.Unlock()

		ds.semRead.Release(1)
	}

	return rows, releaseResourceFunc, err
}

// QueryTx runs a read-only query inside the transaction identified by txID.
// It resolves the entry via getTx(txID, true), runs QueryContext on the
// entry, and returns rows plus a release callback that refreshes the
// entry's expiry (if idleTimeout is set) and clears executing. Also
// returns the datasource index for stats. Caller must call release when
// done reading rows.
func (dm *DsManager) QueryTx(ctx context.Context, timeoutSec int, txID string, sql string, parameters ...any) (*sql.Rows, ReleaseResourceFunc, int, error) {
	entry, dsIdx, err := dm.getTx(txID, true)
	if err != nil {
		return nil, nil, dsIdx, err
	}

	rows, err := entry.QueryContext(ctx, sql, parameters...)

	releaseResourceFunc := func() {
		if entry.idleTimeout != nil {
			entry.ExpiresAt = time.Now().Add(*entry.idleTimeout)
		}
		entry.executing = false
	}

	return rows, releaseResourceFunc, dsIdx, err
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
		ds.mu.Lock()
		ds.runningWrite--
		ds.mu.Unlock()

		ds.semWrite.Release(1)
	}

	return result, releaseResourceFunc, err
}

// ExecuteTx runs a write statement inside the transaction identified by txID.
// It resolves the entry via getTx(txId, true), runs ExecContext on the
// entry, and returns the result plus a release callback that refreshes
// expiry and clears executing. Also returns the datasource index. Caller
// must call release when done.
func (dm *DsManager) ExecuteTx(ctx context.Context, timeoutSec int, txId string, sql string, parameters ...any) (sql.Result, ReleaseResourceFunc, int, error) {
	entry, dsIdx, err := dm.getTx(txId, true)
	if err != nil {
		return nil, nil, -1, err
	}

	result, err := entry.ExecContext(ctx, sql, parameters...)

	releaseResourceFunc := func() {
		if entry.idleTimeout != nil {
			entry.ExpiresAt = time.Now().Add(*entry.idleTimeout)
		}
		entry.executing = false
	}

	return result, releaseResourceFunc, dsIdx, err
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
					ds.runningTx--
					ds.semTx.Release(1)
					ds.semWrite.Release(1)

					go entry.rollback()
					global.GetCtxLogger(context.TODO()).Debug("DatasourceManager", "detail", "Cleaned up expired transaction", "txID", entry.TxID, "runningTx", ds.runningTx)
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

	// Rollback all remaining transactions
	for _, ds := range dm.dss {
		ds.mu.Lock()

		for _, entry := range ds.entries {
			entry.rollback()
		}

		ds.Close()
		ds.mu.Unlock()
	}
}

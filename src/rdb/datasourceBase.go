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
	"database/sql/driver"
	"errors"
	"fmt"
	"smartdatastream/server/global"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

// errAsBadConn wraps an error so that errors.Is(err, driver.ErrBadConn) is true.
// Used to normalize pgx ConnectError for handlers that only check ErrBadConn.
type errAsBadConn struct{ err error }

func (e *errAsBadConn) Error() string        { return e.err.Error() }
func (e *errAsBadConn) Unwrap() error        { return e.err }
func (e *errAsBadConn) Is(target error) bool { return target == driver.ErrBadConn }

// connectErrorToErrBadConnConnector wraps a driver.Connector so that
// *pgconn.ConnectError is returned as an error satisfying
// errors.Is(err, driver.ErrBadConn). Handlers can rely only on ErrBadConn
// without importing pgconn.
type connectErrorToErrBadConnConnector struct {
	driver.Connector
}

// Connect calls the wrapped connector. If the returned error is a
// *pgconn.ConnectError, it is wrapped in errAsBadConn so that
// errors.Is(err, driver.ErrBadConn) is true.
func (c *connectErrorToErrBadConnConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		var connectErr *pgconn.ConnectError
		if errors.As(err, &connectErr) {
			return nil, &errAsBadConn{err: err}
		}
		return nil, err
	}
	return conn, nil
}

type Datasource struct {
	DatasourceID string
	DatabaseName string
	Driver       string
	DB           *sql.DB
	Readonly     bool
}

// NewDatasource creates a Datasource from the given config.
// For driver "postgres" it uses pgx and the ConnectError-to-ErrBadConn
// wrapper. It sets MaxOpenConns, MaxIdleConns, and optionally
// ConnMaxLifetime, then pings the DB. Returns an error on parse failure,
// open failure, or ping failure (and closes the DB on ping error).
func NewDatasource(config global.DatasourceConfig) (*Datasource, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	driverName := config.Driver
	if driverName == "postgres" {
		driverName = "pgx"
	}

	var db *sql.DB
	if driverName == "pgx" {
		connConfig, err := pgx.ParseConfig(config.DSN)
		if err != nil {
			return nil, fmt.Errorf("failed to parse pgx config for %s: %w", config.DatasourceID, err)
		}
		db = sql.OpenDB(&connectErrorToErrBadConnConnector{Connector: stdlib.GetConnector(*connConfig)})
	} else {
		var err error
		db, err = sql.Open(driverName, config.DSN)
		if err != nil {
			return nil, fmt.Errorf("failed to open datasource %s: %w", config.DatasourceID, err)
		}
	}

	// Set connection pool settings
	db.SetMaxOpenConns(config.MaxConns)
	db.SetMaxIdleConns(config.MaxConns)
	if config.MaxConnLifetimeSec > 0 {
		db.SetConnMaxLifetime(time.Duration(config.MaxConnLifetimeSec) * time.Second)
	}

	// Ping to verify connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping datasource %s: %w", config.DatasourceID, err)
	}

	return &Datasource{
		DatasourceID: config.DatasourceID,
		DatabaseName: config.DatabaseName,
		Driver:       driverName,
		DB:           db,
	}, nil
}

// newTx obtains a connection from the pool and begins a transaction.
// If isolationLevel is nil, the driver default is used. On failure the
// connection is closed. Caller is responsible for Commit/Rollback and
// conn.Close().
func (d *Datasource) newTx(isolationLevel *sql.IsolationLevel) (*sql.Conn, *sql.Tx, error) {
	conn, err := d.DB.Conn(context.Background())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get connection: %w", err)
	}

	txOptions := &sql.TxOptions{}
	if isolationLevel != nil {
		txOptions.Isolation = *isolationLevel
	}
	// Begin transaction
	tx, err := conn.BeginTx(context.Background(), txOptions)

	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	return conn, tx, nil
}

// QueryContext runs a read-only query on the datasource's connection pool.
// It delegates to sql.DB.QueryContext with the same args.
func (d *Datasource) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := d.DB.QueryContext(ctx, query, args...)
	return rows, err
}

// ExecContext executes a statement on the datasource's connection pool.
// It delegates to sql.DB.ExecContext with the same args.
func (d *Datasource) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	result, err := d.DB.ExecContext(ctx, query, args...)
	return result, err
}

// Close closes the underlying sql.DB and releases all connections.
func (d *Datasource) Close() error {
	if err := d.DB.Close(); err != nil {
		return err
	}
	return nil
}

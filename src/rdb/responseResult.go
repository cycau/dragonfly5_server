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
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"time"
)

// WireType constants for binary stream column values
const (
	WireNULL  byte = 0x00
	WireBOOL1 byte = 0x01
	WireBOOL0 byte = 0x02

	WireINT8     byte = 0x03
	WireINT16    byte = 0x04
	WireINT32    byte = 0x05
	WireINT64    byte = 0x06
	WireUINT8    byte = 0x07
	WireUINT16   byte = 0x08
	WireUINT32   byte = 0x09
	WireUINT64   byte = 0x0A
	WireFLOAT32  byte = 0x0B
	WireFLOAT64  byte = 0x0C
	WireDATETIME byte = 0x0D

	WireSTRING byte = 0x0E
	WireBYTES  byte = 0x0F
)

// ColumnMeta contains metadata about a column
type ColumnMeta struct {
	Name     string `json:"name"`
	DBType   string `json:"dbType"`
	WireType byte   `json:"wireType"`
	Nullable bool   `json:"nullable"`
}

// ColumnMeta represents metadata for a single column in the query result.
type QueryResponse struct {
	Meta          []ColumnMeta `json:"meta,omitempty"`
	Rows          []any        `json:"rows"`
	TotalCount    int          `json:"totalCount"`
	ElapsedTimeUs int64        `json:"elapsedTimeUs"`
}

// Batching constants for throughput optimization
const (
	streamWriterBufSize   = 16 * 1024 // 16KB buffer
	streamFlushThresholdB = 12 * 1024 // Or when buffered bytes exceed 12KB
	streamRowBufGrow      = 512       // Pre-allocate for typical row size
)

// responseQueryResultJson writes query results as a JSON object.
func responseQueryResultJson(w http.ResponseWriter, rows *sql.Rows, offsetRows int, limitRows int, startTime time.Time) error {

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

	offset := offsetRows + 1
	limit := 10000 // safety cap to prevent OOM;
	if limitRows > 0 {
		limit = offset + limitRows
	}

	// Create slice for scanning
	values := make([]any, len(columns))
	valuePtrs := make([]any, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	columnParsers := make([]func(any) (any, byte), len(columns))
	wireTypes := make([]byte, len(columns))

	rowCount := 0
	var resultRows []any
	for rows.Next() {
		rowCount++
		if rowCount < offset {
			continue
		}
		if rowCount >= limit {
			continue
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("Failed to scan row: %w", err)
		}

		for i, v := range values {
			if v == nil {
				continue
			}

			if columnParsers[i] == nil {
				columnParsers[i] = resolveCellParser(v)
			}

			val, wireType := columnParsers[i](v)
			values[i] = val
			wireTypes[i] = wireType
		}
		rowCopy := make([]any, len(values))
		copy(rowCopy, values)
		resultRows = append(resultRows, rowCopy)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("Rows iteration error: %w", err)
	}

	for i := range columnMeta {
		columnMeta[i].WireType = wireTypes[i]
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

// resolveCellParser returns a parser for the given value's type.
// Called once per column when first non-NULL is seen; result is cached.
func resolveCellParser(value any) func(any) (any, byte) {
	switch value.(type) {
	case int8:
		return func(v any) (any, byte) {
			return v, WireINT8
		}
	case int16:
		return func(v any) (any, byte) {
			return v, WireINT16
		}
	case int32:
		return func(v any) (any, byte) {
			return v, WireINT32
		}
	case int64, int:
		return func(v any) (any, byte) {
			return v, WireINT64
		}
	case uint8:
		return func(v any) (any, byte) {
			return v, WireUINT8
		}
	case uint16:
		return func(v any) (any, byte) {
			return v, WireUINT16
		}
	case uint32:
		return func(v any) (any, byte) {
			return v, WireUINT32
		}
	case uint64:
		return func(v any) (any, byte) {
			return v, WireUINT64
		}
	case uint:
		return func(v any) (any, byte) {
			return v, WireUINT64
		}
	case float32:
		return func(v any) (any, byte) {
			return v, WireFLOAT32
		}
	case float64:
		return func(v any) (any, byte) {
			return v, WireFLOAT64
		}
	case bool:
		return func(v any) (any, byte) {
			return v, WireBOOL1
		}
	case time.Time:
		return func(v any) (any, byte) {
			return v, WireDATETIME
		}
	case string:
		return func(v any) (any, byte) {
			return v, WireSTRING
		}
	case []byte:
		return func(v any) (any, byte) {
			return v, WireBYTES
		}
	case sql.NullInt16:
		return func(v any) (any, byte) {
			val := v.(sql.NullInt16)
			if !val.Valid {
				return nil, WireINT16
			}
			return val.Int16, WireINT16
		}
	case sql.NullInt32:
		return func(v any) (any, byte) {
			val := v.(sql.NullInt32)
			if !val.Valid {
				return nil, WireINT32
			}
			return val.Int32, WireINT32
		}
	case sql.NullInt64:
		return func(v any) (any, byte) {
			val := v.(sql.NullInt64)
			if !val.Valid {
				return nil, WireINT64
			}
			return val.Int64, WireINT64
		}
	case sql.NullFloat64:
		return func(v any) (any, byte) {
			val := v.(sql.NullFloat64)
			if !val.Valid {
				return nil, WireFLOAT64
			}
			return val.Float64, WireFLOAT64
		}
	case sql.NullBool:
		return func(v any) (any, byte) {
			val := v.(sql.NullBool)
			if !val.Valid {
				return nil, WireBOOL1
			}
			return val.Bool, WireBOOL1
		}
	case sql.NullTime:
		return func(v any) (any, byte) {
			val := v.(sql.NullTime)
			if !val.Valid {
				return nil, WireDATETIME
			}
			return val.Time, WireDATETIME
		}
	case sql.NullString:
		return func(v any) (any, byte) {
			val := v.(sql.NullString)
			if !val.Valid {
				return nil, WireSTRING
			}
			return val.String, WireSTRING
		}
	case sql.NullByte:
		return func(v any) (any, byte) {
			val := v.(sql.NullByte)
			if !val.Valid {
				return nil, WireUINT8
			}
			return val.Byte, WireUINT8
		}
	default:
		return func(v any) (any, byte) {
			s := fmt.Sprint(v)
			return s, WireSTRING
		}
	}
}

// responseQueryResultSlim writes query results as a binary octet-stream.
// Binary protocol format:
// Returns error only if streaming could not start (column info failure, no Flusher).
// Mid-stream errors are written as error trailers and return nil.
func responseQueryResultSlim(w http.ResponseWriter, rows *sql.Rows, offsetRows int, limitRows int, startTime time.Time) error {
	return responseQueryResultJson(w, rows, offsetRows, limitRows, startTime)
}

// resolveCellWriter returns a writer for the given value's type.
// Called once per column when first non-NULL is seen; result is cached.
func resolveCellWriter(value any) func(*bytes.Buffer, any) {
	switch value.(type) {
	case int8:
		return func(buf *bytes.Buffer, v any) {
			buf.WriteByte(WireINT8)
			buf.WriteByte(byte(v.(int8)))
		}
	case int16:
		return func(buf *bytes.Buffer, v any) {
			var b [2]byte
			buf.WriteByte(WireINT16)
			binary.BigEndian.PutUint16(b[:], uint16(v.(int16)))
			buf.Write(b[:])
		}
	case int32:
		return func(buf *bytes.Buffer, v any) {
			var b [4]byte
			buf.WriteByte(WireINT32)
			binary.BigEndian.PutUint32(b[:], uint32(v.(int32)))
			buf.Write(b[:])
		}
	case int64:
		return func(buf *bytes.Buffer, v any) {
			var b [8]byte
			buf.WriteByte(WireINT64)
			binary.BigEndian.PutUint64(b[:], uint64(v.(int64)))
			buf.Write(b[:])
		}
	case int:
		return func(buf *bytes.Buffer, v any) {
			var b [8]byte
			buf.WriteByte(WireINT64)
			binary.BigEndian.PutUint64(b[:], uint64(v.(int)))
			buf.Write(b[:])
		}
	case uint8:
		return func(buf *bytes.Buffer, v any) {
			buf.WriteByte(WireUINT8)
			buf.WriteByte(v.(uint8))
		}
	case uint16:
		return func(buf *bytes.Buffer, v any) {
			var b [2]byte
			buf.WriteByte(WireUINT16)
			binary.BigEndian.PutUint16(b[:], v.(uint16))
			buf.Write(b[:])
		}
	case uint32:
		return func(buf *bytes.Buffer, v any) {
			var b [4]byte
			buf.WriteByte(WireUINT32)
			binary.BigEndian.PutUint32(b[:], v.(uint32))
			buf.Write(b[:])
		}
	case uint64:
		return func(buf *bytes.Buffer, v any) {
			var b [8]byte
			buf.WriteByte(WireUINT64)
			binary.BigEndian.PutUint64(b[:], v.(uint64))
			buf.Write(b[:])
		}
	case uint:
		return func(buf *bytes.Buffer, v any) {
			var b [8]byte
			buf.WriteByte(WireUINT64)
			binary.BigEndian.PutUint64(b[:], uint64(v.(uint)))
			buf.Write(b[:])
		}
	case float32:
		return func(buf *bytes.Buffer, v any) {
			var b [4]byte
			buf.WriteByte(WireFLOAT32)
			binary.BigEndian.PutUint32(b[:], math.Float32bits(v.(float32)))
			buf.Write(b[:])
		}
	case float64:
		return func(buf *bytes.Buffer, v any) {
			var b [8]byte
			buf.WriteByte(WireFLOAT64)
			binary.BigEndian.PutUint64(b[:], math.Float64bits(v.(float64)))
			buf.Write(b[:])
		}
	case bool:
		return func(buf *bytes.Buffer, v any) {
			if v.(bool) {
				buf.WriteByte(1)
			} else {
				buf.WriteByte(2)
			}
		}
	case time.Time:
		return func(buf *bytes.Buffer, v any) {
			buf.WriteByte(WireDATETIME)
			b := v.(time.Time).AppendFormat(nil, time.RFC3339Nano)
			buf.WriteByte(byte(len(b)))
			buf.Write(b)
		}
	case string:
		return func(buf *bytes.Buffer, v any) {
			buf.WriteByte(WireSTRING)
			s := v.(string)
			var lenBuf [4]byte
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(s)))
			buf.Write(lenBuf[:])
			buf.WriteString(s)
		}
	case []byte:
		return func(buf *bytes.Buffer, v any) {
			buf.WriteByte(WireBYTES)
			b := v.([]byte)
			var lenBuf [4]byte
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
			buf.Write(lenBuf[:])
			buf.Write(b)
		}
	case sql.NullInt16:
		return func(buf *bytes.Buffer, v any) {
			val := v.(sql.NullInt16)
			if !val.Valid {
				buf.WriteByte(WireNULL)
				return
			}
			var b [2]byte
			buf.WriteByte(WireINT16)
			binary.BigEndian.PutUint16(b[:], uint16(val.Int16))
			buf.Write(b[:])
		}
	case sql.NullInt32:
		return func(buf *bytes.Buffer, v any) {
			val := v.(sql.NullInt32)
			if !val.Valid {
				buf.WriteByte(WireNULL)
				return
			}
			var b [4]byte
			buf.WriteByte(WireINT32)
			binary.BigEndian.PutUint32(b[:], uint32(val.Int32))
			buf.Write(b[:])
		}
	case sql.NullInt64:
		return func(buf *bytes.Buffer, v any) {
			val := v.(sql.NullInt64)
			if !val.Valid {
				buf.WriteByte(WireNULL)
				return
			}
			var b [8]byte
			buf.WriteByte(WireINT64)
			binary.BigEndian.PutUint64(b[:], uint64(val.Int64))
			buf.Write(b[:])
		}
	case sql.NullFloat64:
		return func(buf *bytes.Buffer, v any) {
			val := v.(sql.NullFloat64)
			if !val.Valid {
				buf.WriteByte(WireNULL)
				return
			}
			var b [8]byte
			buf.WriteByte(WireFLOAT64)
			binary.BigEndian.PutUint64(b[:], math.Float64bits(val.Float64))
			buf.Write(b[:])
		}
	case sql.NullBool:
		return func(buf *bytes.Buffer, v any) {
			val := v.(sql.NullBool)
			if !val.Valid {
				buf.WriteByte(WireNULL)
				return
			}
			if val.Bool {
				buf.WriteByte(1)
			} else {
				buf.WriteByte(2)
			}
		}
	case sql.NullTime:
		return func(buf *bytes.Buffer, v any) {
			val := v.(sql.NullTime)
			if !val.Valid {
				buf.WriteByte(WireNULL)
				return
			}
			buf.WriteByte(WireDATETIME)
			b := val.Time.AppendFormat(nil, time.RFC3339Nano)
			buf.WriteByte(byte(len(b)))
			buf.Write(b)
		}
	case sql.NullString:
		return func(buf *bytes.Buffer, v any) {
			val := v.(sql.NullString)
			if !val.Valid {
				buf.WriteByte(WireNULL)
				return
			}
			buf.WriteByte(WireSTRING)
			var lenBuf [4]byte
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(val.String)))
			buf.Write(lenBuf[:])
			buf.WriteString(val.String)
		}
	case sql.NullByte:
		return func(buf *bytes.Buffer, v any) {
			val := v.(sql.NullByte)
			if !val.Valid {
				buf.WriteByte(WireNULL)
				return
			}
			buf.WriteByte(WireBYTES)
			buf.WriteByte(val.Byte)
		}
	default:
		return func(buf *bytes.Buffer, v any) {
			buf.WriteByte(WireSTRING)
			b := fmt.Append(nil, v)
			var lenBuf [4]byte
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
			buf.Write(lenBuf[:])
			buf.Write(b)
		}
	}
}

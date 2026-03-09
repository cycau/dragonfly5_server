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
	"bufio"
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"time"
)

// Binary stream protocol constants
const (
	binProtoVersion   byte   = 0x04
	binEndOfRows      uint32 = 0x00000000
	binTrailerSuccess byte   = 0x00
	binTrailerError   byte   = 0x01
)

// WireType constants for binary stream column values
const (
	WireNULL     byte = 0x00
	WireINT8     byte = 0x01
	WireINT16    byte = 0x02
	WireINT32    byte = 0x03
	WireINT64    byte = 0x04
	WireUINT8    byte = 0x05
	WireUINT16   byte = 0x06
	WireUINT32   byte = 0x07
	WireUINT64   byte = 0x08
	WireFLOAT32  byte = 0x09
	WireFLOAT64  byte = 0x0A
	WireBOOL     byte = 0x0B
	WireDATETIME byte = 0x0C
	WireSTRING   byte = 0x0D
	WireBYTES    byte = 0x0E
)

// Batching constants for throughput optimization
const (
	slimWriterBufSize   = 16 * 1024 // 16KB buffer
	slimFlushThresholdB = 12 * 1024 // Or when buffered bytes exceed 12KB
	slimRowBufGrow      = 256       // Pre-allocate for typical row size
)

var SLIM_RESPONSE_MODE = true

func ResponseQueryResult(w http.ResponseWriter, rows *sql.Rows, offsetRows int, limitRows int, startTime time.Time) error {
	if SLIM_RESPONSE_MODE {
		return responseQueryResultSlim(w, rows, offsetRows, limitRows, startTime)
	}
	return responseQueryResultJson(w, rows, offsetRows, limitRows, startTime)
}

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
	limit := math.MaxInt32
	if limitRows > 0 {
		limit = offset + limitRows
	}

	// Create slice for scanning
	values := make([]any, len(columns))
	valuePtrs := make([]any, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

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

		resultRows = append(resultRows, values)
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

// responseQueryResultSlim writes query results as a binary octet-stream.
// Binary protocol format:
//
//	Section 1 (Meta):   [version:1] [metaCount:2] per-column{[nameLen:2] name [dbTypeLen:2] dbType [nullable:1]}
//	Section 2 (Rows):   per-row{[rowLen:4] per-column{[wireType] [valueLen:4(only when variable length)] value}}
//	Section 3 (Trailer): [endMarker:4=0] [type:1] [totalCount:8] [elapsedUs:8]
//	                 or  [endMarker:4=0] [type:1=err] [msgLen:2] msg
//
// Returns error only if streaming could not start (column info failure, no Flusher).
// Mid-stream errors are written as error trailers and return nil.
func responseQueryResultSlim(w http.ResponseWriter, rows *sql.Rows, offsetRows int, limitRows int, startTime time.Time) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return fmt.Errorf("streaming not supported: ResponseWriter does not implement http.Flusher")
	}

	// Get column info BEFORE writing headers so errors can be returned normally
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("Failed to get columns: %w", err)
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("Failed to get column types: %w", err)
	}

	// Start streaming - after this point, errors are written as trailers
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	bw := bufio.NewWriterSize(w, slimWriterBufSize)

	var u16Buf [2]byte
	var u32Buf [4]byte
	var u64Buf [8]byte

	// === Section 1: Header + Column Metadata (binary) ===
	bw.WriteByte(binProtoVersion)
	binary.BigEndian.PutUint16(u16Buf[:], uint16(len(columns)))
	bw.Write(u16Buf[:])
	for i, colType := range columnTypes {
		nullable, _ := colType.Nullable()
		name := columns[i]
		dbType := colType.DatabaseTypeName()
		binary.BigEndian.PutUint16(u16Buf[:], uint16(len(name)))
		bw.Write(u16Buf[:])
		bw.WriteString(name)
		binary.BigEndian.PutUint16(u16Buf[:], uint16(len(dbType)))
		bw.Write(u16Buf[:])
		bw.WriteString(dbType)
		if nullable {
			bw.WriteByte(1)
		} else {
			bw.WriteByte(0)
		}
	}

	// === Section 2: Row Data (streamed with batched flush) ===
	offset := offsetRows + 1
	limit := math.MaxInt32
	if limitRows > 0 {
		limit = offset + limitRows
	}

	columnWriters := make([]func(*bytes.Buffer, any), len(columns)) // nil = 型未確定
	values := make([]any, len(columns))
	valuePtrs := make([]any, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	rowCount := 0
	rowBuf := new(bytes.Buffer)
	for rows.Next() {
		rowCount++
		if rowCount < offset {
			continue
		}
		if rowCount >= limit {
			continue
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			writeStreamErrorTrailer(bw, flusher, err)
			return nil
		}

		rowBuf.Reset()
		rowBuf.Grow(slimRowBufGrow)
		for i, v := range values {
			if columnWriters[i] != nil {
				columnWriters[i](rowBuf, v)
				continue
			}
			if v == nil {
				rowBuf.WriteByte(WireNULL)
				continue
			}

			fn := resolveCellWriter(v)
			columnWriters[i] = fn
			fn(rowBuf, v)
		}
		rowBytes := rowBuf.Bytes()
		rowLen := len(rowBytes)

		prevBufLen := bw.Buffered()
		binary.BigEndian.PutUint32(u32Buf[:], uint32(rowLen))
		bw.Write(u32Buf[:])
		bw.Write(rowBytes)

		if prevBufLen+rowLen+4 >= slimFlushThresholdB {
			if err := bw.Flush(); err != nil {
				writeStreamErrorTrailer(bw, flusher, err)
				return nil
			}
			flusher.Flush()
		}
	}

	if err := rows.Err(); err != nil {
		writeStreamErrorTrailer(bw, flusher, err)
		return nil
	}

	// === Section 3: Success Trailer ===
	binary.BigEndian.PutUint32(u32Buf[:], binEndOfRows)
	bw.Write(u32Buf[:])
	bw.WriteByte(binTrailerSuccess)
	binary.BigEndian.PutUint64(u64Buf[:], uint64(rowCount))
	bw.Write(u64Buf[:])
	binary.BigEndian.PutUint64(u64Buf[:], uint64(time.Since(startTime).Microseconds()))
	bw.Write(u64Buf[:])

	bw.Flush()
	return nil
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
			buf.WriteByte(WireBOOL)
			if v.(bool) {
				buf.WriteByte(1)
			} else {
				buf.WriteByte(0)
			}
		}
	case string:
		return func(buf *bytes.Buffer, v any) {
			buf.WriteByte(WireSTRING)
			b := []byte(v.(string))
			var lenBuf [4]byte
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
			buf.Write(lenBuf[:])
			buf.Write(b)
		}
	case time.Time:
		return func(buf *bytes.Buffer, v any) {
			var b [8]byte
			buf.WriteByte(WireDATETIME)
			binary.BigEndian.PutUint64(b[:], uint64(v.(time.Time).UnixNano()))
			buf.Write(b[:])
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
			buf.WriteByte(WireBOOL)
			if val.Bool {
				buf.WriteByte(1)
			} else {
				buf.WriteByte(0)
			}
		}
	case sql.NullString:
		return func(buf *bytes.Buffer, v any) {
			val := v.(sql.NullString)
			if !val.Valid {
				buf.WriteByte(WireNULL)
				return
			}
			buf.WriteByte(WireSTRING)
			b := []byte(val.String)
			var lenBuf [4]byte
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
			buf.Write(lenBuf[:])
			buf.Write(b)
		}
	case sql.NullTime:
		return func(buf *bytes.Buffer, v any) {
			val := v.(sql.NullTime)
			if !val.Valid {
				buf.WriteByte(WireNULL)
				return
			}
			var b [8]byte
			buf.WriteByte(WireDATETIME)
			binary.BigEndian.PutUint64(b[:], uint64(val.Time.UnixNano()))
			buf.Write(b[:])
		}
	case sql.NullByte:
		return func(buf *bytes.Buffer, v any) {
			val := v.(sql.NullByte)
			if !val.Valid {
				buf.WriteByte(WireNULL)
				return
			}
			buf.WriteByte(WireUINT8)
			buf.WriteByte(val.Byte)
		}
	default:
		return func(buf *bytes.Buffer, v any) {
			s := fmt.Sprint(v)
			buf.WriteByte(WireSTRING)
			b := []byte(s)
			var lenBuf [4]byte
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
			buf.Write(lenBuf[:])
			buf.Write(b)
		}
	}
}

// writeStreamErrorTrailer writes an end-of-rows marker followed by an error trailer.
func writeStreamErrorTrailer(bw *bufio.Writer, flusher http.Flusher, err error) {
	var u32Buf [4]byte
	var u16Buf [2]byte
	binary.BigEndian.PutUint32(u32Buf[:], binEndOfRows)
	bw.Write(u32Buf[:])
	bw.WriteByte(binTrailerError)
	errMsg := []byte(err.Error())
	if len(errMsg) > 65535 {
		errMsg = errMsg[:65535]
	}
	binary.BigEndian.PutUint16(u16Buf[:], uint16(len(errMsg)))
	bw.Write(u16Buf[:])
	bw.Write(errMsg)
	bw.Flush()
	flusher.Flush()
}

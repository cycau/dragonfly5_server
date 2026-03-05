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
	"fmt"
	"math"
	"net/http"
	"reflect"
	"time"

	"github.com/shopspring/decimal"
)

// Binary stream protocol constants
const (
	binProtoVersion   byte   = 0x01
	binNullMarker     uint32 = 0xFFFFFFFF
	binEndOfRows      uint32 = 0x00000000
	binTrailerSuccess byte   = 0x00
	binTrailerError   byte   = 0x01
)

// WireType constants for binary stream column metadata
const (
	WireINT64    byte = 0x01
	WireFLOAT64  byte = 0x02
	WireBOOL     byte = 0x03
	WireSTRING   byte = 0x04
	WireBYTES    byte = 0x05
	WireDECIMAL  byte = 0x06
	WireDATETIME byte = 0x07
)

// responseQueryResultSlim writes query results as a binary octet-stream.
// The binary protocol (v1) streams rows in real-time via http.Flusher:
//
//		Section 1 (Meta):   [version:1] [metaCount:2] per-column{[nameLen:2] name [dbTypeLen:2] dbType [nullable:1] [wireType:1]}
//		Section 2 (Rows):   per-row{[rowLen:4] per-column{[valueLen:4] value}}  (flushed per row)
//		Section 3 (Trailer): [endMarker:4=0] [type:1] [totalCount:8] [elapsedUs:8]
//	                     or  [endMarker:4=0] [type:1=err] [msgLen:2] msg
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
	bw := bufio.NewWriterSize(w, 8192)

	// === Section 1: Header + Column Metadata ===
	bw.WriteByte(binProtoVersion)
	binary.Write(bw, binary.BigEndian, uint16(len(columns)))

	for i, colType := range columnTypes {
		nullable, _ := colType.Nullable()
		nameBytes := []byte(columns[i])
		dbTypeBytes := []byte(colType.DatabaseTypeName())

		binary.Write(bw, binary.BigEndian, uint16(len(nameBytes)))
		bw.Write(nameBytes)
		binary.Write(bw, binary.BigEndian, uint16(len(dbTypeBytes)))
		bw.Write(dbTypeBytes)
		if nullable {
			bw.WriteByte(0x01)
		} else {
			bw.WriteByte(0x00)
		}
		bw.WriteByte(goTypeToWireType(colType.ScanType()))
	}

	bw.Flush()
	flusher.Flush()

	// === Section 2: Row Data (streamed) ===
	offset := offsetRows + 1
	limit := math.MaxInt32
	if limitRows > 0 {
		limit = offset + limitRows
	}

	var rowBuf bytes.Buffer
	rowCount := 0
	for rows.Next() {
		rowCount++
		if rowCount < offset {
			continue
		}
		if rowCount >= limit {
			continue
		}

		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			writeStreamErrorTrailer(bw, flusher, err)
			return nil
		}

		// Serialize row into buffer to compute RowLen
		rowBuf.Reset()
		for _, val := range values {
			if val == nil {
				binary.Write(&rowBuf, binary.BigEndian, binNullMarker)
			} else {
				b := serializeBinValue(val)
				binary.Write(&rowBuf, binary.BigEndian, uint32(len(b)))
				rowBuf.Write(b)
			}
		}

		// Write RowLen + row data
		binary.Write(bw, binary.BigEndian, uint32(rowBuf.Len()))
		bw.Write(rowBuf.Bytes())

		if err := bw.Flush(); err != nil {
			return nil // client disconnected
		}
		flusher.Flush()
	}

	if err := rows.Err(); err != nil {
		writeStreamErrorTrailer(bw, flusher, err)
		return nil
	}

	// === Section 3: Success Trailer ===
	binary.Write(bw, binary.BigEndian, binEndOfRows)
	bw.WriteByte(binTrailerSuccess)
	binary.Write(bw, binary.BigEndian, int64(rowCount))
	binary.Write(bw, binary.BigEndian, time.Since(startTime).Microseconds())
	bw.Flush()
	flusher.Flush()

	return nil
}

// goTypeToWireType maps a reflect.Type (from ColumnType.ScanType()) to a WireType constant.
func goTypeToWireType(scanType reflect.Type) byte {
	if scanType == nil {
		return WireSTRING
	}
	switch scanType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return WireINT64
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return WireINT64
	case reflect.Float32, reflect.Float64:
		return WireFLOAT64
	case reflect.Bool:
		return WireBOOL
	case reflect.Slice:
		if scanType.Elem().Kind() == reflect.Uint8 {
			return WireBYTES
		}
		return WireSTRING
	case reflect.String:
		return WireSTRING
	default:
		switch scanType {
		case reflect.TypeOf(time.Time{}):
			return WireDATETIME
		case reflect.TypeOf(decimal.Decimal{}):
			return WireDECIMAL
		case reflect.TypeOf(sql.NullInt64{}), reflect.TypeOf(sql.NullInt32{}):
			return WireINT64
		case reflect.TypeOf(sql.NullFloat64{}):
			return WireFLOAT64
		case reflect.TypeOf(sql.NullBool{}):
			return WireBOOL
		case reflect.TypeOf(sql.NullString{}):
			return WireSTRING
		case reflect.TypeOf(sql.NullTime{}):
			return WireDATETIME
		}
		return WireSTRING
	}
}

// serializeBinValue converts a scanned value to its binary wire representation.
func serializeBinValue(val any) []byte {
	switch v := val.(type) {
	case int64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		return buf
	case int32:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		return buf
	case int:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		return buf
	case uint64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, v)
		return buf
	case float64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, math.Float64bits(v))
		return buf
	case float32:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, math.Float64bits(float64(v)))
		return buf
	case bool:
		if v {
			return []byte{0x01}
		}
		return []byte{0x00}
	case []byte:
		return v
	case string:
		return []byte(v)
	case time.Time:
		return []byte(v.Format(time.RFC3339))
	case decimal.Decimal:
		return []byte(v.String())
	default:
		return fmt.Appendf(nil, "%v", v)
	}
}

// writeStreamErrorTrailer writes an end-of-rows marker followed by an error trailer.
func writeStreamErrorTrailer(bw *bufio.Writer, flusher http.Flusher, err error) {
	binary.Write(bw, binary.BigEndian, binEndOfRows)
	bw.WriteByte(binTrailerError)
	errMsg := []byte(err.Error())
	if len(errMsg) > 65535 {
		errMsg = errMsg[:65535]
	}
	binary.Write(bw, binary.BigEndian, uint16(len(errMsg)))
	bw.Write(errMsg)
	bw.Flush()
	flusher.Flush()
}

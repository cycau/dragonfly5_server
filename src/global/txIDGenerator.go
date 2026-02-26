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
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

/*****************************
 * TxIDGenerator
 *****************************/
const (
	issuedAtSecondsSize = 4
	sequenceNumberSize  = 4
	datasourceIndexSize = 1
	randomSize          = 6
	txIDPayloadSize     = issuedAtSecondsSize + sequenceNumberSize + datasourceIndexSize + randomSize
)

var (
	ErrInvalidTxID = errors.New("invalid txID format")
)

// TxIDGenerator generates unique transaction IDs and holds an atomic
// sequence number. Transaction IDs encode datasource index for routing.
type TxIDGenerator struct {
	sequenceNumber uint32 // 0-4294967295 (0xFFFFFFFF) counter
}

// NewTxIDGenerator creates a new TxIDGenerator.
// The sequence number is seeded with 4 random bytes; on rare rand failure it uses 0.
func NewTxIDGenerator() *TxIDGenerator {
	seedBytes := make([]byte, 4)
	if _, err := rand.Read(seedBytes); err != nil {
		// If an error occurs, start from 0 (very rare case)
		return &TxIDGenerator{}
	}
	return &TxIDGenerator{
		sequenceNumber: binary.BigEndian.Uint32(seedBytes),
	}
}

// Generate creates a new transaction ID for the given datasource index.
// Payload is: issuedAtSeconds (4) | sequenceNumber (4) | datasourceIndex (1) |
// randomBytes (6). Result is base64url-encoded. Sequence number is incremented
// atomically. Returns an error only if random bytes cannot be generated.
func (g *TxIDGenerator) Generate(datasourceIndex int) (txID string, err error) {
	// Allocate buffer for payload
	payload := make([]byte, txIDPayloadSize)
	offset := 0

	// issuedAtSeconds: 4 bytes (uint32)
	issuedAtSeconds := uint32(time.Now().Unix())
	binary.BigEndian.PutUint32(payload[offset:], issuedAtSeconds)
	offset += issuedAtSecondsSize

	// sequenceNumber: 4 bytes (0-4294967295で繰り返す)
	seqNum := atomic.AddUint32(&g.sequenceNumber, 1)
	binary.BigEndian.PutUint32(payload[offset:], seqNum)
	offset += sequenceNumberSize

	// datasourceIndex: 1 bytes
	payload[offset] = uint8(datasourceIndex)
	offset += datasourceIndexSize

	// randomBytes: 5 bytes, and also used for security
	randomBytes := make([]byte, randomSize)
	if _, err := rand.Read(randomBytes); err != nil {
		// This should almost never happen, but if it does, return an error
		return "", fmt.Errorf("failed to generate random bytes for security: %w", err)
	}
	copy(payload[offset:], randomBytes)

	// Encode as base64url
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

// GetDsIdxFromTxID decodes the transaction ID and returns the datasource index.
// It decodes base64url, checks that the payload length equals txIDPayloadSize,
// skips issuedAtSeconds and sequenceNumber, then reads the single datasource
// index byte. Returns ErrInvalidTxID on decode or length error.
func GetDsIdxFromTxID(txID string) (dsIdx int, err error) {
	// Decode base64url
	payload, err := base64.RawURLEncoding.DecodeString(txID)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidTxID, err)
	}

	// Check size
	if len(payload) != txIDPayloadSize {
		return 0, fmt.Errorf("%w: invalid size", ErrInvalidTxID)
	}

	// Parse payload
	offset := 0

	// issuedAtSeconds: 4 bytes
	offset += issuedAtSecondsSize

	// sequenceNumber: 4 bytes
	offset += sequenceNumberSize

	// datasourceIndex: 1 byte
	datasourceIndex := payload[offset]
	offset += datasourceIndexSize

	return int(datasourceIndex), nil
}

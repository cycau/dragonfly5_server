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

package cluster

import (
	"context"
	"math"
	"sync"
	"time"

	"dragonfly5/server/global"
)

type NodeStatus string

const (
	STARTING NodeStatus = "STARTING"
	SERVING  NodeStatus = "SERVING"
	DRAINING NodeStatus = "DRAINING"
	STOPPING NodeStatus = "STOPPING"
	HEALZERR NodeStatus = "HEALZERR"
)

type NodeInfo struct {
	NodeID    string     `json:"nodeId"`
	Status    NodeStatus `json:"status"`
	BaseURL   string     `json:"-"`
	SecretKey string     `json:"-"`

	MaxHttpQueue int              `json:"maxHttpQueue"`
	RunningHttp  int              `json:"runningHttp"`
	UpTime       time.Time        `json:"upTime"`
	CheckTime    time.Time        `json:"checkTime"`
	Datasources  []DatasourceInfo `json:"datasources"`
	Mu           sync.Mutex       `json:"-"`
}

type DatasourceInfo struct {
	DatasourceID           string `json:"datasourceId"`
	DatabaseName           string `json:"databaseName"`
	Active                 bool   `json:"active"`
	MaxConns               int    `json:"maxConns"`
	MaxWriteConns          int    `json:"maxWriteConns"`
	MinWriteConns          int    `json:"minWriteConns"`
	DefaultQueryTimeoutSec int    `json:"-"`

	RunningRead   int     `json:"runningRead"`
	RunningWrite  int     `json:"runningWrite"`
	RunningTx     int     `json:"runningTx"`
	LatencyP95Ms  int     `json:"latencyP95Ms"`
	ErrorRate1m   float64 `json:"errorRate1m"`
	TimeoutRate1m float64 `json:"timeoutRate1m"`
}

// NewDatasourceInfo builds a DatasourceInfo from the given config.
// All runtime stats (RunningRead, RunningWrite, RunningTx, LatencyP95Ms,
// ErrorRate1m, TimeoutRate1m) are initialized to zero; Active is true.
const STAT_WINDOW_INTERVAL = 5 * time.Minute

func NewDatasourceInfo(config global.DatasourceConfig) *DatasourceInfo {

	return &DatasourceInfo{
		DatasourceID:           config.DatasourceID,
		DatabaseName:           config.DatabaseName,
		Active:                 true,
		MaxConns:               config.MaxConns,
		MaxWriteConns:          config.MaxWriteConns,
		MinWriteConns:          config.MinWriteConns,
		DefaultQueryTimeoutSec: config.DefaultQueryTimeoutSec,

		RunningRead:   0,
		RunningWrite:  0,
		RunningTx:     0,
		LatencyP95Ms:  0,
		ErrorRate1m:   0,
		TimeoutRate1m: 0,
	}
}

// Clone returns a shallow copy of the node info for serialization.
// BaseURL and SecretKey are replaced with "-" so they are not exposed.
// The Datasources slice is copied so the caller gets a snapshot. The
// caller must not assume the node is locked; external sync may be needed.
func (node *NodeInfo) Clone() NodeInfo {
	node.Mu.Lock()
	defer node.Mu.Unlock()

	datasources := make([]DatasourceInfo, len(node.Datasources))
	for idx := range node.Datasources {
		dsInfo := &node.Datasources[idx]
		datasources[idx] = *dsInfo
	}

	return NodeInfo{
		NodeID:       node.NodeID,
		Status:       node.Status,
		BaseURL:      "-",
		SecretKey:    "-",
		MaxHttpQueue: node.MaxHttpQueue,
		RunningHttp:  node.RunningHttp,
		UpTime:       node.UpTime,
		CheckTime:    node.CheckTime,
		Datasources:  datasources,
	}
}

// GetScore returns scores for each datasource on this node that matches
// tarDbName and supports the endpoint (read for Query, write for Execute/BeginTx).
// It returns early with a possibly empty slice if the node is not SERVING,
// HTTP queue is full, the datasource is inactive, or name/capacity filters fail.
// Each returned ScoreWithWeight has score, weight, and exIndex (datasource index).
func (node *NodeInfo) GetScore(ctx context.Context, tarDbName string, endpoint global.ENDPOINT_TYPE) []*ScoreWithWeight {
	node.Mu.Lock()
	defer node.Mu.Unlock()

	scores := make([]*ScoreWithWeight, 0, len(node.Datasources))

	for dsIdx := range node.Datasources {
		if node.Status != SERVING {
			return scores
		}
		if node.RunningHttp >= node.MaxHttpQueue {
			return scores
		}

		dsInfo := &node.Datasources[dsIdx]

		if !dsInfo.Active {
			return scores
		}
		if dsInfo.MaxWriteConns < 1 && (endpoint == global.EP_Execute || endpoint == global.EP_BeginTx) {
			return scores
		}
		if dsInfo.DatabaseName != tarDbName {
			return scores
		}

		m := calculateMetrics(node, dsInfo)
		score := calculateScore(endpoint, m)

		weight := 0.0
		switch endpoint {
		case global.EP_Query:
			weight = float64(dsInfo.MaxConns - dsInfo.MinWriteConns)
		case global.EP_Execute:
			weight = float64(dsInfo.MaxWriteConns)
		case global.EP_BeginTx:
			weight = float64(dsInfo.MaxWriteConns * 8 / 10)
		default:
			weight = float64(dsInfo.MaxConns)
		}

		scores = append(scores, &ScoreWithWeight{score: score, weight: weight, exIndex: dsIdx})
		global.GetCtxLogger(ctx).Debug("NodeInfo", "detail", "GetScore", "dsIdx", dsIdx, "endpoint", endpoint, "score", score, "weight", weight, "dsInfo", dsInfo)
	}

	return scores
}

// Score calculation thresholds and constants.
const (
	LAT_BAD_MS       = 3000.0 // latency "bad" threshold (ms)
	ERR_RATE_BAD     = 0.05   // error rate "bad" threshold (1m)
	TIMEOUT_RATE_BAD = 0.05   // timeout rate "bad" threshold (1m)
	UPTIME_OK        = 300.0  // uptime considered OK after this many seconds
	TOP_K            = 3      // top-k scores for selection
)

// normalizedMetrics holds normalized (0–1) metrics used to compute the
// node/datasource score: HTTP and read/write/tx capacity freedom, latency/
// error/timeout scores, and uptime. Used by calculateScore with
// endpoint-specific weights.
type normalizedMetrics struct {
	httpFree float64

	readFree  float64
	writeFree float64
	txFree    float64

	latScore  float64
	errScore  float64
	toutScore float64

	uptimeScore float64
}

// ScoreWithWeight is a single candidate for balancer selection.
// score is the computed 0–1 value; weight is used for weighted random;
// exIndex is the datasource index (or node index when aggregating others).
type ScoreWithWeight struct {
	score   float64
	weight  float64
	exIndex int
}

// clamp01 clamps x to the range [0, 1].
func clamp01(x float64) float64 {
	return math.Min(1.0, math.Max(0.0, x))
}

// STAT_COLLECT_INTERVAL is the interval used for stats collection in metrics.
const STAT_COLLECT_INTERVAL = 500 * time.Millisecond

// calculateMetrics computes normalized metrics for the given node and
// datasource: HTTP queue freedom, read/write/tx capacity freedom (with
// quadratic penalty), latency/error/timeout scores (log/linear against
// LAT_BAD_MS, ERR_RATE_BAD, TIMEOUT_RATE_BAD), and uptime score.
func calculateMetrics(node *NodeInfo, dsInfo *DatasourceInfo) normalizedMetrics {
	httpFree := 1 - clamp01(float64(node.RunningHttp)/float64(node.MaxHttpQueue))

	capacityMultiple := float64(node.MaxHttpQueue) / float64(dsInfo.MaxConns)

	readCap := float64(dsInfo.MaxConns-dsInfo.MinWriteConns) * capacityMultiple
	ratio := clamp01(float64(dsInfo.RunningRead) / readCap)
	readFree := 1 - ratio*ratio

	writeCap := float64(dsInfo.MaxWriteConns) * capacityMultiple
	ratio = clamp01(float64(dsInfo.RunningWrite) / writeCap)
	writeFree := 1 - ratio*ratio

	txCap := float64(dsInfo.MaxWriteConns*8/10) * 2
	ratio = clamp01(float64(dsInfo.RunningTx) / txCap)
	txFree := 1 - ratio*ratio

	latScore := 1 - clamp01(math.Log1p(float64(dsInfo.LatencyP95Ms))/math.Log1p(LAT_BAD_MS))
	errScore := 1 - clamp01(dsInfo.ErrorRate1m/ERR_RATE_BAD)
	toutScore := 1 - clamp01(dsInfo.TimeoutRate1m/TIMEOUT_RATE_BAD)

	uptimeScore := clamp01(float64(time.Since(node.UpTime).Seconds()) / UPTIME_OK)

	return normalizedMetrics{
		httpFree: httpFree,

		readFree:  readFree,
		writeFree: writeFree,
		txFree:    txFree,

		latScore:  latScore,
		errScore:  errScore,
		toutScore: toutScore,

		uptimeScore: uptimeScore,
	}
}

// calculateScore returns a single score in [0.05, 1] for the endpoint.
// If httpFree < 0.05 it returns 0.05. Otherwise it combines normalized
// metrics with endpoint-specific weights: Query favors readFree and
// latScore, Execute favors writeFree and errScore, BeginTx favors txFree.
func calculateScore(endpoint global.ENDPOINT_TYPE, m normalizedMetrics) float64 {

	s := 0.05
	if m.httpFree < 0.05 {
		return s
	}

	switch endpoint {
	case global.EP_Query:
		s = 0.25*m.httpFree +
			0.35*m.readFree +
			0.05*m.writeFree +
			0.05*m.txFree +

			0.15*m.latScore +
			0.05*m.errScore +
			0.08*m.toutScore +

			0.02*m.uptimeScore
	case global.EP_Execute:
		s = 0.15*m.httpFree +
			0.05*m.readFree +
			0.35*m.writeFree +
			0.15*m.txFree +

			0.05*m.latScore +
			0.15*m.errScore +
			0.08*m.toutScore +

			0.02*m.uptimeScore
	case global.EP_BeginTx:
		s = 0.05*m.httpFree +
			0.05*m.readFree +
			0.15*m.writeFree +
			0.45*m.txFree +

			0.05*m.latScore +
			0.15*m.errScore +
			0.08*m.toutScore +

			0.02*m.uptimeScore
	}

	return s
}

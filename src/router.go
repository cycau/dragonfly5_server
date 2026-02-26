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

package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"dragonfly5/server/cluster"
	"dragonfly5/server/global"
	"dragonfly5/server/rdb"
)

type Router struct {
	chi.Router
	balancer   *cluster.Balancer
	dsManager  *rdb.DsManager
	dmlHandler *rdb.DmlHandler
	txHandler  *rdb.TxHandler
}

// NewRouter creates and returns the application router.
// It wires chi with middleware: Logger, Recoverer, and balancer.SelectNode.
// Then it builds DmlHandler and TxHandler from the given dsManager,
// registers all routes via setupRoutes, and returns the configured Router.
func NewRouter(balancer *cluster.Balancer, dsManager *rdb.DsManager) *Router {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(balancer.SelectNode)

	router := &Router{
		Router:     r,
		balancer:   balancer,
		dsManager:  dsManager,
		dmlHandler: rdb.NewDmlHandler(dsManager),
		txHandler:  rdb.NewTxHandler(dsManager),
	}

	router.setupRoutes()
	return router
}

// StartHealthTicker starts the cluster health collection flow.
// It waits briefly for the HTTP server to be up, then runs one synchronous
// health collection (collectHealth(true)). It marks the self node as SERVING
// and starts a goroutine that periodically calls collectHealth(false) with a
// random interval of about 2–4 seconds between runs.
func (r *Router) StartHealthTicker() {
	// wait for http server to start
	time.Sleep(500 * time.Millisecond)
	// collect once at startup
	r.collectHealth(true)

	// remove self from other nodes
	otherNodes := make([]*cluster.NodeInfo, 0, len(r.balancer.OtherNodes))
	for _, node := range r.balancer.OtherNodes {
		if r.balancer.SelfNode.NodeID != node.NodeID {
			otherNodes = append(otherNodes, node)
		}
	}
	// r.balancer.OtherNodes = otherNodes // TODO: comment out for debug

	selfNode := r.balancer.SelfNode
	selfNode.Mu.Lock()
	selfNode.Status = cluster.SERVING
	selfNode.UpTime = time.Now()
	selfNode.Mu.Unlock()

	go func() {
		for {
			time.Sleep(time.Duration(2000+rand.Intn(2000)) * time.Millisecond) // 平均3(2-4)秒ランダム待ち
			r.collectHealth(false)
		}
	}()
}

// HEALZ_ERROR_INTERVAL is the minimum time to wait before retrying health checks
// for a node that previously returned an error from /healz.
const HEALZ_ERROR_INTERVAL = 15 * time.Second

// collectHealth updates health information for the cluster.
// For each other node it GETs /healz, then updates that node's NodeInfo from the
// response (or sets status HEALZERR on failure; HEALZERR nodes are skipped
// until HEALZ_ERROR_INTERVAL has passed). If isSync is true, it waits for all
// goroutines to finish before returning. It also refreshes the self node's
// datasource stats (running read/write/tx counts and DmlHandler latency/error/
// timeout rates) from dsManager and dmlHandler.
func (r *Router) collectHealth(isSync bool) {
	var wg sync.WaitGroup
	for _, node := range r.balancer.OtherNodes {
		wg.Add(1)
		go func(node *cluster.NodeInfo) {
			defer wg.Done()

			if node.Status == cluster.HEALZERR {
				if time.Since(node.CheckTime) < HEALZ_ERROR_INTERVAL {
					return
				}
			}

			defer node.Mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, node.BaseURL+"/healz", nil)
			if err != nil {
				slog.Warn("Failed to create request for health", "nodeId", node.NodeID, "err", err)
				node.Mu.Lock()
				node.Status = cluster.HEALZERR
				return
			}
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				slog.Warn("Failed to do request for health", "nodeId", node.NodeID, "err", err)
				node.Mu.Lock()
				node.Status = cluster.HEALZERR
				node.CheckTime = time.Now()
				return
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				slog.Warn("Failed to read response body for health", "nodeId", node.NodeID, "err", err)
				node.Mu.Lock()
				node.Status = cluster.HEALZERR
				return
			}
			var responsedNodeInfo cluster.NodeInfo
			err = json.Unmarshal(body, &responsedNodeInfo)
			if err != nil {
				slog.Warn("Failed to unmarshal health info", "nodeId", node.NodeID, "err", err)
				node.Mu.Lock()
				node.Status = cluster.HEALZERR
				return
			}

			node.Mu.Lock()
			node.NodeID = responsedNodeInfo.NodeID
			node.Status = responsedNodeInfo.Status

			node.MaxHttpQueue = responsedNodeInfo.MaxHttpQueue
			node.RunningHttp = responsedNodeInfo.RunningHttp
			node.UpTime = responsedNodeInfo.UpTime
			node.CheckTime = responsedNodeInfo.CheckTime
			node.Datasources = responsedNodeInfo.Datasources
			node.CheckTime = time.Now()
		}(node)
	}

	if isSync {
		wg.Wait()
	}

	selfNode := r.balancer.SelfNode
	selfNode.Mu.Lock()
	for dsIdx := range selfNode.Datasources {
		runningRead, runningWrite, runningTx := r.dsManager.StatsGet(dsIdx)
		latencyP95Ms, errorRate1m, timeoutRate1m := r.dmlHandler.StatsGet(dsIdx)

		dsInfo := &selfNode.Datasources[dsIdx]
		dsInfo.RunningRead = runningRead
		dsInfo.RunningWrite = runningWrite
		dsInfo.RunningTx = runningTx
		dsInfo.LatencyP95Ms = latencyP95Ms
		dsInfo.ErrorRate1m = errorRate1m
		dsInfo.TimeoutRate1m = timeoutRate1m

		slog.Debug("Router self node datasource stats", "dsIdx", dsIdx, "runningHttp", selfNode.RunningHttp, "runningRead", runningRead, "runningWrite", runningWrite, "runningTx", runningTx, "latencyP95Ms", latencyP95Ms, "errorRate1m", errorRate1m, "timeoutRate1m", timeoutRate1m)
	}
	selfNode.CheckTime = time.Now()
	selfNode.Mu.Unlock()
}

// setupRoutes registers all HTTP routes on the router.
// It mounts GET /healz (returns the self node's cloned NodeInfo as JSON) and
// under /rdb: POST /query and /execute for DML, and under /rdb/tx the
// transaction endpoints (POST /begin, /query, /execute; PUT /commit,
// /rollback, /close).
func (r *Router) setupRoutes() {
	// Health check
	r.Get("/healz", func(nodeInfo *cluster.NodeInfo) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(nodeInfo.Clone())
		}
	}(r.balancer.SelfNode).ServeHTTP)

	// API v1 routes
	r.Route("/rdb", func(router chi.Router) {
		// Execute endpoint
		router.Post(global.EP_PATH_QUERY, r.dmlHandler.Query)
		router.Post(global.EP_PATH_EXECUTE, r.dmlHandler.Execute)

		// Transaction endpoints
		router.Route("/tx", func(txRouter chi.Router) {
			txRouter.Post(global.EP_PATH_TX_BEGIN, r.txHandler.BeginTx)
			txRouter.Post(global.EP_PATH_QUERY, r.dmlHandler.QueryTx)
			txRouter.Post(global.EP_PATH_EXECUTE, r.dmlHandler.ExecuteTx)
			txRouter.Put(global.EP_PATH_TX_COMMIT, r.txHandler.CommitTx)
			txRouter.Put(global.EP_PATH_TX_ROLLBACK, r.txHandler.RollbackTx)
			txRouter.Put(global.EP_PATH_TX_CLOSE, r.txHandler.CloseTx)
		})
	})
}

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
	"dragonfly5/server/global"
	. "dragonfly5/server/global"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Balancer struct {
	SelfNode   *NodeInfo
	OtherNodes []*NodeInfo
}

// NewBalancer constructs a Balancer that holds the self node and the list of
// other cluster nodes (typically in STARTING state until health is collected).
func NewBalancer(selfNode *NodeInfo, otherNodes []*NodeInfo) *Balancer {
	return &Balancer{
		SelfNode:   selfNode,
		OtherNodes: otherNodes,
	}
}

// SelectNode is chi middleware that runs before the actual handler.
// It checks the path: /healz is passed through. Otherwise it validates the
// secret key header, then parses endpoint type, target DB name, txID, and
// redirect count. It increments SelfNode.RunningHttp and defers decrement.
// For non-RDB paths (EP_Other) it passes through. If a txID is present it
// resolves the datasource index from the txID and runs the handler locally.
// Otherwise it scores the self node (selectSelfDatasource); if utilization
// is low enough it serves locally; if redirect count is 0 it either fails
// or forces local. Then it scores other nodes (selectOtherNode); if self
// score is better it serves locally, else it drains the body and responds
// with 307 Temporary Redirect to the chosen node's NodeID.
func (b *Balancer) SelectNode(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/healz") {
			next.ServeHTTP(w, r)
			return
		}
		secretKey := r.Header.Get(HEADER_SECRET_KEY)
		if secretKey != b.SelfNode.SecretKey {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		err, endpoint, tarDbName, txID, redirectCount := parseRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// RunningHttp count
		b.SelfNode.Mu.Lock()
		b.SelfNode.RunningHttp++
		b.SelfNode.Mu.Unlock()
		defer func() {
			b.SelfNode.Mu.Lock()
			b.SelfNode.RunningHttp--
			b.SelfNode.Mu.Unlock()
		}()

		// 対象外のパスはそのまま処理
		if endpoint == EP_Other {
			next.ServeHTTP(w, r)
			return
		}

		// トランザクション処理は常に受け入れる（優先処理）
		if txID != "" {
			dsIdx, err := global.GetDsIdxFromTxID(txID)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			b.runHandler(next, w, r, dsIdx)
			return
		}

		/*** 自分から探す ***/

		// 使用率80%以下なら自分で処理、それ以外は他ノードとの協調で処理
		// Weight抽選で、自分のScoreの方が高い場合は自分で処理
		// それ以外はredirect、Max３回か、自分にリダイレクトされた場合は終了（Client側実装）
		selfBestScore, selfRecommendDs := selectSelfDatasource(b.SelfNode, tarDbName, endpoint)
		slog.Debug("Balancer self best score", "selfBestScore", selfBestScore, "selfRecommendDs", selfRecommendDs)
		if selfRecommendDs != nil {
			slog.Debug("Balancer serving on self node", "exIndex", selfRecommendDs.exIndex)
			b.runHandler(next, w, r, selfRecommendDs.exIndex)
			return
		}
		// Client側で、最後のリダイレクトの場合（リダイレクトを受け付けない場合）
		if redirectCount < 1 {
			if selfBestScore == nil {
				http.Error(w, fmt.Sprintf("No resource to process on this node. Node[%s], RedirectCount[%d]", b.SelfNode.NodeID, redirectCount), http.StatusServiceUnavailable)
				return
			}
			slog.Debug("Balancer forced serving on self node", "exIndex", selfBestScore.exIndex)
			b.runHandler(next, w, r, selfBestScore.exIndex)
			return
		}

		/*** 他のノード選択 ***/
		recommendNodeScore, recommendNode := selectOtherNode(b.OtherNodes, tarDbName, endpoint)
		slog.Debug("Balancer recommend node score", "recommendNodeScore", recommendNodeScore, "recommendNode", recommendNode)
		if recommendNode == nil {
			// ゲート条件チェックは行わない
			// DB枯渇しても、Httpバッファリングが可能な場合は処理を継続させる
			if selfBestScore != nil {
				slog.Debug("Balancer no recommend node, serving on self", "exIndex", selfBestScore.exIndex)
				b.runHandler(next, w, r, selfBestScore.exIndex)
				return
			}
			slog.Debug("Balancer no candidate nodes, processing locally despite lack of capacity")
			http.Error(w, "No candidate nodes available and no capacity to process locally", http.StatusServiceUnavailable)
			return
		}

		// 自分のScoreの方が高い場合は自分で処理
		if selfBestScore.score > recommendNodeScore.score {
			// さらに他ノードBestScoreとの比較はやめる、一瞬他ノードに集中させてしまう恐れあり
			slog.Debug("Balancer self score higher, serving on self", "exIndex", selfBestScore.exIndex)
			b.runHandler(next, w, r, selfBestScore.exIndex)
			return
		}

		// 307 Temporary Redirect（Bodyを読み切らないとKeep-Alive接続で次のリクエストに残りデータが混ざる）
		if r.Body != nil {
			io.Copy(io.Discard, r.Body)
			r.Body.Close()
		}
		w.Header().Set("Location", recommendNode.NodeID)
		w.WriteHeader(http.StatusTemporaryRedirect)
		slog.Debug("Balancer redirecting", "nodeId", recommendNode.NodeID)
	}
	return http.HandlerFunc(fn)
}

// runHandler runs the next handler with the request context updated:
// a timeout is set from HEADER_TIMEOUT_SEC or the datasource's
// DefaultQueryTimeoutSec (capped at 900s), and CTX_DS_IDX is set to dsIdx.
func (b *Balancer) runHandler(next http.Handler, w http.ResponseWriter, r *http.Request, dsIdx int) {

	timeoutSecInt := b.SelfNode.Datasources[dsIdx].DefaultQueryTimeoutSec

	timeoutSec := r.Header.Get(HEADER_TIMEOUT_SEC)
	if timeoutSec != "" {
		timeoutSecHeader, err := strconv.Atoi(timeoutSec)
		if err == nil {
			if timeoutSecHeader > 0 {
				timeoutSecInt = timeoutSecHeader
			}
		}
	}

	ctx, cancelCtx := context.WithTimeout(r.Context(), time.Duration(min(900, timeoutSecInt))*time.Second)
	defer cancelCtx()
	r = r.WithContext(context.WithValue(ctx, CTX_DS_IDX, dsIdx))

	next.ServeHTTP(w, r)
}

// selectSelfDatasource scores all datasources on the self node that match
// tarDbName and the given endpoint. It returns the best score and, when
// utilization is under the recommend threshold (~80%, score >= 0.45), a
// recommended score for weighted random. Otherwise recommendScore is nil
// (caller may still use bestScore to force local execution).
func selectSelfDatasource(selfNode *NodeInfo, tarDbName string, endpoint ENDPOINT_TYPE) (bestScore *ScoreWithWeight, recommendScore *ScoreWithWeight) {

	scores := selfNode.GetScore(tarDbName, endpoint)
	for i, s := range scores {
		if s != nil {
			slog.Debug("Balancer self node score", "i", i, "score", *s)
		}
	}

	// ノード選択（TopK + Weighted Random）
	best, bestRandom := selectBestRandomScore(scores)
	if best == nil {
		return nil, nil
	}

	// 使用率80%相当
	if best.score < 0.45 {
		return best, nil
	}

	return best, bestRandom
}

// selectOtherNode gathers scores from every datasource on every other node
// that matches tarDbName and endpoint (with exIndex set to node index).
// It then selects one score via selectBestRandomScore and returns that
// score and the corresponding NodeInfo. Returns nil,nil if there are no
// candidates.
func selectOtherNode(otherNodes []*NodeInfo, tarDbName string, endpoint ENDPOINT_TYPE) (recommendNodeScore *ScoreWithWeight, recommendNode *NodeInfo) {
	scores := make([]*ScoreWithWeight, 0, 8)

	for nodeIdx, node := range otherNodes {
		nodeScores := node.GetScore(tarDbName, endpoint)
		for _, score := range nodeScores {
			score.exIndex = nodeIdx
		}
		scores = append(scores, nodeScores...)
	}
	for i, s := range scores {
		if s != nil {
			slog.Debug("Balancer other node score", "i", i, "score", *s)
		}
	}

	// ノード選択
	_, bestRandomScore := selectBestRandomScore(scores)
	if bestRandomScore != nil {
		return bestRandomScore, otherNodes[bestRandomScore.exIndex]
	}
	return nil, nil
}

// selectBestRandomScore chooses from the given scores using top-k and
// weighted random. Scores are sorted descending; the top TOP_K are kept.
// Total weight is sum(score*weight); a random value in [0, totalWeight)
// selects one of the top-k. Returns the absolute best (first of top-k)
// and the selected score. For 0 or 1 score, returns that score for both.
func selectBestRandomScore(scores []*ScoreWithWeight) (bestScore *ScoreWithWeight, bestRandomScore *ScoreWithWeight) {
	if len(scores) == 0 {
		return nil, nil
	}
	if len(scores) == 1 {
		return scores[0], scores[0]
	}

	// TopKを抽出
	k := TOP_K
	if len(scores) < k {
		k = len(scores)
	}

	// スコアでソート（降順）
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].score > scores[j].score
	})

	topK := scores[:k]

	// 重み付きランダム選択
	totalWeight := 0.0
	for _, sc := range topK {
		totalWeight += (sc.score * sc.weight)
	}

	if totalWeight <= 0 {
		return nil, nil
	}

	r := rand.Float64() * totalWeight
	current := 0.0
	for _, sc := range topK {
		if sc.weight <= 0 {
			continue
		}
		current += (sc.score * sc.weight)
		if current >= r {
			return topK[0], sc
		}
	}

	return nil, nil
}

// parseRequest extracts routing and transaction info from the request.
// It returns the endpoint type (from path), database name and txID from
// headers, and redirect count from header (integer; parsing errors leave
// redirectCount as 0 and err is currently always nil).
func parseRequest(r *http.Request) (err error, endpoint ENDPOINT_TYPE, dbName string, txID string, redirectCount int) {

	endpoint = GetEndpointType(r.URL.Path)
	dbName = r.Header.Get(HEADER_DB_NAME)
	txID = r.Header.Get(HEADER_TX_ID)
	redirectCount, err = strconv.Atoi(r.Header.Get(HEADER_REDIRECT_COUNT))

	return nil, endpoint, dbName, txID, redirectCount
}

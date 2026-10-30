package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// selectedAuthKeys are the metadata keys CPA publishes before the after-auth
// interceptor runs. They identify the credential the scheduler just picked.
const (
	selectedAuthIDKey    = "selected_auth_id"
	selectedAuthIndexKey = "selected_auth_index"
)

func metadataString(meta map[string]any, key string) string {
	if len(meta) == 0 {
		return ""
	}
	switch value := meta[key].(type) {
	case string:
		return strings.TrimSpace(value)
	case fmt.Stringer:
		return strings.TrimSpace(value.String())
	default:
		return ""
	}
}

// resolveAccount maps a selected credential onto its routing identity, using the
// host runtime callback so bucket keys can be upstream-based.
func resolveAccount(index, id string) accountInfo {
	g := currentGate()
	if acct, ok := g.accountByIndex(index, id); ok {
		return acct
	}
	// Metadata may carry only the auth ID, while the runtime callback is keyed by
	// auth index. Refresh the auth list to learn the index/base URL mapping.
	if index == "" && id != "" {
		refreshAccountsFromHost(g)
		if acct, ok := g.accountByIndex("", id); ok {
			return acct
		}
	}
	acct := accountInfo{ID: id, Index: index}
	if index != "" {
		raw, errCall := callHost("host.auth.get_runtime", map[string]any{"auth_index": index})
		if errCall == nil {
			var resp pluginapi.HostAuthGetRuntimeResponse
			if errUnmarshal := json.Unmarshal(raw, &resp); errUnmarshal == nil {
				entry := resp.Auth
				if entry.ID != "" {
					acct.ID = entry.ID
				}
				if entry.AuthIndex != "" {
					acct.Index = entry.AuthIndex
				}
				acct.Provider = strings.TrimSpace(entry.Provider)
				acct.BaseURL = strings.TrimSpace(entry.BaseURL)
				acct.Label = strings.TrimSpace(entry.Label)
			}
		}
	}
	if acct.ID == "" && acct.Index == "" {
		return acct
	}
	g.rememberAccounts([]accountInfo{acct})
	return acct
}

// refreshAccountsFromHost pulls the host auth list once per refresh window and
// caches the non-secret routing fields.
func refreshAccountsFromHost(g *gate) {
	if g == nil || !g.shouldRefreshAccounts() {
		return
	}
	defer g.finishAccountRefresh()
	raw, errCall := callHost("host.auth.list", map[string]any{})
	if errCall != nil {
		hostLog("debug", "concurrency-gate auth list refresh failed", map[string]any{"error": errCall.Error()})
		return
	}
	var resp struct {
		Files []pluginapi.HostAuthFileEntry `json:"files"`
	}
	if errUnmarshal := json.Unmarshal(raw, &resp); errUnmarshal != nil {
		hostLog("debug", "concurrency-gate auth list decode failed", map[string]any{"error": errUnmarshal.Error()})
		return
	}
	accounts := make([]accountInfo, 0, len(resp.Files))
	for _, entry := range resp.Files {
		acct := accountInfo{
			ID:       strings.TrimSpace(entry.ID),
			Index:    strings.TrimSpace(entry.AuthIndex),
			Provider: strings.TrimSpace(entry.Provider),
			BaseURL:  strings.TrimSpace(entry.BaseURL),
			Label:    strings.TrimSpace(entry.Label),
		}
		if name := strings.TrimSpace(entry.Name); name != "" && acct.CompatName == "" {
			// openai-compatibility credentials expose the compat name via provider.
			if strings.HasPrefix(strings.ToLower(acct.Provider), "openai-compat") {
				acct.CompatName = strings.TrimSuffix(name, ".json")
			}
		}
		accounts = append(accounts, acct)
	}
	g.rememberAccounts(accounts)
}

func interceptAfterAuth(raw []byte) ([]byte, error) {
	var req pluginapi.RequestInterceptRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	g := currentGate()
	cfg := g.config()

	// Pass through untouched when disabled or explicitly bypassed, while still
	// echoing the current headers/body so the request is unchanged.
	passThrough := func() ([]byte, error) {
		return okEnvelope(pluginapi.RequestInterceptResponse{Headers: req.Headers, Body: req.Body})
	}
	if !bool(cfg.Enabled) {
		return passThrough()
	}
	if cfg.SkipHeader != "" && requestHeader(req, cfg.SkipHeader) != "" {
		return passThrough()
	}

	index := metadataString(req.Metadata, selectedAuthIndexKey)
	id := metadataString(req.Metadata, selectedAuthIDKey)
	acct := resolveAccount(index, id)
	g.mu.Lock()
	g.lastSeen = map[string]any{
		"metadata": req.Metadata,
		"resolved": acct,
		"model":    req.Model,
	}
	g.mu.Unlock()
	limit := limitFor(cfg, acct, req.Model)
	if limit <= 0 {
		return passThrough()
	}

	key := gateKey(cfg, acct, req.Model)
	errAcquire := g.acquire(context.Background(), req.RequestID, key, req.Model, acct.ID, limit)
	if errAcquire != nil {
		if bool(cfg.LogDecisions) {
			hostLog("warn", "concurrency-gate rejected request", map[string]any{
				"bucket":    key,
				"limit":     limit,
				"in_flight": g.load(key),
				"model":     req.Model,
				"reason":    errAcquire.Error(),
			})
		}
		return terminatedResponse(cfg, key, limit, g.load(key))
	}
	if bool(cfg.LogDecisions) {
		hostLog("info", "concurrency-gate admitted request", map[string]any{
			"bucket":    key,
			"limit":     limit,
			"in_flight": g.load(key),
			"model":     req.Model,
			"account":   acct.ID,
			"index":     acct.Index,
			"provider":  acct.Provider,
			"base_url":  acct.BaseURL,
			"compat":    acct.CompatName,
		})
	}
	return passThrough()
}

// terminatedResponse builds the downstream error body returned when a request
// cannot be admitted. It mirrors the OpenAI/Anthropic error envelope shape.
func terminatedResponse(cfg pluginConfig, key string, limit, inFlight int) ([]byte, error) {
	status := int(cfg.RejectStatusCode)
	if status < 400 || status > 599 {
		status = http.StatusTooManyRequests
	}
	errorType := "rate_limit_error"
	errorCode := "rate_limit_exceeded"
	if status != http.StatusTooManyRequests {
		errorType = "server_error"
		errorCode = "concurrency_limit_exceeded"
	}
	message := fmt.Sprintf("concurrency limit reached for %s (%d/%d in flight)", key, inFlight, limit)
	body, errMarshal := json.Marshal(map[string]any{
		"error": map[string]any{
			"type":    errorType,
			"code":    errorCode,
			"message": message,
		},
	})
	if errMarshal != nil {
		return nil, errMarshal
	}
	headers := http.Header{"Content-Type": []string{"application/json"}}
	if seconds := int(cfg.RetryAfterSeconds); seconds > 0 {
		headers.Set("Retry-After", strconv.Itoa(seconds))
	}
	return okEnvelope(pluginapi.RequestInterceptResponse{
		Terminate:       true,
		StatusCode:      status,
		ResponseHeaders: headers,
		ResponseBody:    body,
	})
}

func completeRequest(raw []byte) ([]byte, error) {
	var completion pluginapi.RequestCompletion
	if errUnmarshal := json.Unmarshal(raw, &completion); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	currentGate().release(completion.RequestID)
	return okEnvelope(struct{}{})
}

// pickAuth biases credential selection toward accounts with spare concurrency
// capacity. It never rejects: when every candidate is saturated it returns the
// least loaded one and lets the after-auth interceptor apply the configured
// reject/wait policy.
func pickAuth(raw []byte) ([]byte, error) {
	var req pluginapi.SchedulerPickRequest
	if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	g := currentGate()
	cfg := g.config()
	if !bool(cfg.Enabled) || !bool(cfg.LeastLoadedPick) || len(req.Candidates) == 0 {
		return okEnvelope(pluginapi.SchedulerPickResponse{Handled: false})
	}

	type scored struct {
		id     string
		load   int
		limit  int
		full   bool
		prio   int
		seq    uint64
		acctID string
	}
	scoredList := make([]scored, 0, len(req.Candidates))
	learned := make([]accountInfo, 0, len(req.Candidates))
	for _, candidate := range req.Candidates {
		if strings.TrimSpace(candidate.ID) == "" {
			continue
		}
		acct := candidateAccount(candidate)
		// Candidates carry base_url/compat_name in Attributes; the after-auth
		// metadata only carries the auth ID, so cache the mapping here.
		if acct.BaseURL != "" || acct.CompatName != "" || acct.Provider != "" {
			learned = append(learned, acct)
		}
		limit := limitFor(cfg, acct, req.Model)
		load := 0
		if limit > 0 {
			load = g.load(gateKey(cfg, acct, req.Model))
		}
		scoredList = append(scoredList, scored{
			id:     candidate.ID,
			load:   load,
			limit:  limit,
			full:   limit > 0 && load >= limit,
			prio:   candidate.Priority,
			acctID: acct.ID,
		})
	}
	g.rememberAccounts(learned)
	if len(scoredList) == 0 {
		return okEnvelope(pluginapi.SchedulerPickResponse{Handled: false})
	}

	hasCapacity := false
	for _, item := range scoredList {
		if !item.full {
			hasCapacity = true
			break
		}
	}

	sort.SliceStable(scoredList, func(i, j int) bool {
		if hasCapacity && scoredList[i].full != scoredList[j].full {
			return !scoredList[i].full
		}
		// Compare load ratio so different ceilings stay comparable.
		left := loadRatio(scoredList[i].load, scoredList[i].limit)
		right := loadRatio(scoredList[j].load, scoredList[j].limit)
		if left != right {
			return left < right
		}
		if scoredList[i].load != scoredList[j].load {
			return scoredList[i].load < scoredList[j].load
		}
		if scoredList[i].prio != scoredList[j].prio {
			return scoredList[i].prio > scoredList[j].prio
		}
		return scoredList[i].id < scoredList[j].id
	})

	chosen := scoredList[0]
	if !hasCapacity {
		// Every candidate is saturated. Delegate to the host's own selector so the
		// after-auth interceptor can emit the configured reject/wait response:
		// a scheduler rejection cannot carry an HTTP status and would surface as 500.
		if bool(cfg.LogDecisions) {
			hostLog("debug", "concurrency-gate scheduler has no capacity, delegating", map[string]any{
				"candidates": len(scoredList),
				"model":      req.Model,
			})
		}
		g.mu.Lock()
		g.stats.PickDelegated++
		g.mu.Unlock()
		return okEnvelope(pluginapi.SchedulerPickResponse{Handled: false})
	}

	if bool(cfg.LogDecisions) {
		hostLog("debug", "concurrency-gate scheduler pick", map[string]any{
			"chosen":     chosen.id,
			"load":       chosen.load,
			"limit":      chosen.limit,
			"candidates": len(scoredList),
			"capacity":   hasCapacity,
		})
	}
	g.nextPickSeq()
	g.mu.Lock()
	g.stats.PickChosen++
	g.lastCandidates = append([]pluginapi.SchedulerAuthCandidate(nil), req.Candidates...)
	g.lastPickModel = req.Model
	g.mu.Unlock()
	return okEnvelope(pluginapi.SchedulerPickResponse{AuthID: chosen.id, Handled: true})
}

func loadRatio(load, limit int) float64 {
	if limit <= 0 {
		return 0
	}
	return float64(load) / float64(limit)
}

// candidateAccount rebuilds routing identity from a scheduler candidate without
// any host callback, so the pick stays cheap on the hot path.
func candidateAccount(candidate pluginapi.SchedulerAuthCandidate) accountInfo {
	acct := accountInfo{ID: strings.TrimSpace(candidate.ID), Provider: strings.TrimSpace(candidate.Provider)}
	if candidate.Attributes != nil {
		if value := strings.TrimSpace(candidate.Attributes["base_url"]); value != "" {
			acct.BaseURL = value
		}
		if value := strings.TrimSpace(candidate.Attributes["compat_name"]); value != "" {
			acct.CompatName = value
		}
	}
	if acct.ID != "" {
		if cached, ok := currentGate().accountByIndex("", acct.ID); ok {
			if acct.BaseURL == "" {
				acct.BaseURL = cached.BaseURL
			}
			if acct.CompatName == "" {
				acct.CompatName = cached.CompatName
			}
			if acct.Provider == "" {
				acct.Provider = cached.Provider
			}
			acct.Index = cached.Index
		}
	}
	return acct
}

func managementRegistration() map[string]any {
	return map[string]any{
		"resources": []map[string]any{
			{
				"Path":        "/status",
				"Menu":        "Concurrency Gate",
				"Description": "Live per-bucket in-flight counters, effective limits and decision statistics.",
			},
			{
				"Path":        "/accounts",
				"Menu":        "Concurrency Gate Accounts",
				"Description": "Cached credential routing identity (provider, base URL, compat name) plus the raw host auth list.",
			},
		},
	}
}

// accountsReport exposes the credential identity cache so operators can verify
// that bucket keys resolve to the upstreams they expect.
func accountsReport() ([]byte, error) {
	g := currentGate()
	refreshAccountsFromHost(g)
	g.mu.Lock()
	cached := make([]accountInfo, 0, len(g.accounts))
	for _, acct := range g.accounts {
		cached = append(cached, acct)
	}
	last := g.lastSeen
	candidates := append([]pluginapi.SchedulerAuthCandidate(nil), g.lastCandidates...)
	pickModel := g.lastPickModel
	g.mu.Unlock()
	sort.Slice(cached, func(i, j int) bool { return cached[i].Index < cached[j].Index })

	report := map[string]any{
		"cached":          cached,
		"last_seen":       last,
		"last_pick_model": pickModel,
		"last_candidates": candidates,
	}
	if raw, errCall := callHost("host.auth.list", map[string]any{}); errCall == nil {
		report["host_auth_list"] = json.RawMessage(raw)
	} else {
		report["host_auth_list_error"] = errCall.Error()
	}
	return json.MarshalIndent(report, "", "  ")
}

func handleManagement(raw []byte) ([]byte, error) {
	var req pluginapi.ManagementRequest
	if len(raw) > 0 {
		if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
			return nil, errUnmarshal
		}
	}
	g := currentGate()
	cfg := g.config()
	if !bool(cfg.ExposeManagementAPI) {
		return okEnvelope(pluginapi.ManagementResponse{
			StatusCode: http.StatusNotFound,
			Headers:    http.Header{"Content-Type": []string{"application/json"}},
			Body:       []byte(`{"error":"management api disabled"}`),
		})
	}
	snapshot := g.snapshot()
	sort.Slice(snapshot.Buckets, func(i, j int) bool {
		return snapshot.Buckets[i].Key < snapshot.Buckets[j].Key
	})
	var body []byte
	var errMarshal error
	if strings.HasSuffix(strings.TrimRight(req.Path, "/"), "/accounts") {
		body, errMarshal = accountsReport()
	} else {
		body, errMarshal = json.MarshalIndent(snapshot, "", "  ")
	}
	if errMarshal != nil {
		return nil, errMarshal
	}
	return okEnvelope(pluginapi.ManagementResponse{
		StatusCode: http.StatusOK,
		Headers:    http.Header{"Content-Type": []string{"application/json"}},
		Body:       body,
	})
}

func (g *gate) statsSnapshot() gateStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stats
}

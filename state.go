package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

var errNoSlot = errors.New("concurrency limit reached")

// bucket is one limiter cell: a hard ceiling plus a FIFO of waiters.
type bucket struct {
	limit   int
	used    int
	waiters []chan struct{}
}

// entry records one admitted request so it can be released exactly once and
// swept if the terminal callback never arrives.
type entry struct {
	key       string
	model     string
	account   string
	startedAt time.Time
}

// gate holds all limiter state. It is process-local: the plugin runs inside the
// CPA process, so in-memory accounting is exact for a single node.
type gate struct {
	mu       sync.Mutex
	cfg      pluginConfig
	buckets  map[string]*bucket
	entries  map[string]entry
	accounts map[string]accountInfo // auth index -> account
	byID     map[string]accountInfo // auth id -> account
	loadedAt time.Time
	loading  bool
	pickSeq  uint64
	stats    gateStats
	// lastSeen / lastCandidates keep the most recent routing inputs so the
	// management resource can explain how a bucket key resolved.
	lastSeen       map[string]any
	lastCandidates []pluginapi.SchedulerAuthCandidate
	lastPickModel  string
}

type gateStats struct {
	Admitted      uint64 `json:"admitted"`
	Rejected      uint64 `json:"rejected"`
	Waited        uint64 `json:"waited"`
	WaitTimeout   uint64 `json:"wait_timeout"`
	Released      uint64 `json:"released"`
	Swept         uint64 `json:"swept"`
	PickChosen    uint64 `json:"scheduler_picks"`
	PickDelegated uint64 `json:"scheduler_delegated"`
}

func newGate(cfg pluginConfig) *gate {
	return &gate{
		cfg:      cfg.normalized(),
		buckets:  make(map[string]*bucket),
		entries:  make(map[string]entry),
		accounts: make(map[string]accountInfo),
		byID:     make(map[string]accountInfo),
	}
}

func (g *gate) config() pluginConfig {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.cfg
}

func (g *gate) setConfig(cfg pluginConfig) {
	normalized := cfg.normalized()
	g.mu.Lock()
	g.cfg = normalized
	// Ceilings are re-resolved per admission, so a lowered limit takes effect on
	// the next request without dropping in-flight accounting.
	g.mu.Unlock()
}

func (g *gate) bucketLocked(key string) *bucket {
	existing, ok := g.buckets[key]
	if !ok {
		existing = &bucket{limit: int(g.cfg.DefaultLimit)}
		g.buckets[key] = existing
	}
	return existing
}

// acquire reserves a slot for requestID under key. It is idempotent: a retry of
// the same request ID against the same key keeps the existing slot, and a retry
// against a different key moves the slot.
func (g *gate) acquire(ctx context.Context, requestID, key, model, account string, limit int) error {
	if requestID == "" {
		requestID = key + "|" + time.Now().Format(time.RFC3339Nano)
	}
	g.mu.Lock()
	if existing, ok := g.entries[requestID]; ok {
		if existing.key == key {
			g.mu.Unlock()
			return nil
		}
		g.releaseLocked(requestID)
	}
	if limit <= 0 {
		g.entries[requestID] = entry{key: "", model: model, account: account, startedAt: time.Now()}
		g.stats.Admitted++
		g.mu.Unlock()
		return nil
	}
	bucket := g.bucketLocked(key)
	bucket.limit = limit
	if bucket.used < bucket.limit {
		bucket.used++
		g.entries[requestID] = entry{key: key, model: model, account: account, startedAt: time.Now()}
		g.stats.Admitted++
		g.mu.Unlock()
		return nil
	}

	waitMS := int(g.cfg.WaitTimeoutMS)
	waitMode := g.cfg.OnLimit == onLimitWait
	if !waitMode || waitMS <= 0 {
		g.stats.Rejected++
		g.mu.Unlock()
		return errNoSlot
	}

	var deadline <-chan time.Time
	timer := time.NewTimer(time.Duration(waitMS) * time.Millisecond)
	defer timer.Stop()
	deadline = timer.C

	for {
		waiter := make(chan struct{})
		bucket.waiters = append(bucket.waiters, waiter)
		g.stats.Waited++
		g.mu.Unlock()

		var wakeErr error
		select {
		case <-waiter:
			// Slot handed over; loop re-checks under the lock.
		case <-ctx.Done():
			wakeErr = ctx.Err()
		case <-deadline:
			wakeErr = errNoSlot
		}

		g.mu.Lock()
		bucket = g.bucketLocked(key)
		if wakeErr != nil {
			bucket.waiters = removeWaiter(bucket.waiters, waiter)
			if errors.Is(wakeErr, errNoSlot) {
				g.stats.WaitTimeout++
			} else {
				g.stats.Rejected++
			}
			g.mu.Unlock()
			return wakeErr
		}
		if bucket.used < bucket.limit {
			bucket.used++
			g.entries[requestID] = entry{key: key, model: model, account: account, startedAt: time.Now()}
			g.stats.Admitted++
			g.mu.Unlock()
			return nil
		}
		// Spurious wakeup: wait again on the same deadline.
	}
}

func removeWaiter(waiters []chan struct{}, target chan struct{}) []chan struct{} {
	for index, waiter := range waiters {
		if waiter == target {
			return append(waiters[:index], waiters[index+1:]...)
		}
	}
	return waiters
}

func (g *gate) release(requestID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.releaseLocked(requestID)
}

func (g *gate) releaseLocked(requestID string) bool {
	existing, ok := g.entries[requestID]
	if !ok {
		return false
	}
	delete(g.entries, requestID)
	if existing.key != "" {
		if bucket, okBucket := g.buckets[existing.key]; okBucket {
			if bucket.used > 0 {
				bucket.used--
			}
			if len(bucket.waiters) > 0 {
				waiter := bucket.waiters[0]
				bucket.waiters = bucket.waiters[1:]
				close(waiter)
			}
			if bucket.used == 0 && len(bucket.waiters) == 0 {
				delete(g.buckets, existing.key)
			}
		}
	}
	g.stats.Released++
	return true
}

// load reports the current in-flight count for one key.
func (g *gate) load(key string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	if bucket, ok := g.buckets[key]; ok {
		return bucket.used
	}
	return 0
}

// sweep releases entries whose terminal callback never arrived.
func (g *gate) sweep(now time.Time) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	ttl := time.Duration(g.cfg.StateTTLSeconds) * time.Second
	if ttl <= 0 {
		return 0
	}
	swept := 0
	for requestID, existing := range g.entries {
		if now.Sub(existing.startedAt) < ttl {
			continue
		}
		g.releaseLocked(requestID)
		swept++
	}
	g.stats.Swept += uint64(swept)
	return swept
}

func (g *gate) rememberAccounts(accounts []accountInfo) {
	if len(accounts) == 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, acct := range accounts {
		if acct.Index != "" {
			g.accounts[acct.Index] = acct
		}
		if acct.ID != "" {
			g.byID[acct.ID] = acct
		}
	}
	g.loadedAt = time.Now()
}

func (g *gate) accountByIndex(index, id string) (accountInfo, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if index != "" {
		if acct, ok := g.accounts[index]; ok {
			return acct, true
		}
	}
	if id != "" {
		if acct, ok := g.byID[id]; ok {
			return acct, true
		}
	}
	return accountInfo{}, false
}

func (g *gate) shouldRefreshAccounts() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.loading {
		return false
	}
	if g.loadedAt.IsZero() {
		g.loading = true
		return true
	}
	if time.Since(g.loadedAt) < 10*time.Second {
		return false
	}
	g.loading = true
	return true
}

func (g *gate) finishAccountRefresh() {
	g.mu.Lock()
	g.loading = false
	g.loadedAt = time.Now()
	g.mu.Unlock()
}

// nextPickSeq returns a monotonically increasing counter used to rotate ties in
// least-loaded scheduling.
func (g *gate) nextPickSeq() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.pickSeq++
	return g.pickSeq
}

type bucketSnapshot struct {
	Key      string `json:"key"`
	InFlight int    `json:"in_flight"`
	Limit    int    `json:"limit"`
	Waiting  int    `json:"waiting"`
}

type gateSnapshot struct {
	Enabled        bool             `json:"enabled"`
	KeyBy          string           `json:"key_by"`
	PerModel       bool             `json:"per_model"`
	DefaultLimit   int              `json:"default_limit"`
	OnLimit        string           `json:"on_limit"`
	WaitTimeoutMS  int              `json:"wait_timeout_ms"`
	StateTTLSecond int              `json:"state_ttl_seconds"`
	Limits         []limitRule      `json:"limits"`
	Buckets        []bucketSnapshot `json:"buckets"`
	InFlight       int              `json:"in_flight_total"`
	Stats          gateStats        `json:"stats"`
	// HostSchema / PluginSchema make version mismatches visible without digging
	// through host logs.
	HostSchema   uint32 `json:"host_schema_version"`
	PluginSchema uint32 `json:"plugin_schema_version"`
}

func (g *gate) snapshot() gateSnapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := gateSnapshot{
		Enabled:        bool(g.cfg.Enabled),
		KeyBy:          g.cfg.KeyBy,
		PerModel:       bool(g.cfg.PerModel),
		DefaultLimit:   int(g.cfg.DefaultLimit),
		OnLimit:        g.cfg.OnLimit,
		WaitTimeoutMS:  int(g.cfg.WaitTimeoutMS),
		StateTTLSecond: int(g.cfg.StateTTLSeconds),
		Limits:         append([]limitRule(nil), g.cfg.Limits...),
		InFlight:       len(g.entries),
		Stats:          g.stats,
		HostSchema:     hostSchemaVersion(),
		PluginSchema:   currentSchemaVersion(),
	}
	for key, bucket := range g.buckets {
		out.Buckets = append(out.Buckets, bucketSnapshot{
			Key:      key,
			InFlight: bucket.used,
			Limit:    bucket.limit,
			Waiting:  len(bucket.waiters),
		})
	}
	return out
}

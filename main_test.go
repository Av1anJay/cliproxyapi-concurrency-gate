package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
	"gopkg.in/yaml.v3"
)

func decodeConfig(t *testing.T, raw string) pluginConfig {
	t.Helper()
	cfg := defaultConfig()
	if err := yaml.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("yaml.Unmarshal() error = %v", err)
	}
	return cfg.normalized()
}

func TestDecodeConfigCompactAndMappingLimits(t *testing.T) {
	cfg := decodeConfig(t, `
enabled: true
default_limit: 8
key_by: upstream
per_model: true
on_limit: wait
wait_timeout_ms: 1500
limits:
  - "provider:codex=2"
  - match: "upstream:api.example.com"
    limit: 4
    models: ["gpt-5*"]
  - provider: claude
    limit: 1
`)
	if cfg.DefaultLimit != 8 {
		t.Fatalf("DefaultLimit = %d, want 8", cfg.DefaultLimit)
	}
	if cfg.KeyBy != keyByUpstream || !bool(cfg.PerModel) {
		t.Fatalf("key settings = %q/%v", cfg.KeyBy, cfg.PerModel)
	}
	if cfg.OnLimit != onLimitWait || cfg.WaitTimeoutMS != 1500 {
		t.Fatalf("on_limit settings = %q/%d", cfg.OnLimit, cfg.WaitTimeoutMS)
	}
	if len(cfg.Limits) != 3 {
		t.Fatalf("len(Limits) = %d, want 3", len(cfg.Limits))
	}
	if cfg.Limits[0].Match != "provider:codex" || cfg.Limits[0].Limit != 2 {
		t.Fatalf("compact rule = %+v", cfg.Limits[0])
	}
	if cfg.Limits[1].Limit != 4 || len(cfg.Limits[1].Models) != 1 {
		t.Fatalf("mapping rule = %+v", cfg.Limits[1])
	}
	if cfg.Limits[2].Match != "provider:claude" || cfg.Limits[2].Limit != 1 {
		t.Fatalf("bare mapping rule = %+v", cfg.Limits[2])
	}
}

func TestDecodeConfigAcceptsQuotedScalars(t *testing.T) {
	cfg := decodeConfig(t, `
enabled: "true"
default_limit: "3"
on_limit: ""
per_model: "false"
wait_timeout_ms: ""
reject_status_code: "503"
`)
	if !bool(cfg.Enabled) {
		t.Fatal("Enabled = false, want true")
	}
	if cfg.DefaultLimit != 3 {
		t.Fatalf("DefaultLimit = %d, want 3", cfg.DefaultLimit)
	}
	if cfg.OnLimit != onLimitReject {
		t.Fatalf("OnLimit = %q, want %q", cfg.OnLimit, onLimitReject)
	}
	if bool(cfg.PerModel) {
		t.Fatal("PerModel = true, want false")
	}
	if cfg.WaitTimeoutMS != defaultWaitTimeoutMS {
		t.Fatalf("WaitTimeoutMS = %d, want default %d", cfg.WaitTimeoutMS, defaultWaitTimeoutMS)
	}
	if cfg.RejectStatusCode != 503 {
		t.Fatalf("RejectStatusCode = %d, want 503", cfg.RejectStatusCode)
	}
}

func TestDecodeConfigRejectsBadLimits(t *testing.T) {
	for _, raw := range []string{
		"limits:\n  - \"provider:codex\"\n",
		"limits:\n  - \"provider:codex=abc\"\n",
		"limits:\n  - limit: 2\n",
	} {
		cfg := defaultConfig()
		if err := yaml.Unmarshal([]byte(raw), &cfg); err == nil {
			t.Fatalf("yaml.Unmarshal(%q) error = nil, want error", raw)
		}
	}
}

func TestLimitForMatching(t *testing.T) {
	cfg := decodeConfig(t, `
default_limit: 6
limits:
  - "provider:codex=2"
  - match: "upstream:api.example.com"
    limit: 4
    models: ["gpt-5*"]
  - "compat:openrouter=1"
  - "label:prod*=3"
`)
	codex := accountInfo{ID: "codex-a", Provider: "codex"}
	if got := limitFor(cfg, codex, "gpt-5"); got != 2 {
		t.Fatalf("codex limit = %d, want 2", got)
	}
	compat := accountInfo{ID: "or-a", Provider: "openai-compatibility", CompatName: "openrouter", BaseURL: "https://openrouter.ai/api/v1"}
	if got := limitFor(cfg, compat, "gpt-5"); got != 1 {
		t.Fatalf("compat limit = %d, want 1", got)
	}
	other := accountInfo{ID: "claude-a", Provider: "claude"}
	if got := limitFor(cfg, other, "claude-3"); got != 6 {
		t.Fatalf("default limit = %d, want 6", got)
	}
}

func TestLimitForModelScopedRule(t *testing.T) {
	cfg := decodeConfig(t, `
default_limit: 0
limits:
  - match: "upstream:api.example.com"
    limit: 4
    models: ["gpt-5*"]
`)
	acct := accountInfo{ID: "x", BaseURL: "https://api.example.com/v1"}
	if got := limitFor(cfg, acct, "gpt-5.1-codex"); got != 4 {
		t.Fatalf("matching model limit = %d, want 4", got)
	}
	if got := limitFor(cfg, acct, "claude-3"); got != 0 {
		t.Fatalf("non-matching model limit = %d, want 0", got)
	}
}

func TestGateKeyModes(t *testing.T) {
	acct := accountInfo{ID: "acct-id", Index: "idx-1", Provider: "openai", CompatName: "openrouter", BaseURL: "https://openrouter.ai/api/v1"}

	byAccount := decodeConfig(t, "key_by: account\n")
	if got := gateKey(byAccount, acct, "gpt-5"); got != "acct:idx-1" {
		t.Fatalf("account key = %q", got)
	}
	byUpstream := decodeConfig(t, "key_by: upstream\n")
	if got := gateKey(byUpstream, acct, "gpt-5"); got != "up:openrouter" {
		t.Fatalf("upstream key = %q", got)
	}
	byProvider := decodeConfig(t, "key_by: provider\nper_model: true\n")
	if got := gateKey(byProvider, acct, "GPT-5"); got != "prov:openai|gpt-5" {
		t.Fatalf("provider+model key = %q", got)
	}
}

func TestAcquireRejectsAtLimit(t *testing.T) {
	g := newGate(decodeConfig(t, "default_limit: 2\n"))
	if err := g.acquire(context.Background(), "r1", "k", "m", "a", 2); err != nil {
		t.Fatalf("acquire r1: %v", err)
	}
	if err := g.acquire(context.Background(), "r2", "k", "m", "a", 2); err != nil {
		t.Fatalf("acquire r2: %v", err)
	}
	if err := g.acquire(context.Background(), "r3", "k", "m", "a", 2); !errors.Is(err, errNoSlot) {
		t.Fatalf("acquire r3 error = %v, want errNoSlot", err)
	}
	if load := g.load("k"); load != 2 {
		t.Fatalf("load = %d, want 2", load)
	}
}

func TestAcquireIsIdempotentPerRequestID(t *testing.T) {
	g := newGate(decodeConfig(t, "default_limit: 1\n"))
	if err := g.acquire(context.Background(), "r1", "k", "m", "a", 1); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	// A retry of the same request (same bucket) must not consume a second slot.
	if err := g.acquire(context.Background(), "r1", "k", "m", "a", 1); err != nil {
		t.Fatalf("retry acquire: %v", err)
	}
	if load := g.load("k"); load != 1 {
		t.Fatalf("load after retry = %d, want 1", load)
	}
}

func TestReleaseFreesSlotAndWakesWaiter(t *testing.T) {
	g := newGate(decodeConfig(t, "default_limit: 1\non_limit: wait\nwait_timeout_ms: 2000\n"))
	if err := g.acquire(context.Background(), "r1", "k", "m", "a", 1); err != nil {
		t.Fatalf("acquire r1: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- g.acquire(context.Background(), "r2", "k", "m", "a", 1)
	}()
	// Give the waiter time to queue.
	time.Sleep(50 * time.Millisecond)
	if load := g.load("k"); load != 1 {
		t.Fatalf("load while waiting = %d, want 1", load)
	}
	g.release("r1")
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waiter acquire error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter was never woken")
	}
	if load := g.load("k"); load != 1 {
		t.Fatalf("load after handover = %d, want 1", load)
	}
}

func TestWaitTimeoutReturnsNoSlot(t *testing.T) {
	g := newGate(decodeConfig(t, "default_limit: 1\non_limit: wait\nwait_timeout_ms: 100\n"))
	if err := g.acquire(context.Background(), "r1", "k", "m", "a", 1); err != nil {
		t.Fatalf("acquire r1: %v", err)
	}
	start := time.Now()
	err := g.acquire(context.Background(), "r2", "k", "m", "a", 1)
	if !errors.Is(err, errNoSlot) {
		t.Fatalf("waiter error = %v, want errNoSlot", err)
	}
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Fatalf("waited %s, expected ~100ms", elapsed)
	}
	if stats := g.statsSnapshot(); stats.WaitTimeout != 1 {
		t.Fatalf("WaitTimeout = %d, want 1", stats.WaitTimeout)
	}
}

func TestWaitHonoursContextCancellation(t *testing.T) {
	g := newGate(decodeConfig(t, "default_limit: 1\non_limit: wait\nwait_timeout_ms: 5000\n"))
	if err := g.acquire(context.Background(), "r1", "k", "m", "a", 1); err != nil {
		t.Fatalf("acquire r1: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	err := g.acquire(ctx, "r2", "k", "m", "a", 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter error = %v, want context.Canceled", err)
	}
	// The canceled waiter must not leak into the queue.
	g.mu.Lock()
	waiting := 0
	if bucket, ok := g.buckets["k"]; ok {
		waiting = len(bucket.waiters)
	}
	g.mu.Unlock()
	if waiting != 0 {
		t.Fatalf("waiters = %d, want 0", waiting)
	}
}

func TestUnlimitedLimitSkipsBuckets(t *testing.T) {
	g := newGate(decodeConfig(t, "default_limit: 0\n"))
	for index := 0; index < 50; index++ {
		if err := g.acquire(context.Background(), "r", "k", "m", "a", 0); err != nil {
			t.Fatalf("acquire %d: %v", index, err)
		}
	}
	if load := g.load("k"); load != 0 {
		t.Fatalf("load = %d, want 0 for unlimited", load)
	}
}

func TestConcurrentAcquireNeverExceedsLimit(t *testing.T) {
	const limit = 3
	g := newGate(decodeConfig(t, "default_limit: 3\n"))
	var wg sync.WaitGroup
	var mu sync.Mutex
	admitted := 0
	rejected := 0
	peak := 0
	for index := 0; index < 64; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			requestID := "r" + string(rune('a'+index%26)) + string(rune('a'+index/26))
			err := g.acquire(context.Background(), requestID, "k", "m", "a", limit)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				admitted++
				if current := g.load("k"); current > peak {
					peak = current
				}
			} else {
				rejected++
			}
		}(index)
	}
	wg.Wait()
	if peak > limit {
		t.Fatalf("peak in-flight = %d, want <= %d", peak, limit)
	}
	if admitted+rejected != 64 {
		t.Fatalf("admitted+rejected = %d, want 64", admitted+rejected)
	}
}

func TestSweepReleasesStaleEntries(t *testing.T) {
	g := newGate(decodeConfig(t, "default_limit: 1\nstate_ttl_seconds: 1\n"))
	if err := g.acquire(context.Background(), "r1", "k", "m", "a", 1); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if swept := g.sweep(time.Now()); swept != 0 {
		t.Fatalf("sweep released fresh entry: %d", swept)
	}
	if swept := g.sweep(time.Now().Add(2 * time.Second)); swept != 1 {
		t.Fatalf("sweep released %d, want 1", swept)
	}
	if load := g.load("k"); load != 0 {
		t.Fatalf("load after sweep = %d, want 0", load)
	}
}

func TestCompleteRequestReleasesSlot(t *testing.T) {
	g := newGate(decodeConfig(t, "default_limit: 1\n"))
	swapGate(t, g)
	if err := g.acquire(context.Background(), "req-1", "k", "m", "a", 1); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	raw, errMarshal := json.Marshal(pluginapi.RequestCompletion{RequestID: "req-1", Outcome: pluginapi.RequestCompletionSucceeded})
	if errMarshal != nil {
		t.Fatalf("marshal completion: %v", errMarshal)
	}
	if _, errComplete := completeRequest(raw); errComplete != nil {
		t.Fatalf("completeRequest() error = %v", errComplete)
	}
	if load := g.load("k"); load != 0 {
		t.Fatalf("load after completion = %d, want 0", load)
	}
}

func swapGate(t *testing.T, g *gate) {
	t.Helper()
	stateMu.Lock()
	previous := state
	state = g
	stateMu.Unlock()
	t.Cleanup(func() {
		stateMu.Lock()
		state = previous
		stateMu.Unlock()
	})
}

func TestPickAuthPrefersFreeCandidate(t *testing.T) {
	cfg := decodeConfig(t, "default_limit: 1\nleast_loaded_pick: true\n")
	g := newGate(cfg)
	swapGate(t, g)
	// Saturate the first candidate's bucket.
	busy := accountInfo{ID: "busy", Index: "idx-busy", Provider: "codex"}
	if err := g.acquire(context.Background(), "r1", gateKey(cfg, busy, "gpt-5"), "gpt-5", busy.ID, 1); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	g.rememberAccounts([]accountInfo{busy, {ID: "idle", Index: "idx-idle", Provider: "codex"}})

	raw, errMarshal := json.Marshal(pluginapi.SchedulerPickRequest{
		Model: "gpt-5",
		Candidates: []pluginapi.SchedulerAuthCandidate{
			{ID: "busy", Provider: "codex"},
			{ID: "idle", Provider: "codex"},
		},
	})
	if errMarshal != nil {
		t.Fatalf("marshal pick request: %v", errMarshal)
	}
	respRaw, errPick := pickAuth(raw)
	if errPick != nil {
		t.Fatalf("pickAuth() error = %v", errPick)
	}
	var env envelope
	if errUnmarshal := json.Unmarshal(respRaw, &env); errUnmarshal != nil {
		t.Fatalf("decode envelope: %v", errUnmarshal)
	}
	var resp pluginapi.SchedulerPickResponse
	if errUnmarshal := json.Unmarshal(env.Result, &resp); errUnmarshal != nil {
		t.Fatalf("decode pick response: %v", errUnmarshal)
	}
	if !resp.Handled || resp.AuthID != "idle" {
		t.Fatalf("pick response = %+v, want idle", resp)
	}
}

func TestPickAuthDelegatesWhenAllSaturated(t *testing.T) {
	cfg := decodeConfig(t, "default_limit: 1\non_limit: reject\nleast_loaded_pick: true\n")
	g := newGate(cfg)
	swapGate(t, g)
	acct := accountInfo{ID: "only", Index: "idx-only", Provider: "codex"}
	g.rememberAccounts([]accountInfo{acct})
	if err := g.acquire(context.Background(), "r1", gateKey(cfg, acct, "gpt-5"), "gpt-5", acct.ID, 1); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	raw, _ := json.Marshal(pluginapi.SchedulerPickRequest{
		Model:      "gpt-5",
		Candidates: []pluginapi.SchedulerAuthCandidate{{ID: "only", Provider: "codex"}},
	})
	respRaw, errPick := pickAuth(raw)
	if errPick != nil {
		t.Fatalf("pickAuth() error = %v", errPick)
	}
	var env envelope
	if errUnmarshal := json.Unmarshal(respRaw, &env); errUnmarshal != nil {
		t.Fatalf("decode envelope: %v", errUnmarshal)
	}
	var resp pluginapi.SchedulerPickResponse
	if errUnmarshal := json.Unmarshal(env.Result, &resp); errUnmarshal != nil {
		t.Fatalf("decode pick response: %v", errUnmarshal)
	}
	// A scheduler rejection cannot carry an HTTP status (CPA maps it to 500), so
	// the plugin must hand the pick back and let the after-auth gate reject.
	if resp.Handled || resp.Reject {
		t.Fatalf("pick response = %+v, want unhandled delegation", resp)
	}
	if stats := g.statsSnapshot(); stats.PickDelegated != 1 {
		t.Fatalf("PickDelegated = %d, want 1", stats.PickDelegated)
	}
}

func TestPickAuthUnhandledWhenDisabled(t *testing.T) {
	g := newGate(decodeConfig(t, "enabled: false\ndefault_limit: 1\n"))
	swapGate(t, g)
	raw, _ := json.Marshal(pluginapi.SchedulerPickRequest{
		Model:      "gpt-5",
		Candidates: []pluginapi.SchedulerAuthCandidate{{ID: "a", Provider: "codex"}},
	})
	respRaw, errPick := pickAuth(raw)
	if errPick != nil {
		t.Fatalf("pickAuth() error = %v", errPick)
	}
	var env envelope
	if errUnmarshal := json.Unmarshal(respRaw, &env); errUnmarshal != nil {
		t.Fatalf("decode envelope: %v", errUnmarshal)
	}
	var resp pluginapi.SchedulerPickResponse
	if errUnmarshal := json.Unmarshal(env.Result, &resp); errUnmarshal != nil {
		t.Fatalf("decode pick response: %v", errUnmarshal)
	}
	if resp.Handled {
		t.Fatalf("Handled = true, want false when disabled")
	}
}

func TestConfigureRejectsLegacySchema(t *testing.T) {
	raw, _ := json.Marshal(lifecycleRequest{SchemaVersion: 1})
	if err := configure(raw); err == nil {
		t.Fatal("configure() error = nil for schema version 1")
	}
}

func TestConfigureAppliesDefaults(t *testing.T) {
	g := newGate(defaultConfig())
	swapGate(t, g)
	raw, _ := json.Marshal(lifecycleRequest{SchemaVersion: 8, ConfigYAML: []byte("enabled: true\ndefault_limit: 5\n")})
	if err := configure(raw); err != nil {
		t.Fatalf("configure() error = %v", err)
	}
	cfg := g.config()
	if cfg.DefaultLimit != 5 {
		t.Fatalf("DefaultLimit = %d, want 5", cfg.DefaultLimit)
	}
	if cfg.KeyBy != keyByAccount {
		t.Fatalf("KeyBy = %q, want %q", cfg.KeyBy, keyByAccount)
	}
}

func TestInterceptAfterAuthPassesThroughWhenUnlimited(t *testing.T) {
	g := newGate(decodeConfig(t, "enabled: true\ndefault_limit: 0\n"))
	swapGate(t, g)
	raw, _ := json.Marshal(pluginapi.RequestInterceptRequest{
		RequestID: "r1",
		Model:     "gpt-5",
		Metadata:  map[string]any{selectedAuthIDKey: "acct-1"},
	})
	respRaw, errIntercept := interceptAfterAuth(raw)
	if errIntercept != nil {
		t.Fatalf("interceptAfterAuth() error = %v", errIntercept)
	}
	var env envelope
	if errUnmarshal := json.Unmarshal(respRaw, &env); errUnmarshal != nil {
		t.Fatalf("decode envelope: %v", errUnmarshal)
	}
	var resp pluginapi.RequestInterceptResponse
	if errUnmarshal := json.Unmarshal(env.Result, &resp); errUnmarshal != nil {
		t.Fatalf("decode intercept response: %v", errUnmarshal)
	}
	if resp.Terminate {
		t.Fatalf("Terminate = true, want pass-through")
	}
}

func TestInterceptAfterAuthRejectsOverLimit(t *testing.T) {
	g := newGate(decodeConfig(t, "enabled: true\ndefault_limit: 1\non_limit: reject\nreject_status_code: 429\nretry_after_seconds: 2\n"))
	swapGate(t, g)
	payload := func(requestID string) []byte {
		raw, _ := json.Marshal(pluginapi.RequestInterceptRequest{
			RequestID: requestID,
			Model:     "gpt-5",
			Metadata:  map[string]any{selectedAuthIndexKey: "idx-1", selectedAuthIDKey: "acct-1"},
		})
		return raw
	}
	if _, err := interceptAfterAuth(payload("r1")); err != nil {
		t.Fatalf("first intercept: %v", err)
	}
	respRaw, errIntercept := interceptAfterAuth(payload("r2"))
	if errIntercept != nil {
		t.Fatalf("second intercept: %v", errIntercept)
	}
	var env envelope
	if errUnmarshal := json.Unmarshal(respRaw, &env); errUnmarshal != nil {
		t.Fatalf("decode envelope: %v", errUnmarshal)
	}
	var resp pluginapi.RequestInterceptResponse
	if errUnmarshal := json.Unmarshal(env.Result, &resp); errUnmarshal != nil {
		t.Fatalf("decode intercept response: %v", errUnmarshal)
	}
	if !resp.Terminate || resp.StatusCode != 429 {
		t.Fatalf("response = %+v, want terminate 429", resp)
	}
	if got := resp.ResponseHeaders.Get("Retry-After"); got != "2" {
		t.Fatalf("Retry-After = %q, want 2", got)
	}
	var body struct {
		Error struct {
			Type    string `json:"type"`
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if errUnmarshal := json.Unmarshal(resp.ResponseBody, &body); errUnmarshal != nil {
		t.Fatalf("decode error body: %v", errUnmarshal)
	}
	if body.Error.Type != "rate_limit_error" || body.Error.Code != "rate_limit_exceeded" {
		t.Fatalf("error body = %+v", body.Error)
	}
}

func TestInterceptAfterAuthHonoursSkipHeader(t *testing.T) {
	g := newGate(decodeConfig(t, "enabled: true\ndefault_limit: 1\nskip_header: X-Skip-Gate\n"))
	swapGate(t, g)
	raw, _ := json.Marshal(pluginapi.RequestInterceptRequest{
		RequestID: "r1",
		Model:     "gpt-5",
		Headers:   map[string][]string{"X-Skip-Gate": {"1"}},
		Metadata:  map[string]any{selectedAuthIDKey: "acct-1"},
	})
	respRaw, errIntercept := interceptAfterAuth(raw)
	if errIntercept != nil {
		t.Fatalf("interceptAfterAuth() error = %v", errIntercept)
	}
	var env envelope
	_ = json.Unmarshal(respRaw, &env)
	var resp pluginapi.RequestInterceptResponse
	_ = json.Unmarshal(env.Result, &resp)
	if resp.Terminate {
		t.Fatal("skip header did not bypass the gate")
	}
	if load := g.load("acct:acct-1"); load != 0 {
		t.Fatalf("load = %d, want 0 for bypassed request", load)
	}
}

// TestExampleConfigParses keeps config.example.yaml honest: the documented
// plugin subtree must decode and validate with the real parser.
func TestExampleConfigParses(t *testing.T) {
	raw, errRead := os.ReadFile("config.example.yaml")
	if errRead != nil {
		t.Fatalf("read config.example.yaml: %v", errRead)
	}
	var doc struct {
		Plugins struct {
			Configs map[string]pluginConfig `yaml:"configs"`
		} `yaml:"plugins"`
	}
	if errUnmarshal := yaml.Unmarshal(raw, &doc); errUnmarshal != nil {
		t.Fatalf("config.example.yaml does not parse: %v", errUnmarshal)
	}
	cfg, ok := doc.Plugins.Configs["concurrency-gate"]
	if !ok {
		t.Fatal("config.example.yaml has no plugins.configs.concurrency-gate block")
	}
	if errValidate := validateConfig(cfg); errValidate != nil {
		t.Fatalf("config.example.yaml fails validation: %v", errValidate)
	}
	if got := int(cfg.DefaultLimit); got != 2 {
		t.Fatalf("default_limit = %d, want 2", got)
	}
	if got := string(cfg.KeyBy); got != keyByUpstream {
		t.Fatalf("key_by = %q, want %q", got, keyByUpstream)
	}
	if len(cfg.Limits) != 4 {
		t.Fatalf("limits parsed = %d, want 4", len(cfg.Limits))
	}
	if !cfg.Limits[3].modelsMatch("deepseek-reasoner") {
		t.Fatal("model-scoped rule did not match deepseek-reasoner")
	}
	if cfg.Limits[3].modelsMatch("gpt-5") {
		t.Fatal("model-scoped rule matched an unrelated model")
	}
}

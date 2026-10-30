# cliproxyapi-concurrency-gate

Per-upstream / per-account concurrency limiter for [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI) v8, delivered as a native plugin (`.so`).

CLIProxyAPI has no built-in "max in-flight requests per upstream or per credential" setting — `credential_concurrency` and the home-pool limiter only cover internal scheduling, not a per-upstream cap. This plugin composes three public plugin hooks to build one:

| Hook | Role |
| --- | --- |
| `scheduler.pick` | Prefers credentials that still have a free slot, so CPA spreads load instead of hammering one account. |
| `request.intercept_after` | Runs after auth selection, where the credential identity is known — this is where the slot is acquired or the request is rejected/queued. |
| `request.complete` | Runs on success, failure, cancel and stream end — this is where the slot is always released, so counters cannot leak. |

## What it does

- Buckets keyed **per credential** (`key_by: account`), **per upstream** (`key_by: upstream`, by compat name or base URL) or **per provider** (`key_by: provider`).
- Optional per-model buckets (`per_model: true`).
- Hard ceiling per bucket with two policies: `on_limit: reject` (fast `429` + `Retry-After`) or `on_limit: wait` (FIFO queue up to `wait_timeout_ms`).
- Scoped overrides via `limits`, e.g. one upstream gets 4 slots while everything else gets 1.
- Live state over the management API (`/status`, `/accounts`) for tuning and debugging.

## Install

```bash
make linux           # -> bin/concurrency-gate-go.so
```

Copy the artifact into the directory CPA loads plugins from and point the config at it:

```yaml
plugins:
  enabled: true
  dir: "/etc/cliproxyapi/plugins"
  configs:
    concurrency-gate:
      enabled: true
      priority: 10
      default_limit: 2
      key_by: upstream
      on_limit: reject
```

Restart CPA. `plugins.configs.concurrency-gate` keys map 1:1 to the fields below; the same fields are exposed in the CPA management UI (they are declared as `ConfigFields` in the plugin registration).

## Configuration

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `enabled` | bool | `true` | Master switch. Disabled = pure pass-through. |
| `default_limit` | int | `0` | Ceiling applied to any bucket without a matching rule. `0` = unlimited. |
| `key_by` | enum | `account` | `account` = one credential; `upstream` = compat name or base URL; `provider` = provider family. |
| `per_model` | bool | `false` | Append `\|<model>` to the bucket key. |
| `on_limit` | enum | `reject` | `reject` or `wait`. |
| `wait_timeout_ms` | int | `30000` | Max queue time when `on_limit: wait`. |
| `limits` | list | `[]` | Scoped ceilings, see below. |
| `reject_status_code` | int | `429` | Status returned on rejection. |
| `retry_after_seconds` | int | `1` | `Retry-After` header on rejection. `0` omits it. |
| `state_ttl_seconds` | int | `1800` | Idle bucket/lease retention before the sweeper drops it. |
| `log_decisions` | bool | `false` | Log every admit/reject/wait with bucket and account identity. |
| `least_loaded_pick` | bool | `true` | Use `scheduler.pick` to prefer credentials with free slots. |
| `skip_header` | string | `""` | When set, requests carrying this header bypass the gate. |
| `expose_management_api` | bool | `true` | Register the `/status` and `/accounts` management resources. |

### `limits` syntax

Two interchangeable forms — a compact string, or a mapping when you also want to scope by model:

```yaml
limits:
  - "upstream:openrouter=1"            # compat name or base URL
  - "provider:codex=4"                 # provider family
  - "label:prod*=3"                    # label glob
  - "acct:openai-compatibility:x:ab12=1"
  - match: "compat:deepseek"
    limit: 2
    models: ["deepseek-reasoner"]      # only these models count
```

Matcher prefixes: `acct:`, `upstream:`, `compat:`, `provider:`, `label:`, `model:`. The first rule that matches wins, so put the narrowest rules first.

## Management resources

With `expose_management_api: true`:

- `GET /v0/resource/plugins/concurrency-gate/status` — per-bucket `in_flight` / `limit` / `waiting`, plus `admitted`, `rejected`, `waited`, `wait_timeout`, `released`, `swept`, `scheduler_picks`, `scheduler_delegated`.
- `GET /v0/resource/plugins/concurrency-gate/accounts` — cached credential routing identity (provider, base URL, compat name), the last intercepted request's metadata, the last scheduler candidate list, and the raw `host.auth.list` reply. Use this when a bucket key looks wrong.

## Verified behaviour

End-to-end against a real `cli-proxy-api` v8.0.47 binary with local fake upstreams (2 s latency each), counting the upstream's own peak in-flight:

| Scenario | Result |
| --- | --- |
| `key_by: upstream`, limit 2, 6 concurrent | 2 × 200, 4 × 429, upstream peak **2** |
| `key_by: account`, 2 upstreams, limit 1 each, 6 concurrent | 2 × 200, 4 × 429, each upstream peak **1** |
| `limits: [upstream:a=2, compat:b=1]`, `default_limit: 0`, 8 concurrent | 3 × 200 (2 + 1), 5 × 429 |
| `on_limit: wait`, limit 1, 6 concurrent | 6 × 200, upstream peak **1**, wall clock ≈ 6 × latency |
| Streaming (`stream: true`), 6 concurrent, limit 2 + 1 | 3 × 200 with full SSE bodies, 3 × 429, in-flight back to **0** |

After every scenario `in_flight_total` returns to `0` and `admitted == released`, i.e. no slot leaks on success, failure, cancel or stream end.

## Notes and limitations

- **Rejection status.** Scheduler-level rejection cannot carry an HTTP status through `SchedulerPickResponse`, so the plugin never rejects from the scheduler. When every candidate is saturated it delegates to the host selector (`scheduler_delegated`) and the after-auth interceptor produces the clean `429` + `Retry-After`. If you see `500` on rejection, the gate is not the source.
- **Bucket identity depends on host metadata.** `request.intercept_after` exposes only `selected_auth_id` / `selected_auth_index`. `base_url` and `compat_name` are learned from `scheduler.pick` candidates and `host.auth.list`, so `key_by: upstream` needs at least one scheduler pick per credential. Until then the bucket falls back to `unknown|<model>`.
- One `openai-compatibility` block with several `keys:` becomes **one** auth entry in CPA, not one per key. Use separate `openai-compatibility` blocks (or separate auth files) if you want per-key buckets.
- `plugins.configs.concurrency-gate.enabled: false` makes the **host** skip loading the plugin entirely, so the management resources also disappear (the endpoint returns `404`). The plugin's own `enabled` field only matters for a hot reconfigure.
- The gate counts requests, not tokens. Token/TPM shaping is out of scope.

## Build

```bash
make linux        # .so (Linux)
make darwin       # .dylib
make windows      # .dll
make test         # unit tests
```

Requires Go with CGO enabled (`CGO_ENABLED=1`); the plugin ABI is `-buildmode=c-shared`.

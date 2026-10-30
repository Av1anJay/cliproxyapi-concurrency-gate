package main

import (
	"fmt"
	"path"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// flexBool/flexInt tolerate the loose YAML the CPA management UI writes
// (quoted numbers, empty strings, nulls) without discarding plugin defaults.
type flexBool bool

func (b *flexBool) UnmarshalYAML(node *yaml.Node) error {
	if b == nil || yamlNodeEmpty(node) {
		return nil
	}
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("expected boolean, got YAML kind %d", node.Kind)
	}
	raw := strings.TrimSpace(node.Value)
	if raw == "" {
		return nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return fmt.Errorf("invalid boolean %q", raw)
	}
	*b = flexBool(value)
	return nil
}

type flexInt int

func (i *flexInt) UnmarshalYAML(node *yaml.Node) error {
	if i == nil || yamlNodeEmpty(node) {
		return nil
	}
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("expected integer, got YAML kind %d", node.Kind)
	}
	raw := strings.TrimSpace(node.Value)
	if raw == "" {
		return nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fmt.Errorf("invalid integer %q", raw)
	}
	*i = flexInt(value)
	return nil
}

type stringList []string

func (l *stringList) UnmarshalYAML(node *yaml.Node) error {
	if l == nil || yamlNodeEmpty(node) {
		return nil
	}
	var out []string
	appendOne := func(raw string) {
		if raw = strings.TrimSpace(raw); raw != "" {
			out = append(out, raw)
		}
	}
	switch node.Kind {
	case yaml.SequenceNode:
		for _, item := range node.Content {
			if item != nil {
				appendOne(item.Value)
			}
		}
	case yaml.ScalarNode:
		for _, part := range strings.Split(node.Value, ",") {
			appendOne(part)
		}
	default:
		return fmt.Errorf("expected string list, got YAML kind %d", node.Kind)
	}
	if len(out) > 0 {
		*l = out
	}
	return nil
}

func yamlNodeEmpty(node *yaml.Node) bool {
	if node == nil || node.Kind == 0 {
		return true
	}
	if node.Tag == "!!null" {
		return true
	}
	return node.Kind == yaml.ScalarNode && strings.TrimSpace(node.Value) == ""
}

// limitRule scopes a concurrency ceiling to a set of upstreams, providers,
// accounts or labels.
type limitRule struct {
	Match  string
	Limit  int
	Models stringList
}

// limitRules accepts both the mapping form
//
//   - match: "provider:codex"
//     limit: 2
//
// and the compact string form "- provider:codex=2".
type limitRules []limitRule

func (r *limitRules) UnmarshalYAML(node *yaml.Node) error {
	if r == nil || yamlNodeEmpty(node) {
		return nil
	}
	if node.Kind != yaml.SequenceNode {
		return fmt.Errorf("limits must be a sequence, got YAML kind %d", node.Kind)
	}
	var out []limitRule
	for _, item := range node.Content {
		if item == nil || yamlNodeEmpty(item) {
			continue
		}
		switch item.Kind {
		case yaml.ScalarNode:
			rule, err := parseCompactRule(item.Value)
			if err != nil {
				return err
			}
			out = append(out, rule)
		case yaml.MappingNode:
			rule, err := parseMappingRule(item)
			if err != nil {
				return err
			}
			out = append(out, rule)
		default:
			return fmt.Errorf("limits entries must be strings or mappings, got YAML kind %d", item.Kind)
		}
	}
	*r = out
	return nil
}

func parseCompactRule(raw string) (limitRule, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return limitRule{}, nil
	}
	separator := strings.LastIndexAny(raw, "=")
	if separator <= 0 {
		return limitRule{}, fmt.Errorf("compact limit %q must look like \"match=limit\"", raw)
	}
	match := strings.TrimSpace(raw[:separator])
	limit, err := strconv.Atoi(strings.TrimSpace(raw[separator+1:]))
	if err != nil {
		return limitRule{}, fmt.Errorf("invalid limit in %q: %w", raw, err)
	}
	return limitRule{Match: match, Limit: limit}, nil
}

func parseMappingRule(node *yaml.Node) (limitRule, error) {
	var wire struct {
		Match    string     `yaml:"match"`
		Provider string     `yaml:"provider"`
		Upstream string     `yaml:"upstream"`
		Compat   string     `yaml:"compat"`
		Account  string     `yaml:"account"`
		Label    string     `yaml:"label"`
		Limit    flexInt    `yaml:"limit"`
		Models   stringList `yaml:"models"`
	}
	if err := node.Decode(&wire); err != nil {
		return limitRule{}, err
	}
	match := strings.TrimSpace(wire.Match)
	if match == "" {
		// Shorthand keys are equivalent to "match: <kind>:<value>".
		for _, candidate := range []struct{ kind, value string }{
			{"provider", wire.Provider},
			{"upstream", wire.Upstream},
			{"compat", wire.Compat},
			{"account", wire.Account},
			{"label", wire.Label},
		} {
			if value := strings.TrimSpace(candidate.value); value != "" {
				match = candidate.kind + ":" + value
				break
			}
		}
	}
	if match == "" {
		return limitRule{}, fmt.Errorf("limits entry is missing \"match\"")
	}
	return limitRule{Match: match, Limit: int(wire.Limit), Models: wire.Models}, nil
}

// pluginConfig is the subtree under plugins.configs.concurrency-gate.
type pluginConfig struct {
	Enabled             flexBool    `yaml:"enabled"`
	DefaultLimit        flexInt     `yaml:"default_limit"`
	OnLimit             string      `yaml:"on_limit"`
	WaitTimeoutMS       flexInt     `yaml:"wait_timeout_ms"`
	KeyBy               string      `yaml:"key_by"`
	PerModel            flexBool    `yaml:"per_model"`
	Limits              limitRules  `yaml:"limits"`
	StateTTLSeconds     flexInt     `yaml:"state_ttl_seconds"`
	LogDecisions        flexBool    `yaml:"log_decisions"`
	SkipHeader          string      `yaml:"skip_header"`
	LeastLoadedPick     flexBool    `yaml:"least_loaded_pick"`
	ExposeManagementAPI flexBool    `yaml:"expose_management_api"`
	RejectStatusCode    flexInt     `yaml:"reject_status_code"`
	RetryAfterSeconds   flexInt     `yaml:"retry_after_seconds"`
	Empty               yaml.Node   `yaml:"-"`
	unknown             [0]struct{} `yaml:"-"`
}

const (
	onLimitReject = "reject"
	onLimitWait   = "wait"

	keyByAccount  = "account"
	keyByUpstream = "upstream"
	keyByProvider = "provider"

	defaultWaitTimeoutMS  = 30000
	defaultStateTTLSecond = 1800
)

func defaultConfig() pluginConfig {
	return pluginConfig{
		Enabled:             true,
		OnLimit:             onLimitReject,
		WaitTimeoutMS:       defaultWaitTimeoutMS,
		KeyBy:               keyByAccount,
		StateTTLSeconds:     defaultStateTTLSecond,
		LeastLoadedPick:     true,
		ExposeManagementAPI: true,
		RejectStatusCode:    429,
		RetryAfterSeconds:   1,
	}
}

func (c pluginConfig) normalized() pluginConfig {
	out := c
	if strings.TrimSpace(string(out.OnLimit)) == "" {
		out.OnLimit = onLimitReject
	}
	out.OnLimit = strings.ToLower(strings.TrimSpace(out.OnLimit))
	if out.OnLimit != onLimitWait {
		out.OnLimit = onLimitReject
	}
	if strings.TrimSpace(string(out.KeyBy)) == "" {
		out.KeyBy = keyByAccount
	}
	out.KeyBy = strings.ToLower(strings.TrimSpace(out.KeyBy))
	switch out.KeyBy {
	case keyByAccount, keyByUpstream, keyByProvider:
	default:
		out.KeyBy = keyByAccount
	}
	if out.WaitTimeoutMS < 0 {
		out.WaitTimeoutMS = 0
	}
	if out.StateTTLSeconds <= 0 {
		out.StateTTLSeconds = defaultStateTTLSecond
	}
	if out.RejectStatusCode < 400 || out.RejectStatusCode > 599 {
		out.RejectStatusCode = 429
	}
	if out.RetryAfterSeconds < 0 {
		out.RetryAfterSeconds = 0
	}
	out.SkipHeader = strings.TrimSpace(out.SkipHeader)
	return out
}

// accountInfo is the routing identity of one upstream credential as far as the
// gate is concerned. Only non-secret fields are ever cached.
type accountInfo struct {
	ID         string
	Index      string
	Provider   string
	BaseURL    string
	CompatName string
	Label      string
}

func (a accountInfo) accountKey() string {
	switch {
	case strings.TrimSpace(a.Index) != "":
		return "acct:" + strings.TrimSpace(a.Index)
	case strings.TrimSpace(a.ID) != "":
		return "acct:" + strings.TrimSpace(a.ID)
	default:
		return ""
	}
}

func (a accountInfo) upstreamKey() string {
	if name := strings.TrimSpace(a.CompatName); name != "" {
		return "up:" + strings.ToLower(name)
	}
	if base := strings.TrimSpace(a.BaseURL); base != "" {
		return "up:" + strings.ToLower(strings.TrimSuffix(strings.TrimSuffix(base, "/"), "/v1"))
	}
	if provider := strings.TrimSpace(a.Provider); provider != "" {
		return "up:" + strings.ToLower(provider)
	}
	return ""
}

func (a accountInfo) providerKey() string {
	if provider := strings.TrimSpace(a.Provider); provider != "" {
		return "prov:" + strings.ToLower(provider)
	}
	return ""
}

// gateKey resolves the limiter bucket for one request.
func gateKey(cfg pluginConfig, acct accountInfo, model string) string {
	var base string
	switch cfg.KeyBy {
	case keyByUpstream:
		base = acct.upstreamKey()
	case keyByProvider:
		base = acct.providerKey()
	default:
		base = acct.accountKey()
	}
	if base == "" {
		base = "unknown"
	}
	if bool(cfg.PerModel) && strings.TrimSpace(model) != "" {
		base += "|" + strings.ToLower(strings.TrimSpace(model))
	}
	return base
}

// limitFor returns the effective ceiling for one account/model pair.
// 0 means unlimited.
func limitFor(cfg pluginConfig, acct accountInfo, model string) int {
	for _, rule := range cfg.Limits {
		if !rule.modelsMatch(model) {
			continue
		}
		if rule.matchAccount(acct) {
			return rule.Limit
		}
	}
	return int(cfg.DefaultLimit)
}

func (r limitRule) modelsMatch(model string) bool {
	if len(r.Models) == 0 {
		return true
	}
	model = strings.ToLower(strings.TrimSpace(model))
	for _, pattern := range r.Models {
		if globMatch(strings.ToLower(strings.TrimSpace(pattern)), model) {
			return true
		}
	}
	return false
}

func (r limitRule) matchAccount(acct accountInfo) bool {
	match := strings.TrimSpace(r.Match)
	if match == "" {
		return false
	}
	if match == "*" {
		return true
	}
	kind := ""
	value := match
	if index := strings.IndexByte(match, ':'); index > 0 {
		kind = strings.ToLower(strings.TrimSpace(match[:index]))
		value = strings.TrimSpace(match[index+1:])
	}
	if value == "" {
		return false
	}
	switch kind {
	case "provider":
		return strings.EqualFold(value, acct.Provider)
	case "upstream":
		base := strings.ToLower(acct.BaseURL)
		name := strings.ToLower(acct.CompatName)
		needle := strings.ToLower(value)
		return (base != "" && strings.Contains(base, needle)) || (name != "" && globMatch(needle, name))
	case "compat":
		return globMatch(strings.ToLower(value), strings.ToLower(acct.CompatName))
	case "account", "auth":
		return globMatch(strings.ToLower(value), strings.ToLower(acct.ID)) ||
			globMatch(strings.ToLower(value), strings.ToLower(acct.Index))
	case "label":
		return globMatch(strings.ToLower(value), strings.ToLower(acct.Label))
	case "":
		// Bare token: provider first, then upstream/compat name, then account.
		if strings.EqualFold(value, acct.Provider) {
			return true
		}
		if acct.CompatName != "" && globMatch(strings.ToLower(value), strings.ToLower(acct.CompatName)) {
			return true
		}
		if acct.BaseURL != "" && strings.Contains(strings.ToLower(acct.BaseURL), strings.ToLower(value)) {
			return true
		}
		return globMatch(strings.ToLower(value), strings.ToLower(acct.ID)) ||
			globMatch(strings.ToLower(value), strings.ToLower(acct.Index))
	default:
		return false
	}
}

func globMatch(pattern, value string) bool {
	if pattern == "" || value == "" {
		return false
	}
	if pattern == "*" {
		return true
	}
	if !strings.ContainsAny(pattern, "*?[") {
		return pattern == value
	}
	ok, err := path.Match(pattern, value)
	return err == nil && ok
}

package main

/*
#include <stdint.h>
#include <stdlib.h>

typedef struct {
	void* ptr;
	size_t len;
} cliproxy_buffer;

typedef int (*cliproxy_host_call_fn)(void*, const char*, const uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_host_free_fn)(void*, size_t);

typedef struct {
	uint32_t abi_version;
	void* host_ctx;
	cliproxy_host_call_fn call;
	cliproxy_host_free_fn free_buffer;
} cliproxy_host_api;

typedef int (*cliproxy_plugin_call_fn)(char*, uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_plugin_free_fn)(void*, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);

typedef struct {
	uint32_t abi_version;
	cliproxy_plugin_call_fn call;
	cliproxy_plugin_free_fn free_buffer;
	cliproxy_plugin_shutdown_fn shutdown;
} cliproxy_plugin_api;

extern int cliproxyPluginCall(char*, uint8_t*, size_t, cliproxy_buffer*);
extern void cliproxyPluginFree(void*, size_t);
extern void cliproxyPluginShutdown(void);

static const cliproxy_host_api* stored_host;

static void store_host_api(const cliproxy_host_api* host) {
	stored_host = host;
}

static int call_host_api(const char* method, const uint8_t* request, size_t request_len, cliproxy_buffer* response) {
	if (stored_host == NULL || stored_host->call == NULL) {
		return 1;
	}
	return stored_host->call(stored_host->host_ctx, method, request, request_len, response);
}

static void free_host_buffer(void* ptr, size_t len) {
	if (stored_host != NULL && stored_host->free_buffer != NULL && ptr != NULL) {
		stored_host->free_buffer(ptr, len);
	}
}
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
	"gopkg.in/yaml.v3"
)

const (
	pluginID      = "concurrency-gate"
	pluginVersion = "0.1.0"
	// minSchemaVersion is required for request.complete and active termination.
	minSchemaVersion = 2
)

var (
	stateMu sync.RWMutex
	state   = newGate(defaultConfig())
	// sweepOnce starts the janitor exactly once per process.
	sweepOnce sync.Once
)

type envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *envelopeError  `json:"error,omitempty"`
}

type envelopeError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type lifecycleRequest struct {
	ConfigYAML    []byte `json:"config_yaml"`
	SchemaVersion uint32 `json:"schema_version"`
}

type registration struct {
	SchemaVersion uint32                 `json:"schema_version"`
	Metadata      pluginapi.Metadata     `json:"metadata"`
	Capabilities  registrationCapability `json:"capabilities"`
}

type registrationCapability struct {
	Scheduler                 bool `json:"scheduler"`
	SchedulerAcrossPriorities bool `json:"scheduler_across_priorities,omitempty"`
	RequestInterceptor        bool `json:"request_interceptor"`
	RequestLifecyclePlugin    bool `json:"request_lifecycle_plugin"`
	ManagementAPI             bool `json:"management_api"`
}

func main() {}

//export cliproxy_plugin_init
func cliproxy_plugin_init(host *C.cliproxy_host_api, plugin *C.cliproxy_plugin_api) C.int {
	if plugin == nil {
		return 1
	}
	C.store_host_api(host)
	plugin.abi_version = C.uint32_t(pluginabi.ABIVersion)
	plugin.call = C.cliproxy_plugin_call_fn(C.cliproxyPluginCall)
	plugin.free_buffer = C.cliproxy_plugin_free_fn(C.cliproxyPluginFree)
	plugin.shutdown = C.cliproxy_plugin_shutdown_fn(C.cliproxyPluginShutdown)
	return 0
}

//export cliproxyPluginCall
func cliproxyPluginCall(method *C.char, request *C.uint8_t, requestLen C.size_t, response *C.cliproxy_buffer) C.int {
	if response != nil {
		response.ptr = nil
		response.len = 0
	}
	if method == nil {
		writeResponse(response, errorEnvelope("invalid_method", "method is required"))
		return 1
	}
	var requestBytes []byte
	if request != nil && requestLen > 0 {
		requestBytes = C.GoBytes(unsafe.Pointer(request), C.int(requestLen))
	}
	raw, errHandle := handleMethod(C.GoString(method), requestBytes)
	if errHandle != nil {
		writeResponse(response, errorEnvelope("plugin_error", errHandle.Error()))
		return 1
	}
	writeResponse(response, raw)
	return 0
}

//export cliproxyPluginFree
func cliproxyPluginFree(ptr unsafe.Pointer, len C.size_t) {
	if ptr != nil {
		C.free(ptr)
	}
	_ = len
}

//export cliproxyPluginShutdown
func cliproxyPluginShutdown() {
	stateMu.Lock()
	state = newGate(defaultConfig())
	stateMu.Unlock()
}

func currentGate() *gate {
	stateMu.RLock()
	defer stateMu.RUnlock()
	return state
}

func handleMethod(method string, request []byte) ([]byte, error) {
	switch method {
	case pluginabi.MethodPluginRegister, pluginabi.MethodPluginReconfigure:
		if errConfigure := configure(request); errConfigure != nil {
			return nil, errConfigure
		}
		return okEnvelope(pluginRegistration())
	case pluginabi.MethodRequestInterceptAfter:
		return interceptAfterAuth(request)
	case pluginabi.MethodRequestComplete:
		return completeRequest(request)
	case pluginabi.MethodSchedulerPick:
		return pickAuth(request)
	case pluginabi.MethodManagementRegister:
		return okEnvelope(managementRegistration())
	case pluginabi.MethodManagementHandle:
		return handleManagement(request)
	case pluginabi.MethodRequestInterceptBefore:
		// Declared for symmetry only: this plugin admits after credential
		// selection, so the before-auth phase is a pass-through.
		return passThroughRequest(request)
	default:
		return errorEnvelope("unknown_method", "unknown method: "+method), nil
	}
}

func configure(raw []byte) error {
	var req lifecycleRequest
	if len(raw) > 0 {
		if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
			return errUnmarshal
		}
	}
	if req.SchemaVersion < minSchemaVersion {
		return fmt.Errorf("concurrency-gate requires host schema version %d or newer (got %d)", minSchemaVersion, req.SchemaVersion)
	}
	cfg := defaultConfig()
	if len(req.ConfigYAML) > 0 {
		if errUnmarshal := yaml.Unmarshal(req.ConfigYAML, &cfg); errUnmarshal != nil {
			return fmt.Errorf("parse plugin config: %w", errUnmarshal)
		}
	}
	if errValidate := validateConfig(cfg); errValidate != nil {
		return errValidate
	}
	currentGate().setConfig(cfg)
	startJanitor()
	return nil
}

func validateConfig(cfg pluginConfig) error {
	normalized := cfg.normalized()
	if int(normalized.DefaultLimit) < 0 {
		return fmt.Errorf("default_limit must be >= 0")
	}
	for _, rule := range cfg.Limits {
		if rule.Limit < 0 {
			return fmt.Errorf("limit for %q must be >= 0", rule.Match)
		}
	}
	if int(cfg.WaitTimeoutMS) < 0 {
		return fmt.Errorf("wait_timeout_ms must be >= 0")
	}
	return nil
}

func startJanitor() {
	sweepOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for now := range ticker.C {
				currentGate().sweep(now)
			}
		}()
	})
}

func pluginRegistration() registration {
	return registration{
		SchemaVersion: pluginabi.SchemaVersion,
		Metadata: pluginapi.Metadata{
			Name:             pluginID,
			Version:          pluginVersion,
			Author:           "Av1anJay",
			GitHubRepository: "https://github.com/AvianJay/cliproxyapi-concurrency-gate",
			ConfigFields:     configFields(),
		},
		Capabilities: registrationCapability{
			Scheduler:                 true,
			SchedulerAcrossPriorities: true,
			RequestInterceptor:        true,
			RequestLifecyclePlugin:    true,
			ManagementAPI:             true,
		},
	}
}

func configFields() []pluginapi.ConfigField {
	return []pluginapi.ConfigField{
		{
			Name:        "default_limit",
			Type:        pluginapi.ConfigFieldTypeInteger,
			Default:     0,
			Description: "Default in-flight ceiling per bucket. 0 means unlimited.",
		},
		{
			Name:        "key_by",
			Type:        pluginapi.ConfigFieldTypeEnum,
			Default:     keyByAccount,
			EnumValues:  []string{keyByAccount, keyByUpstream, keyByProvider},
			Description: "Bucket identity: per credential, per upstream (base URL / compat name), or per provider.",
		},
		{
			Name:        "per_model",
			Type:        pluginapi.ConfigFieldTypeBoolean,
			Default:     false,
			Description: "Append the model name to the bucket key so each model is limited separately.",
		},
		{
			Name:        "on_limit",
			Type:        pluginapi.ConfigFieldTypeEnum,
			Default:     onLimitReject,
			EnumValues:  []string{onLimitReject, onLimitWait},
			Description: "reject returns an error immediately; wait queues up to wait_timeout_ms for a slot.",
		},
		{
			Name:        "wait_timeout_ms",
			Type:        pluginapi.ConfigFieldTypeInteger,
			Default:     defaultWaitTimeoutMS,
			Description: "Maximum queue time when on_limit=wait.",
		},
		{
			Name:        "limits",
			Type:        pluginapi.ConfigFieldTypeArray,
			Description: "Scoped ceilings, e.g. [{match: \"provider:codex\", limit: 2}] or [\"upstream:api.example.com=4\"].",
		},
		{
			Name:        "state_ttl_seconds",
			Type:        pluginapi.ConfigFieldTypeInteger,
			Default:     defaultStateTTLSecond,
			Description: "Safety sweep: release slots whose terminal callback never arrived.",
		},
		{
			Name:        "least_loaded_pick",
			Type:        pluginapi.ConfigFieldTypeBoolean,
			Default:     true,
			Description: "Scheduler mode: prefer candidates with the fewest in-flight requests, and skip full accounts when another candidate has capacity.",
		},
		{
			Name:        "reject_status_code",
			Type:        pluginapi.ConfigFieldTypeInteger,
			Default:     429,
			Description: "HTTP status returned to the client when a request is rejected.",
		},
		{
			Name:        "retry_after_seconds",
			Type:        pluginapi.ConfigFieldTypeInteger,
			Default:     1,
			Description: "Retry-After hint returned with rejections.",
		},
		{
			Name:        "log_decisions",
			Type:        pluginapi.ConfigFieldTypeBoolean,
			Default:     false,
			Description: "Emit a host log line for every admission decision.",
		},
		{
			Name:        "skip_header",
			Type:        pluginapi.ConfigFieldTypeString,
			Description: "When a request carries this header (any value), it bypasses the gate.",
		},
	}
}

func okEnvelope(v any) ([]byte, error) {
	raw, errMarshal := json.Marshal(v)
	if errMarshal != nil {
		return nil, errMarshal
	}
	return json.Marshal(envelope{OK: true, Result: raw})
}

func errorEnvelope(code, message string) []byte {
	raw, errMarshal := json.Marshal(envelope{OK: false, Error: &envelopeError{Code: code, Message: message}})
	if errMarshal != nil {
		return []byte(`{"ok":false,"error":{"code":"plugin_error","message":"encode error"}}`)
	}
	return raw
}

func writeResponse(response *C.cliproxy_buffer, raw []byte) {
	if response == nil || len(raw) == 0 {
		return
	}
	ptr := C.CBytes(raw)
	if ptr == nil {
		return
	}
	response.ptr = ptr
	response.len = C.size_t(len(raw))
}

// callHost invokes a host callback method and decodes the result envelope.
func callHost(method string, payload any) (json.RawMessage, error) {
	rawPayload, errMarshal := json.Marshal(payload)
	if errMarshal != nil {
		return nil, fmt.Errorf("marshal host callback %s: %w", method, errMarshal)
	}
	cMethod := C.CString(method)
	defer C.free(unsafe.Pointer(cMethod))

	var response C.cliproxy_buffer
	var requestPtr *C.uint8_t
	if len(rawPayload) > 0 {
		cPayload := C.CBytes(rawPayload)
		if cPayload == nil {
			return nil, fmt.Errorf("allocate host callback payload %s", method)
		}
		defer C.free(cPayload)
		requestPtr = (*C.uint8_t)(cPayload)
	}
	callCode := C.call_host_api(cMethod, requestPtr, C.size_t(len(rawPayload)), &response)
	var rawResponse []byte
	if response.ptr != nil && response.len > 0 {
		rawResponse = C.GoBytes(response.ptr, C.int(response.len))
	}
	if response.ptr != nil {
		C.free_host_buffer(response.ptr, response.len)
	}
	if len(rawResponse) == 0 {
		return nil, fmt.Errorf("host callback %s returned no response (code=%d)", method, int(callCode))
	}
	var env envelope
	if errUnmarshal := json.Unmarshal(rawResponse, &env); errUnmarshal != nil {
		return nil, fmt.Errorf("decode host callback envelope %s: %w", method, errUnmarshal)
	}
	if !env.OK {
		if env.Error != nil {
			return nil, fmt.Errorf("%s: %s", env.Error.Code, env.Error.Message)
		}
		return nil, fmt.Errorf("host callback %s failed", method)
	}
	if callCode != 0 {
		return nil, fmt.Errorf("host callback %s returned code=%d", method, int(callCode))
	}
	return append(json.RawMessage(nil), env.Result...), nil
}

func hostLog(level, message string, fields map[string]any) {
	payload := map[string]any{"level": level, "message": message}
	if len(fields) > 0 {
		payload["fields"] = fields
	}
	_, _ = callHost(pluginabi.MethodHostLog, payload)
}

func passThroughRequest(raw []byte) ([]byte, error) {
	var req pluginapi.RequestInterceptRequest
	if len(raw) > 0 {
		if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
			return nil, errUnmarshal
		}
	}
	return okEnvelope(pluginapi.RequestInterceptResponse{Headers: req.Headers, Body: req.Body})
}

func requestHeader(req pluginapi.RequestInterceptRequest, name string) string {
	if req.Headers == nil || strings.TrimSpace(name) == "" {
		return ""
	}
	return strings.TrimSpace(req.Headers.Get(name))
}

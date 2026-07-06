package config

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

// fakeEnv builds a getenv func from a map.
func fakeEnv(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

// noFile is a readFile that always fails (no mounted token).
func noFile(string) ([]byte, error) { return nil, os.ErrNotExist }

const validToken = "khz_agt_" + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestLoad_Minimal(t *testing.T) {
	c, err := Load(fakeEnv(map[string]string{
		EnvClusterID: "kubehz.in.net",
		EnvAPIURL:    "https://api.kubehz.cloud",
	}), noFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.ClusterID != "kubehz.in.net" {
		t.Errorf("clusterID = %q", c.ClusterID)
	}
	if c.APIURL != "https://api.kubehz.cloud" {
		t.Errorf("apiURL = %q", c.APIURL)
	}
	if c.HasToken() {
		t.Errorf("expected no token (API fallback), got one")
	}
	if c.FullInterval != DefaultFullInterval || c.Debounce != DefaultDebounce || c.MinGap != DefaultMinGap {
		t.Errorf("cadence defaults not applied: %s", c)
	}
	if c.ReportNamespaces {
		t.Errorf("ReportNamespaces must default to false (privacy)")
	}
	if c.Namespace != DefaultNamespace || c.SecretName != DefaultSecretName {
		t.Errorf("secret locator defaults wrong: %s", c)
	}
}

func TestLoad_MissingRequired(t *testing.T) {
	if _, err := Load(fakeEnv(map[string]string{EnvAPIURL: "https://x"}), noFile); err == nil {
		t.Error("expected error for missing CLUSTER_ID")
	}
	if _, err := Load(fakeEnv(map[string]string{EnvClusterID: "d"}), noFile); err == nil {
		t.Error("expected error for missing KUBEHZ_API_URL")
	}
}

// CLUSTER_ID lands in the heartbeat URL path — anything that could alter the
// path (separators, whitespace) must be rejected at load time.
func TestLoad_ValidatesClusterID(t *testing.T) {
	for _, ok := range []string{"kubehz.in.net", "dev-1.local", "a", "K8S_lab.example-1.com"} {
		if _, err := Load(fakeEnv(map[string]string{
			EnvClusterID: ok, EnvAPIURL: "https://x",
		}), noFile); err != nil {
			t.Errorf("valid cluster ID %q rejected: %v", ok, err)
		}
	}
	for _, bad := range []string{"a/b", "a b", "a?b", "../up", "#frag", "-lead", "trail-", ".dot", "a%2Fb"} {
		if _, err := Load(fakeEnv(map[string]string{
			EnvClusterID: bad, EnvAPIURL: "https://x",
		}), noFile); err == nil {
			t.Errorf("expected rejection for cluster ID %q", bad)
		}
	}
}

func TestLoad_RejectsPlainHTTP(t *testing.T) {
	_, err := Load(fakeEnv(map[string]string{
		EnvClusterID: "d", EnvAPIURL: "http://api.kubehz.cloud",
	}), noFile)
	if err == nil || !strings.Contains(err.Error(), "https") {
		t.Errorf("expected https rejection, got %v", err)
	}
}

func TestLoad_AllowsHTTPLoopback(t *testing.T) {
	for _, host := range []string{"http://localhost:3000", "http://127.0.0.1:8080"} {
		if _, err := Load(fakeEnv(map[string]string{
			EnvClusterID: "d", EnvAPIURL: host,
		}), noFile); err != nil {
			t.Errorf("loopback %s should be allowed: %v", host, err)
		}
	}
}

func TestLoad_TrimsTrailingSlash(t *testing.T) {
	c, err := Load(fakeEnv(map[string]string{
		EnvClusterID: "d", EnvAPIURL: "https://api.kubehz.cloud/",
	}), noFile)
	if err != nil {
		t.Fatal(err)
	}
	if c.APIURL != "https://api.kubehz.cloud" {
		t.Errorf("trailing slash not trimmed: %q", c.APIURL)
	}
}

func TestLoad_TokenFromEnv(t *testing.T) {
	c, err := Load(fakeEnv(map[string]string{
		EnvClusterID: "d", EnvAPIURL: "https://x", EnvAgentToken: validToken,
	}), noFile)
	if err != nil {
		t.Fatal(err)
	}
	if !c.HasToken() || c.AgentToken != validToken {
		t.Errorf("token not loaded from env")
	}
	// String() must never leak the token.
	if strings.Contains(c.String(), validToken) {
		t.Errorf("String() leaked the token: %s", c)
	}
}

func TestLoad_TokenFromFile(t *testing.T) {
	rf := func(path string) ([]byte, error) {
		if path != DefaultTokenFile {
			return nil, os.ErrNotExist
		}
		return []byte(validToken + "\n"), nil // trailing newline must be trimmed
	}
	c, err := Load(fakeEnv(map[string]string{
		EnvClusterID: "d", EnvAPIURL: "https://x",
	}), rf)
	if err != nil {
		t.Fatal(err)
	}
	if c.AgentToken != validToken {
		t.Errorf("token from file = %q, want %q", c.AgentToken, validToken)
	}
}

func TestLoad_MalformedTokenIsError(t *testing.T) {
	_, err := Load(fakeEnv(map[string]string{
		EnvClusterID: "d", EnvAPIURL: "https://x", EnvAgentToken: "not-a-token",
	}), noFile)
	if err == nil {
		t.Error("expected error for malformed token")
	}
	// The error must not echo the (albeit invalid) token material.
	if err != nil && strings.Contains(err.Error(), "not-a-token") {
		t.Errorf("error leaked token value: %v", err)
	}
}

func TestLoad_CadenceOverrides(t *testing.T) {
	c, err := Load(fakeEnv(map[string]string{
		EnvClusterID: "d", EnvAPIURL: "https://x",
		EnvFullInterval: "30s", EnvDebounce: "5s", EnvMinGap: "8s",
		EnvReportNamespaces: "true",
	}), noFile)
	if err != nil {
		t.Fatal(err)
	}
	if c.FullInterval != 30*time.Second || c.Debounce != 5*time.Second || c.MinGap != 8*time.Second {
		t.Errorf("cadence overrides not applied: %s", c)
	}
	if !c.ReportNamespaces {
		t.Errorf("ReportNamespaces override not applied")
	}
}

// KUBEHZ_DESIRED_POLL_SECONDS is an INTEGER of seconds (the unit is in the
// name), defaulting to 60s; zero/negative/non-numeric must fail fast.
func TestLoad_DesiredPollSeconds(t *testing.T) {
	c, err := Load(fakeEnv(map[string]string{
		EnvClusterID: "d", EnvAPIURL: "https://x",
	}), noFile)
	if err != nil {
		t.Fatal(err)
	}
	if c.DesiredPoll != DefaultDesiredPoll {
		t.Errorf("DesiredPoll default = %s, want %s", c.DesiredPoll, DefaultDesiredPoll)
	}

	c, err = Load(fakeEnv(map[string]string{
		EnvClusterID: "d", EnvAPIURL: "https://x", EnvDesiredPollSeconds: "90",
	}), noFile)
	if err != nil {
		t.Fatal(err)
	}
	if c.DesiredPoll != 90*time.Second {
		t.Errorf("DesiredPoll = %s, want 90s", c.DesiredPoll)
	}

	for _, bad := range []string{"0", "-5", "60s", "abc"} {
		if _, err := Load(fakeEnv(map[string]string{
			EnvClusterID: "d", EnvAPIURL: "https://x", EnvDesiredPollSeconds: bad,
		}), noFile); err == nil {
			t.Errorf("expected rejection for %s=%q", EnvDesiredPollSeconds, bad)
		}
	}
}

func TestLoad_RejectsNonPositiveDuration(t *testing.T) {
	_, err := Load(fakeEnv(map[string]string{
		EnvClusterID: "d", EnvAPIURL: "https://x", EnvMinGap: "0s",
	}), noFile)
	if err == nil {
		t.Error("expected error for non-positive duration")
	}
}

func TestValidateToken(t *testing.T) {
	if _, err := ValidateToken(validToken); err != nil {
		t.Errorf("valid token rejected: %v", err)
	}
	for _, bad := range []string{"", "khz_agt_short", "khzc_" + strings.Repeat("a", 64), validToken + "x"} {
		if _, err := ValidateToken(bad); err == nil {
			t.Errorf("expected rejection for %q", bad)
		}
	}
}

func TestResolveToken_FileErrorIsNonFatal(t *testing.T) {
	// A failing readFile must not fail Load — the API fallback covers it.
	_, err := resolveToken(fakeEnv(nil), func(string) ([]byte, error) {
		return nil, errors.New("permission denied")
	})
	if err != nil {
		t.Errorf("file read error should be non-fatal, got %v", err)
	}
}

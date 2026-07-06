// Package config resolves the kubehz-agent's runtime configuration from the
// environment (injected by the Deployment's ConfigMap) and the mounted
// agent-token Secret.
//
// Trust/privacy invariants enforced here by construction:
//   - The agent holds NO inbound credential — it only ever reads its OWN
//     outbound bearer token (A) and the two non-secret config values
//     (CLUSTER_ID, KUBEHZ_API_URL). There is no kubeconfig, SSH key, or
//     platform-pushed secret anywhere in this program.
//   - The token A is NEVER logged and NEVER echoed. Config.String() and every
//     log line in this repo redact it.
//   - Plain HTTP is rejected for a non-localhost API URL (AGENTS.md: "Plain
//     HTTP is a red flag"). Production MUST be https.
package config

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Env var names. CLUSTER_ID + KUBEHZ_API_URL mirror the existing bash agent's
// ConfigMap keys (lok8s .lok8s/libs/kubehz/manifests/agent/configmap.yaml) so
// the same deploy plumbing feeds both agents.
const (
	EnvClusterID        = "CLUSTER_ID"
	EnvAPIURL           = "KUBEHZ_API_URL"
	EnvAgentTokenFile   = "KUBEHZ_AGENT_TOKEN_FILE"
	EnvAgentToken       = "KUBEHZ_AGENT_TOKEN" // discouraged; tests/local only
	EnvFullInterval     = "KUBEHZ_FULL_INTERVAL"
	EnvDebounce         = "KUBEHZ_DEBOUNCE"
	EnvMinGap           = "KUBEHZ_MIN_GAP"
	EnvReportNamespaces = "KUBEHZ_REPORT_NAMESPACES"
	EnvNamespace        = "KUBEHZ_NAMESPACE" // for the Secret API fallback
	EnvSecretName       = "KUBEHZ_SECRET_NAME"
	// EnvDesiredPollSeconds is the desired-state pull cadence in WHOLE SECONDS
	// (an integer, not a Go duration — the name says the unit). The effective
	// wait adds up to 10% jitter to desynchronize a fleet.
	EnvDesiredPollSeconds = "KUBEHZ_DESIRED_POLL_SECONDS"
	// EnvMDNamespace is where the executor looks for MachineDeployments.
	// KubeOne's machine-controller serves them in kube-system by default.
	EnvMDNamespace = "KUBEHZ_MD_NAMESPACE"
	// EnvMaxReplicas is the agent-side per-pool replica CEILING (a guard-rail,
	// not an enable switch): a desiredReplicas outside 0..max is REFUSED and
	// reported failed — never rewritten to the bound and applied.
	EnvMaxReplicas = "KUBEHZ_MAX_REPLICAS"
)

// Defaults. The push cadence follows managed-platform-spec §2: a full snapshot
// every 60s, and an on-change push behind a 10s debounce with a 15s min-gap.
const (
	DefaultTokenFile    = "/var/run/secrets/kubehz/agent-token"
	DefaultNamespace    = "kubehz-system"
	DefaultSecretName   = "kubehz-agent"
	DefaultSecretKey    = "agent-token"
	DefaultFullInterval = 60 * time.Second
	DefaultDebounce     = 10 * time.Second
	DefaultMinGap       = 15 * time.Second
	// DefaultDesiredPoll: ~60s matches the server's ETag-cheap 304 path; the
	// desired-state loop needs no sub-minute latency (spec §3).
	DefaultDesiredPoll = 60 * time.Second
	// DefaultMDNamespace is KubeOne's machine-controller home for
	// MachineDeployments.
	DefaultMDNamespace = "kube-system"
	// DefaultMaxReplicas is the agent-side scaling ceiling. Deliberately BELOW
	// the API's own per-pool bound (kubehz-api allows 0..100): the agent's
	// guard-rail is the tighter fence, and an operator running larger pools
	// raises it consciously.
	DefaultMaxReplicas = 50
	// MaxMaxReplicas bounds the override itself — a ceiling of a million
	// nodes is a typo, not a fleet.
	MaxMaxReplicas = 10_000
)

// agentTokenRE is the format of secret A (§1.7.1): khz_agt_<hex(32B)> — 64 hex
// chars. Validating the shape here fails fast on a misconfigured mount rather
// than shipping a guaranteed-401 bearer.
var agentTokenRE = regexp.MustCompile(`^khz_agt_[0-9a-f]{64}$`)

// namespaceRE is the DNS-1123 label shape a Kubernetes namespace must have.
// KUBEHZ_MD_NAMESPACE feeds API request paths, so it is validated up front.
var namespaceRE = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)

// clusterIDRE constrains CLUSTER_ID to a DNS-name-like shape (the cluster
// domain, e.g. "kubehz.in.net"). The value is embedded in the heartbeat URL
// path, so anything with separators/whitespace ('/', '?', '#', spaces, …) is
// rejected up front — a typo'd CLUSTER_ID must fail fast, not silently POST to
// a mangled path. The publisher additionally path-escapes it (defense in depth).
var clusterIDRE = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9._-]{0,251}[A-Za-z0-9])?$`)

// Config is the fully-resolved agent configuration.
type Config struct {
	ClusterID string // cluster domain, e.g. "kubehz.in.net"
	APIURL    string // kubehz-api base, e.g. "https://api.kubehz.cloud"

	// AgentToken is bearer A. May be empty here when it is to be read from the
	// k8s API at runtime (see EnvAgentTokenFile vs the API fallback in package
	// kube). NEVER log this field directly — use String().
	AgentToken string

	// Namespace/SecretName locate the agent Secret for the API-read fallback.
	Namespace  string
	SecretName string

	FullInterval time.Duration
	Debounce     time.Duration
	MinGap       time.Duration

	// DesiredPoll is the desired-state pull cadence (P3). NOTE: there is
	// deliberately NO config that can turn acting ON — execution is entirely
	// server-gated (the /desired execution{} flags); the agent only exposes
	// guard-rails (poll cadence here; MDNamespace/MaxReplicas below), never
	// an enable override.
	DesiredPoll time.Duration

	// MDNamespace is where the scaling executor looks for MachineDeployments
	// (KubeOne default: kube-system).
	MDNamespace string
	// MaxReplicas is the agent-side per-pool ceiling: desiredReplicas outside
	// 0..MaxReplicas is refused (reported failed), never clamped-and-applied.
	MaxReplicas int

	// ReportNamespaces gates the privacy-sensitive fields (per-namespace pod
	// counts, event namespaces + messages). Default FALSE — spec §2's
	// "workload visibility without workload contents". A deployer opts in.
	ReportNamespaces bool
}

// HasToken reports whether a bearer token was already resolved from env/file.
// When false, the caller reads it from the k8s API before starting.
func (c *Config) HasToken() bool { return c.AgentToken != "" }

// String redacts the token so a Config is safe to log.
func (c *Config) String() string {
	tok := "<unset>"
	if c.AgentToken != "" {
		tok = "khz_agt_***redacted***"
	}
	return fmt.Sprintf(
		"Config{clusterID=%q apiURL=%q token=%s ns=%q secret=%q full=%s debounce=%s minGap=%s reportNamespaces=%t desiredPoll=%s mdNamespace=%q maxReplicas=%d}",
		c.ClusterID, c.APIURL, tok, c.Namespace, c.SecretName,
		c.FullInterval, c.Debounce, c.MinGap, c.ReportNamespaces,
		c.DesiredPoll, c.MDNamespace, c.MaxReplicas,
	)
}

// Load resolves the configuration. getenv abstracts os.LookupEnv and readFile
// abstracts os.ReadFile so the whole loader is unit-testable without touching
// the real environment or filesystem.
func Load(getenv func(string) (string, bool), readFile func(string) ([]byte, error)) (*Config, error) {
	c := &Config{
		Namespace:        lookupDefault(getenv, EnvNamespace, DefaultNamespace),
		SecretName:       lookupDefault(getenv, EnvSecretName, DefaultSecretName),
		FullInterval:     DefaultFullInterval,
		Debounce:         DefaultDebounce,
		MinGap:           DefaultMinGap,
		DesiredPoll:      DefaultDesiredPoll,
		MDNamespace:      lookupDefault(getenv, EnvMDNamespace, DefaultMDNamespace),
		MaxReplicas:      DefaultMaxReplicas,
		ReportNamespaces: false,
	}
	if !namespaceRE.MatchString(c.MDNamespace) {
		return nil, fmt.Errorf("%s must be a DNS-1123 label (got %q)", EnvMDNamespace, c.MDNamespace)
	}

	c.ClusterID = strings.TrimSpace(lookupDefault(getenv, EnvClusterID, ""))
	if c.ClusterID == "" {
		return nil, fmt.Errorf("%s is required", EnvClusterID)
	}
	if !clusterIDRE.MatchString(c.ClusterID) {
		return nil, fmt.Errorf("%s must be a DNS-style name (letters, digits, '.', '-', '_'; max 253 chars; got %q)",
			EnvClusterID, c.ClusterID)
	}

	rawURL := strings.TrimSpace(lookupDefault(getenv, EnvAPIURL, ""))
	if rawURL == "" {
		return nil, fmt.Errorf("%s is required", EnvAPIURL)
	}
	normURL, err := validateAPIURL(rawURL)
	if err != nil {
		return nil, err
	}
	c.APIURL = normURL

	// Token resolution order: explicit env (tests/local) → mounted file →
	// left empty for the runtime k8s-API fallback. A present-but-malformed
	// token is a hard error (fail fast, don't ship a 401-guaranteed bearer).
	tok, err := resolveToken(getenv, readFile)
	if err != nil {
		return nil, err
	}
	c.AgentToken = tok

	if err := applyDurations(getenv, c); err != nil {
		return nil, err
	}
	if b, err := lookupBool(getenv, EnvReportNamespaces); err != nil {
		return nil, err
	} else {
		c.ReportNamespaces = b
	}

	if secs, ok, err := lookupPositiveInt(getenv, EnvDesiredPollSeconds); err != nil {
		return nil, err
	} else if ok {
		c.DesiredPoll = time.Duration(secs) * time.Second
	}

	if maxR, ok, err := lookupPositiveInt(getenv, EnvMaxReplicas); err != nil {
		return nil, err
	} else if ok {
		if maxR > MaxMaxReplicas {
			return nil, fmt.Errorf("%s must be at most %d (got %d)", EnvMaxReplicas, MaxMaxReplicas, maxR)
		}
		c.MaxReplicas = maxR
	}

	return c, nil
}

// lookupPositiveInt parses an env var as a positive integer. Returns
// (0, false, nil) when unset/blank. A zero, negative, or non-numeric value is
// a hard error — a mistyped cadence must fail fast, not silently poll at a
// surprising rate.
func lookupPositiveInt(getenv func(string) (string, bool), key string) (int, bool, error) {
	v, ok := getenv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return 0, false, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return 0, false, fmt.Errorf("%s is not a valid integer: %w", key, err)
	}
	if n <= 0 {
		return 0, false, fmt.Errorf("%s must be positive (got %d)", key, n)
	}
	return n, true, nil
}

func resolveToken(getenv func(string) (string, bool), readFile func(string) ([]byte, error)) (string, error) {
	if v, ok := getenv(EnvAgentToken); ok {
		v = strings.TrimSpace(v)
		if v != "" {
			return validateToken(v)
		}
	}
	path := lookupDefault(getenv, EnvAgentTokenFile, DefaultTokenFile)
	b, err := readFile(path)
	if err != nil {
		// Not fatal: the token may be read from the k8s API at runtime instead.
		return "", nil
	}
	v := strings.TrimSpace(string(b))
	if v == "" {
		return "", nil
	}
	return validateToken(v)
}

// ValidateToken checks bearer A's shape. Exported so the k8s-API fallback path
// can validate a token read from the Secret with the same rule.
func ValidateToken(tok string) (string, error) { return validateToken(tok) }

func validateToken(tok string) (string, error) {
	tok = strings.TrimSpace(tok)
	if !agentTokenRE.MatchString(tok) {
		// SECURITY: never include the token value in the error.
		return "", fmt.Errorf("agent token has an invalid format (want khz_agt_<64 hex chars>)")
	}
	return tok, nil
}

// validateAPIURL requires an absolute https URL. Plain http is allowed ONLY for
// a loopback host (local dev); anything else is rejected as an unencrypted
// transport for a credential-bearing request.
func validateAPIURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("%s is not a valid URL: %w", EnvAPIURL, err)
	}
	if u.Host == "" {
		return "", fmt.Errorf("%s must be an absolute URL (got %q)", EnvAPIURL, raw)
	}
	switch u.Scheme {
	case "https":
		// ok
	case "http":
		if !isLoopback(u.Hostname()) {
			return "", fmt.Errorf("%s must use https (refusing plain http to non-loopback host %q)", EnvAPIURL, u.Hostname())
		}
	default:
		return "", fmt.Errorf("%s must use https (got scheme %q)", EnvAPIURL, u.Scheme)
	}
	// Normalize: drop any trailing slash so path joins are unambiguous.
	return strings.TrimRight(raw, "/"), nil
}

func isLoopback(host string) bool {
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

func applyDurations(getenv func(string) (string, bool), c *Config) error {
	for _, d := range []struct {
		env string
		dst *time.Duration
	}{
		{EnvFullInterval, &c.FullInterval},
		{EnvDebounce, &c.Debounce},
		{EnvMinGap, &c.MinGap},
	} {
		v, ok := getenv(d.env)
		if !ok || strings.TrimSpace(v) == "" {
			continue
		}
		parsed, err := time.ParseDuration(strings.TrimSpace(v))
		if err != nil {
			return fmt.Errorf("%s is not a valid duration: %w", d.env, err)
		}
		if parsed <= 0 {
			return fmt.Errorf("%s must be positive (got %s)", d.env, parsed)
		}
		*d.dst = parsed
	}
	return nil
}

func lookupBool(getenv func(string) (string, bool), key string) (bool, error) {
	v, ok := getenv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return false, nil
	}
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	if err != nil {
		return false, fmt.Errorf("%s is not a valid boolean: %w", key, err)
	}
	return b, nil
}

func lookupDefault(getenv func(string) (string, bool), key, def string) string {
	if v, ok := getenv(key); ok && v != "" {
		return v
	}
	return def
}

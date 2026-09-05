// Package controlplane fills the two schema-1 facts the bash heartbeat
// CronJob reports and the website promises: control-plane component health
// (`components[]`) and certificate expiry (`certificates`). Operator mode
// silences the CronJob (lok8s KUBEHZ_HEARTBEAT_OWNER=operator), so without
// this package those two facts vanish for every cluster on the live agent.
//
// The reads are the CronJob's, ported to typed client-go calls:
//
//   - apiserver + etcd from `/readyz?verbose` (the overall result → apiserver,
//     the per-check lines → etcd), with `/version` as the reachability
//     fallback. Both are nonResourceURLs the default
//     system:public-info-viewer binding already grants; the RBAC declares
//     them explicitly for hardened clusters (deploy/base/rbac.yaml).
//   - scheduler + controller-manager from the kube-system static pods'
//     Ready condition (label component=kube-scheduler / kube-controller-
//     manager), read off the SAME pod informer cache the live view uses —
//     no extra RBAC.
//   - certificate expiry = the earliest NotAfter across the certificates
//     issued on CertificateSigningRequests (certificates.k8s.io, list — the
//     one RBAC delta). Approved CSRs are garbage-collected an hour after
//     issuance, so the field is present only while a recent rotation is
//     visible; that matches the CronJob's coverage.
//
// Every probe fails soft: a failed read OMITS its entry — the heartbeat never
// fails because a health probe did, and a missing component is never
// reported as Unhealthy.
package controlplane

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	certv1 "k8s.io/api/certificates/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/kernpilot/kubehz-agent/internal/state"
)

var errNoRESTClient = errors.New("discovery client has no REST client")

// Component status vocabulary — the schema-1 wire values the dashboard
// renders (the CronJob's exact strings).
const (
	Healthy   = "Healthy"
	Unhealthy = "Unhealthy"
)

// Probe reads from the apiserver. Function fields so the collector is
// unit-testable without a cluster; NewManager wires the real client.
type Probe struct {
	// Readyz returns the body of GET /readyz?verbose and the request error.
	// On a non-2xx the body is still returned: the apiserver embeds the
	// [+]/[-] check lines in the 500 body, which is exactly what is parsed.
	Readyz func(ctx context.Context) ([]byte, error)
	// Version returns nil when GET /version succeeds (reachability fallback).
	Version func(ctx context.Context) error
	// Pods lists the cached pods (kube-system is filtered here).
	Pods func() ([]*corev1.Pod, error)
	// CSRs lists the cluster's CertificateSigningRequests ONE PAGE at a
	// time: cont is the continue token from the previous page ("" for the
	// first); the returned token is "" on the last page. Paged so a cluster
	// whose CSR cleaner is down (the failure this probe would report) cannot
	// hand the 256Mi agent tens of thousands of CSRs in one response.
	CSRs func(ctx context.Context, cont string) ([]certv1.CertificateSigningRequest, string, error)
}

// CSR paging: csrPageSize items per List, at most csrPageBudget pages per
// refresh. Past the budget the certificates field is DROPPED for that beat
// (fail-soft) — an earliest-of-a-prefix would be a wrong fact, not a partial
// one — and Collect reports errCSRBudget so the manager can log it once.
const (
	csrPageSize   = 500
	csrPageBudget = 20
)

var errCSRBudget = errors.New("certificate signing requests exceed the page budget; certificates omitted")

// Snapshot is what a beat carries: nil/empty means "omit the key".
type Snapshot struct {
	Components   []state.Component
	Certificates *state.CertInfo
}

// Collect runs every probe once. Each failed probe drops its own entries
// (fail toward report-only); the only error returned is errCSRBudget, which
// is informational — the snapshot is still valid without certificates.
func Collect(ctx context.Context, p Probe) (Snapshot, error) {
	var snap Snapshot
	var collectErr error

	if p.Readyz != nil {
		body, err := p.Readyz(ctx)
		apiserver, etcd := ParseReadyz(body, err)
		if apiserver == "" && err != nil && p.Version != nil && p.Version(ctx) == nil {
			apiserver = Healthy
		}
		if apiserver != "" {
			snap.Components = append(snap.Components, state.Component{Name: "apiserver", Status: apiserver})
		}
		if etcd != "" {
			snap.Components = append(snap.Components, state.Component{Name: "etcd", Status: etcd})
		}
	}

	if p.Pods != nil {
		if pods, err := p.Pods(); err == nil {
			snap.Components = append(snap.Components, ComponentsFromPods(pods)...)
		}
	}

	if p.CSRs != nil {
		exp, err := earliestCSRExpiry(ctx, p.CSRs)
		switch {
		case errors.Is(err, errCSRBudget):
			collectErr = err
		case err != nil:
			// forbidden / unreachable: omit, like every other probe
		case !exp.IsZero():
			// Reported as-is even when already past: an expired cert is the
			// loudest fact the field can carry.
			snap.Certificates = &state.CertInfo{ExpiresAt: exp.UTC().Format(time.RFC3339)}
		}
	}
	return snap, collectErr
}

// earliestCSRExpiry folds EarliestCertExpiry over the CSR pages without
// retaining any page. It stops at csrPageBudget pages with errCSRBudget.
func earliestCSRExpiry(ctx context.Context, list func(context.Context, string) ([]certv1.CertificateSigningRequest, string, error)) (time.Time, error) {
	var earliest time.Time
	cont := ""
	for page := 0; ; page++ {
		if page == csrPageBudget {
			return time.Time{}, errCSRBudget
		}
		items, next, err := list(ctx, cont)
		if err != nil {
			return time.Time{}, err
		}
		if e := EarliestCertExpiry(items); !e.IsZero() && (earliest.IsZero() || e.Before(earliest)) {
			earliest = e
		}
		if next == "" {
			return earliest, nil
		}
		cont = next
	}
}

// ParseReadyz maps a /readyz?verbose response to (apiserver, etcd) statuses.
// An empty string means "unknown — omit". Mirrors the CronJob: a 2xx is
// Healthy; a failure whose body carries a "[-]" check line is Unhealthy; a
// failure with no check lines is unknown (the caller tries /version).
func ParseReadyz(body []byte, err error) (apiserver, etcd string) {
	text := string(body)
	if err == nil {
		apiserver = Healthy
	} else if strings.Contains(text, "[-]") {
		apiserver = Unhealthy
	}
	// The per-check lines are trusted only when the overall verdict is: a
	// failed transport with a partial body must not yield an etcd status.
	if apiserver == "" {
		return "", ""
	}
	switch {
	case strings.Contains(text, "[-]etcd"):
		etcd = Unhealthy
	case strings.Contains(text, "[+]etcd ok"):
		etcd = Healthy
	}
	return apiserver, etcd
}

// staticPods maps the kube-system static-pod `component` label to the
// reported component name (the CronJob strips the kube- prefix).
var staticPods = []struct{ label, name string }{
	{"kube-scheduler", "scheduler"},
	{"kube-controller-manager", "controller-manager"},
}

// ComponentsFromPods derives scheduler / controller-manager health from the
// kube-system static pods. Any Ready pod → Healthy; pods present but none
// Ready → Unhealthy; no pods at all (managed or external control plane) →
// omitted, never a false Unhealthy.
func ComponentsFromPods(pods []*corev1.Pod) []state.Component {
	var out []state.Component
	for _, sp := range staticPods {
		seen, ready := false, false
		for _, pod := range pods {
			if pod == nil || pod.Namespace != metav1.NamespaceSystem || pod.Labels["component"] != sp.label {
				continue
			}
			seen = true
			if podReady(pod) {
				ready = true
				break
			}
		}
		switch {
		case ready:
			out = append(out, state.Component{Name: sp.name, Status: Healthy})
		case seen:
			out = append(out, state.Component{Name: sp.name, Status: Unhealthy})
		}
	}
	return out
}

func podReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// EarliestCertExpiry returns the earliest NotAfter across every certificate
// issued on the given CSRs (status.certificate, PEM), or the zero time when
// none parses. Pending, denied, and malformed CSRs are skipped.
func EarliestCertExpiry(csrs []certv1.CertificateSigningRequest) time.Time {
	var earliest time.Time
	for i := range csrs {
		rest := csrs[i].Status.Certificate
		for len(rest) > 0 {
			var block *pem.Block
			block, rest = pem.Decode(rest)
			if block == nil {
				break
			}
			if block.Type != "CERTIFICATE" {
				continue
			}
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				continue
			}
			if earliest.IsZero() || cert.NotAfter.Before(earliest) {
				earliest = cert.NotAfter
			}
		}
	}
	return earliest
}

// Manager refreshes a Snapshot at the full-beat cadence and hands it to the
// payload builder, the same shape as the inventory manager.
type Manager struct {
	probe    Probe
	interval time.Duration
	notify   func()
	log      *slog.Logger

	mu           sync.RWMutex
	snap         Snapshot
	budgetLogged bool
}

// NewManager wires the real probes: /readyz and /version through the
// clientset's REST client, CSRs through the typed certificates client, and
// pods through the caller's informer lister. notify wakes the coalescer when
// the snapshot changed; logger may be nil.
func NewManager(client kubernetes.Interface, pods func() ([]*corev1.Pod, error), interval time.Duration, notify func(), logger *slog.Logger) *Manager {
	// A fake/offline clientset has no REST client; the health URLs then read
	// as unreachable (omitted), never as a panic in the beat loop.
	return newManager(newProbe(client, pods), interval, notify, logger)
}

// newProbe wires the real reads; split from NewManager so a test can drive
// the paging closure against a fake clientset.
func newProbe(client kubernetes.Interface, pods func() ([]*corev1.Pod, error)) Probe {
	rc := client.Discovery().RESTClient()
	return Probe{
		Readyz: func(ctx context.Context) ([]byte, error) {
			if rc == nil {
				return nil, errNoRESTClient
			}
			return rc.Get().AbsPath("/readyz").Param("verbose", "").DoRaw(ctx)
		},
		Version: func(ctx context.Context) error {
			if rc == nil {
				return errNoRESTClient
			}
			_, err := rc.Get().AbsPath("/version").DoRaw(ctx)
			return err
		},
		Pods: pods,
		CSRs: func(ctx context.Context, cont string) ([]certv1.CertificateSigningRequest, string, error) {
			list, err := client.CertificatesV1().CertificateSigningRequests().List(ctx,
				metav1.ListOptions{Limit: csrPageSize, Continue: cont})
			if err != nil {
				return nil, "", err
			}
			return list.Items, list.Continue, nil
		},
	}
}

func newManager(probe Probe, interval time.Duration, notify func(), logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	if interval <= 0 {
		interval = time.Minute
	}
	return &Manager{probe: probe, interval: interval, notify: notify, log: logger}
}

// Run refreshes immediately, then every interval, until ctx is cancelled.
func (m *Manager) Run(ctx context.Context) {
	m.refresh(ctx)
	t := time.NewTicker(m.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.refresh(ctx)
		}
	}
}

// probeTimeout bounds one refresh: a hung apiserver (the moment /readyz is
// interesting) must not park the manager forever on a stale snapshot. The
// in-cluster rest.Config sets no Timeout of its own.
const probeTimeout = 10 * time.Second

func (m *Manager) refresh(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	snap, err := Collect(ctx, m.probe)
	m.mu.Lock()
	changed := !equal(m.snap, snap)
	m.snap = snap
	budgetHit := err != nil && !m.budgetLogged
	if budgetHit {
		m.budgetLogged = true
	}
	m.mu.Unlock()
	if budgetHit {
		// Once, not per refresh: the condition persists until the CSR
		// cleaner catches up, and a per-minute line would be noise.
		m.log.Debug("control-plane probe", "warning", err.Error(), "pageBudget", csrPageBudget, "pageSize", csrPageSize)
	}
	if changed {
		m.log.Debug("control-plane snapshot changed", "components", len(snap.Components), "certificates", snap.Certificates != nil)
		if m.notify != nil {
			m.notify()
		}
	}
}

// Snapshot returns a copy of the latest control-plane view.
func (m *Manager) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := Snapshot{Components: append([]state.Component(nil), m.snap.Components...)}
	if m.snap.Certificates != nil {
		c := *m.snap.Certificates
		out.Certificates = &c
	}
	return out
}

func equal(a, b Snapshot) bool {
	if len(a.Components) != len(b.Components) {
		return false
	}
	for i := range a.Components {
		if a.Components[i] != b.Components[i] {
			return false
		}
	}
	switch {
	case a.Certificates == nil && b.Certificates == nil:
		return true
	case a.Certificates == nil || b.Certificates == nil:
		return false
	}
	return *a.Certificates == *b.Certificates
}

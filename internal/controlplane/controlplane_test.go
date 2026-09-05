package controlplane

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	certv1 "k8s.io/api/certificates/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/kernpilot/kubehz-agent/internal/state"
)

const readyzOK = `[+]ping ok
[+]etcd ok
[+]etcd-readiness ok
[+]informer-sync ok
readyz check passed
`

const readyzEtcdDown = `[+]ping ok
[-]etcd failed: reason withheld
[+]informer-sync ok
readyz check failed
`

func staticPod(component string, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: component + "-cp-1", Namespace: "kube-system", Labels: map[string]string{"component": component}},
		Status:     corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}},
	}
}

func issuedCSR(t *testing.T, name string, notAfter time.Time) certv1.CertificateSigningRequest {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    notAfter.Add(-24 * time.Hour),
		NotAfter:     notAfter,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return certv1.CertificateSigningRequest{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status:     certv1.CertificateSigningRequestStatus{Certificate: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})},
	}
}

func find(comps []state.Component, name string) string {
	for _, c := range comps {
		if c.Name == name {
			return c.Status
		}
	}
	return ""
}

func TestParseReadyz(t *testing.T) {
	cases := []struct {
		name          string
		body          string
		err           error
		api, etcdWant string
	}{
		{"healthy", readyzOK, nil, Healthy, Healthy},
		{"etcd down (500 with check lines)", readyzEtcdDown, errors.New("500"), Unhealthy, Unhealthy},
		{"transport error, no body", "", errors.New("dial tcp: refused"), "", ""},
		{"transport error, partial body", "[+]etcd ok\n", errors.New("unexpected EOF"), "", ""},
		{"2xx without etcd line", "[+]ping ok\nreadyz check passed\n", nil, Healthy, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			api, etcd := ParseReadyz([]byte(tc.body), tc.err)
			if api != tc.api || etcd != tc.etcdWant {
				t.Errorf("got (%q, %q), want (%q, %q)", api, etcd, tc.api, tc.etcdWant)
			}
		})
	}
}

func TestComponentsFromPods(t *testing.T) {
	pods := []*corev1.Pod{
		staticPod("kube-scheduler", false),
		staticPod("kube-scheduler", true), // one Ready replica is enough
		staticPod("kube-controller-manager", false),
		// A same-labelled pod OUTSIDE kube-system must not count.
		{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Labels: map[string]string{"component": "kube-controller-manager"}},
			Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}},
	}
	got := ComponentsFromPods(pods)
	if find(got, "scheduler") != Healthy {
		t.Errorf("scheduler = %q, want Healthy", find(got, "scheduler"))
	}
	if find(got, "controller-manager") != Unhealthy {
		t.Errorf("controller-manager = %q, want Unhealthy", find(got, "controller-manager"))
	}
	// Managed / external control plane: no static pods → nothing reported,
	// never a false Unhealthy.
	if got := ComponentsFromPods(nil); len(got) != 0 {
		t.Errorf("no pods → %+v, want empty", got)
	}
}

func TestEarliestCertExpiry(t *testing.T) {
	base := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	csrs := []certv1.CertificateSigningRequest{
		issuedCSR(t, "kubelet-a", base.Add(72*time.Hour)),
		issuedCSR(t, "kubelet-b", base.Add(24*time.Hour)), // earliest
		{ObjectMeta: metav1.ObjectMeta{Name: "pending"}},  // no certificate
		{ObjectMeta: metav1.ObjectMeta{Name: "garbage"}, Status: certv1.CertificateSigningRequestStatus{Certificate: []byte("not pem")}},
	}
	if got := EarliestCertExpiry(csrs); !got.Equal(base.Add(24 * time.Hour)) {
		t.Errorf("earliest = %s, want %s", got, base.Add(24*time.Hour))
	}
	if got := EarliestCertExpiry(csrs[2:]); !got.IsZero() {
		t.Errorf("no issued certs → %s, want zero", got)
	}
}

func TestCollect_FullAndFailSoft(t *testing.T) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	full := Probe{
		Readyz:  func(context.Context) ([]byte, error) { return []byte(readyzOK), nil },
		Version: func(context.Context) error { return nil },
		Pods: func() ([]*corev1.Pod, error) {
			return []*corev1.Pod{staticPod("kube-scheduler", true), staticPod("kube-controller-manager", true)}, nil
		},
		CSRs: func(context.Context, string) ([]certv1.CertificateSigningRequest, string, error) {
			return []certv1.CertificateSigningRequest{issuedCSR(t, "kubelet", now.Add(48*time.Hour))}, "", nil
		},
	}
	snap, err := Collect(context.Background(), full)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	want := []state.Component{
		{Name: "apiserver", Status: Healthy},
		{Name: "etcd", Status: Healthy},
		{Name: "scheduler", Status: Healthy},
		{Name: "controller-manager", Status: Healthy},
	}
	if len(snap.Components) != len(want) {
		t.Fatalf("components = %+v, want %+v", snap.Components, want)
	}
	for i := range want {
		if snap.Components[i] != want[i] {
			t.Errorf("components[%d] = %+v, want %+v", i, snap.Components[i], want[i])
		}
	}
	if snap.Certificates == nil || snap.Certificates.ExpiresAt != "2026-09-07T12:00:00Z" {
		t.Errorf("certificates = %+v, want expiresAt 2026-09-07T12:00:00Z", snap.Certificates)
	}

	// /readyz unreachable but /version answers → apiserver Healthy via the
	// fallback, etcd unknown (omitted); CSR list forbidden → no certificates.
	degraded := Probe{
		Readyz:  func(context.Context) ([]byte, error) { return nil, errors.New("timeout") },
		Version: func(context.Context) error { return nil },
		Pods:    func() ([]*corev1.Pod, error) { return nil, errors.New("lister down") },
		CSRs: func(context.Context, string) ([]certv1.CertificateSigningRequest, string, error) {
			return nil, "", errors.New("forbidden")
		},
	}
	snap, _ = Collect(context.Background(), degraded)
	if len(snap.Components) != 1 || snap.Components[0] != (state.Component{Name: "apiserver", Status: Healthy}) {
		t.Errorf("degraded components = %+v, want only apiserver Healthy", snap.Components)
	}
	if snap.Certificates != nil {
		t.Errorf("degraded certificates = %+v, want nil (omitted)", snap.Certificates)
	}

	// Everything down → empty snapshot, no panic, no error surfaced.
	dead := Probe{
		Readyz:  func(context.Context) ([]byte, error) { return nil, errors.New("down") },
		Version: func(context.Context) error { return errors.New("down") },
	}
	snap, _ = Collect(context.Background(), dead)
	if len(snap.Components) != 0 || snap.Certificates != nil {
		t.Errorf("dead cluster → %+v, want empty", snap)
	}
}

func TestManager_NotifiesOnChangeOnly(t *testing.T) {
	var body atomic.Value
	body.Store(readyzOK)
	probe := Probe{
		Readyz: func(context.Context) ([]byte, error) { return []byte(body.Load().(string)), nil },
	}
	var notified int32
	m := newManager(probe, time.Hour, func() { atomic.AddInt32(&notified, 1) }, nil)

	m.refresh(context.Background())
	m.refresh(context.Background()) // unchanged → no second notify
	if n := atomic.LoadInt32(&notified); n != 1 {
		t.Errorf("notifications after two identical refreshes = %d, want 1", n)
	}
	body.Store(readyzEtcdDown)
	m.refresh(context.Background())
	if n := atomic.LoadInt32(&notified); n != 2 {
		t.Errorf("notifications after a change = %d, want 2", n)
	}
	snap := m.Snapshot()
	if find(snap.Components, "etcd") != Unhealthy {
		t.Errorf("snapshot etcd = %q, want Unhealthy", find(snap.Components, "etcd"))
	}
	// Snapshot is a copy: mutating it must not touch the manager's state.
	snap.Components[0].Status = "tampered"
	if m.Snapshot().Components[0].Status == "tampered" {
		t.Error("Snapshot returned the manager's own slice")
	}
}

// csrPager serves CSR pages through the REAL List closure (newProbe) with a
// reactor on a fake clientset: page n carries a continue token until the
// last one. The fake tracker ignores Limit/Continue, so the reactor is what
// makes the client paginate.
func csrPager(t *testing.T, pages [][]certv1.CertificateSigningRequest, endless bool) (Probe, *int32) {
	t.Helper()
	client := fake.NewClientset()
	var calls int32
	client.PrependReactor("list", "certificatesigningrequests", func(k8stesting.Action) (bool, runtime.Object, error) {
		n := int(atomic.AddInt32(&calls, 1)) - 1
		list := &certv1.CertificateSigningRequestList{}
		if n < len(pages) {
			list.Items = pages[n]
		}
		if endless || n < len(pages)-1 {
			list.Continue = "page-" + string(rune('a'+n))
		}
		return true, list, nil
	})
	return newProbe(client, nil), &calls
}

// More than one page: the earliest NotAfter must come from the SECOND page,
// which the old single List never saw.
func TestCollect_CSRsAcrossPages(t *testing.T) {
	base := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	probe, calls := csrPager(t, [][]certv1.CertificateSigningRequest{
		{issuedCSR(t, "p1-a", base.Add(72*time.Hour)), issuedCSR(t, "p1-b", base.Add(96*time.Hour))},
		{issuedCSR(t, "p2-a", base.Add(24*time.Hour))},
		{issuedCSR(t, "p3-a", base.Add(48*time.Hour))},
	}, false)
	snap, err := Collect(context.Background(), probe)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if got := atomic.LoadInt32(calls); got != 3 {
		t.Errorf("List calls = %d, want 3 (one per page)", got)
	}
	if snap.Certificates == nil || snap.Certificates.ExpiresAt != base.Add(24*time.Hour).Format(time.RFC3339) {
		t.Errorf("certificates = %+v, want the second page's earliest NotAfter", snap.Certificates)
	}
}

// A cluster whose CSR cleaner is down pages forever: stop at the budget,
// drop the field (never report an earliest-of-a-prefix), and say so once.
func TestCollect_CSRPageBudget(t *testing.T) {
	base := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	probe, calls := csrPager(t, [][]certv1.CertificateSigningRequest{
		{issuedCSR(t, "p1", base.Add(24*time.Hour))},
	}, true)
	snap, err := Collect(context.Background(), probe)
	if !errors.Is(err, errCSRBudget) {
		t.Fatalf("Collect err = %v, want errCSRBudget", err)
	}
	if got := atomic.LoadInt32(calls); got != csrPageBudget {
		t.Errorf("List calls = %d, want exactly the budget (%d)", got, csrPageBudget)
	}
	if snap.Certificates != nil {
		t.Errorf("certificates = %+v, want omitted past the budget", snap.Certificates)
	}

	// The manager logs the budget once and keeps serving components.
	m := newManager(probe, time.Hour, nil, nil)
	m.refresh(context.Background())
	m.refresh(context.Background())
	if !m.budgetLogged {
		t.Error("budget hit was not recorded for the one-time log")
	}
	if m.Snapshot().Certificates != nil {
		t.Error("snapshot carries certificates past the budget")
	}
}

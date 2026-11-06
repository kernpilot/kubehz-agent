package staterenew

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"

	"github.com/kernpilot/kubehz-agent/internal/k3supgrade"
)

// Host defaults.
const (
	DefaultService    = "k3s"
	DefaultKubeconfig = "/etc/rancher/k3s/k3s.yaml"
	DefaultK3sBinary  = "/usr/local/bin/k3s"

	pollEvery   = 5 * time.Second
	callTimeout = 10 * time.Second
)

var k3sVersionOut = regexp.MustCompile(`(?m)^k3s version (v[0-9]+\.[0-9]+\.[0-9]+\+k3s[0-9]+)\b`)

// SystemHost is the Host of a k3s server with systemd.
type SystemHost struct {
	Service    string
	Kubeconfig string
	K3sBinary  string

	// run executes a command; tests replace it.
	run func(ctx context.Context, name string, args ...string) ([]byte, error)
	// pidBefore is the k3s main PID before the last restart (0: unknown).
	pidBefore int
	// poll is the wait between two readiness checks.
	poll time.Duration
}

// NewSystemHost returns a SystemHost with the defaults for empty values.
func NewSystemHost(service, kubeconfig, k3sBinary string) *SystemHost {
	h := &SystemHost{Service: service, Kubeconfig: kubeconfig, K3sBinary: k3sBinary, poll: pollEvery}
	if h.Service == "" {
		h.Service = DefaultService
	}
	if h.Kubeconfig == "" {
		h.Kubeconfig = DefaultKubeconfig
	}
	if h.K3sBinary == "" {
		h.K3sBinary = DefaultK3sBinary
	}
	h.run = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		// #nosec G204 -- fixed binaries; the arguments are flags and the unit name.
		return exec.CommandContext(ctx, name, args...).CombinedOutput()
	}
	return h
}

// K3sVersion runs "k3s --version".
func (h *SystemHost) K3sVersion(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	out, err := h.run(ctx, h.K3sBinary, "--version")
	if err != nil {
		return "", fmt.Errorf("%s --version: %w", h.K3sBinary, err)
	}
	m := k3sVersionOut.FindSubmatch(out)
	if m == nil {
		return "", fmt.Errorf("%s --version printed no k3s version", h.K3sBinary)
	}
	return string(m[1]), nil
}

func (h *SystemHost) mainPID(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	out, err := h.run(ctx, "systemctl", "show", "--property=MainPID", "--value", h.Service)
	if err != nil {
		return 0, fmt.Errorf("systemctl show %s: %w", h.Service, err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		return 0, fmt.Errorf("systemctl show %s printed no PID", h.Service)
	}
	return pid, nil
}

// RestartK3s asks systemd for a restart and does not wait for it.
// WaitDatastore then waits for the new process.
func (h *SystemHost) RestartK3s(ctx context.Context) error {
	pid, err := h.mainPID(ctx)
	if err != nil {
		pid = 0
	}
	h.pidBefore = pid
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	if out, err := h.run(ctx, "systemctl", "restart", "--no-block", h.Service); err != nil {
		return fmt.Errorf("systemctl restart %s: %w: %s", h.Service, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// WaitDatastore waits until a NEW k3s process (after RestartK3s) answers
// /readyz/etcd: the apiserver reads from the datastore.
func (h *SystemHost) WaitDatastore(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var last error
	t := time.NewTicker(h.poll)
	defer t.Stop()
	for {
		err := h.datastoreReady(ctx)
		if err == nil {
			return nil
		}
		// Keep the last real answer: the call that the timeout cuts off
		// says only "deadline exceeded".
		if ctx.Err() == nil || last == nil {
			last = err
		}
		select {
		case <-ctx.Done():
			return last
		case <-t.C:
		}
	}
}

func (h *SystemHost) datastoreReady(ctx context.Context) error {
	if h.pidBefore > 0 {
		pid, err := h.mainPID(ctx)
		if err != nil {
			return err
		}
		if pid == 0 || pid == h.pidBefore {
			return errors.New("k3s has not started again yet")
		}
	}
	cfg, err := loadKubeconfig(h.Kubeconfig)
	if err != nil {
		return err
	}
	hc, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return fmt.Errorf("build the local apiserver client: %w", err)
	}
	rctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodGet, strings.TrimRight(cfg.Host, "/")+"/readyz/etcd", nil)
	if err != nil {
		return err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("local apiserver: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("local apiserver /readyz/etcd answered HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// UpgradeInProgress lists the agent's upgrade Plans through the local
// apiserver.
func (h *SystemHost) UpgradeInProgress(ctx context.Context) (bool, error) {
	cfg, err := loadKubeconfig(h.Kubeconfig)
	if err != nil {
		return false, err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	list, err := dyn.Resource(k3supgrade.PlanGVR).Namespace(k3supgrade.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: k3supgrade.LabelManagedBy + "=" + k3supgrade.ManagedBy,
		Limit:         1,
	})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return len(list.Items) > 0, nil
}

// kubeconfig is the part of a kubeconfig file that k3s writes.
type kubeconfig struct {
	CurrentContext string `json:"current-context"`
	Clusters       []struct {
		Name    string `json:"name"`
		Cluster struct {
			Server string `json:"server"`
			CAData string `json:"certificate-authority-data"`
		} `json:"cluster"`
	} `json:"clusters"`
	Users []struct {
		Name string `json:"name"`
		User struct {
			CertData string `json:"client-certificate-data"`
			KeyData  string `json:"client-key-data"`
		} `json:"user"`
	} `json:"users"`
	Contexts []struct {
		Name    string `json:"name"`
		Context struct {
			Cluster string `json:"cluster"`
			User    string `json:"user"`
		} `json:"context"`
	} `json:"contexts"`
}

// loadKubeconfig reads the admin kubeconfig of k3s (embedded certificate
// data only, as k3s writes it).
func loadKubeconfig(path string) (*rest.Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var kc kubeconfig
	if err := yaml.Unmarshal(b, &kc); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	clusterName, userName := "", ""
	for _, c := range kc.Contexts {
		if c.Name == kc.CurrentContext {
			clusterName, userName = c.Context.Cluster, c.Context.User
		}
	}
	cfg := &rest.Config{Timeout: callTimeout}
	found := false
	for _, c := range kc.Clusters {
		if c.Name == clusterName || (clusterName == "" && len(kc.Clusters) == 1) {
			cfg.Host = c.Cluster.Server
			if cfg.CAData, err = base64.StdEncoding.DecodeString(c.Cluster.CAData); err != nil {
				return nil, fmt.Errorf("%s: decode the cluster CA: %w", path, err)
			}
			found = true
		}
	}
	if !found || cfg.Host == "" {
		return nil, fmt.Errorf("%s has no cluster for the current context", path)
	}
	found = false
	for _, u := range kc.Users {
		if u.Name == userName || (userName == "" && len(kc.Users) == 1) {
			if cfg.CertData, err = base64.StdEncoding.DecodeString(u.User.CertData); err != nil {
				return nil, fmt.Errorf("%s: decode the client certificate: %w", path, err)
			}
			if cfg.KeyData, err = base64.StdEncoding.DecodeString(u.User.KeyData); err != nil {
				return nil, fmt.Errorf("%s: decode the client key: %w", path, err)
			}
			found = true
		}
	}
	if !found || len(cfg.CertData) == 0 {
		return nil, fmt.Errorf("%s has no client certificate for the current context", path)
	}
	return cfg, nil
}

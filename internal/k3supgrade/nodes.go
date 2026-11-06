package k3supgrade

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// APIServerVersionFunc returns the version of the apiserver that runs on
// one server node. A load-balanced /version is not the proof (design 4.3
// step 7): the executor asks each server node itself.
type APIServerVersionFunc func(ctx context.Context, node *corev1.Node) (string, error)

// apiserverPort is the k3s supervisor port. It serves the apiserver API.
const apiserverPort = "6443"

// probeTimeout bounds one per-node /version call.
const probeTimeout = 5 * time.Second

// inClusterServerName is a name that every k3s apiserver serving
// certificate carries. The probe connects to the node address and checks
// the certificate against this name and the cluster CA.
const inClusterServerName = "kubernetes.default.svc"

// NewAPIServerProber returns an APIServerVersionFunc that calls GET /version
// on https://<node InternalIP>:6443 with the agent's own service account
// credentials (base RBAC already allows /version).
func NewAPIServerProber(cfg *rest.Config) APIServerVersionFunc {
	return func(ctx context.Context, node *corev1.Node) (string, error) {
		ip := internalIP(node)
		if ip == "" {
			return "", fmt.Errorf("node %s has no InternalIP address", node.Name)
		}
		c := rest.CopyConfig(cfg)
		c.Host = "https://" + net.JoinHostPort(ip, apiserverPort)
		c.APIPath = ""
		c.ServerName = inClusterServerName
		c.Timeout = probeTimeout
		cs, err := kubernetes.NewForConfig(c)
		if err != nil {
			return "", fmt.Errorf("build client for node %s: %w", node.Name, err)
		}
		pctx, cancel := context.WithTimeout(ctx, probeTimeout)
		defer cancel()
		raw, err := cs.Discovery().RESTClient().Get().AbsPath("/version").Do(pctx).Raw()
		if err != nil {
			return "", fmt.Errorf("GET /version on node %s (%s): %w", node.Name, ip, err)
		}
		var info version.Info
		if err := json.Unmarshal(raw, &info); err != nil {
			return "", fmt.Errorf("decode /version of node %s: %w", node.Name, err)
		}
		return info.GitVersion, nil
	}
}

func internalIP(node *corev1.Node) string {
	for _, a := range node.Status.Addresses {
		if a.Type == corev1.NodeInternalIP && a.Address != "" {
			return a.Address
		}
	}
	return ""
}

// nodeState is what the executor knows about one node.
type nodeState struct {
	name          string
	server        bool
	kubelet       string
	apiserver     string // servers only; "" when unknown
	probeErr      string // why apiserver is unknown
	ready         bool
	unschedulable bool
}

// isServer reports whether the node runs the k3s server (control plane).
func isServer(n *corev1.Node) bool {
	return n.Labels[ControlPlaneLabel] == "true"
}

func isReady(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// observeNodes reads every node, and asks each server node for its
// apiserver version. Sorted by name.
func observeNodes(ctx context.Context, nodes []corev1.Node, probe APIServerVersionFunc) []nodeState {
	out := make([]nodeState, 0, len(nodes))
	for i := range nodes {
		n := &nodes[i]
		st := nodeState{
			name:          n.Name,
			server:        isServer(n),
			kubelet:       n.Status.NodeInfo.KubeletVersion,
			ready:         isReady(n),
			unschedulable: n.Spec.Unschedulable,
		}
		if st.server && probe != nil {
			v, err := probe(ctx, n)
			if err != nil {
				st.probeErr = err.Error()
			} else {
				st.apiserver = v
			}
		}
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// atTarget reports whether one node runs the target version and is Ready.
// A server must also serve the target from its own apiserver.
func (n nodeState) atTarget(target string) bool {
	if !n.ready || n.kubelet != target {
		return false
	}
	return !n.server || n.apiserver == target
}

func allAtTarget(nodes []nodeState, target string) bool {
	if len(nodes) == 0 {
		return false
	}
	for _, n := range nodes {
		if !n.atTarget(target) {
			return false
		}
	}
	return true
}

// k3sVersion is a parsed k3s release version: v<major>.<minor>.<patch>+k3s<build>.
type k3sVersion struct{ major, minor, patch, build int }

func parseK3sVersion(s string) (k3sVersion, error) {
	var v k3sVersion
	rest, ok := strings.CutPrefix(s, "v")
	if !ok {
		return v, fmt.Errorf("version %q has no leading v", s)
	}
	core, build, ok := strings.Cut(rest, "+k3s")
	if !ok {
		return v, fmt.Errorf("version %q is not a k3s version (no +k3s suffix)", s)
	}
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return v, fmt.Errorf("version %q does not have three parts", s)
	}
	nums := make([]int, 4)
	for i, p := range append(parts, build) {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return v, fmt.Errorf("version %q has a part that is not a number", s)
		}
		nums[i] = n
	}
	return k3sVersion{nums[0], nums[1], nums[2], nums[3]}, nil
}

func (a k3sVersion) less(b k3sVersion) bool {
	for _, d := range [][2]int{{a.major, b.major}, {a.minor, b.minor}, {a.patch, b.patch}, {a.build, b.build}} {
		if d[0] != d[1] {
			return d[0] < d[1]
		}
	}
	return false
}

// errStep is a target version that the upgrade path does not allow.
var errStep = errors.New("upgrade step refused")

// checkStep is the agent's own check of design 4.3 step 1 (the api checks
// it first): no downgrade on any node, and at most one minor step above the
// lowest server. Nodes that already run the target pass.
func checkStep(target string, nodes []nodeState) error {
	t, err := parseK3sVersion(target)
	if err != nil {
		return fmt.Errorf("%w: %v", errStep, err)
	}
	lowestMinor := -1
	for _, n := range nodes {
		v, err := parseK3sVersion(n.kubelet)
		if err != nil {
			return fmt.Errorf("%w: node %s: %v", errStep, n.name, err)
		}
		if t.less(v) {
			return fmt.Errorf("%w: target %s is older than %s on node %s (no downgrade)", errStep, target, n.kubelet, n.name)
		}
		if v.major != t.major {
			return fmt.Errorf("%w: node %s runs %s, another major than target %s", errStep, n.name, n.kubelet, target)
		}
		if n.server && (lowestMinor < 0 || v.minor < lowestMinor) {
			lowestMinor = v.minor
		}
	}
	if lowestMinor < 0 {
		return fmt.Errorf("%w: no server node found (label %s=true)", errStep, ControlPlaneLabel)
	}
	if t.minor > lowestMinor+1 {
		return fmt.Errorf("%w: target %s is more than one minor step above v1.%d on a server", errStep, target, lowestMinor)
	}
	return nil
}

// Package kube wires the in-cluster Kubernetes client. It is deliberately thin:
// the agent needs only a typed clientset (for informers + discovery) and a
// one-shot Secret read as the token fallback. No controller-runtime, no dynamic
// client — the smaller the dependency surface that runs on a customer's nodes,
// the smaller the audit + CVE surface.
package kube

import (
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// NewInClusterClientset builds a typed clientset from the pod's mounted
// ServiceAccount. userAgent identifies the agent in the apiserver audit log.
func NewInClusterClientset(userAgent string) (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}
	cfg.UserAgent = userAgent
	// Modest client-side limits: the agent's list/watch load is tiny and we do
	// not want it to be a noisy neighbour on a small pilot apiserver.
	if cfg.QPS == 0 {
		cfg.QPS = 20
		cfg.Burst = 30
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build clientset: %w", err)
	}
	return cs, nil
}

// ServerVersion returns the cluster's server gitVersion (e.g. "v1.35.5") from
// the discovery /version endpoint — the authoritative CLUSTER k8s version, not
// a client build tag (the bash agent's `kubectl version` first-match bug).
func ServerVersion(client kubernetes.Interface) (string, error) {
	info, err := client.Discovery().ServerVersion()
	if err != nil {
		return "", fmt.Errorf("discovery server version: %w", err)
	}
	return info.GitVersion, nil
}

// ReadAgentToken reads bearer A from the in-cluster Secret as the FALLBACK when
// it is not provided via a mounted file/env (config.EnvAgentTokenFile). The
// least-privilege deployment mounts the Secret as a read-only file instead, so
// this path — and its Secret get RBAC — is only needed when the operator opts
// out of the file mount. The value is returned verbatim; the caller validates
// its format. It is never logged here.
func ReadAgentToken(ctx context.Context, client kubernetes.Interface, namespace, name, key string) (string, error) {
	sec, err := client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("read secret %s/%s: %w", namespace, name, err)
	}
	raw, ok := sec.Data[key]
	if !ok {
		return "", fmt.Errorf("secret %s/%s has no key %q", namespace, name, key)
	}
	return strings.TrimSpace(string(raw)), nil
}

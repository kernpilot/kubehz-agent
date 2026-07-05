// Command kubehz-agent is the managed-tier, informer-based live-view agent for
// kubehz. It runs as a long-running Deployment in the user's own cluster, reads
// nodes/pods/warning-events via typed client-go informers, and POSTs a debounced
// schema-2 snapshot to kubehz-api authenticated with the in-cluster agent-token
// (the same P0 identity the bash heartbeat uses).
//
// It is OUTBOUND-ONLY: no inbound listener, no port, no analytics. The only
// egress is the authenticated heartbeat POST.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kernpilot/kubehz-agent/internal/agent"
	"github.com/kernpilot/kubehz-agent/internal/buildinfo"
	"github.com/kernpilot/kubehz-agent/internal/config"
	"github.com/kernpilot/kubehz-agent/internal/kube"
)

// tokenReadTimeout bounds the one-shot Secret read for the token fallback.
const tokenReadTimeout = 10 * time.Second

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel()}))
	slog.SetDefault(log)
	log.Info("kubehz-agent starting", "version", buildinfo.Version)

	cfg, err := config.Load(os.LookupEnv, os.ReadFile)
	if err != nil {
		log.Error("configuration error", "error", err.Error())
		os.Exit(1)
	}
	log.Info("configuration loaded", "config", cfg.String())

	client, err := kube.NewInClusterClientset("kubehz-agent/" + buildinfo.Version)
	if err != nil {
		log.Error("kubernetes client error", "error", err.Error())
		os.Exit(1)
	}

	// Token fallback: when not mounted as a file/env, read it once from the
	// in-cluster Secret (requires the scoped Secret get RBAC). Validated so a
	// misconfigured mount fails fast rather than shipping a 401-guaranteed beat.
	if !cfg.HasToken() {
		ctx, cancel := context.WithTimeout(context.Background(), tokenReadTimeout)
		raw, err := kube.ReadAgentToken(ctx, client, cfg.Namespace, cfg.SecretName, config.DefaultSecretKey)
		cancel()
		if err != nil {
			log.Error("could not resolve agent token from file/env or the k8s API",
				"error", err.Error(),
				"hint", "mount Secret "+cfg.Namespace+"/"+cfg.SecretName+" at "+config.DefaultTokenFile+" or grant scoped Secret get")
			os.Exit(1)
		}
		tok, err := config.ValidateToken(raw)
		if err != nil {
			log.Error("agent token from Secret has invalid format", "error", err.Error())
			os.Exit(1)
		}
		cfg.AgentToken = tok
		log.Info("agent token resolved from in-cluster Secret (API fallback)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := agent.New(cfg, client, log).Run(ctx); err != nil && ctx.Err() == nil {
		log.Error("agent stopped with error", "error", err.Error())
		os.Exit(1)
	}
	log.Info("kubehz-agent stopped")
}

func logLevel() slog.Level {
	switch os.Getenv("KUBEHZ_LOG_LEVEL") {
	case "debug", "DEBUG":
		return slog.LevelDebug
	case "warn", "WARN":
		return slog.LevelWarn
	case "error", "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

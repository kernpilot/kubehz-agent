package staterenew

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"
)

// DefaultAPIURL is the public kubehz api.
const DefaultAPIURL = "https://api.kubehz.cloud"

// Exit codes of the host commands.
const (
	ExitOK    = 0
	ExitFail  = 1
	ExitUsage = 2
)

// RenewMain runs "kubehz-agent state-renew" (one renewal pass, from the
// systemd timer).
func RenewMain(ctx context.Context, args []string, stderr io.Writer, version string) int {
	fs := flag.NewFlagSet("state-renew", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", DefaultDir, "state folder")
	kubeconfig := fs.String("kubeconfig", DefaultKubeconfig, "admin kubeconfig of the local k3s server")
	service := fs.String("service", DefaultService, "systemd unit of k3s")
	connect := fs.Duration("connect-timeout", DefaultConnectTimeout, "time k3s gets to connect with a new certificate")
	verbose := fs.Bool("v", false, "also log debug messages")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(stderr, "state-renew: unexpected argument %q; run state-renew -h for the flags\n", fs.Arg(0))
		return ExitUsage
	}
	log := hostLogger(stderr, *verbose)

	release, err := Lock(*dir)
	if errors.Is(err, ErrLocked) {
		log.Info("another state command runs; this run stops", "dir", *dir)
		return ExitOK
	}
	if err != nil {
		log.Error("cannot lock the state folder", "error", err.Error())
		return ExitFail
	}
	defer release()

	s, err := ReadSettings(*dir)
	if err != nil {
		log.Error("cannot read the renewal settings; run the install first", "error", err.Error())
		return ExitFail
	}
	client, err := NewClient(s.APIURL, s.ClusterID, "kubehz-agent/"+version, nil)
	if err != nil {
		log.Error("renewal settings are not valid", "error", err.Error())
		return ExitFail
	}
	r := &Renewer{
		Store:          Store{Dir: *dir},
		API:            client,
		Host:           NewSystemHost(*service, *kubeconfig, ""),
		Log:            log,
		ConnectTimeout: *connect,
		CommonName:     "kubehz-state-" + s.MachineID,
	}
	if err := r.Run(ctx); err != nil {
		log.Error("renewal failed", "error", err.Error())
		return ExitFail
	}
	return ExitOK
}

// RedeemMain runs "kubehz-agent state-redeem": the install step that
// redeems the ticket (read from stdin) and writes the state folder.
func RedeemMain(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, version string) int {
	fs := flag.NewFlagSet("state-redeem", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", DefaultDir, "state folder")
	apiURL := fs.String("api-url", DefaultAPIURL, "kubehz api URL")
	label := fs.String("label", defaultLabel(), "machine label in the dashboard (a DNS label)")
	role := fs.String("role", RoleServer, "machine role, the role of the ticket: server or agent")
	recovery := fs.Bool("recovery", false, "the ticket is a recovery ticket (server only): send no agent token")
	timeout := fs.Duration("timeout", 2*time.Minute, "time limit of the redemption")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(stderr, "state-redeem: unexpected argument %q; give the ticket on stdin, not as an argument\n", fs.Arg(0))
		return ExitUsage
	}
	log := hostLogger(stderr, false)

	ticket, err := ReadTicket(stdin)
	if err != nil {
		log.Error("no usable ticket", "error", err.Error())
		return ExitFail
	}
	if err := os.MkdirAll(*dir, 0o700); err != nil {
		log.Error("cannot make the state folder", "dir", *dir, "error", err.Error())
		return ExitFail
	}
	release, err := Lock(*dir)
	if err != nil {
		log.Error("cannot lock the state folder", "error", err.Error())
		return ExitFail
	}
	defer release()

	client, err := NewClient(*apiURL, "", "kubehz-agent/"+version, nil)
	if err != nil {
		log.Error("api URL is not valid", "error", err.Error())
		return ExitUsage
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	res, err := (&Redeemer{
		Store:    Store{Dir: *dir},
		API:      client,
		APIURL:   strings.TrimRight(*apiURL, "/"),
		Label:    *label,
		Role:     *role,
		Recovery: *recovery,
	}).Run(ctx, ticket)
	if err != nil {
		log.Error("redemption failed", "error", err.Error())
		return ExitFail
	}
	switch {
	case res.Role == RoleAgent:
		_, _ = fmt.Fprintf(stdout, "redeemed: agent machine %s\n", res.MachineID)
	case res.AgentToken:
		_, _ = fmt.Fprintf(stdout, "redeemed: cluster %s, machine %s; this install made the cluster's kubehz agent token\n", res.ClusterID, res.MachineID)
	default:
		_, _ = fmt.Fprintf(stdout, "redeemed: cluster %s, machine %s\n", res.ClusterID, res.MachineID)
	}
	return ExitOK
}

// AckMain runs "kubehz-agent state-ack": the install step after k3s started
// with the first certificate. It acknowledges that certificate (contract
// 10.1), and tries again after a transient failure until the timeout. Exit
// 0 means: the certificate is acknowledged.
func AckMain(ctx context.Context, args []string, stderr io.Writer, version string) int {
	fs := flag.NewFlagSet("state-ack", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", DefaultDir, "state folder")
	kubeconfig := fs.String("kubeconfig", DefaultKubeconfig, "admin kubeconfig of the local k3s server")
	service := fs.String("service", DefaultService, "systemd unit of k3s")
	k3sBin := fs.String("k3s", DefaultK3sBinary, "k3s binary (for the version in the acknowledgement)")
	timeout := fs.Duration("timeout", 5*time.Minute, "time limit of all attempts")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(stderr, "state-ack: unexpected argument %q; run state-ack -h for the flags\n", fs.Arg(0))
		return ExitUsage
	}
	log := hostLogger(stderr, false)
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	// A run of the renewal timer can hold the lock; it also finishes the
	// acknowledgement. Wait for it.
	release, err := lockWait(ctx, *dir)
	if err != nil {
		log.Error("cannot lock the state folder", "error", err.Error())
		return ExitFail
	}
	defer release()

	s, err := ReadSettings(*dir)
	if err != nil {
		log.Error("cannot read the renewal settings; run the install first", "error", err.Error())
		return ExitFail
	}
	client, err := NewClient(s.APIURL, s.ClusterID, "kubehz-agent/"+version, nil)
	if err != nil {
		log.Error("renewal settings are not valid", "error", err.Error())
		return ExitFail
	}
	r := &Renewer{
		Store: Store{Dir: *dir},
		API:   client,
		Host:  NewSystemHost(*service, *kubeconfig, *k3sBin),
		Log:   log,
	}
	if err := r.Acknowledge(ctx); err != nil {
		log.Error("acknowledgement failed", "error", err.Error())
		return ExitFail
	}
	return ExitOK
}

// lockWait takes the lock of the state folder, and waits while another run
// holds it, until ctx ends.
func lockWait(ctx context.Context, dir string) (func(), error) {
	for {
		release, err := Lock(dir)
		if !errors.Is(err, ErrLocked) {
			return release, err
		}
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(time.Second):
		}
	}
}

func hostLogger(w io.Writer, verbose bool) *slog.Logger {
	lvl := slog.LevelInfo
	if verbose {
		lvl = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: lvl}))
}

// defaultLabel is the short host name in lowercase.
func defaultLabel() string {
	h, err := os.Hostname()
	if err != nil {
		return ""
	}
	h, _, _ = strings.Cut(strings.ToLower(h), ".")
	return h
}

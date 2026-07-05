// Package buildinfo carries the agent version, stamped at build time via
// -ldflags "-X github.com/kernpilot/kubehz-agent/internal/buildinfo.Version=..."
// It defaults to "dev" for local builds.
package buildinfo

// Version is the agent's semantic version (or git describe). Reported in the
// payload's agent.version and the client User-Agent.
var Version = "dev"

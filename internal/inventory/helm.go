package inventory

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

// Helm release records: the OBSERVED half of the inventory block.
//
// The lo-written ClusterInventory CR (inventory.go / manager.go) is the
// authoritative source, but it only exists on clusters deployed by a recent
// `lo`. Every other cluster reported no inventory at all, so the dashboard's
// addon overview stayed dark. Helm's own release records are the same facts,
// observed instead of declared: helm 3 stores one Secret per release REVISION,
// `type: helm.sh/release.v1`, named `sh.helm.release.v1.<release>.v<revision>`,
// whose `release` key holds base64(gzip(JSON)) of the release — chart name,
// chart version, appVersion and status included.
//
// PRIVACY — the release payload also contains the rendered manifests and the
// merged values, i.e. potentially credentials. None of that may enter the
// informer cache, let alone a heartbeat. ProjectHelmRelease is an informer
// TRANSFORM: it runs once per delivered object BEFORE the object reaches the
// cache, decodes the payload, keeps seven metadata strings, and drops `Data`
// entirely. From that point on nothing in this process holds release contents,
// and the per-beat cost is a map read rather than a gunzip.

const (
	// HelmReleaseSecretType is the Secret type helm 3 writes release records
	// under. It is a SELECTABLE field for Secrets, so the informer restricts
	// its list+watch to exactly these objects at the apiserver
	// (`--field-selector type=helm.sh/release.v1`) — the same trick the events
	// informer uses for type=Warning.
	HelmReleaseSecretType = "helm.sh/release.v1"

	// releaseDataKey is the Secret data key holding the encoded release.
	releaseDataKey = "release"

	// SourceHelm marks an addon entry the agent OBSERVED from a helm release,
	// as opposed to the CR's lo-declared `addon`/`target`. kubehz-api keeps
	// `source` a capped free string precisely so a new source kind stays
	// additive (validation.ts), and the dashboard only special-cases `target`.
	SourceHelm = "helm"

	// statusUninstalled is helm's tombstone status: with `--keep-history` an
	// uninstalled release keeps its records, and reporting those as installed
	// would be a lie.
	statusUninstalled = "uninstalled"

	// maxReleaseBytes bounds the DECOMPRESSED release document. A helm release
	// secret is bounded by etcd's ~1MiB value limit, so 16MiB of expansion is
	// far past any real chart while still refusing a zip bomb.
	maxReleaseBytes = 16 << 20
)

// magicGzip is the gzip header helm checks for. Releases written before helm
// added compression are plain JSON after the base64 layer, and helm still
// decodes them, so the agent does too.
var magicGzip = []byte{0x1f, 0x8b, 0x08}

// Projection keys. They live in the Secret's StringData — a WRITE-ONLY API
// field that is always empty on objects read from the apiserver, which makes
// it an unambiguous carrier for the derived metadata and leaves no doubt that
// the original payload is gone (Data is nil'd in the same step).
const (
	projName         = "name"
	projChart        = "chart"
	projChartVersion = "chartVersion"
	projAppVersion   = "appVersion"
	projStatus       = "status"
	projRevision     = "revision"
	projLastDeployed = "lastDeployed"
)

// release is one helm release revision, reduced to the metadata the heartbeat
// inventory block can carry.
type release struct {
	Name         string
	Namespace    string
	Revision     int
	Status       string
	Chart        string
	ChartVersion string
	AppVersion   string
	LastDeployed string
}

// ProjectHelmRelease is the informer transform for the helm-release Secret
// watch. It NEVER returns an error (an error would make the informer drop the
// object with a log line on every resync) and it NEVER lets a Secret's data
// reach the cache:
//
//   - managedFields and annotations go (bookkeeping, often kilobytes);
//   - Data is dropped unconditionally — before any decoding, so a secret that
//     is not a helm release, or whose payload is undecodable, is cached as an
//     empty husk;
//   - a decodable helm release leaves seven metadata strings behind in
//     StringData (see the projection keys), which is all Observe reads.
func ProjectHelmRelease(obj any) (any, error) {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		if inner, err := ProjectHelmRelease(d.Obj); err == nil {
			d.Obj = inner
		}
		return d, nil
	}
	s, ok := obj.(*corev1.Secret)
	if !ok {
		return obj, nil
	}
	data := s.Data
	// Mutating pre-store is safe (the object is freshly decoded and nobody else
	// can see it yet) and is what the nodes/pods/events transform already does.
	s.Data = nil
	s.StringData = nil
	s.ManagedFields = nil
	s.Annotations = nil

	if s.Type != HelmReleaseSecretType {
		return s, nil
	}
	rel, err := decodeRelease(data[releaseDataKey])
	if err != nil {
		return s, nil // fail-soft: an undecodable release is simply not reported
	}
	s.StringData = map[string]string{
		projName:         rel.Name,
		projChart:        rel.Chart,
		projChartVersion: rel.ChartVersion,
		projAppVersion:   rel.AppVersion,
		projStatus:       rel.Status,
		projRevision:     strconv.Itoa(rel.Revision),
		projLastDeployed: rel.LastDeployed,
	}
	return s, nil
}

// releaseFromSecret reads a projected Secret back. The second return is false
// for anything ProjectHelmRelease did not project (a foreign secret type, an
// undecodable payload, or — belt and braces — an object that never went
// through the transform at all: without a projection there is nothing to
// report, and the raw payload is deliberately unreachable from here).
func releaseFromSecret(s *corev1.Secret) (release, bool) {
	if s == nil || s.Type != HelmReleaseSecretType || len(s.StringData) == 0 {
		return release{}, false
	}
	name := s.StringData[projName]
	if name == "" {
		return release{}, false
	}
	rev, _ := strconv.Atoi(s.StringData[projRevision])
	return release{
		Name:         name,
		Namespace:    s.Namespace,
		Revision:     rev,
		Status:       s.StringData[projStatus],
		Chart:        s.StringData[projChart],
		ChartVersion: s.StringData[projChartVersion],
		AppVersion:   s.StringData[projAppVersion],
		LastDeployed: s.StringData[projLastDeployed],
	}, true
}

// decodeRelease turns a helm release Secret's `release` value into the
// metadata subset. The encoding is helm's own (storage/driver): base64 of
// (optionally gzipped) JSON. Every failure mode returns an error and is
// treated by the caller as "skip this release" — a corrupt or foreign record
// must never fail the beat.
func decodeRelease(raw []byte) (*release, error) {
	if len(raw) == 0 {
		return nil, errors.New("empty release payload")
	}
	b, err := base64.StdEncoding.DecodeString(string(bytes.TrimSpace(raw)))
	if err != nil {
		return nil, fmt.Errorf("base64: %w", err)
	}
	if len(b) > 3 && bytes.Equal(b[:3], magicGzip) {
		zr, err := gzip.NewReader(bytes.NewReader(b))
		if err != nil {
			return nil, fmt.Errorf("gzip: %w", err)
		}
		defer func() { _ = zr.Close() }()
		// LimitReader with one spare byte: reading the spare proves the
		// document is over the bound, so an absurd expansion is refused
		// instead of parsed.
		b, err = io.ReadAll(io.LimitReader(zr, maxReleaseBytes+1))
		if err != nil {
			return nil, fmt.Errorf("gunzip: %w", err)
		}
		if len(b) > maxReleaseBytes {
			return nil, fmt.Errorf("release exceeds %d bytes", maxReleaseBytes)
		}
	}

	// Only the fields below are unmarshalled; the manifests, values, hooks and
	// notes in the same document are discarded by encoding/json.
	var doc struct {
		Name string `json:"name"`
		Info struct {
			Status       string `json:"status"`
			LastDeployed string `json:"last_deployed"`
		} `json:"info"`
		Chart struct {
			Metadata struct {
				Name       string `json:"name"`
				Version    string `json:"version"`
				AppVersion string `json:"appVersion"`
			} `json:"metadata"`
		} `json:"chart"`
		Version   int    `json:"version"`
		Namespace string `json:"namespace"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("json: %w", err)
	}
	name := strings.TrimSpace(doc.Name)
	if name == "" {
		return nil, errors.New("release has no name")
	}
	return &release{
		Name:         name,
		Namespace:    doc.Namespace,
		Revision:     doc.Version,
		Status:       doc.Info.Status,
		Chart:        doc.Chart.Metadata.Name,
		ChartVersion: doc.Chart.Metadata.Version,
		AppVersion:   doc.Chart.Metadata.AppVersion,
		LastDeployed: doc.Info.LastDeployed,
	}, nil
}

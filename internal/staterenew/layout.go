package staterenew

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// File layout on the host (design 5.1). k3s reads its datastore files
// through the symlink "current", which points to one complete generation:
//
//	/etc/kubehz/state/
//	  renew.json            renewer settings (api URL, cluster, machine)
//	  k3s-token             the k3s server token (0600)
//	  current -> gen-2      switched with ONE rename(2)
//	  gen-1/                the previous generation (kept for a switch back)
//	  gen-2/
//	    client.key          made on this machine, never sent (0600)
//	    request.csr         kept, so a lost response is retried with the same CSR
//	    client.crt          the signed client certificate
//	    server-ca.crt       the platform serving CA of the datastore endpoint
//	    meta.json           the generation's state
const (
	DefaultDir = "/etc/kubehz/state"

	currentLink = "current"
	genPrefix   = "gen-"
	lockFile    = ".lock"

	KeyFile  = "client.key"
	CertFile = "client.crt"
	CAFile   = "server-ca.crt"
	CSRFile  = "request.csr"
	metaFile = "meta.json"

	ConfigFile      = "renew.json"
	TokenFile       = "k3s-token"
	AgentTokenFile  = "agent-token"
	InstallEnvFile  = "install.env"
	AuthnConfigFile = "authentication-config.yaml"
	SSOManifestFile = "sso-rbac.yaml"
	lastErrorFile   = "last-error"
)

// Generation states. A generation moves keyed -> issued -> switched ->
// acked, or ends failed.
const (
	StateKeyed    = "keyed"    // key and CSR written
	StateIssued   = "issued"   // certificate written, not in use
	StateSwitched = "switched" // "current" points here, not acknowledged
	StateAcked    = "acked"    // acknowledged: the api's active serial
	StateFailed   = "failed"   // abandoned
)

// Meta is the state of one generation (meta.json).
type Meta struct {
	Generation   int       `json:"generation"`
	State        string    `json:"state"`
	CAGeneration int64     `json:"caGeneration,omitempty"`
	Serial       string    `json:"serial,omitempty"`
	Previous     int       `json:"previous,omitempty"`
	CreatedAt    time.Time `json:"createdAt"`
	RestartedAt  time.Time `json:"restartedAt,omitzero"`
	FailedAt     time.Time `json:"failedAt,omitzero"`
	Error        string    `json:"error,omitempty"`
}

// Store is the state directory.
type Store struct{ Dir string }

func genName(n int) string { return genPrefix + strconv.Itoa(n) }

func (s Store) genDir(n int) string { return filepath.Join(s.Dir, genName(n)) }

// Path returns the path of a file of generation n.
func (s Store) Path(n int, name string) string { return filepath.Join(s.genDir(n), name) }

// ErrNoCurrent means that the "current" symlink does not exist.
var ErrNoCurrent = errors.New("no current generation")

// Current returns the generation that "current" points to.
func (s Store) Current() (int, error) {
	target, err := os.Readlink(filepath.Join(s.Dir, currentLink))
	if errors.Is(err, fs.ErrNotExist) {
		return 0, ErrNoCurrent
	}
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", filepath.Join(s.Dir, currentLink), err)
	}
	n, ok := parseGen(target)
	if !ok {
		return 0, fmt.Errorf("%s points to %q, not to a generation folder", filepath.Join(s.Dir, currentLink), target)
	}
	return n, nil
}

func parseGen(name string) (int, bool) {
	rest, ok := strings.CutPrefix(filepath.Base(name), genPrefix)
	if !ok || filepath.Dir(name) != "." {
		return 0, false
	}
	n, err := strconv.Atoi(rest)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

// Gens returns the generation numbers that exist, ascending.
func (s Store) Gens() ([]int, error) {
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", s.Dir, err)
	}
	var out []int
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if n, ok := parseGen(e.Name()); ok {
			out = append(out, n)
		}
	}
	sort.Ints(out)
	return out, nil
}

// NewGen makes the folder of the next generation (mode 0700).
func (s Store) NewGen() (int, error) {
	gens, err := s.Gens()
	if err != nil {
		return 0, err
	}
	n := 1
	if len(gens) > 0 {
		n = gens[len(gens)-1] + 1
	}
	if err := os.Mkdir(s.genDir(n), 0o700); err != nil {
		return 0, fmt.Errorf("make %s: %w", s.genDir(n), err)
	}
	if err := syncDir(s.Dir); err != nil {
		return 0, err
	}
	return n, nil
}

// ReadMeta reads the state of generation n.
func (s Store) ReadMeta(n int) (Meta, error) {
	var m Meta
	b, err := os.ReadFile(s.Path(n, metaFile))
	if err != nil {
		return m, fmt.Errorf("read state of %s: %w", genName(n), err)
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return m, fmt.Errorf("decode state of %s: %w", genName(n), err)
	}
	return m, nil
}

// WriteMeta writes the state of generation n atomically.
func (s Store) WriteMeta(n int, m Meta) error {
	m.Generation = n
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return WriteFileAtomic(s.Path(n, metaFile), append(b, '\n'), 0o600)
}

// Switch points "current" to generation n with one rename(2): a reader
// sees the old or the new generation, never a mix.
func (s Store) Switch(n int) error {
	tmp := filepath.Join(s.Dir, currentLink+".tmp")
	if err := os.Remove(tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove %s: %w", tmp, err)
	}
	if err := os.Symlink(genName(n), tmp); err != nil {
		return fmt.Errorf("make symlink %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, filepath.Join(s.Dir, currentLink)); err != nil {
		return fmt.Errorf("switch %s to %s: %w", filepath.Join(s.Dir, currentLink), genName(n), err)
	}
	return syncDir(s.Dir)
}

// Remove deletes generation n. It refuses the current generation.
func (s Store) Remove(n int) error {
	if cur, err := s.Current(); err == nil && cur == n {
		return fmt.Errorf("refusing to remove the current generation %s", genName(n))
	}
	return os.RemoveAll(s.genDir(n))
}

// WriteFileAtomic writes data to path: a temporary file in the same folder,
// fsync, rename(2), fsync of the folder. A crash leaves the old file or the
// new file, never a part of one.
func WriteFileAtomic(path string, data []byte, perm fs.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("make temporary file in %s: %w", dir, err)
	}
	name := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			_ = tmp.Close()
			_ = os.Remove(name)
		}
	}()
	if err := tmp.Chmod(perm); err != nil {
		return fmt.Errorf("chmod %s: %w", name, err)
	}
	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync %s: %w", name, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close %s: %w", name, err)
	}
	if err := os.Rename(name, path); err != nil {
		return fmt.Errorf("rename %s to %s: %w", name, path, err)
	}
	ok = true
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open %s: %w", dir, err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync %s: %w", dir, err)
	}
	return nil
}

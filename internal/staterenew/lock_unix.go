//go:build linux || darwin

package staterenew

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// ErrLocked means that another run holds the lock of the state folder.
var ErrLocked = errors.New("another run holds the lock")

// Lock takes the lock of the state folder without a wait. The caller calls
// the returned function to release it.
func Lock(dir string) (func(), error) {
	path := filepath.Join(dir, lockFile)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600) // #nosec G304 -- the fixed lock file of the state folder
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	fd := int(f.Fd()) // #nosec G115 -- a file descriptor fits in an int
	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%s: %w", path, ErrLocked)
		}
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	return func() {
		_ = syscall.Flock(fd, syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}

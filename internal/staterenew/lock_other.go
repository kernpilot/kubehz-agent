//go:build !(linux || darwin)

package staterenew

import "errors"

// ErrLocked means that another run holds the lock of the state folder.
var ErrLocked = errors.New("another run holds the lock")

// Lock is not supported on this operating system: the host commands run on
// Linux only.
func Lock(string) (func(), error) {
	return nil, errors.New("the state commands run on Linux only")
}

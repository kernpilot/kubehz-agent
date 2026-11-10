// Package leasetest makes the fake clientset's Leases behave like the
// apiserver's in leader-election tests. The fake's object tracker stores
// whatever it gets, so a renewal could silently overwrite a peer's Lease and
// a compare-and-swap could never fail. With EnforceResourceVersion every
// Lease write sets a new resourceVersion, and an update that carries an old
// one fails with a Conflict.
package leasetest

import (
	"fmt"
	"strconv"
	"sync"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// LeaseGVR is the Lease resource, for direct writes to client.Tracker().
var LeaseGVR = coordinationv1.SchemeGroupVersion.WithResource("leases")

// Versions hands out resourceVersions. Tests that write to the tracker
// directly take the next one from here, so it never repeats.
type Versions struct {
	mu   sync.Mutex
	next int
}

// Next returns a resourceVersion that no Lease had before.
func (v *Versions) Next() string {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.next++
	return strconv.Itoa(v.next)
}

// EnforceResourceVersion installs the Lease reactors on client: a create
// gets a new resourceVersion; an update must carry the stored one (or none,
// which is an unconditional update, as on the apiserver) and gets a new one.
// Install it before any reactor a test wants to run first.
func EnforceResourceVersion(client *fake.Clientset) *Versions {
	v := &Versions{}
	client.PrependReactor("create", "leases", func(a k8stesting.Action) (bool, runtime.Object, error) {
		ca, ok := a.(k8stesting.CreateAction)
		if !ok {
			return false, nil, nil
		}
		if l, ok := ca.GetObject().(*coordinationv1.Lease); ok {
			l.ResourceVersion = v.Next()
		}
		return false, nil, nil // the default reactor stores it
	})
	client.PrependReactor("update", "leases", func(a k8stesting.Action) (bool, runtime.Object, error) {
		ua, ok := a.(k8stesting.UpdateAction)
		if !ok {
			return false, nil, nil
		}
		l, ok := ua.GetObject().(*coordinationv1.Lease)
		if !ok {
			return false, nil, nil
		}
		stored, err := client.Tracker().Get(LeaseGVR, l.Namespace, l.Name)
		if err != nil {
			return false, nil, nil // the default reactor answers NotFound
		}
		if l.ResourceVersion != "" && l.ResourceVersion != stored.(*coordinationv1.Lease).ResourceVersion {
			return true, nil, apierrors.NewConflict(LeaseGVR.GroupResource(), l.Name,
				fmt.Errorf("the object has been modified; resourceVersion %s is not the stored one", l.ResourceVersion))
		}
		l.ResourceVersion = v.Next()
		return false, nil, nil
	})
	return v
}

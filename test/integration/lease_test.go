//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/kernpilot/kubehz-agent/internal/leader"
)

// TestObserverAgainstRealAPIServer proves the one failed Lease read that the
// Observer takes as evidence, on the real thing: when a peer's Lease is
// deleted, the apiserver's NotFound ends the peer's count. The unit tests
// build that NotFound by hand; this test reads the one the apiserver sends.
// The replica stays out of the race (MayLead says no), so it only reads the
// Lease and never acts.
func TestObserverAgainstRealAPIServer(t *testing.T) {
	ctx := context.Background()
	client, err := kubernetes.NewForConfig(restConfig(t))
	if err != nil {
		t.Fatalf("kubernetes client: %v", err)
	}
	ns := createNamespace(ctx, t, client)

	peer, thirty := "peer-replica_0000", int32(30)
	now := metav1.NewMicroTime(time.Now())
	_, err = client.CoordinationV1().Leases(ns).Create(ctx, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: leader.LeaseName, Namespace: ns},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: &peer, LeaseDurationSeconds: &thirty,
			AcquireTime: &now, RenewTime: &now,
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create the peer's lease: %v", err)
	}

	obs := leader.NewObserver()
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- leader.Run(runCtx, leader.Config{
			Client:        client,
			Namespace:     ns,
			Identity:      "it-replica_0000",
			LeaseDuration: 2 * time.Second,
			RenewDeadline: time.Second,
			RetryPeriod:   100 * time.Millisecond,
			Observer:      obs,
			MayLead:       func() error { return errors.New("out of the race for this test") },
		}, func(context.Context) {
			t.Error("the replica acted although MayLead said no")
		})
	}()
	defer func() {
		cancel()
		<-done
	}()

	waitFor(t, "the observer to see the peer", obs.PeerHolds)
	if err := client.CoordinationV1().Leases(ns).Delete(ctx, leader.LeaseName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete the peer's lease: %v", err)
	}
	waitFor(t, "the apiserver's NotFound to end the peer's count", func() bool { return !obs.PeerHolds() })
}

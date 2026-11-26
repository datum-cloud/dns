// SPDX-License-Identifier: AGPL-3.0-only

package controlplane

import (
	"slices"
	"sync"
	"testing"
	"time"

	dnsv1alpha1 "go.miloapis.com/dns/api/v1alpha1"
	"go.miloapis.com/dns/internal/internaldns/pubstore"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// newPerZoneStoreHarness keeps the publication records in a publication store
// on a memory bucket, as staging and production do, and hears manifest
// changes from it.
func newPerZoneStoreHarness(t *testing.T) (*perZoneHarness, *dnsv1alpha1.DNSZone) {
	t.Helper()
	h, zone := newPerZoneHarness(t)
	h.ctx = t.Context()
	store := pubstore.New(h.c, pubstore.NewMemoryKV())
	if err := store.Start(h.ctx); err != nil {
		t.Fatal(err)
	}
	h.c, h.r.Client = store, store
	h.r.Options.ManifestChanges = store.SubscribeManifests
	return h, zone
}

// recordedQueue records the keys a source queues.
type recordedQueue struct {
	workqueue.TypedRateLimitingInterface[reconcile.Request]
	mu   sync.Mutex
	keys []reconcile.Request
}

func (q *recordedQueue) AddAfter(req reconcile.Request, _ time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.keys = append(q.keys, req)
}

func (q *recordedQueue) take() []reconcile.Request {
	q.mu.Lock()
	defer q.mu.Unlock()
	keys := q.keys
	q.keys = nil
	return keys
}

// watchVerdicts starts the zone controller's verdict source on a queue of its
// own.
func (h *perZoneHarness) watchVerdicts() *recordedQueue {
	h.t.Helper()
	q := &recordedQueue{}
	if err := h.r.verdictSource(h.c).Start(h.ctx, q); err != nil {
		h.t.Fatal(err)
	}
	return q
}

// expectQueued waits for the source to queue len(want) keys and asks that it
// queued exactly want. The source hears changes in order, so once the key of
// the last change arrives, every earlier change has queued what it would.
func (h *perZoneHarness) expectQueued(q *recordedQueue, want ...reconcile.Request) {
	h.t.Helper()
	var got []reconcile.Request
	for deadline := time.Now().Add(5 * time.Second); len(got) < len(want) && time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		got = append(got, q.take()...)
	}
	if got = append(got, q.take()...); !slices.Equal(got, want) {
		h.t.Fatalf("the verdict source queued %v, want %v", got, want)
	}
}

func (h *perZoneHarness) zoneHeld(zone *dnsv1alpha1.DNSZone) bool {
	h.t.Helper()
	var current dnsv1alpha1.DNSZone
	err := h.c.Get(h.ctx, client.ObjectKeyFromObject(zone), &current)
	if apierrors.IsNotFound(err) {
		return false
	}
	if err != nil {
		h.t.Fatal(err)
	}
	return true
}

// activeManifest is the manifest the zone's ownership commits to.
func (h *perZoneHarness) activeManifest() *dnsv1alpha1.DNSPublicationManifest {
	h.t.Helper()
	var owners dnsv1alpha1.DNSPublicationOwnershipList
	if err := h.c.List(h.ctx, &owners); err != nil || len(owners.Items) != 1 {
		h.t.Fatalf("want one ownership, got %d: %v", len(owners.Items), err)
	}
	var m dnsv1alpha1.DNSPublicationManifest
	if err := h.c.Get(h.ctx, client.ObjectKey{Namespace: h.r.Options.PlatformNamespace, Name: owners.Items[0].Spec.ActiveManifestName}, &m); err != nil {
		h.t.Fatal(err)
	}
	return &m
}

// writeStatus writes a manifest's status as the acknowledgement sink does.
func (h *perZoneHarness) writeStatus(m *dnsv1alpha1.DNSPublicationManifest, change func(*dnsv1alpha1.DNSPublicationManifestStatus)) {
	h.t.Helper()
	change(&m.Status)
	if err := h.c.Status().Update(h.ctx, m); err != nil {
		h.t.Fatal(err)
	}
}

func verified(s *dnsv1alpha1.DNSPublicationManifestStatus) {
	apimeta.SetStatusCondition(&s.Conditions, metav1.Condition{Type: "Published", Status: metav1.ConditionTrue, Reason: "ReplicaVerification"})
}

// deleteAndWait deletes the zone and runs its key once: the key publishes the
// tombstone and waits, with no requeue, for the tombstone's verdict.
func (h *perZoneHarness) deleteAndWait(zone *dnsv1alpha1.DNSZone) reconcile.Request {
	h.t.Helper()
	if err := h.c.Delete(h.ctx, zone); err != nil {
		h.t.Fatal(err)
	}
	key := zoneRequest(zone.Namespace, zone.Name)
	if res := h.run(key.Name); res != (ctrl.Result{}) || !h.zoneHeld(zone) {
		h.t.Fatalf("a deleted zone that waits for its tombstone's verdict was released or requeued: held=%v, %+v", h.zoneHeld(zone), res)
	}
	if !h.activeManifest().Spec.Tombstone {
		h.t.Fatal("the deleted zone's key published no tombstone")
	}
	return key
}

func TestPerZoneTombstoneVerdictWakesTheDeletedZone(t *testing.T) {
	for name, namespace := range map[string]string{"project namespace": "project", "project across namespaces": ""} {
		t.Run(name, func(t *testing.T) {
			h, zone := newPerZoneStoreHarness(t)
			h.r.Options.ProjectNamespace = namespace
			h.settle()
			queue := h.watchVerdicts()
			key := h.deleteAndWait(zone)
			h.expectQueued(queue, key)
			if res := h.run(key.Name); res != (ctrl.Result{}) || !h.zoneHeld(zone) {
				t.Fatalf("before a replica verified the tombstone, the zone's key released it or requeued: %+v", res)
			}

			h.writeStatus(h.activeManifest(), verified)
			h.expectQueued(queue, key)
			h.run(key.Name)
			if h.zoneHeld(zone) {
				t.Fatal("a replica verified the tombstone and its zone's key ran, but the zone still holds its finalizer")
			}
		})
	}
}

func TestPerZoneTombstoneWithNoServingTargetsReleasesTheZoneOnItsCreate(t *testing.T) {
	h, zone := newPerZoneStoreHarness(t)
	if err := h.c.Delete(h.ctx, &dnsv1alpha1.DNSZoneAssociation{ObjectMeta: metav1.ObjectMeta{Namespace: "project", Name: "zone-prod"}}); err != nil {
		t.Fatal(err)
	}
	h.settle()
	queue := h.watchVerdicts()
	key := h.deleteAndWait(zone)
	if targets := h.activeManifest().Spec.ServingTargets; len(targets) != 0 {
		t.Fatalf("tombstone serves %v; the case needs one with no serving targets", targets)
	}
	h.expectQueued(queue, key)
	h.run(key.Name)
	if h.zoneHeld(zone) {
		t.Fatal("a tombstone with no serving targets woke its zone, but the zone still holds its finalizer")
	}
}

func TestPerZoneVerdictSourceQueuesOnlyItsProjectsTombstoneVerdicts(t *testing.T) {
	h, zone := newPerZoneStoreHarness(t)
	h.settle()
	queue := h.watchVerdicts()
	h.writeStatus(h.activeManifest(), verified)
	other := &dnsv1alpha1.DNSPublicationManifest{ObjectMeta: metav1.ObjectMeta{Name: "other-project-tombstone", Namespace: h.r.Options.PlatformNamespace, Labels: map[string]string{referenceProjectLabel: "project-b"}},
		Spec: dnsv1alpha1.DNSPublicationManifestSpec{ZoneRef: dnsv1alpha1.DNSObjectReference{Name: zone.Name, UID: zone.UID}, Tombstone: true}}
	if err := h.c.Create(h.ctx, other); err != nil {
		t.Fatal(err)
	}
	h.writeStatus(other, verified)
	key := h.deleteAndWait(zone)
	h.expectQueued(queue, key)

	tombstone := h.activeManifest()
	h.writeStatus(tombstone, func(s *dnsv1alpha1.DNSPublicationManifestStatus) { s.ExportAttempts++ })
	h.writeStatus(tombstone, verified)
	h.expectQueued(queue, key)
}

func TestPerZoneSetupRefusesAReconcilerWithNoManifestChanges(t *testing.T) {
	s := runtime.NewScheme()
	if err := addReferenceStateToScheme(s); err != nil {
		t.Fatal(err)
	}
	// Setup only registers watches, so it never calls this API.
	mgr, err := ctrl.NewManager(&rest.Config{Host: "https://platform.invalid"}, ctrl.Options{Scheme: s, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0"})
	if err != nil {
		t.Fatal(err)
	}
	r := &Reconciler{Options: ReconcilerOptions{ProjectUID: "project-a", ProjectNamespace: "project", PerZoneReconcile: true}}
	if err := r.SetupWithManager(mgr); err == nil {
		t.Fatal("per-zone setup accepted a reconciler with no ManifestChanges, whose deleted zones would wait for the resync")
	}
}

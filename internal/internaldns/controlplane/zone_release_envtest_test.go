// SPDX-License-Identifier: AGPL-3.0-only

package controlplane

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	dnsv1alpha1 "go.miloapis.com/dns/api/v1alpha1"
	"go.miloapis.com/dns/internal/internaldns/pubstore"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// releaseWithin is far inside zoneResync, so only the watch on the verdict
// can release the zone in time.
const releaseWithin = 30 * time.Second

func TestPerZoneControllerReleasesADeletedZoneOnItsVerdict(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" {
		t.Skip("set KUBEBUILDER_ASSETS to run the zone controller against the API server")
	}
	for name, namespace := range map[string]string{"project namespace": "project", "project across namespaces": ""} {
		t.Run(name, func(t *testing.T) {
			e := startZoneController(t, assets, namespace)
			zone := e.publishZone(t)
			if err := e.api.Delete(t.Context(), zone); err != nil {
				t.Fatal(err)
			}
			tombstone := e.waitForManifest(t, zone, func(m *dnsv1alpha1.DNSPublicationManifest) bool { return m.Spec.Tombstone })
			verified(&tombstone.Status)
			if err := e.store.Status().Update(t.Context(), tombstone); err != nil {
				t.Fatal(err)
			}
			if !eventually(releaseWithin, func() bool {
				return apierrors.IsNotFound(e.api.Get(t.Context(), client.ObjectKeyFromObject(zone), zone))
			}) {
				t.Fatalf("a replica verified the deleted zone's withdrawal, and %v later the zone still holds its finalizer", releaseWithin)
			}
		})
	}
}

type zoneControllerEnv struct {
	api   client.Client
	store *pubstore.PublicationStore
}

// startZoneController runs the per-zone controller as a project's control
// plane runs it: set up by SetupWithManager, its publication records in a
// publication store on a memory bucket.
func startZoneController(t *testing.T, assets, projectNamespace string) zoneControllerEnv {
	t.Helper()
	environment := &envtest.Environment{BinaryAssetsDirectory: assets, CRDDirectoryPaths: []string{filepath.Join("..", "..", "..", "config", "crd", "bases")}, ErrorIfCRDPathMissing: true}
	cfg, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})
	s := runtime.NewScheme()
	requireAccessTestSuccess(t, addReferenceStateToScheme(s))
	requireAccessTestSuccess(t, corev1.AddToScheme(s))
	api, err := client.New(cfg, client.Options{Scheme: s})
	requireAccessTestSuccess(t, err)
	for _, ns := range []string{"project", "internal-dns-system"} {
		requireAccessTestSuccess(t, api.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
	}
	store := pubstore.New(api, pubstore.NewMemoryKV())
	requireAccessTestSuccess(t, store.Start(t.Context()))

	cacheOptions := cache.Options{}
	if projectNamespace != "" {
		cacheOptions.DefaultNamespaces = map[string]cache.Config{projectNamespace: {}}
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: s, Cache: cacheOptions, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0",
		Controller: config.Controller{SkipNameValidation: ptr.To(true)}})
	requireAccessTestSuccess(t, err)
	r := &Reconciler{Client: store, Scheme: s, Options: ReconcilerOptions{ProjectUID: "project-a", ProjectNamespace: projectNamespace, PlatformNamespace: "internal-dns-system", Identity: "compiler-a",
		ServingRegions: []ServingRegion{{Region: "east", Shard: "s1"}}, AddressAllocator: testAllocator{}, PerZoneReconcile: true, ManifestChanges: store.SubscribeManifests}}
	requireAccessTestSuccess(t, r.SetupWithManager(mgr))
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() { stopped <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-stopped; err != nil {
			t.Error(err)
		}
	})
	return zoneControllerEnv{api: api, store: store}
}

// publishZone creates a private zone that one resolver context serves in one
// region, and waits until the zone is published to a serving target.
func (e zoneControllerEnv) publishZone(t *testing.T) *dnsv1alpha1.DNSZone {
	t.Helper()
	ctx := t.Context()
	network := &dnsv1alpha1.DNSResolverContext{ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "project"}, Spec: dnsv1alpha1.DNSResolverContextSpec{ConsumerID: "consumer-a"}}
	requireAccessTestSuccess(t, e.api.Create(ctx, network))
	zone := &dnsv1alpha1.DNSZone{ObjectMeta: metav1.ObjectMeta{Name: "private", Namespace: "project"}, Spec: dnsv1alpha1.DNSZoneSpec{DomainName: "corp.internal", Visibility: dnsv1alpha1.DNSZoneVisibilityPrivate}}
	for _, obj := range []client.Object{
		zone,
		&dnsv1alpha1.DNSZoneAssociation{ObjectMeta: metav1.ObjectMeta{Name: "zone-prod", Namespace: "project"}, Spec: dnsv1alpha1.DNSZoneAssociationSpec{DNSZoneRef: dnsv1alpha1.DNSNameReference{Name: zone.Name}, ResolverContextRef: dnsv1alpha1.DNSNameReference{Name: network.Name}}},
		&dnsv1alpha1.DNSResolverAccessBinding{ObjectMeta: metav1.ObjectMeta{Name: "prod-east", Namespace: "project"}, Spec: dnsv1alpha1.DNSResolverAccessBindingSpec{ContextRef: dnsv1alpha1.DNSObjectReference{Name: network.Name, UID: network.UID}, Region: "east", DestinationAddress: "fd53::11"}},
	} {
		requireAccessTestSuccess(t, e.api.Create(ctx, obj))
	}
	e.waitForManifest(t, zone, func(m *dnsv1alpha1.DNSPublicationManifest) bool {
		return !m.Spec.Tombstone && len(m.Spec.ServingTargets) > 0
	})
	return zone
}

// waitForManifest waits until the manifest that the zone's ownership commits
// to is one that want accepts.
func (e zoneControllerEnv) waitForManifest(t *testing.T, zone *dnsv1alpha1.DNSZone, want func(*dnsv1alpha1.DNSPublicationManifest) bool) *dnsv1alpha1.DNSPublicationManifest {
	t.Helper()
	var found *dnsv1alpha1.DNSPublicationManifest
	if !eventually(time.Minute, func() bool {
		found = e.activeManifest(t.Context(), zone.UID)
		return found != nil && want(found)
	}) {
		t.Fatalf("zone %s never reached the manifest the test waits for; the last one was %+v", zone.Name, found)
	}
	return found
}

func (e zoneControllerEnv) activeManifest(ctx context.Context, zoneUID types.UID) *dnsv1alpha1.DNSPublicationManifest {
	var owners dnsv1alpha1.DNSPublicationOwnershipList
	if err := e.store.List(ctx, &owners); err != nil {
		return nil
	}
	for _, own := range owners.Items {
		if own.Spec.ZoneUID != zoneUID || own.Spec.ActiveManifestName == "" {
			continue
		}
		var m dnsv1alpha1.DNSPublicationManifest
		if err := e.store.Get(ctx, client.ObjectKey{Namespace: own.Namespace, Name: own.Spec.ActiveManifestName}, &m); err == nil {
			return &m
		}
	}
	return nil
}

func eventually(within time.Duration, done func() bool) bool {
	for deadline := time.Now().Add(within); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if done() {
			return true
		}
	}
	return done()
}

// SPDX-License-Identifier: AGPL-3.0-only

package runtime

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	dnsv1alpha1 "go.miloapis.com/dns/api/v1alpha1"
	"go.miloapis.com/dns/internal/internaldns/controlplane"
	"go.miloapis.com/dns/internal/internaldns/model"
	"go.miloapis.com/dns/internal/internaldns/pubstore"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

const manifestNamespace = "internal-dns-system"

var unreachable = &rest.Config{Host: "https://platform.invalid"}

type heardManifest struct {
	previous, current *dnsv1alpha1.DNSPublicationManifest
}

func TestWatchManifestsHearsThePublicationStore(t *testing.T) {
	scheme := testScheme(t)
	store := pubstore.New(fake.NewClientBuilder().WithScheme(scheme).Build(), pubstore.NewMemoryKV())
	if err := store.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	changes, platformCache, err := watchManifests(t.Context(), Config{PlatformNamespace: manifestNamespace, PerZoneReconcile: true}, store, unreachable, scheme)
	if err != nil || platformCache != nil {
		t.Fatalf("with a publication store: platform cache %v, %v; want none", platformCache, err)
	}
	expectVerdictHeard(t, store, hearProjectA(t, changes))
}

func TestPerZoneProjectReconcilerSetsUpWithTheStoresChanges(t *testing.T) {
	scheme := testScheme(t)
	store := pubstore.New(fake.NewClientBuilder().WithScheme(scheme).Build(), pubstore.NewMemoryKV())
	if err := store.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	cfg := Config{PlatformNamespace: manifestNamespace, PerZoneReconcile: true}
	changes, _, err := watchManifests(t.Context(), cfg, store, unreachable, scheme)
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := ctrl.NewManager(unreachable, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0",
		Controller: config.Controller{SkipNameValidation: ptr.To(true)}})
	if err != nil {
		t.Fatal(err)
	}
	project := ProjectConfig{Name: "project-a", ProjectUID: "project-a", Namespace: "project"}
	r := &controlplane.Reconciler{Scheme: scheme, Options: cfg.reconcilerOptions(project, projectShared{manifestChanges: changes})}
	if err := r.SetupWithManager(mgr); err != nil {
		t.Fatalf("a per-zone project's reconciler did not set up: %v", err)
	}
}

func TestWatchManifestsHearsTheAPIServerWithoutAPublicationStore(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" {
		t.Skip("set KUBEBUILDER_ASSETS to watch manifests in the API server")
	}
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
	scheme := testScheme(t)
	api, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	if err := api.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: manifestNamespace}}); err != nil {
		t.Fatal(err)
	}
	changes, platformCache, err := watchManifests(t.Context(), Config{PlatformNamespace: manifestNamespace, PerZoneReconcile: true}, nil, cfg, scheme)
	if err != nil || platformCache == nil {
		t.Fatalf("without a publication store: platform cache %v, %v; want one", platformCache, err)
	}
	go func() { _ = platformCache.Start(t.Context()) }()
	heard := hearProjectA(t, changes)
	if !platformCache.WaitForCacheSync(t.Context()) {
		t.Fatal("the platform manifest cache never synced")
	}
	expectVerdictHeard(t, api, heard)
}

func TestWatchManifestsWithoutPerZoneReconcileOpensNoCache(t *testing.T) {
	changes, platformCache, err := watchManifests(t.Context(), Config{PlatformNamespace: manifestNamespace}, nil, unreachable, testScheme(t))
	if changes != nil || platformCache != nil || err != nil {
		t.Fatalf("whole-project mode without a publication store watched manifests: cache %v, %v", platformCache, err)
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme, err := newScheme()
	if err != nil {
		t.Fatal(err)
	}
	return scheme
}

func hearProjectA(t *testing.T, changes controlplane.ManifestChanges) <-chan heardManifest {
	t.Helper()
	if changes == nil {
		t.Fatal("no manifest changes to hear")
	}
	heard := make(chan heardManifest, 8)
	if err := changes(t.Context(), labels.SelectorFromSet(labels.Set{"project": "a"}), func(previous, current *dnsv1alpha1.DNSPublicationManifest) {
		heard <- heardManifest{previous: previous, current: current}
	}); err != nil {
		t.Fatal(err)
	}
	return heard
}

func tombstoneOf(project string) *dnsv1alpha1.DNSPublicationManifest {
	return &dnsv1alpha1.DNSPublicationManifest{ObjectMeta: metav1.ObjectMeta{Name: "tombstone-" + project, Namespace: manifestNamespace, Labels: map[string]string{"project": project}},
		Spec: dnsv1alpha1.DNSPublicationManifestSpec{ZoneRef: dnsv1alpha1.DNSObjectReference{Name: "zone", UID: types.UID("zone-" + project)}, ZoneApex: "private.example.", ContextUIDs: []types.UID{},
			WriterEpoch: 1, Revision: 1, Tombstone: true, Chunks: []dnsv1alpha1.DNSPublicationChunkReference{}, ContentHash: model.EmptyContentHash(), GeneratedAt: metav1.Now()}}
}

// expectVerdictHeard writes project b's tombstone, project a's, then a
// replica's verdict on project a's, and asks that only project a's create and
// verdict are heard.
func expectVerdictHeard(t *testing.T, c client.Client, heard <-chan heardManifest) {
	t.Helper()
	other, own := tombstoneOf("b"), tombstoneOf("a")
	for _, m := range []*dnsv1alpha1.DNSPublicationManifest{other, own} {
		if err := c.Create(t.Context(), m); err != nil {
			t.Fatal(err)
		}
	}
	if created := receiveManifest(t, heard); created.current.Name != own.Name || created.previous != nil {
		t.Fatalf("first change heard: %s after %v; want project a's tombstone, and nothing of project b", created.current.Name, created.previous)
	}
	apimeta.SetStatusCondition(&own.Status.Conditions, metav1.Condition{Type: "Published", Status: metav1.ConditionTrue, Reason: "ReplicaVerification"})
	if err := c.Status().Update(t.Context(), own); err != nil {
		t.Fatal(err)
	}
	verdict := receiveManifest(t, heard)
	if verdict.previous == nil || !apimeta.IsStatusConditionTrue(verdict.current.Status.Conditions, "Published") {
		t.Fatalf("heard %s after %v with conditions %v; want the verdict after the create", verdict.current.Name, verdict.previous, verdict.current.Status.Conditions)
	}
}

func receiveManifest(t *testing.T, heard <-chan heardManifest) heardManifest {
	t.Helper()
	select {
	case change := <-heard:
		return change
	case <-time.After(10 * time.Second):
		t.Fatal("heard no manifest change")
	}
	return heardManifest{}
}

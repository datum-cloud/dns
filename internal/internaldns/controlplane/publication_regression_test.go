package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	dnsv1alpha1 "go.miloapis.com/dns/api/v1alpha1"
	"go.miloapis.com/dns/internal/internaldns/model"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestTerminatingInputsWithdrawRecordsAndRetainFences(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		mark func(*CompileInput)
	}{
		{"registration", func(in *CompileInput) {
			in.Registrations[0].Finalizers = []string{"example.test/hold"}
			in.Registrations[0].DeletionTimestamp = &metav1.Time{Time: now.Add(-time.Second)}
		}},
		{"contribution", func(in *CompileInput) {
			in.Contributions[0].Finalizers = []string{"example.test/hold"}
			in.Contributions[0].DeletionTimestamp = &metav1.Time{Time: now.Add(-time.Second)}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := fixture(now)
			before, err := Compile(in)
			if err != nil {
				t.Fatal(err)
			}
			in.Previous = &before.Plan
			tc.mark(&in)
			got, err := Compile(in)
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Plan.RRsets) != 0 || got.Accepted[in.Contributions[0].UID] {
				t.Fatalf("control retained terminating %s: %#v", tc.name, got)
			}
			if len(got.Plan.Contributions) != 1 || got.Plan.Contributions[0].Sequence != 18 {
				t.Fatalf("lost withdrawal high-water fence: %#v", got.Plan.Contributions)
			}
		})
	}
}

func TestCommittedUntargetedTombstoneCompletes(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	scheme := runtime.NewScheme()
	if err := addReferenceStateToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&dnsv1alpha1.DNSPublicationManifest{}).Build()
	r := &Reconciler{Client: cl, Options: ReconcilerOptions{PlatformNamespace: "platform", ChunkSize: 1024}}
	zone := &dnsv1alpha1.DNSZone{ObjectMeta: metav1.ObjectMeta{Name: "orphan", UID: "zone-orphan"}, Spec: dnsv1alpha1.DNSZoneSpec{DomainName: "orphan.internal", Visibility: dnsv1alpha1.DNSZoneVisibilityPrivate}}
	manifest, err := r.persistPublication(ctx, zone, "tombstone", 1, 1, 0, true, nil, model.EmptyContentHash(), nil, nil, nil, nil, nil, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	owner := &dnsv1alpha1.DNSPublicationOwnership{ObjectMeta: metav1.ObjectMeta{Name: "owner", Namespace: "platform"}, Spec: dnsv1alpha1.DNSPublicationOwnershipSpec{ZoneUID: zone.UID, WriterEpoch: 1, ActiveManifestName: manifest.Name}}
	if err := cl.Create(ctx, owner); err != nil {
		t.Fatal(err)
	}
	var outboxes dnsv1alpha1.DNSTransportOutboxList
	if err := cl.List(ctx, &outboxes); err != nil {
		t.Fatal(err)
	}
	if len(outboxes.Items) != 0 {
		t.Fatalf("untargeted tombstone unexpectedly has a delivery path: %d", len(outboxes.Items))
	}
	if acknowledged, err := r.tombstoneAcknowledged(ctx, owner); err != nil || !acknowledged {
		t.Fatalf("committed untargeted tombstone did not complete: %v", err)
	}
	owner.Spec.WriterEpoch++
	if acknowledged, err := r.tombstoneAcknowledged(ctx, owner); err != nil || !acknowledged {
		t.Fatalf("takeover lost the committed zero-target withdrawal: %v", err)
	}
}

func TestTombstoneVerdictReadErrorIsReturned(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := addReferenceStateToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	unavailable := errors.New("publication store unavailable")
	cl := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
		return unavailable
	}}).Build()
	r := &Reconciler{Client: cl, Options: ReconcilerOptions{PlatformNamespace: "platform"}}
	owner := &dnsv1alpha1.DNSPublicationOwnership{ObjectMeta: metav1.ObjectMeta{Name: "owner", Namespace: "platform"}, Spec: dnsv1alpha1.DNSPublicationOwnershipSpec{ZoneUID: "zone-a", WriterEpoch: 1, ActiveManifestName: "tombstone"}}
	acknowledged, err := r.tombstoneAcknowledged(context.Background(), owner)
	if !errors.Is(err, unavailable) || acknowledged {
		t.Fatalf("a failed read of the tombstone returned (%v, %v); want the read's error, so the zone's key is retried", acknowledged, err)
	}
}

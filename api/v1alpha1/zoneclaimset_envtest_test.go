// SPDX-License-Identifier: AGPL-3.0-only

package v1alpha1

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestZoneClaimSetAtAPIServer(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" {
		t.Skip("set KUBEBUILDER_ASSETS to test the ZoneClaimSet schema against the API server")
	}
	environment := &envtest.Environment{
		BinaryAssetsDirectory: assets,
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd-authoritative")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := environment.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := environment.Stop(); err != nil {
			t.Error(err)
		}
	})
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	const ns = "accounting"
	if err := cl.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatal(err)
	}

	since := metav1.NewTime(time.Now().Truncate(time.Second))
	active := func(name, project, zone string) ZoneClaimEntry {
		return ZoneClaimEntry{
			Name:    name,
			Project: &ZoneClaimProject{Name: project},
			Zone:    &ZoneClaimZone{Cluster: project, Namespace: "default", Name: zone, UID: zone + "-uid"},
			State:   ZoneClaimActive,
			Since:   since,
		}
	}
	contested := func(e ZoneClaimEntry, reason ZoneClaimReason, blockedBy string) ZoneClaimEntry {
		e.State, e.Reason, e.BlockedBy = ZoneClaimContested, reason, blockedBy
		return e
	}

	t.Run("keeps entries in arrival order", func(t *testing.T) {
		imported := active("legacy.example.com", "acme-prod", "legacy")
		imported.Zone.UID = ""
		imported.PreexistingOverlap = true
		reserved := active("old.example.com", "acme-prod", "old")
		reserved.State, reserved.Zone = ZoneClaimReserved, nil
		lostZone := contested(active("lost.example.com", "acme-dev", "lost"), ZoneClaimParentClaimedByAnotherProject, "example.com")
		lostZone.Zone = nil
		first := active("example.com", "acme-prod", "example-com")
		first.Project.Organization = "acme"
		want := []ZoneClaimEntry{
			first,
			active("shop.example.com", "acme-prod", "shop-example-com"),
			contested(active("example.com", "squatter", "mine"), ZoneClaimClaimedByAnotherProject, "example.com"),
			contested(active("example.com", "acme-prod", "copy"), ZoneClaimDuplicateZoneInProject, ""),
			{Name: "reserved.example.com", State: ZoneClaimReserved, OperatorSeeded: true, Since: since},
			imported,
			reserved,
			lostZone,
			active("a-label-longer-than-sixty-three-characters-is-still-a-valid-zone-name.example.com", "acme-prod", "long"),
		}
		set := &ZoneClaimSet{ObjectMeta: metav1.ObjectMeta{Name: "example.com", Namespace: ns}, Spec: ZoneClaimSetSpec{Entries: want}}
		if err := cl.Create(ctx, set); err != nil {
			t.Fatal(err)
		}
		var got ZoneClaimSet
		if err := cl.Get(ctx, client.ObjectKeyFromObject(set), &got); err != nil {
			t.Fatal(err)
		}
		if len(got.Spec.Entries) != len(want) {
			t.Fatalf("got %d entries, want %d", len(got.Spec.Entries), len(want))
		}
		if !apiequality.Semantic.DeepEqual(got.Spec.Entries, want) {
			t.Fatalf("entries changed on the round trip:\ngot  %+v\nwant %+v", got.Spec.Entries, want)
		}
	})

	for _, tc := range []struct {
		name    string
		mutate  func(*ZoneClaimEntry)
		message string
	}{
		{"unknown state", func(e *ZoneClaimEntry) { e.State = "Winning" }, "Unsupported value"},
		{"uppercase name", func(e *ZoneClaimEntry) { e.Name = "Example.com" }, "should match"},
		{"trailing dot", func(e *ZoneClaimEntry) { e.Name = "example.com." }, "should match"},
		{"leading slash in cluster", func(e *ZoneClaimEntry) { e.Zone.Cluster = "/acme-prod" }, "should match"},
		{"contested without reason", func(e *ZoneClaimEntry) { e.State = ZoneClaimContested }, "reason is set exactly when the entry is Contested"},
		{"reason while active", func(e *ZoneClaimEntry) { e.Reason = ZoneClaimDuplicateZoneInProject }, "reason is set exactly when the entry is Contested"},
		{"another project without blocker", func(e *ZoneClaimEntry) {
			*e = contested(*e, ZoneClaimClaimedByAnotherProject, "")
		}, "blockedBy is set exactly when another project's entry blocks this one"},
		{"duplicate with blocker", func(e *ZoneClaimEntry) {
			*e = contested(*e, ZoneClaimDuplicateZoneInProject, "example.net")
		}, "blockedBy is set exactly when another project's entry blocks this one"},
		{"active without zone", func(e *ZoneClaimEntry) { e.Zone = nil }, "an Active entry has a zone"},
		{"reserved with zone", func(e *ZoneClaimEntry) { e.State = ZoneClaimReserved }, "a Reserved entry has no zone"},
		{"single label", func(e *ZoneClaimEntry) { e.Name = "net" }, "should match"},
		{"no project", func(e *ZoneClaimEntry) { e.Project = nil }, "project is set exactly when the entry is not operator-seeded"},
		{"operator-seeded with project", func(e *ZoneClaimEntry) {
			e.OperatorSeeded, e.State, e.Zone = true, ZoneClaimReserved, nil
		}, "project is set exactly when the entry is not operator-seeded"},
		{"operator-seeded while active", func(e *ZoneClaimEntry) {
			e.OperatorSeeded, e.Project = true, nil
		}, "an operator-seeded entry is always Reserved"},
	} {
		t.Run("rejects "+tc.name, func(t *testing.T) {
			e := active("example.net", "acme-prod", "example-net")
			tc.mutate(&e)
			set := &ZoneClaimSet{ObjectMeta: metav1.ObjectMeta{Name: "example.net", Namespace: ns}, Spec: ZoneClaimSetSpec{Entries: []ZoneClaimEntry{e}}}
			err := cl.Create(ctx, set)
			if !apierrors.IsInvalid(err) {
				t.Fatalf("expected API validation rejection, got %v", err)
			}
			if !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("rejected for the wrong reason: want %q in %v", tc.message, err)
			}
		})
	}
}

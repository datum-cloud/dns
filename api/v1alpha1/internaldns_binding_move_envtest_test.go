// SPDX-License-Identifier: AGPL-3.0-only

package v1alpha1

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func validateResolverBindingMoves(t *testing.T, cl client.Client) {
	t.Helper()
	ctx := context.Background()
	newBinding := func(name string) *DNSResolverBinding {
		return &DNSResolverBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "validation"}, Spec: DNSResolverBindingSpec{
			Source:        DNSResolverBindingSource{ProjectUID: "project-uid", ResolverContextRef: DNSObjectReference{Name: "context", UID: "context-uid"}, AccessBindingRef: DNSObjectReference{Name: "access", UID: "access-uid", Generation: 1}},
			Placement:     DNSResolverBindingPlacement{Region: "central", Shard: "shared"},
			Configuration: DNSResolverBindingConfiguration{Generation: 1, Revision: 1, Query: DNSResolverBindingQuery{NodeAddress: "fd70:100::1", RegionalAddress: "fd54::1"}, ZoneRefs: []DNSObjectReference{}},
			Version:       DNSResolverAccessVersion{Epoch: 1, Sequence: 1},
		}}
	}
	update := func(t *testing.T, name string, mutate func(*DNSResolverBindingSpec)) error {
		t.Helper()
		b := newBinding(name)
		if err := cl.Create(ctx, b); err != nil {
			t.Fatal(err)
		}
		mutate(&b.Spec)
		return cl.Update(ctx, b)
	}
	for name, tc := range map[string]struct {
		mutate func(*DNSResolverBindingSpec)
		ok     bool
	}{
		"regional address with a newer revision": {func(s *DNSResolverBindingSpec) {
			s.Configuration.Query.RegionalAddress = "fd70:202::1:0:5"
			s.Configuration.Revision++
		}, true},
		"regional address at the same revision": {func(s *DNSResolverBindingSpec) { s.Configuration.Query.RegionalAddress = "fd70:202::1:0:5" }, false},
		"a newer access generation":             {func(s *DNSResolverBindingSpec) { s.Source.AccessBindingRef.Generation = 2 }, true},
		"another access":                        {func(s *DNSResolverBindingSpec) { s.Source.AccessBindingRef.UID = "other-access" }, false},
		"another context":                       {func(s *DNSResolverBindingSpec) { s.Source.ResolverContextRef.UID = "other-context" }, false},
		"another project":                       {func(s *DNSResolverBindingSpec) { s.Source.ProjectUID = "other-project" }, false},
	} {
		t.Run("binding-move-"+name, func(t *testing.T) {
			err := update(t, "move-"+metav1.Now().Format("150405.000000000"), tc.mutate)
			if tc.ok && err != nil {
				t.Fatalf("a valid change was refused: %v", err)
			}
			if !tc.ok && !apierrors.IsInvalid(err) {
				t.Fatalf("expected API validation rejection, got %v", err)
			}
		})
	}
}

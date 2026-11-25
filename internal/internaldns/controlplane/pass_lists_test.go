// SPDX-License-Identifier: AGPL-3.0-only

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"testing"

	dnsv1alpha1 "go.miloapis.com/dns/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const unlabelledBindingLists = "DNSResolverBindingList !" + referenceProjectLabel

func labelledBindingLists(project string) string {
	return "DNSResolverBindingList " + referenceProjectLabel + "=" + project
}

// listCounts counts the LISTs sent through it by kind and label selector. It
// refuses the next failUnlabelled LISTs of unlabelled bindings, and runs
// beforeList ahead of each LIST it passes on.
type listCounts struct {
	client.Client
	mu             sync.Mutex
	lists          map[string]int
	failUnlabelled int
	beforeList     func(client.ObjectList)
}

func (c *listCounts) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	key := listKey(list, opts)
	c.mu.Lock()
	if c.lists == nil {
		c.lists = map[string]int{}
	}
	c.lists[key]++
	fail := key == unlabelledBindingLists && c.failUnlabelled > 0
	if fail {
		c.failUnlabelled--
	}
	c.mu.Unlock()
	if fail {
		return errors.New("list of unlabelled bindings refused")
	}
	if c.beforeList != nil {
		c.beforeList(list)
	}
	return c.Client.List(ctx, list, opts...)
}

func listKey(list client.ObjectList, opts []client.ListOption) string {
	o := &client.ListOptions{}
	o.ApplyOptions(opts)
	key := fmt.Sprintf("%T", list)
	key = key[strings.LastIndex(key, ".")+1:]
	if o.LabelSelector != nil && !o.LabelSelector.Empty() {
		key += " " + o.LabelSelector.String()
	}
	return key
}

// take returns the LISTs counted since the last take, by kind and selector.
func (c *listCounts) take() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	counted := maps.Clone(c.lists)
	c.lists = map[string]int{}
	return counted
}

// orphanBinding is a binding of the harness's project and context whose
// access binding no longer exists.
func orphanBinding(name string, labelled bool) *dnsv1alpha1.DNSResolverBinding {
	labels := map[string]string{"internal-dns.miloapis.com/source-namespace": "project", "internal-dns.miloapis.com/context-uid": "vpc-a"}
	if labelled {
		labels[referenceProjectLabel] = "project-a"
	}
	return &dnsv1alpha1.DNSResolverBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "internal-dns-system", Labels: labels},
		Spec: dnsv1alpha1.DNSResolverBindingSpec{
			Source: dnsv1alpha1.DNSResolverBindingSource{
				ProjectUID:         "project-a",
				ResolverContextRef: dnsv1alpha1.DNSObjectReference{Name: "prod", UID: "vpc-a"},
				AccessBindingRef:   dnsv1alpha1.DNSObjectReference{Name: "gone", UID: "gone-uid", Generation: 1},
			},
			Placement:     dnsv1alpha1.DNSResolverBindingPlacement{Region: "east", Shard: "s1"},
			Configuration: dnsv1alpha1.DNSResolverBindingConfiguration{Generation: 1, Revision: 1, Query: dnsv1alpha1.DNSResolverBindingQuery{NodeAddress: "fd53::99", RegionalAddress: "fd54::99"}},
			Version:       dnsv1alpha1.DNSResolverAccessVersion{Epoch: 1, Sequence: 1},
		},
	}
}

func (h *perZoneHarness) binding(name string) *dnsv1alpha1.DNSResolverBinding {
	h.t.Helper()
	var b dnsv1alpha1.DNSResolverBinding
	if err := h.c.Get(h.ctx, client.ObjectKey{Namespace: "internal-dns-system", Name: name}, &b); err != nil {
		h.t.Fatal(err)
	}
	return &b
}

func TestPassWithdrawsAnOrphanedBindingWithOrWithoutItsProjectLabel(t *testing.T) {
	h, _ := newPerZoneHarness(t)
	if err := h.c.Create(h.ctx, orphanBinding("unlabelled", false)); err != nil {
		t.Fatal(err)
	}
	h.run(projectQueueKey)
	if b := h.binding("unlabelled"); !b.Spec.Tombstone || b.Labels[referenceProjectLabel] != "project-a" {
		t.Fatalf("the first pass left an unlabelled binding whose access is gone: tombstone %v, labels %v", b.Spec.Tombstone, b.Labels)
	}
	if err := h.c.Create(h.ctx, orphanBinding("labelled", true)); err != nil {
		t.Fatal(err)
	}
	h.run(projectQueueKey)
	if b := h.binding("labelled"); !b.Spec.Tombstone {
		t.Fatal("a later pass left a labelled binding whose access is gone")
	}
}

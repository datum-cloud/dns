// SPDX-License-Identifier: AGPL-3.0-only

package pubstore

import (
	"context"
	"errors"
	"testing"
	"time"

	dnsv1 "go.miloapis.com/dns/api/v1alpha1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// heardChange is one change a subscriber heard, with the resource version the
// subscribed store's index held for the manifest when it heard it.
type heardChange struct {
	previous, current *dnsv1.DNSPublicationManifest
	indexed           string
}

func TestManifestSubscriberHearsAnotherReplicasVerdict(t *testing.T) {
	ctx := t.Context()
	a, _, kv := newStore(t)
	b := New(fake.NewClientBuilder().Build(), kv)
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.SubscribeManifests(ctx, labels.Everything(), func(_, _ *dnsv1.DNSPublicationManifest) { <-ctx.Done() }); err != nil {
		t.Fatal(err)
	}
	heard := subscribeHeard(t, a, labels.SelectorFromSet(labels.Set{"project": "a"}))

	other := &dnsv1.DNSPublicationManifest{ObjectMeta: metav1.ObjectMeta{Name: "other-project", Namespace: "platform", Labels: map[string]string{"project": "b"}}, Spec: dnsv1.DNSPublicationManifestSpec{Tombstone: true}}
	tombstone := &dnsv1.DNSPublicationManifest{ObjectMeta: metav1.ObjectMeta{Name: "tombstone", Namespace: "platform", Labels: map[string]string{"project": "a"}}, Spec: dnsv1.DNSPublicationManifestSpec{Tombstone: true}}
	for _, m := range []*dnsv1.DNSPublicationManifest{other, tombstone} {
		if err := b.Create(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	created := receiveChange(t, heard)
	assertHeard(t, created, tombstone.Name, "")

	base := tombstone.DeepCopy()
	apimeta.SetStatusCondition(&tombstone.Status.Conditions, metav1.Condition{Type: "Published", Status: metav1.ConditionTrue, Reason: "ReplicaVerification"})
	if err := b.Status().Patch(ctx, tombstone, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	verdict := receiveChange(t, heard)
	assertHeard(t, verdict, tombstone.Name, created.current.ResourceVersion)
	if !apimeta.IsStatusConditionTrue(verdict.current.Status.Conditions, "Published") {
		t.Fatalf("verdict heard without Published=True: %v", verdict.current.Status.Conditions)
	}
}

func TestManifestSubscriberLeavesWhenItsContextEnds(t *testing.T) {
	store, _, _ := newStore(t)
	ctx, cancel := context.WithCancel(t.Context())
	if err := store.SubscribeManifests(ctx, labels.Everything(), func(_, _ *dnsv1.DNSPublicationManifest) {}); err != nil {
		t.Fatal(err)
	}
	cancel()
	waitUnsubscribed(t, store)

	live := subscribeHeard(t, store, labels.Everything())
	createManifest(t, store, "after-cancel")
	assertHeard(t, receiveChange(t, live), "after-cancel", "")
}

func TestManifestSubscriberRefusesAnEndedContext(t *testing.T) {
	store, _, _ := newStore(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := store.SubscribeManifests(ctx, labels.Everything(), func(_, _ *dnsv1.DNSPublicationManifest) {})
	if !errors.Is(err, context.Canceled) || subscriberCount(store) != 0 {
		t.Fatalf("subscribing with an ended context returned %v and left %d subscribers; want context.Canceled and none", err, subscriberCount(store))
	}
}

func TestManifestSubscriberStopsMidBacklogWhenItsContextEnds(t *testing.T) {
	store, _, _ := newStore(t)
	ctx, cancel := context.WithCancel(t.Context())
	entered := make(chan string, 3)
	release := make(chan struct{})
	if err := store.SubscribeManifests(ctx, labels.Everything(), func(_, current *dnsv1.DNSPublicationManifest) {
		entered <- current.Name
		<-release
	}); err != nil {
		t.Fatal(err)
	}
	createManifest(t, store, "first")
	expectEntered(t, entered, "first")
	createManifest(t, store, "second")
	createManifest(t, store, "third")
	release <- struct{}{}
	expectEntered(t, entered, "second")
	cancel()
	close(release)
	waitUnsubscribed(t, store)
	select {
	case name := <-entered:
		t.Fatalf("the subscriber heard %s from its backlog after its context ended", name)
	default:
	}
}

func createManifest(t *testing.T, store *PublicationStore, name string) {
	t.Helper()
	if err := store.Create(t.Context(), &dnsv1.DNSPublicationManifest{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "platform"}}); err != nil {
		t.Fatal(err)
	}
}

func expectEntered(t *testing.T, entered <-chan string, want string) {
	t.Helper()
	select {
	case name := <-entered:
		if name != want {
			t.Fatalf("the subscriber heard %s; want %s", name, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("the subscriber did not hear %s", want)
	}
}

func waitUnsubscribed(t *testing.T, store *PublicationStore) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for subscriberCount(store) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the subscriber stayed subscribed after its context ended")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func subscriberCount(store *PublicationStore) int {
	store.subscribers.mu.Lock()
	defer store.subscribers.mu.Unlock()
	return len(store.subscribers.all)
}

// subscribeHeard subscribes to store and reports each change with the
// revision store's index held when the subscriber heard it.
func subscribeHeard(t *testing.T, store *PublicationStore, selector labels.Selector) <-chan heardChange {
	t.Helper()
	ctx := t.Context()
	heard := make(chan heardChange, 8)
	if err := store.SubscribeManifests(ctx, selector, func(previous, current *dnsv1.DNSPublicationManifest) {
		heard <- heardChange{previous: previous, current: current, indexed: indexedVersion(ctx, t, store, current.Name)}
	}); err != nil {
		t.Fatal(err)
	}
	return heard
}

func indexedVersion(ctx context.Context, t *testing.T, store *PublicationStore, name string) string {
	var list dnsv1.DNSPublicationManifestList
	if err := store.List(ctx, &list); err != nil {
		t.Error(err)
	}
	for _, m := range list.Items {
		if m.Name == name {
			return m.ResourceVersion
		}
	}
	return ""
}

// assertHeard checks a change names the manifest, follows the revision
// before it ("" for none), and was heard once the index held it.
func assertHeard(t *testing.T, change heardChange, name, before string) {
	t.Helper()
	previous := ""
	if change.previous != nil {
		previous = change.previous.ResourceVersion
	}
	if change.current.Name != name || previous != before {
		t.Fatalf("heard %s after revision %q; want %s after %q, and nothing of another project", change.current.Name, previous, name, before)
	}
	if change.indexed != change.current.ResourceVersion {
		t.Fatalf("heard revision %s while the index held %q", change.current.ResourceVersion, change.indexed)
	}
}

func receiveChange(t *testing.T, heard <-chan heardChange) heardChange {
	t.Helper()
	select {
	case change := <-heard:
		return change
	case <-time.After(5 * time.Second):
		t.Fatal("the subscriber heard no manifest change")
	}
	return heardChange{}
}

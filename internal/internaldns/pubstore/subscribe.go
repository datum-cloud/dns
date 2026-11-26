// SPDX-License-Identifier: AGPL-3.0-only

package pubstore

import (
	"context"
	"sync"

	dnsv1 "go.miloapis.com/dns/api/v1alpha1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SubscribeManifests calls notify with each manifest revision the index takes
// whose labels selector matches, from the bucket's watch or from this store's
// own writes, until ctx ends. previous is the revision the index held before,
// or nil. Both are the index's own objects, so notify must not change them.
// As with an informer's handler, one subscriber hears its changes in order on
// a goroutine of its own, so a slow subscriber holds up neither the index nor
// another subscriber.
func (s *PublicationStore) SubscribeManifests(ctx context.Context, selector labels.Selector, notify func(previous, current *dnsv1.DNSPublicationManifest)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	sub := &subscriber{selector: selector, wake: make(chan struct{}, 1)}
	s.subscribers.add(sub)
	go func() {
		defer s.subscribers.remove(sub)
		for {
			select {
			case <-ctx.Done():
				return
			case <-sub.wake:
			}
			for _, change := range sub.take() {
				if ctx.Err() != nil {
					return
				}
				notify(change.previous, change.current)
			}
		}
	}()
	return nil
}

type manifestChange struct {
	previous, current *dnsv1.DNSPublicationManifest
}

type subscribers struct {
	mu  sync.Mutex
	all map[*subscriber]bool
}

func (s *subscribers) add(sub *subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.all == nil {
		s.all = map[*subscriber]bool{}
	}
	s.all[sub] = true
}

func (s *subscribers) remove(sub *subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.all, sub)
}

// publish hands each subscriber that selects it a manifest revision the index
// took. It runs under the index lock, so each subscriber's changes keep the
// index's order, and it only appends, so it never waits for a subscriber.
func (s *subscribers) publish(previous, current client.Object) {
	manifest, ok := current.(*dnsv1.DNSPublicationManifest)
	if !ok {
		return
	}
	before, _ := previous.(*dnsv1.DNSPublicationManifest)
	s.mu.Lock()
	defer s.mu.Unlock()
	for sub := range s.all {
		if sub.selector.Matches(labels.Set(manifest.Labels)) {
			sub.push(manifestChange{previous: before, current: manifest})
		}
	}
}

// subscriber buffers its changes without a bound, as an informer's listener
// does, so publishing to it never blocks.
type subscriber struct {
	selector labels.Selector
	mu       sync.Mutex
	pending  []manifestChange
	wake     chan struct{}
}

func (sub *subscriber) push(change manifestChange) {
	sub.mu.Lock()
	sub.pending = append(sub.pending, change)
	sub.mu.Unlock()
	select {
	case sub.wake <- struct{}{}:
	default:
	}
}

func (sub *subscriber) take() []manifestChange {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	pending := sub.pending
	sub.pending = nil
	return pending
}

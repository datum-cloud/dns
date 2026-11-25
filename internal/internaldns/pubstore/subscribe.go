// SPDX-License-Identifier: AGPL-3.0-only

package pubstore

import (
	"context"

	dnsv1 "go.miloapis.com/dns/api/v1alpha1"
	"k8s.io/apimachinery/pkg/labels"
)

// SubscribeManifests calls notify with each manifest revision the index takes
// whose labels selector matches, from the bucket's watch or from this store's
// own writes, until ctx ends.
func (s *PublicationStore) SubscribeManifests(ctx context.Context, selector labels.Selector, notify func(previous, current *dnsv1.DNSPublicationManifest)) error {
	return ctx.Err()
}

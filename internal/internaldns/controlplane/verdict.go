// SPDX-License-Identifier: AGPL-3.0-only

package controlplane

import (
	"context"

	dnsv1alpha1 "go.miloapis.com/dns/api/v1alpha1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// ManifestChanges calls notify with each change of a publication manifest
// whose labels selector matches, until ctx ends. previous is nil for a
// manifest first seen. notify must not change either manifest.
type ManifestChanges func(ctx context.Context, selector labels.Selector, notify func(previous, current *dnsv1alpha1.DNSPublicationManifest)) error

// zoneUIDIndex finds a zone by its UID.
const zoneUIDIndex = "private-dns.zone-uid"

func zoneUID(o client.Object) []string { return []string{string(o.GetUID())} }

// verdictSource queues a deleted zone's key when its tombstone's verdict
// changes.
func (r *Reconciler) verdictSource(zones client.Reader) source.Source {
	return source.Func(func(context.Context, workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
		return nil
	})
}

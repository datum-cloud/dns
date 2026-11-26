// SPDX-License-Identifier: AGPL-3.0-only

package controlplane

import (
	"context"

	dnsv1alpha1 "go.miloapis.com/dns/api/v1alpha1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// ManifestChanges calls notify with each change of a publication manifest
// whose labels selector matches, until ctx ends. previous is nil for a
// manifest first seen. notify must not change either manifest. An informer
// first replays the manifests it holds; the publication store starts with the
// next change. Either way, the project's first pass queues each zone whose
// lease it holds.
type ManifestChanges func(ctx context.Context, selector labels.Selector, notify func(previous, current *dnsv1alpha1.DNSPublicationManifest)) error

const zoneUIDIndex = "private-dns.zone-uid"

func zoneUID(o client.Object) []string { return []string{string(o.GetUID())} }

// verdictSource queues a deleted zone's key when its tombstone is created or a
// replica's verdict on it changes, so the key waits for the verdict with no
// timer. The project pass's resync is the backstop for a lost change.
func (r *Reconciler) verdictSource(zones client.Reader) source.Source {
	selector := labels.SelectorFromSet(labels.Set{referenceProjectLabel: string(r.Options.ProjectUID)})
	return source.Func(func(ctx context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
		return r.Options.ManifestChanges(ctx, selector, func(previous, current *dnsv1alpha1.DNSPublicationManifest) {
			if !current.Spec.Tombstone || previous != nil && verdict(previous) == verdict(current) {
				return
			}
			if req, ok := r.tombstoneZone(ctx, zones, current); ok {
				q.AddAfter(req, zoneQueueDelay)
			}
		})
	})
}

func verdict(m *dnsv1alpha1.DNSPublicationManifest) metav1.ConditionStatus {
	if c := apimeta.FindStatusCondition(m.Status.Conditions, "Published"); c != nil {
		return c.Status
	}
	return metav1.ConditionUnknown
}

// tombstoneZone names the key of the zone a tombstone withdraws. A manifest
// names its zone only by name and UID, so a project across namespaces finds
// the zone's namespace by its UID.
func (r *Reconciler) tombstoneZone(ctx context.Context, zones client.Reader, m *dnsv1alpha1.DNSPublicationManifest) (reconcile.Request, bool) {
	if ns := r.Options.ProjectNamespace; ns != "" {
		return zoneRequest(ns, m.Spec.ZoneRef.Name), true
	}
	var found dnsv1alpha1.DNSZoneList
	if err := zones.List(ctx, &found, client.MatchingFields{zoneUIDIndex: string(m.Spec.ZoneRef.UID)}); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "find the zone of a tombstone", "manifest", m.Name)
		return reconcile.Request{}, false
	}
	if len(found.Items) != 1 {
		ctrl.LoggerFrom(ctx).V(1).Info("no single zone for a tombstone; the project pass's resync reconciles it", "manifest", m.Name, "zoneUID", m.Spec.ZoneRef.UID, "zones", len(found.Items))
		return reconcile.Request{}, false
	}
	return zoneRequest(found.Items[0].Namespace, found.Items[0].Name), true
}

// SPDX-License-Identifier: AGPL-3.0-only

package runtime

import (
	"context"
	"fmt"

	dnsv1alpha1 "go.miloapis.com/dns/api/v1alpha1"
	"go.miloapis.com/dns/internal/internaldns/controlplane"
	"go.miloapis.com/dns/internal/internaldns/pubstore"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
)

// watchManifests is where the zone keys hear their publication manifests
// change: the publication store's index, or, when the manifests are API
// objects, a watch cache of the platform namespace, which the caller starts.
// Only per-zone reconcile listens, so without it the API objects are not
// watched.
func watchManifests(ctx context.Context, cfg Config, store *pubstore.PublicationStore, platformREST *rest.Config, scheme *runtime.Scheme) (controlplane.ManifestChanges, cache.Cache, error) {
	if store != nil {
		return store.SubscribeManifests, nil, nil
	}
	if !cfg.PerZoneReconcile {
		return nil, nil, nil
	}
	platformCache, err := cache.New(platformREST, cache.Options{Scheme: scheme, DefaultNamespaces: map[string]cache.Config{cfg.PlatformNamespace: {}}})
	if err != nil {
		return nil, nil, fmt.Errorf("create the platform manifest cache: %w", err)
	}
	informer, err := platformCache.GetInformer(ctx, &dnsv1alpha1.DNSPublicationManifest{}, cache.BlockUntilSynced(false))
	if err != nil {
		return nil, nil, fmt.Errorf("watch publication manifests: %w", err)
	}
	return informerManifestChanges(informer), platformCache, nil
}

// informerManifestChanges hears manifest changes through an informer's
// handler, which runs on a goroutine of its own.
func informerManifestChanges(informer cache.Informer) controlplane.ManifestChanges {
	return func(ctx context.Context, selector labels.Selector, notify func(previous, current *dnsv1alpha1.DNSPublicationManifest)) error {
		selected := func(obj any) (*dnsv1alpha1.DNSPublicationManifest, bool) {
			m, ok := obj.(*dnsv1alpha1.DNSPublicationManifest)
			return m, ok && selector.Matches(labels.Set(m.Labels))
		}
		registration, err := informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {
				if m, ok := selected(obj); ok {
					notify(nil, m)
				}
			},
			UpdateFunc: func(oldObj, newObj any) {
				if m, ok := selected(newObj); ok {
					previous, _ := oldObj.(*dnsv1alpha1.DNSPublicationManifest)
					notify(previous, m)
				}
			},
		})
		if err != nil {
			return err
		}
		go func() {
			<-ctx.Done()
			_ = informer.RemoveEventHandler(registration)
		}()
		return nil
	}
}

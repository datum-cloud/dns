// SPDX-License-Identifier: AGPL-3.0-only

package runtime

import (
	"context"

	"go.miloapis.com/dns/internal/internaldns/controlplane"
	"go.miloapis.com/dns/internal/internaldns/pubstore"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
)

// watchManifests is where the zone keys hear their publication manifests
// change.
func watchManifests(ctx context.Context, cfg Config, store *pubstore.PublicationStore, platformREST *rest.Config, scheme *runtime.Scheme) (controlplane.ManifestChanges, cache.Cache, error) {
	return nil, nil, nil
}

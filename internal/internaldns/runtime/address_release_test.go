// SPDX-License-Identifier: AGPL-3.0-only

package runtime

import (
	"context"
	"testing"

	"go.miloapis.com/dns/internal/internaldns/platform"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestRegionalAddressReleaseSweepsOncePerInterval(t *testing.T) {
	// An allocator without a client fails every sweep it runs, so a nil
	// result shows the check did not sweep.
	release := regionalAddressRelease(&platform.Allocator{}, true)
	if err := release(context.Background()); err == nil {
		t.Fatal("the first check did not sweep")
	}
	if err := release(context.Background()); err == nil {
		t.Fatal("a check after a failed sweep reported success")
	}
}

func TestRegionalAddressReleaseChecksDoNotSweep(t *testing.T) {
	scheme, err := newScheme()
	if err != nil {
		t.Fatal(err)
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	calls := 0
	counting := interceptor.NewClient(cl, interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		calls++
		return c.List(ctx, list, opts...)
	}})
	release := regionalAddressRelease(&platform.Allocator{Client: counting, Namespace: "platform"}, true)
	if err := release(context.Background()); err != nil {
		t.Fatal(err)
	}
	swept := calls
	if swept == 0 {
		t.Fatal("the first check did not sweep")
	}
	for range 2 {
		if err := release(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if calls != swept {
		t.Fatalf("checks inside the interval swept again: %d lists, want %d", calls, swept)
	}
}

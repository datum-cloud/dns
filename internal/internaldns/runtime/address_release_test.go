// SPDX-License-Identifier: AGPL-3.0-only

package runtime

import (
	"context"
	"testing"

	"go.miloapis.com/dns/internal/internaldns/platform"
)

func TestRegionalAddressReleaseSweepsOncePerInterval(t *testing.T) {
	// An allocator without a client fails every sweep it runs, so a nil
	// result shows the check did not sweep.
	release := regionalAddressRelease(&platform.Allocator{}, true)
	if err := release(context.Background()); err == nil {
		t.Fatal("the first check did not sweep")
	}
	if err := release(context.Background()); err != nil {
		t.Fatalf("a check inside the interval swept again: %v", err)
	}
}

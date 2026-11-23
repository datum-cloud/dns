// SPDX-License-Identifier: AGPL-3.0-only

package serving

import (
	"context"
	"testing"
	"time"

	"go.miloapis.com/dns/internal/internaldns/model"
)

func TestAMemberAppliesABindingMovedToANewRegionalAddress(t *testing.T) {
	now := time.Now().UTC()
	a, err := NewAgent(regionalTestConfig(t), Dependencies{Runner: &recordingRunner{}, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	active := testSnapshot(now)
	if err := a.Handle(context.Background(), envelope(t, model.KindServingSnapshot, "shard", 1, 1, active)); err != nil {
		t.Fatal(err)
	}
	moved := active
	moved.Bindings = append([]model.Binding(nil), active.Bindings...)
	moved.Bindings[0].ClusterAddress = "fd70:202::1:0:5"
	moved.Bindings[0].ConfigurationRevision++
	moved.ConfigurationRevision++
	if err := a.Handle(context.Background(), envelope(t, model.KindServingSnapshot, "shard", 1, 2, moved)); err != nil {
		t.Fatalf("a binding moved with a newer revision was refused: %v", err)
	}
	if got := a.state.BindingFences[moved.Bindings[0].BindingUID].Identity; got != bindingIdentity(moved.Bindings[0]) {
		t.Fatal("the member did not take the moved binding")
	}
}

func TestBindingFencesMoveAddressesOnlyWithANewerRevision(t *testing.T) {
	now := time.Now().UTC()
	snapshot := testSnapshot(now)
	b := snapshot.Bindings[0]
	prior := map[string]bindingFence{b.BindingUID: bindingFenceFor(b)}
	for name, tc := range map[string]struct {
		change func(*model.Binding)
		ok     bool
	}{
		"regional address with a newer revision": {func(m *model.Binding) { m.ClusterAddress = "fd70:202::1:0:5"; m.ConfigurationRevision++ }, true},
		"node address with a newer revision":     {func(m *model.Binding) { m.ConsumerAddress = "fd70:100::99"; m.ConfigurationRevision++ }, true},
		"regional address at the same revision":  {func(m *model.Binding) { m.ClusterAddress = "fd70:202::1:0:5" }, false},
		"source context":                         {func(m *model.Binding) { m.ContextUID = "other"; m.ConfigurationRevision++ }, false},
		"listener port":                          {func(m *model.Binding) { m.Port = 5353; m.ConfigurationRevision++ }, false},
	} {
		t.Run(name, func(t *testing.T) {
			next := b
			tc.change(&next)
			moved := snapshot
			moved.Bindings = []model.Binding{next}
			_, err := advanceBindingFences(prior, moved)
			if (err == nil) != tc.ok {
				t.Fatalf("ok = %v, want %v: %v", err == nil, tc.ok, err)
			}
		})
	}
}

func TestAFenceFromBeforePinnedIdentityAcceptsAMove(t *testing.T) {
	now := time.Now().UTC()
	snapshot := testSnapshot(now)
	b := snapshot.Bindings[0]
	legacy := bindingFenceFor(b)
	legacy.Pinned = ""
	next := b
	next.ClusterAddress = "fd70:202::1:0:5"
	next.ConfigurationRevision++
	moved := snapshot
	moved.Bindings = []model.Binding{next}
	fences, err := advanceBindingFences(map[string]bindingFence{b.BindingUID: legacy}, moved)
	if err != nil {
		t.Fatalf("a move against a fence written before pinned identity was refused: %v", err)
	}
	if fences[b.BindingUID].Pinned == "" {
		t.Fatal("the advanced fence did not record its pinned identity")
	}
	moved.Bindings[0] = b
	if _, err := advanceBindingFences(map[string]bindingFence{b.BindingUID: legacy}, moved); err != nil {
		t.Fatalf("an unchanged binding against an old fence was refused: %v", err)
	}
}

// SPDX-License-Identifier: AGPL-3.0-only

package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dnsv1alpha1 "go.miloapis.com/dns/api/v1alpha1"
	"go.miloapis.com/dns/internal/internaldns/model"
)

type bindingFixture struct {
	t        *testing.T
	now      time.Time
	client   client.Client
	sink     *AckSink
	member   model.Member
	binding  client.ObjectKey
	revision uint64
}

func newBindingFixture(t *testing.T, member model.Member) *bindingFixture {
	t.Helper()
	binding := &dnsv1alpha1.DNSResolverBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "binding-a", Namespace: "platform", UID: "binding-uid", Labels: map[string]string{"internal-dns.miloapis.com/region": "east", "internal-dns.miloapis.com/shard": "shared"}},
		Spec:       dnsv1alpha1.DNSResolverBindingSpec{Configuration: dnsv1alpha1.DNSResolverBindingConfiguration{Generation: 1, Revision: 2}, Version: dnsv1alpha1.DNSResolverAccessVersion{Epoch: 1, Sequence: 1}},
	}
	f := &bindingFixture{t: t, now: time.Date(2026, 11, 21, 21, 0, 0, 0, time.UTC), member: member, binding: client.ObjectKeyFromObject(binding)}
	f.client = testClient(t, binding)
	f.commit(7)
	f.sink = &AckSink{Client: f.client, Namespace: "platform", Region: "east", Shard: "shared",
		Members: []model.Member{member}, Now: func() time.Time { return f.now }}
	return f
}

// commit makes revision the shard's committed serving snapshot, with the
// binding's configuration unchanged.
func (f *bindingFixture) commit(revision uint64) {
	f.t.Helper()
	ctx := context.Background()
	snapshot := model.ServingSnapshot{Region: "east", Shard: "shared", ConfigurationEpoch: 1, ConfigurationRevision: revision, GeneratedAt: f.now,
		Bindings: []model.Binding{{BindingUID: "binding-uid", BindingGeneration: 1, ConfigurationRevision: 2, Version: model.AccessVersion{Epoch: 1, Sequence: 1}}}}
	envelope, err := model.NewEnvelope(model.KindServingSnapshot, fmt.Sprintf("serving-1-%d", revision), "east", "shared", "owner-uid", 1, revision, f.now, snapshot)
	if err != nil {
		f.t.Fatal(err)
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		f.t.Fatal(err)
	}
	out := &dnsv1alpha1.DNSTransportOutbox{ObjectMeta: metav1.ObjectMeta{Name: envelope.EventID, Namespace: "platform"}, Spec: dnsv1alpha1.DNSTransportOutboxSpec{WriterEpoch: 1, Revision: int64(revision), Payload: payload, PayloadHash: model.Hash(payload)}}
	if err := f.client.Create(ctx, out); err != nil {
		f.t.Fatal(err)
	}
	state, err := json.Marshal(shardState{Epoch: 1, ActiveOutbox: out.Name})
	if err != nil {
		f.t.Fatal(err)
	}
	var owner corev1.ConfigMap
	key := client.ObjectKey{Namespace: "platform", Name: "dns-shard-owner-" + model.OpaqueToken("east/shared")}
	if err := f.client.Get(ctx, key, &owner); apierrors.IsNotFound(err) {
		owner = corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}, Data: map[string]string{"state": string(state)}}
		err = f.client.Create(ctx, &owner)
		if err != nil {
			f.t.Fatal(err)
		}
	} else if err != nil {
		f.t.Fatal(err)
	} else {
		owner.Data["state"] = string(state)
		if err := f.client.Update(ctx, &owner); err != nil {
			f.t.Fatal(err)
		}
	}
	f.revision = revision
}

func (f *bindingFixture) ack(phase model.AckPhase, detail string) {
	f.t.Helper()
	a := model.MemberAck{MemberID: f.member.MemberID, ReplicaID: f.member.ReplicaID, ResourceUID: "binding-uid", Kind: model.KindServingSnapshot, Epoch: 1, Revision: f.revision, Phase: phase, Error: detail, ObservedAt: f.now, ValidUntil: f.now.Add(90 * time.Second)}
	env, err := model.NewEnvelope(model.KindMemberAck, "ack-"+f.now.String(), "east", "shared", "binding-uid", 1, f.revision, f.now, a)
	if err != nil {
		f.t.Fatal(err)
	}
	if err := f.sink.Apply(context.Background(), model.AckSubject("east", "shared", f.member.MemberID), env); err != nil {
		f.t.Fatal(err)
	}
}

func (f *bindingFixture) get() dnsv1alpha1.DNSResolverBinding {
	f.t.Helper()
	var b dnsv1alpha1.DNSResolverBinding
	if err := f.client.Get(context.Background(), f.binding, &b); err != nil {
		f.t.Fatal(err)
	}
	return b
}

func (f *bindingFixture) lease() string {
	f.t.Helper()
	var lease corev1.ConfigMap
	if err := f.client.Get(context.Background(), client.ObjectKey{Namespace: "platform", Name: memberLeaseName("east", "shared", f.member.MemberID)}, &lease); err != nil {
		f.t.Fatal(err)
	}
	return lease.Data[memberLeaseKey]
}

func TestServingRenewalRenewsTheMemberLeaseNotTheBinding(t *testing.T) {
	f := newBindingFixture(t, model.Member{MemberID: "node-0", ReplicaID: "node-0", Role: "node"})
	f.ack(model.AckVerified, "")
	first := f.get()
	if first.Status.Phase != dnsValueServing {
		t.Fatalf("first verification did not serve: %#v", first.Status)
	}

	for range 4 {
		f.now = f.now.Add(30 * time.Second)
		f.ack(model.AckVerified, "")
	}
	if renewed := f.get(); renewed.ResourceVersion != first.ResourceVersion {
		t.Fatalf("a renewal rewrote the binding status: version %s -> %s", first.ResourceVersion, renewed.ResourceVersion)
	}
	if got, want := f.lease(), f.now.Add(90*time.Second).Format(time.RFC3339Nano); got != want {
		t.Fatalf("node member lease = %q, want %q", got, want)
	}

	f.now = f.now.Add(memberRowLease - memberRowRenewBefore)
	f.ack(model.AckVerified, "")
	got := f.get()
	if got.ResourceVersion == first.ResourceVersion || !got.Status.MemberAcknowledgements[0].ValidUntil.Time.Equal(f.now.Add(memberRowLease)) {
		t.Fatalf("a row near lapsing was not renewed: %#v", got.Status.MemberAcknowledgements)
	}
}

func TestServingChangeIsRecordedAtOnce(t *testing.T) {
	for _, tc := range []struct {
		name   string
		setup  func(*bindingFixture)
		change func(*bindingFixture)
		want   func(dnsv1alpha1.DNSApplyAcknowledgement) bool
	}{
		{
			name:   "phase",
			change: func(f *bindingFixture) { f.ack(model.AckRejected, "bad zone") },
			want: func(r dnsv1alpha1.DNSApplyAcknowledgement) bool {
				return r.Phase == string(model.AckRejected)
			},
		},
		{
			name:   "error",
			setup:  func(f *bindingFixture) { f.ack(model.AckRejected, "bad zone") },
			change: func(f *bindingFixture) { f.ack(model.AckRejected, "bad address") },
			want: func(r dnsv1alpha1.DNSApplyAcknowledgement) bool {
				return r.Error == "bad address"
			},
		},
		{
			name: "snapshot fence",
			change: func(f *bindingFixture) {
				f.commit(8)
				f.ack(model.AckVerified, "")
			},
			want: func(r dnsv1alpha1.DNSApplyAcknowledgement) bool {
				return r.SnapshotRevision == 8 && r.Phase == string(model.AckVerified)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newBindingFixture(t, model.Member{MemberID: "regional-0", ReplicaID: "regional-0", Role: "regional"})
			f.ack(model.AckVerified, "")
			if tc.setup != nil {
				f.now = f.now.Add(time.Second)
				tc.setup(f)
			}
			before := f.get()
			f.now = f.now.Add(time.Second)
			tc.change(f)
			got := f.get()
			if got.ResourceVersion == before.ResourceVersion || len(got.Status.MemberAcknowledgements) != 1 || !tc.want(got.Status.MemberAcknowledgements[0]) {
				t.Fatalf("the change was not recorded at once: %#v", got.Status.MemberAcknowledgements)
			}
		})
	}
}

func TestLapsedMemberLeaseMakesItsBindingsStale(t *testing.T) {
	for _, member := range []model.Member{
		{MemberID: "node-0", ReplicaID: "node-0", Role: "node"},
		{MemberID: "regional-0", ReplicaID: "regional-0", Role: "regional"},
	} {
		t.Run(member.Role, func(t *testing.T) {
			f := newBindingFixture(t, member)
			f.ack(model.AckVerified, "")
			f.now = f.now.Add(91 * time.Second)
			if err := f.sink.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
			b := f.get()
			if b.Status.Phase == dnsValueServing {
				t.Fatal("a member that stopped renewing its lease still serves the binding")
			}
			if !b.Status.MemberAcknowledgements[0].ValidUntil.After(f.now) {
				t.Fatal("the staleness must come from the member lease, not the row")
			}

			f.now = f.now.Add(time.Second)
			f.ack(model.AckVerified, "")
			if got := f.get(); got.Status.Phase != dnsValueServing {
				t.Fatalf("a member renewing again did not restore serving: %#v", got.Status)
			}
		})
	}
}

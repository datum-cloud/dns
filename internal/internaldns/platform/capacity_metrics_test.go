// SPDX-License-Identifier: AGPL-3.0-only

package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	dnsv1alpha1 "go.miloapis.com/dns/api/v1alpha1"
	"go.miloapis.com/dns/internal/internaldns/model"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const capacityRegion = "east"

func capacityPlanner(t *testing.T, cfg PlannerConfig, objects ...client.Object) *Planner {
	t.Helper()
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	cfg.Namespace, cfg.Region, cfg.Identity = "platform", capacityRegion, "planner"
	cfg.ClusterBackends = []model.Backend{{MemberID: "cluster-1", Address: "fd00::20", Port: 5300}}
	state, _ := json.Marshal(shardState{Holder: cfg.Identity, Epoch: 1, NextRevision: 1, LeaseUntil: now.Add(time.Minute)})
	owner := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "dns-shard-owner-" + model.OpaqueToken(cfg.Region+"/"+cfg.Shard), Namespace: cfg.Namespace, UID: "shard-owner-uid"}, Data: map[string]string{"state": string(state)}}
	cl := testClient(t, append([]client.Object{owner}, objects...)...)
	return &Planner{Client: cl, Allocator: &Allocator{Client: cl, Namespace: cfg.Namespace}, Config: cfg, Now: func() time.Time { return now }}
}

func shardBinding(shard string, index int) *dnsv1alpha1.DNSResolverBinding {
	vpc := types.UID(fmt.Sprintf("vpc-%d", index))
	return &dnsv1alpha1.DNSResolverBinding{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("binding-%d", index), Namespace: "platform", UID: types.UID(fmt.Sprintf("binding-uid-%d", index)), Labels: map[string]string{"internal-dns.miloapis.com/region": capacityRegion, "internal-dns.miloapis.com/shard": shard}},
		Spec: dnsv1alpha1.DNSResolverBindingSpec{
			Source:        dnsv1alpha1.DNSResolverBindingSource{ProjectUID: "project", ResolverContextRef: dnsv1alpha1.DNSObjectReference{Name: string(vpc), UID: vpc}},
			Placement:     dnsv1alpha1.DNSResolverBindingPlacement{Region: capacityRegion, Shard: shard},
			Configuration: dnsv1alpha1.DNSResolverBindingConfiguration{Generation: 1, Revision: 1, Query: dnsv1alpha1.DNSResolverBindingQuery{NodeAddress: fmt.Sprintf("fd53::%d", index), RegionalAddress: fmt.Sprintf("fd54::%d", index)}},
			Version:       dnsv1alpha1.DNSResolverAccessVersion{Epoch: 1, Sequence: 1},
		},
	}
}

func publishedEnvelope(t *testing.T, p *Planner) []byte {
	t.Helper()
	ctx := context.Background()
	var owner corev1.ConfigMap
	if err := p.Client.Get(ctx, p.ownerKey(), &owner); err != nil {
		t.Fatal(err)
	}
	var state shardState
	if err := json.Unmarshal([]byte(owner.Data["state"]), &state); err != nil {
		t.Fatal(err)
	}
	var out dnsv1alpha1.DNSTransportOutbox
	if err := p.Client.Get(ctx, client.ObjectKey{Namespace: p.Config.Namespace, Name: state.ActiveOutbox}, &out); err != nil {
		t.Fatal(err)
	}
	return out.Spec.Payload
}

// shardMetric reads one of the shard's series from the registry that the
// control plane serves on /metrics. labels adds name and value pairs.
func shardMetric(t *testing.T, name, shard string, labels ...string) (float64, bool) {
	t.Helper()
	want := map[string]string{"region": capacityRegion, "shard": shard}
	for i := 0; i+1 < len(labels); i += 2 {
		want[labels[i]] = labels[i+1]
	}
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, m := range family.GetMetric() {
			if hasLabels(m, want) {
				return metricValue(m), true
			}
		}
	}
	return 0, false
}

func hasLabels(m *dto.Metric, want map[string]string) bool {
	if len(m.GetLabel()) != len(want) {
		return false
	}
	for _, label := range m.GetLabel() {
		if want[label.GetName()] != label.GetValue() {
			return false
		}
	}
	return true
}

func metricValue(m *dto.Metric) float64 {
	if m.GetCounter() != nil {
		return m.GetCounter().GetValue()
	}
	return m.GetGauge().GetValue()
}

func requireShardMetric(t *testing.T, name, shard string, labels ...string) float64 {
	t.Helper()
	got, ok := shardMetric(t, name, shard, labels...)
	if !ok {
		series := name
		if len(labels) > 0 {
			series = fmt.Sprintf("%s%v", name, labels)
		}
		t.Fatalf("shard %s exports no %s", shard, series)
	}
	return got
}

func TestPlannerExportsTheServingEnvelopeSizeAtEachPublish(t *testing.T) {
	ctx := context.Background()
	shard := "envelope-size"
	p := capacityPlanner(t, PlannerConfig{Shard: shard}, shardBinding(shard, 1))
	if err := p.Step(ctx); err != nil {
		t.Fatal(err)
	}
	first := len(publishedEnvelope(t, p))
	if got := requireShardMetric(t, "internal_dns_serving_envelope_bytes", shard); got != float64(first) {
		t.Fatalf("envelope size %v, the published envelope has %d bytes", got, first)
	}
	if err := p.Client.Create(ctx, shardBinding(shard, 2)); err != nil {
		t.Fatal(err)
	}
	if err := p.Step(ctx); err != nil {
		t.Fatal(err)
	}
	second := len(publishedEnvelope(t, p))
	if second <= first {
		t.Fatalf("a second binding did not grow the envelope: %d, then %d bytes", first, second)
	}
	if got := requireShardMetric(t, "internal_dns_serving_envelope_bytes", shard); got != float64(second) {
		t.Fatalf("envelope size %v after the next publish, the published envelope has %d bytes", got, second)
	}
}

func TestPlannerExportsTheServingEnvelopeBudget(t *testing.T) {
	for _, tc := range []struct {
		shard      string
		configured int
		want       float64
	}{
		{shard: "envelope-budget", configured: 65536, want: 65536},
		{shard: "envelope-budget-default", want: 512 * 1024},
	} {
		t.Run(tc.shard, func(t *testing.T) {
			p := capacityPlanner(t, PlannerConfig{Shard: tc.shard, MaxSnapshotBytes: tc.configured}, shardBinding(tc.shard, 1))
			if err := p.Step(context.Background()); err != nil {
				t.Fatal(err)
			}
			if got := requireShardMetric(t, "internal_dns_serving_envelope_budget_bytes", tc.shard); got != tc.want {
				t.Fatalf("envelope budget %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPlannerExportsTheServingBindingCount(t *testing.T) {
	shard := "binding-count"
	withdrawn := shardBinding(shard, 3)
	withdrawn.Spec.Tombstone = true
	p := capacityPlanner(t, PlannerConfig{Shard: shard}, shardBinding(shard, 1), shardBinding(shard, 2), withdrawn)
	if err := p.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := requireShardMetric(t, "internal_dns_serving_bindings", shard); got != 3 {
		t.Fatalf("binding count %v, want 3: two bindings and a withdrawal, each a place under maxBindings", got)
	}
}

func TestPlannerExportsTheServingBindingBudget(t *testing.T) {
	for _, tc := range []struct {
		shard      string
		configured int
		want       float64
	}{
		{shard: "binding-budget", configured: 200, want: 200},
		{shard: "binding-budget-default", want: 256},
	} {
		t.Run(tc.shard, func(t *testing.T) {
			p := capacityPlanner(t, PlannerConfig{Shard: tc.shard, MaxBindings: tc.configured}, shardBinding(tc.shard, 1))
			if err := p.Step(context.Background()); err != nil {
				t.Fatal(err)
			}
			if got := requireShardMetric(t, "internal_dns_serving_bindings_budget", tc.shard); got != tc.want {
				t.Fatalf("binding budget %v, want %v", got, tc.want)
			}
		})
	}
}

// requirePublishRefusals requires each refusal reason's count, a reason
// missing from want at zero.
func requirePublishRefusals(t *testing.T, shard string, want map[string]float64) {
	t.Helper()
	for _, reason := range []string{"max_snapshot_bytes", "established_binding"} {
		if got := requireShardMetric(t, "internal_dns_serving_publish_refusals_total", shard, "reason", reason); got != want[reason] {
			t.Fatalf("shard %s: %s refusals %v, want %v", shard, reason, got, want[reason])
		}
	}
}

func TestPlannerCountsNoRefusedPublishBeforeTheFirst(t *testing.T) {
	shard := "refused-none"
	p := capacityPlanner(t, PlannerConfig{Shard: shard}, shardBinding(shard, 1))
	if err := p.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	requirePublishRefusals(t, shard, nil)
}

func TestPlannerCountsAnEnvelopeOverItsBudget(t *testing.T) {
	shard := "refused-envelope"
	p := capacityPlanner(t, PlannerConfig{Shard: shard, MaxSnapshotBytes: 100})
	if err := p.Step(context.Background()); err == nil {
		t.Fatal("an envelope over its 100-byte budget was published")
	}
	requirePublishRefusals(t, shard, map[string]float64{"max_snapshot_bytes": 1})
	if got := requireShardMetric(t, "internal_dns_serving_envelope_bytes", shard); got <= 100 {
		t.Fatalf("envelope size %v for an envelope refused over its 100-byte budget", got)
	}
}

func TestPlannerCountsAServedBindingThatNoLongerFits(t *testing.T) {
	ctx := context.Background()
	shard := "refused-established"
	p := capacityPlanner(t, PlannerConfig{Shard: shard}, shardBinding(shard, 1), shardBinding(shard, 2))
	if err := p.Step(ctx); err != nil {
		t.Fatal(err)
	}
	p.Config.MaxBindings = 1
	if err := p.Step(ctx); err == nil {
		t.Fatal("a snapshot that drops a served binding was published")
	}
	requirePublishRefusals(t, shard, map[string]float64{"established_binding": 1})
}

// requireBindingRefusals requires each refusal reason's count, a reason
// missing from want at zero.
func requireBindingRefusals(t *testing.T, shard string, want map[string]float64) {
	t.Helper()
	for _, reason := range []string{"max_bindings", "max_snapshot_bytes", "established_binding"} {
		if got := requireShardMetric(t, "internal_dns_serving_binding_refusals_total", shard, "reason", reason); got != want[reason] {
			t.Fatalf("shard %s: %s binding refusals %v, want %v", shard, reason, got, want[reason])
		}
	}
}

func TestPlannerCountsABindingOverTheBindingBudgetAtEachPass(t *testing.T) {
	ctx := context.Background()
	shard := "refused-binding-count"
	p := capacityPlanner(t, PlannerConfig{Shard: shard, MaxBindings: 2}, shardBinding(shard, 1), shardBinding(shard, 2), shardBinding(shard, 3))
	if err := p.Step(ctx); err != nil {
		t.Fatal(err)
	}
	requireBindingRefusals(t, shard, map[string]float64{"max_bindings": 1})
	if err := p.Step(ctx); err != nil {
		t.Fatal(err)
	}
	requireBindingRefusals(t, shard, map[string]float64{"max_bindings": 2})
}

func TestPlannerCountsABindingOverTheByteBudget(t *testing.T) {
	shard := "refused-binding-bytes"
	large := shardBinding(shard, 2)
	large.Spec.Configuration.ZoneApices = map[string]string{}
	for i := range 64 {
		zone := types.UID(fmt.Sprintf("zone-%d", i))
		large.Spec.Configuration.ZoneRefs = append(large.Spec.Configuration.ZoneRefs, dnsv1alpha1.DNSObjectReference{Name: string(zone), UID: zone})
		large.Spec.Configuration.ZoneApices[string(zone)] = fmt.Sprintf("z%d.internal", i)
	}
	p := capacityPlanner(t, PlannerConfig{Shard: shard, MaxSnapshotBytes: 4096}, shardBinding(shard, 1), large)
	if err := p.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireBindingRefusals(t, shard, map[string]float64{"max_snapshot_bytes": 1})
}

func TestPlannerCountsTheNewBindingsOfARefusedSnapshot(t *testing.T) {
	ctx := context.Background()
	shard := "refused-binding-established"
	p := capacityPlanner(t, PlannerConfig{Shard: shard}, shardBinding(shard, 1), shardBinding(shard, 2))
	if err := p.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.Client.Create(ctx, shardBinding(shard, 3)); err != nil {
		t.Fatal(err)
	}
	p.Config.MaxBindings = 1
	if err := p.Step(ctx); err == nil {
		t.Fatal("a snapshot that drops a served binding was published")
	}
	requireBindingRefusals(t, shard, map[string]float64{"max_bindings": 1, "established_binding": 1})
}

func TestPlannerStopsExportingAShardAnotherProcessHolds(t *testing.T) {
	ctx := context.Background()
	shard := "held-elsewhere"
	gauges := []string{"internal_dns_serving_envelope_bytes", "internal_dns_serving_envelope_budget_bytes", "internal_dns_serving_bindings", "internal_dns_serving_bindings_budget"}
	p := capacityPlanner(t, PlannerConfig{Shard: shard}, shardBinding(shard, 1))
	if err := p.Step(ctx); err != nil {
		t.Fatal(err)
	}
	for _, name := range gauges {
		requireShardMetric(t, name, shard)
	}
	var owner corev1.ConfigMap
	if err := p.Client.Get(ctx, p.ownerKey(), &owner); err != nil {
		t.Fatal(err)
	}
	state, _ := json.Marshal(shardState{Holder: "another-process", Epoch: 2, NextRevision: 2, LeaseUntil: p.Now().Add(time.Minute)})
	owner.Data["state"] = string(state)
	if err := p.Client.Update(ctx, &owner); err != nil {
		t.Fatal(err)
	}
	if err := p.Step(ctx); err != nil {
		t.Fatal(err)
	}
	for _, name := range gauges {
		if got, ok := shardMetric(t, name, shard); ok {
			t.Fatalf("shard %s is held by another process, and this one still exports %s %v", shard, name, got)
		}
	}
}

func requirePublishedSize(t *testing.T, p *Planner, bindings float64) {
	t.Helper()
	shard := p.Config.Shard
	if got, want := requireShardMetric(t, "internal_dns_serving_envelope_bytes", shard), len(publishedEnvelope(t, p)); got != float64(want) {
		t.Fatalf("envelope size %v, the published envelope has %d bytes", got, want)
	}
	if got := requireShardMetric(t, "internal_dns_serving_bindings", shard); got != bindings {
		t.Fatalf("binding count %v, want %v", got, bindings)
	}
}

// A restarted process with a configured identity holds its shard under the
// same identity, and starts with no series: forget stands in for the restart.
func TestPlannerRestartedOnAnUnchangedShardExportsItsSize(t *testing.T) {
	ctx := context.Background()
	shard := "restart-unchanged"
	p := capacityPlanner(t, PlannerConfig{Shard: shard}, shardBinding(shard, 1))
	if err := p.Step(ctx); err != nil {
		t.Fatal(err)
	}
	shardCapacity{region: capacityRegion, shard: shard}.forget()
	restarted := &Planner{Client: p.Client, Allocator: p.Allocator, Config: p.Config, Now: p.Now}
	if err := restarted.Step(ctx); err != nil {
		t.Fatal(err)
	}
	requirePublishedSize(t, restarted, 1)
}

func TestPlannerTakingOverAnUnchangedShardExportsItsSize(t *testing.T) {
	ctx := context.Background()
	shard := "handover-unchanged"
	p := capacityPlanner(t, PlannerConfig{Shard: shard}, shardBinding(shard, 1))
	if err := p.Step(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Release(ctx, p.Now()); err != nil {
		t.Fatal(err)
	}
	shardCapacity{region: capacityRegion, shard: shard}.forget()
	next := &Planner{Client: p.Client, Allocator: p.Allocator, Config: p.Config, Now: p.Now}
	next.Config.Identity = "next-process"
	if err := next.Step(ctx); err != nil {
		t.Fatal(err)
	}
	requirePublishedSize(t, next, 1)
}

var shardGauges = []string{"internal_dns_serving_envelope_bytes", "internal_dns_serving_envelope_budget_bytes", "internal_dns_serving_bindings", "internal_dns_serving_bindings_budget"}

func requireShardGauges(t *testing.T, shard string) {
	t.Helper()
	for _, name := range shardGauges {
		requireShardMetric(t, name, shard)
	}
}

// clockedPlanner is capacityPlanner with a clock that the test moves.
func clockedPlanner(t *testing.T, cfg PlannerConfig, objects ...client.Object) (*Planner, *time.Time) {
	t.Helper()
	p := capacityPlanner(t, cfg, objects...)
	clock := p.Now()
	p.Now = func() time.Time { return clock }
	return p, &clock
}

// failReads fails each Get of p's client while the flag it returns is set, as
// a publication store that p cannot read.
func failReads(p *Planner) *bool {
	down := new(bool)
	p.Client = interceptor.NewClient(p.Client.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if *down {
			return errors.New("publication store unavailable")
		}
		return c.Get(ctx, key, obj, opts...)
	}})
	return down
}

func TestPlannerStopsExportingAShardItHasNotMeasuredForALease(t *testing.T) {
	ctx := context.Background()
	shard := "unmeasured-for-a-lease"
	lease := 2 * time.Minute
	p, clock := clockedPlanner(t, PlannerConfig{Shard: shard, LeaseDuration: lease}, shardBinding(shard, 1))
	down := failReads(p)
	if err := p.Step(ctx); err != nil {
		t.Fatal(err)
	}
	requireShardGauges(t, shard)
	measured := *clock
	*down = true
	// Passes start at most 10 s apart, so a failed pass must not count as a
	// measurement: the gauges go at the first pass one lease on, and stay gone.
	for end := measured.Add(2 * lease); clock.Before(end); {
		*clock = clock.Add(10 * time.Second)
		if err := p.Step(ctx); err == nil {
			t.Fatal("a pass that could not read the shard's lease succeeded")
		}
		if clock.Sub(measured) < lease {
			continue
		}
		for _, name := range shardGauges {
			if got, ok := shardMetric(t, name, shard); ok {
				t.Fatalf("shard %s was last measured %v ago, at passes 10 s apart, and this process still exports %s %v", shard, clock.Sub(measured), name, got)
			}
		}
	}
}

func TestPlannerKeepsExportingAnUnchangedShardForLessThanALeaseOfFailedReads(t *testing.T) {
	ctx := context.Background()
	shard := "measured-within-a-lease"
	lease := 2 * time.Minute
	p, clock := clockedPlanner(t, PlannerConfig{Shard: shard, LeaseDuration: lease}, shardBinding(shard, 1))
	down := failReads(p)
	if err := p.Step(ctx); err != nil {
		t.Fatal(err)
	}
	*clock = clock.Add(10 * time.Second)
	if err := p.Step(ctx); err != nil {
		t.Fatal(err)
	}
	*down = true
	*clock = clock.Add(lease - time.Second)
	if err := p.Step(ctx); err == nil {
		t.Fatal("a pass that could not read the shard's lease succeeded")
	}
	requireShardGauges(t, shard)
}

func TestPlannerExportsAShardAgainWhenItMeasuresItAfterAnExpiry(t *testing.T) {
	ctx := context.Background()
	shard := "measured-after-an-expiry"
	lease := 2 * time.Minute
	p, clock := clockedPlanner(t, PlannerConfig{Shard: shard, LeaseDuration: lease}, shardBinding(shard, 1))
	down := failReads(p)
	if err := p.Step(ctx); err != nil {
		t.Fatal(err)
	}
	*down = true
	*clock = clock.Add(lease)
	if err := p.Step(ctx); err == nil {
		t.Fatal("a pass that could not read the shard's lease succeeded")
	}
	*down = false
	*clock = clock.Add(10 * time.Second)
	if err := p.Step(ctx); err != nil {
		t.Fatal(err)
	}
	requireShardGauges(t, shard)
	requirePublishedSize(t, p, 1)
}

func TestPlannerThatPlansEachPassKeepsItsGaugesThroughManyLeases(t *testing.T) {
	ctx := context.Background()
	shard := "planned-for-many-leases"
	lease := 2 * time.Minute
	p, clock := clockedPlanner(t, PlannerConfig{Shard: shard, LeaseDuration: lease}, shardBinding(shard, 1))
	for end := clock.Add(3 * lease); !clock.After(end); *clock = clock.Add(10 * time.Second) {
		if err := p.Step(ctx); err != nil {
			t.Fatal(err)
		}
	}
	requireShardGauges(t, shard)
	requirePublishedSize(t, p, 1)
}

func TestPlannerOverItsByteBudgetExportsItsEnvelopeAfterALeaseOfRefusalsOnOneReplica(t *testing.T) {
	ctx := context.Background()
	shard := "refused-envelope-one-replica"
	lease := 2 * time.Minute
	p, clock := clockedPlanner(t, PlannerConfig{Shard: shard, LeaseDuration: lease, MaxSnapshotBytes: 100})
	for end := clock.Add(lease + 10*time.Second); !clock.After(end); *clock = clock.Add(10 * time.Second) {
		if err := p.Step(ctx); err == nil {
			t.Fatal("an envelope over its 100-byte budget was published")
		}
	}
	if got := requireShardMetric(t, "internal_dns_serving_envelope_bytes", shard); got <= 100 {
		t.Fatalf("envelope size %v after a lease of passes refused over a 100-byte budget", got)
	}
}

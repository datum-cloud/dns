// SPDX-License-Identifier: AGPL-3.0-only

package platform

import (
	"context"
	"encoding/json"
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

// SPDX-License-Identifier: AGPL-3.0-only

package platform

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var servingEnvelopeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "internal_dns_serving_envelope_bytes",
	Help: "Size of the serving envelope the planner last encoded for the shard.",
}, []string{"region", "shard"})

var servingEnvelopeBudgetBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "internal_dns_serving_envelope_budget_bytes",
	Help: "The shard's maxSnapshotBytes: the planner publishes no serving envelope larger than this.",
}, []string{"region", "shard"})

var servingBindings = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "internal_dns_serving_bindings",
	Help: "Bindings in the serving snapshot the planner last encoded for the shard, withdrawals included, as maxBindings counts them.",
}, []string{"region", "shard"})

var servingBindingsBudget = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "internal_dns_serving_bindings_budget",
	Help: "The shard's maxBindings: the planner places no more bindings than this in its serving snapshot.",
}, []string{"region", "shard"})

var servingPublishRefusals = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "internal_dns_serving_publish_refusals_total",
	Help: "Planner passes that published no serving snapshot for the shard, by reason: max_snapshot_bytes when the envelope is over its budget, established_binding when a binding the shard serves no longer fits. The planner retries at each pass, so a refusal that lasts counts at each pass.",
}, []string{"region", "shard", "reason"})

var servingBindingRefusals = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "internal_dns_serving_binding_refusals_total",
	Help: "Bindings the planner left out of the shard's serving snapshot, by reason: max_bindings, max_snapshot_bytes, or established_binding when the whole snapshot is refused. Each pass counts each binding it leaves out, so a refusal that lasts counts at each pass.",
}, []string{"region", "shard", "reason"})

func init() {
	metrics.Registry.MustRegister(servingEnvelopeBytes, servingEnvelopeBudgetBytes, servingBindings, servingBindingsBudget, servingPublishRefusals, servingBindingRefusals)
}

type shardCapacity struct{ region, shard string }

// budgets also exports each refusal reason at zero, so that increase() sees a
// shard's first refusal.
func (s shardCapacity) budgets(maxBindings, maxBytes int) {
	servingBindingsBudget.WithLabelValues(s.region, s.shard).Set(float64(maxBindings))
	servingEnvelopeBudgetBytes.WithLabelValues(s.region, s.shard).Set(float64(maxBytes))
	for _, reason := range []string{refusedMaxSnapshotBytes, refusedEstablishedBinding} {
		servingPublishRefusals.WithLabelValues(s.region, s.shard, reason)
	}
	for _, reason := range []string{refusedMaxBindings, refusedMaxSnapshotBytes, refusedEstablishedBinding} {
		servingBindingRefusals.WithLabelValues(s.region, s.shard, reason)
	}
}

func (s shardCapacity) encoded(envelopeBytes, bindings int) {
	servingEnvelopeBytes.WithLabelValues(s.region, s.shard).Set(float64(envelopeBytes))
	servingBindings.WithLabelValues(s.region, s.shard).Set(float64(bindings))
}

func (s shardCapacity) refusedPublish(reason string) {
	servingPublishRefusals.WithLabelValues(s.region, s.shard, reason).Inc()
}

func (s shardCapacity) fitted(rejected map[string]string, err error) {
	for _, reason := range rejected {
		servingBindingRefusals.WithLabelValues(s.region, s.shard, reason).Inc()
	}
	if errors.Is(err, errEstablishedBindingDoesNotFit) {
		s.refusedPublish(refusedEstablishedBinding)
	}
}

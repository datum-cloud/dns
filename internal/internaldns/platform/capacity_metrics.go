// SPDX-License-Identifier: AGPL-3.0-only

package platform

import (
	"errors"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	regionLabel = "region"
	shardLabel  = "shard"
)

var (
	shardLabels   = []string{regionLabel, shardLabel}
	refusalLabels = []string{regionLabel, shardLabel, "reason"}
)

var servingEnvelopeBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "internal_dns_serving_envelope_bytes",
	Help: "Size of the serving envelope the planner last encoded for the shard, or of the active one while the shard's content is unchanged.",
}, shardLabels)

var servingEnvelopeBudgetBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "internal_dns_serving_envelope_budget_bytes",
	Help: "The shard's maxSnapshotBytes: the planner publishes no serving envelope larger than this.",
}, shardLabels)

var servingBindings = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "internal_dns_serving_bindings",
	Help: "Bindings in the serving snapshot that internal_dns_serving_envelope_bytes measures, withdrawals included, as maxBindings counts them.",
}, shardLabels)

var servingBindingsBudget = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "internal_dns_serving_bindings_budget",
	Help: "The shard's maxBindings: the planner places no more bindings than this in its serving snapshot.",
}, shardLabels)

var servingPublishRefusals = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "internal_dns_serving_publish_refusals_total",
	Help: "Planner passes that published no serving snapshot for the shard, by reason: " + strings.Join(publishRefusalReasons, ", ") + ". The planner retries at each pass, so a refusal that lasts counts at each pass.",
}, refusalLabels)

var servingBindingRefusals = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "internal_dns_serving_binding_refusals_total",
	Help: "Bindings the planner left out of the shard's serving snapshot, by reason: " + strings.Join(bindingRefusalReasons, ", ") + ". Each pass counts each binding it leaves out, so a refusal that lasts counts at each pass.",
}, refusalLabels)

func init() {
	metrics.Registry.MustRegister(servingEnvelopeBytes, servingEnvelopeBudgetBytes, servingBindings, servingBindingsBudget, servingPublishRefusals, servingBindingRefusals)
}

type shardCapacity struct{ region, shard string }

// budgets also exports each refusal reason at zero, so that increase() sees a
// shard's first refusal.
func (s shardCapacity) budgets(maxBindings, maxBytes int) {
	servingBindingsBudget.WithLabelValues(s.region, s.shard).Set(float64(maxBindings))
	servingEnvelopeBudgetBytes.WithLabelValues(s.region, s.shard).Set(float64(maxBytes))
	for _, reason := range publishRefusalReasons {
		servingPublishRefusals.WithLabelValues(s.region, s.shard, reason)
	}
	for _, reason := range bindingRefusalReasons {
		servingBindingRefusals.WithLabelValues(s.region, s.shard, reason)
	}
}

// forget removes the shard's gauges from a process that no longer plans
// it, so that only the shard's holder exports its size.
func (s shardCapacity) forget() {
	for _, gauge := range []*prometheus.GaugeVec{servingEnvelopeBytes, servingEnvelopeBudgetBytes, servingBindings, servingBindingsBudget} {
		gauge.DeleteLabelValues(s.region, s.shard)
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

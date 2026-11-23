// SPDX-License-Identifier: AGPL-3.0-only

package platform

import (
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

func init() {
	metrics.Registry.MustRegister(servingEnvelopeBytes, servingEnvelopeBudgetBytes, servingBindings)
}

// SPDX-License-Identifier: AGPL-3.0-only

package activitypolicy_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/google/cel-go/cel"
	"gopkg.in/yaml.v3"
)

// TestPolicies_DryRunIsAQueryParameter checks that only the dryRun query
// parameter makes a write a dry run. A label selector can hold the text
// "dryRun=" too, and such a write is a real change, so it is an activity.
func TestPolicies_DryRunIsAQueryParameter(t *testing.T) {
	t.Parallel()
	env, err := cel.NewEnv(cel.Variable("audit", cel.MapType(cel.StringType, cel.DynType)))
	if err != nil {
		t.Fatalf("cel env: %v", err)
	}
	recordSet := loadDNSRecordSetPolicy(t)
	zone := loadPolicy(t, "dnszone-policy.yaml")
	mergePatch := `{"spec":{"records":[{"name":"ca-central-1.edge","a":{"content":"192.0.2.10"}}]}}`

	cases := []struct {
		name, query, wantRule string
		pol                   activityPolicy
		audit                 map[string]any
	}{
		{name: "record set patch, dryRun= in a label selector", query: "?labelSelector=dryRun=false",
			pol: recordSet, audit: recordSetAuditEvent(t, "patch", mergePatch, ""), wantRule: "update-a-aaaa"},
		{name: "record set patch, dryRun as the second parameter", query: "?fieldManager=portal&dryRun=All",
			pol: recordSet, audit: recordSetAuditEvent(t, "patch", mergePatch, "")},
		{name: "zone create, dryRun= in a label selector", query: "?labelSelector=dryRun=false",
			pol: zone, audit: zoneAuditEvent("create"), wantRule: "create"},
		{name: "zone update, dryRun= in a label selector", query: "?labelSelector=dryRun=false",
			pol: zone, audit: zoneAuditEvent("update"), wantRule: "update"},
		{name: "zone delete, dryRun= in a label selector", query: "?labelSelector=dryRun=false",
			pol: zone, audit: zoneAuditEvent("delete"), wantRule: "delete"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			c.audit["requestURI"] = c.audit["requestURI"].(string) + c.query
			if matched := firstMatchingRule(t, env, c.pol, c.audit); matched != c.wantRule {
				t.Fatalf("matched rule %q, want %q", matched, c.wantRule)
			}
		})
	}
}

// zoneAuditEvent returns a successful audit event for the zone example-zone of
// the domain example.com, with the given verb.
func zoneAuditEvent(verb string) map[string]any {
	spec := map[string]any{"spec": map[string]any{"domainName": "example.com"}}
	return map[string]any{
		"verb":           verb,
		"user":           map[string]any{"username": "alice@example.com"},
		"objectRef":      map[string]any{"resource": "dnszones", "namespace": "default", "name": "example-zone"},
		"requestURI":     "/apis/dns.networking.miloapis.com/v1alpha1/namespaces/default/dnszones/example-zone",
		"responseStatus": map[string]any{"code": 200},
		"requestObject":  spec,
		"responseObject": spec,
	}
}

// loadPolicy reads the activity policy of the given file name from
// config/milo/activity/policies.
func loadPolicy(t *testing.T, name string) activityPolicy {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(file), "../../config/milo/activity/policies", name))
	if err != nil {
		t.Fatalf("read policy: %v", err)
	}
	var pol activityPolicy
	if err := yaml.Unmarshal(data, &pol); err != nil {
		t.Fatalf("unmarshal policy: %v", err)
	}
	return pol
}

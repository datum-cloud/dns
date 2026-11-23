// SPDX-License-Identifier: AGPL-3.0-only

package activitypolicy_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/cel-go/cel"
)

// TestDNSRecordSetPolicy_PatchShapes evaluates the audit rules against the
// request shapes that reach the activity processor, as the processor does: in
// order, stopping at the first rule that matches or errors. An error sends the
// event to the dead-letter queue, so it fails the test. The two JSON patches
// are the shapes that failed in production on 2026-11-21. A dry run and a
// refused request change nothing, so they are not an activity.
func TestDNSRecordSetPolicy_PatchShapes(t *testing.T) {
	t.Parallel()
	pol := loadDNSRecordSetPolicy(t)
	env, err := cel.NewEnv(cel.Variable("audit", cel.MapType(cel.StringType, cel.DynType)))
	if err != nil {
		t.Fatalf("cel env: %v", err)
	}

	added := `,"dns.networking.miloapis.com/activity-change":"added",` +
		`"dns.networking.miloapis.com/activity-name":"ca-central-1.edge.datum.net",` +
		`"dns.networking.miloapis.com/activity-value":"192.0.2.10"`
	dryRun := func(audit map[string]any) { audit["requestURI"] = audit["requestURI"].(string) + "?dryRun=All" }
	refused := func(audit map[string]any) {
		audit["responseStatus"] = map[string]any{"code": 422}
		audit["responseObject"] = map[string]any{"kind": "Status", "status": "Failure", "code": 422}
	}
	mergePatchWithSpec := `{"spec":{"records":[{"name":"ca-central-1.edge","a":{"content":"192.0.2.10"}}]}}`
	shapes := []struct {
		name, verb, request, activity, wantRule string
		change                                  func(audit map[string]any)
	}{
		{
			name: "JSON patch of managedFields only, from a move to server-side apply", verb: "patch",
			request: `[{"op":"replace","path":"/metadata/managedFields","value":[{"manager":"datum-infra-release"}]},` +
				`{"op":"replace","path":"/metadata/resourceVersion","value":"1"}]`,
		},
		{
			name: "JSON patch that adds a record", verb: "patch", activity: added, wantRule: "update-activity-added-a-aaaa",
			request: `[{"op":"test","path":"/metadata/resourceVersion","value":"1"},` +
				`{"op":"add","path":"/spec/records/-","value":{"name":"ca-central-1.edge","a":{"content":"192.0.2.10"}}}]`,
		},
		{
			name: "merge patch with a spec", verb: "patch", wantRule: "update-a-aaaa", request: mergePatchWithSpec,
		},
		{
			name: "dry-run merge patch with a spec", verb: "patch", request: mergePatchWithSpec, change: dryRun,
		},
		{
			name: "refused create", verb: "create", change: refused,
			request: `{"spec":{"recordType":"A","records":[{"name":"ca-central-1.edge","a":{"content":"192.0.2.10"}}]}}`,
		},
		{
			name: "merge patch of metadata only", verb: "patch",
			request: `{"metadata":{"labels":{"team":"x"}}}`,
		},
		{
			name: "update with the full object", verb: "update", wantRule: "update-a-aaaa",
			request: `{"metadata":{"name":"datum-net-xq088a-a"},"spec":{"recordType":"A","records":[]}}`,
		},
	}

	for _, s := range shapes {
		t.Run(s.name, func(t *testing.T) {
			t.Parallel()
			audit := recordSetAuditEvent(t, s.verb, s.request, s.activity)
			if s.change != nil {
				s.change(audit)
			}
			if matched := firstMatchingRule(t, env, pol, audit); matched != s.wantRule {
				t.Fatalf("matched rule %q, want %q", matched, s.wantRule)
			}
		})
	}
}

// recordSetAuditEvent returns a successful audit event for the A record set
// datum-net-xq088a-a, with the given verb and request body. activity adds
// response annotations to the set's display annotations.
func recordSetAuditEvent(t *testing.T, verb, request, activity string) map[string]any {
	t.Helper()
	raw := fmt.Sprintf(`{"verb":%q,"user":{"username":"jsmith@datum.net"},
 "objectRef":{"resource":"dnsrecordsets","namespace":"default","name":"datum-net-xq088a-a"},
 "requestURI":"/apis/dns.networking.miloapis.com/v1alpha1/namespaces/default/dnsrecordsets/datum-net-xq088a-a",
 "responseStatus":{"code":200},
 "requestObject":%s,
 "responseObject":{"metadata":{"name":"datum-net-xq088a-a","annotations":{
  "dns.networking.miloapis.com/display-name":"ca-central-1.edge.datum.net",
  "dns.networking.miloapis.com/display-value":"192.0.2.10"%s}},
  "spec":{"recordType":"A","records":[{"name":"ca-central-1.edge","a":{"content":"192.0.2.10"}}]}}}`,
		verb, request, activity)
	var audit map[string]any
	if err := json.Unmarshal([]byte(raw), &audit); err != nil {
		t.Fatalf("audit event: %v", err)
	}
	return audit
}

// firstMatchingRule returns the name of the first audit rule that matches,
// or "" when none does. Like the activity processor, it stops at the first
// rule that errors; the processor then sends the event to the DLQ, so the
// test fails.
func firstMatchingRule(t *testing.T, env *cel.Env, pol activityPolicy, audit map[string]any) string {
	t.Helper()
	for _, rule := range pol.Spec.AuditRules {
		ast, issues := env.Compile(rule.Match)
		if issues != nil && issues.Err() != nil {
			t.Fatalf("compile %s: %v", rule.Name, issues.Err())
		}
		prg, err := env.Program(ast)
		if err != nil {
			t.Fatalf("program %s: %v", rule.Name, err)
		}
		out, _, err := prg.Eval(map[string]any{"audit": audit})
		if err != nil {
			t.Fatalf("rule %s errors, so the processor sends the event to the DLQ: %v", rule.Name, err)
		}
		if isCELTrue(out) {
			return rule.Name
		}
	}
	return ""
}

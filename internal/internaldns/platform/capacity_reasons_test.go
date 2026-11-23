// SPDX-License-Identifier: AGPL-3.0-only

package platform

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// refusalSites holds what the package's source gives a refusal: the reasons
// fitSnapshot records for a binding, and the reasons counted for a publish,
// passed to refusedPublish or returned by publishRefusals.
type refusalSites struct {
	constants        map[string]string
	publish, binding []ast.Expr
}

func readRefusalSites(t *testing.T) refusalSites {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	sites := refusalSites{constants: map[string]string{}}
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, sites.visit)
	}
	return sites
}

func (s *refusalSites) visit(n ast.Node) bool {
	switch n := n.(type) {
	case *ast.ValueSpec:
		s.constant(n)
	case *ast.FuncDecl:
		if n.Name.Name == "publishRefusals" {
			s.publish = append(s.publish, returned(n.Body)...)
		}
	case *ast.CallExpr:
		if sel, ok := n.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "refusedPublish" {
			s.publish = append(s.publish, n.Args...)
		}
	case *ast.AssignStmt:
		s.assignment(n)
	}
	return true
}

func (s *refusalSites) constant(spec *ast.ValueSpec) {
	for i, name := range spec.Names {
		if i >= len(spec.Values) {
			return
		}
		if lit, ok := spec.Values[i].(*ast.BasicLit); ok && lit.Kind == token.STRING {
			s.constants[name.Name], _ = strconv.Unquote(lit.Value)
		}
	}
}

// assignment records a reason stored in rejected, and each value that the
// refusal closure returns.
func (s *refusalSites) assignment(stmt *ast.AssignStmt) {
	for i, lhs := range stmt.Lhs {
		if i >= len(stmt.Rhs) {
			return
		}
		switch target := lhs.(type) {
		case *ast.IndexExpr:
			if ident, ok := target.X.(*ast.Ident); ok && ident.Name == "rejected" {
				s.binding = append(s.binding, stmt.Rhs[i])
			}
		case *ast.Ident:
			if fn, ok := stmt.Rhs[i].(*ast.FuncLit); ok && target.Name == "refusal" {
				s.binding = append(s.binding, returned(fn.Body)...)
			}
		}
	}
}

func returned(body *ast.BlockStmt) []ast.Expr {
	var results []ast.Expr
	ast.Inspect(body, func(n ast.Node) bool {
		if ret, ok := n.(*ast.ReturnStmt); ok {
			results = append(results, ret.Results...)
		}
		return true
	})
	return results
}

// reasons resolves each expression to its strings: a constant, a literal, or
// a list of them. A variable, nil and the empty string, which means no
// refusal, are left out.
func (s refusalSites) reasons(exprs []ast.Expr) []string {
	var reasons []string
	for _, expr := range exprs {
		if list, ok := expr.(*ast.CompositeLit); ok {
			reasons = append(reasons, s.reasons(list.Elts)...)
			continue
		}
		if reason := s.value(expr); reason != "" {
			reasons = append(reasons, reason)
		}
	}
	return reasons
}

func (s refusalSites) value(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.BasicLit:
		reason, _ := strconv.Unquote(e.Value)
		return reason
	case *ast.Ident:
		return s.constants[e.Name]
	}
	return ""
}

func requireListed(t *testing.T, counter string, given, listed []string) {
	t.Helper()
	if len(given) == 0 {
		t.Fatalf("read no %s refusal reason in the package's source", counter)
	}
	for _, reason := range given {
		if !slices.Contains(listed, reason) {
			t.Fatalf("the planner gives the %s refusal reason %q, which its list %v does not hold: it would start at no zero and miss from the Help text", counter, reason, listed)
		}
	}
	for _, reason := range listed {
		if !slices.Contains(given, reason) {
			t.Fatalf("the %s refusal list holds %q, which the planner never gives", counter, reason)
		}
	}
}

func TestEveryRefusalReasonIsInItsCountersList(t *testing.T) {
	sites := readRefusalSites(t)
	requireListed(t, "publish", sites.reasons(sites.publish), publishRefusalReasons)
	requireListed(t, "binding", sites.reasons(sites.binding), bindingRefusalReasons)
}

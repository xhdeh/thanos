// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package rules

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/testutil"
	"gopkg.in/yaml.v2"
)

type nopAppendable struct{}

func (n nopAppendable) Add(l labels.Labels, t int64, v float64) (uint64, error)       { return 0, nil }
func (n nopAppendable) AddFast(l labels.Labels, ref uint64, t int64, v float64) error { return nil }
func (n nopAppendable) Commit() error                                                 { return nil }
func (n nopAppendable) Rollback() error                                               { return nil }
func (n nopAppendable) Appender() (storage.Appender, error)                           { return n, nil }

// Regression test against https://github.com/thanos-io/thanos/issues/1779.
func TestRun(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_rule_run")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	testutil.Ok(t, ioutil.WriteFile(filepath.Join(dir, "rule.yaml"), []byte(`
groups:
- name: "rule with subquery"
  partial_response_strategy: "warn"
  rules:
  - record: "test"
    expr: "rate(some_metric[1h:5m] offset 1d)"
`), os.ModePerm))

	var (
		queryDone = make(chan struct{})
		queryOnce sync.Once
		query     string
	)
	thanosRuleMgr := NewManager(
		context.Background(),
		nil,
		dir,
		rules.ManagerOptions{
			Logger:     log.NewLogfmtLogger(os.Stderr),
			Context:    context.Background(),
			Appendable: nopAppendable{},
		},
		func(partialResponseStrategy storepb.PartialResponseStrategy) rules.QueryFunc {
			return func(ctx context.Context, q string, t time.Time) (vectors promql.Vector, e error) {
				queryOnce.Do(func() {
					query = q
					close(queryDone)
				})
				return promql.Vector{}, nil
			}
		},
		labels.FromStrings("replica", "1"),
	)
	testutil.Ok(t, thanosRuleMgr.Update(10*time.Second, []string{filepath.Join(dir, "rule.yaml")}))

	thanosRuleMgr.Run()
	defer thanosRuleMgr.Stop()

	select {
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout while waiting on rule manager query evaluation")
	case <-queryDone:
	}

	testutil.Equals(t, "rate(some_metric[1h:5m] offset 1d)", query)
}

func TestUpdate_Error_UpdatePartial(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_rule_rule_groups")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()
	err = os.MkdirAll(filepath.Join(dir, "subdir"), 0775)
	testutil.Ok(t, err)

	testutil.Ok(t, ioutil.WriteFile(filepath.Join(dir, "no_strategy.yaml"), []byte(`
groups:
- name: "something1"
  rules:
  - alert: "some"
    expr: "up"
`), os.ModePerm))
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(dir, "abort.yaml"), []byte(`
groups:
- name: "something2"
  partial_response_strategy: "abort"
  rules:
  - alert: "some"
    expr: "up"
`), os.ModePerm))
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(dir, "warn.yaml"), []byte(`
groups:
- name: "something3"
  partial_response_strategy: "warn"
  rules:
  - alert: "some"
    expr: "up"
`), os.ModePerm))
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(dir, "wrong.yaml"), []byte(`
groups:
- name: "something4"
  partial_response_strategy: "afafsdgsdgs" # Err 1
  rules:
  - alert: "some"
    expr: "up"
`), os.ModePerm))
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(dir, "combined.yaml"), []byte(`
groups:
- name: "something5"
  partial_response_strategy: "warn"
  rules:
  - alert: "some"
    expr: "up"
- name: "something6"
  partial_response_strategy: "abort"
  rules:
  - alert: "some"
    expr: "up"
- name: "something7"
  rules:
  - alert: "some"
    expr: "up"
`), os.ModePerm))
	// Same filename as the first rule file but different path.
	testutil.Ok(t, ioutil.WriteFile(filepath.Join(dir, "subdir", "no_strategy.yaml"), []byte(`
groups:
- name: "something8"
  rules:
  - alert: "some"
    expr: "up"
`), os.ModePerm))

	thanosRuleMgr := NewManager(
		context.Background(),
		nil,
		dir,
		rules.ManagerOptions{
			Logger: log.NewLogfmtLogger(os.Stderr),
		},
		func(partialResponseStrategy storepb.PartialResponseStrategy) rules.QueryFunc {
			return func(ctx context.Context, q string, t time.Time) (promql.Vector, error) {
				return nil, nil
			}
		},
		labels.FromStrings("replica", "1"),
	)
	err = thanosRuleMgr.Update(10*time.Second, []string{
		filepath.Join(dir, "no_strategy.yaml"),
		filepath.Join(dir, "abort.yaml"),
		filepath.Join(dir, "warn.yaml"),
		filepath.Join(dir, "wrong.yaml"),
		filepath.Join(dir, "combined.yaml"),
		filepath.Join(dir, "non_existing.yaml"),
		filepath.Join(dir, "subdir", "no_strategy.yaml"),
	})

	testutil.NotOk(t, err)
	testutil.Assert(t, strings.Contains(err.Error(), "wrong.yaml: failed to unmarshal \"afafsdgsdgs\" as 'partial_response_strategy'"), err.Error())
	testutil.Assert(t, strings.Contains(err.Error(), "non_existing.yaml: no such file or directory"), err.Error())

	g := thanosRuleMgr.RuleGroups()
	sort.Slice(g, func(i, j int) bool {
		return g[i].Name() < g[j].Name()
	})

	exp := []struct {
		name     string
		file     string
		strategy storepb.PartialResponseStrategy
	}{
		{
			name:     "something1",
			file:     filepath.Join(dir, "no_strategy.yaml"),
			strategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			name:     "something2",
			file:     filepath.Join(dir, "abort.yaml"),
			strategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			name:     "something3",
			file:     filepath.Join(dir, "warn.yaml"),
			strategy: storepb.PartialResponseStrategy_WARN,
		},
		{
			name:     "something5",
			file:     filepath.Join(dir, "combined.yaml"),
			strategy: storepb.PartialResponseStrategy_WARN,
		},
		{
			name:     "something6",
			file:     filepath.Join(dir, "combined.yaml"),
			strategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			name:     "something7",
			file:     filepath.Join(dir, "combined.yaml"),
			strategy: storepb.PartialResponseStrategy_ABORT,
		},
		{
			name:     "something8",
			file:     filepath.Join(dir, "subdir", "no_strategy.yaml"),
			strategy: storepb.PartialResponseStrategy_ABORT,
		},
	}
	testutil.Equals(t, len(exp), len(g))

	for i := range exp {
		t.Run(exp[i].name, func(t *testing.T) {
			testutil.Equals(t, exp[i].strategy, g[i].PartialResponseStrategy)
			testutil.Equals(t, exp[i].name, g[i].Name())

			p := g[i].toProto()
			testutil.Equals(t, exp[i].strategy, p.PartialResponseStrategy)
			testutil.Equals(t, exp[i].name, p.Name)
			testutil.Equals(t, exp[i].file, p.File)
		})
	}
}

func TestRuleGroupMarshalYAML(t *testing.T) {
	const expected = `groups:
- name: something1
  rules:
  - alert: some
    expr: up
- name: something2
  rules:
  - alert: some
    expr: rate(some_metric[1h:5m] offset 1d)
  partial_response_strategy: ABORT
`

	a := storepb.PartialResponseStrategy_ABORT
	var input = configRuleGroups{
		Groups: []configRuleGroup{
			{
				RuleGroup: rulefmt.RuleGroup{
					Name: "something1",
					Rules: []rulefmt.Rule{
						{
							Alert: "some",
							Expr:  "up",
						},
					},
				},
			},
			{
				RuleGroup: rulefmt.RuleGroup{
					Name: "something2",
					Rules: []rulefmt.Rule{
						{
							Alert: "some",
							Expr:  "rate(some_metric[1h:5m] offset 1d)",
						},
					},
				},
				PartialResponseStrategy: &a,
			},
		},
	}

	b, err := yaml.Marshal(input)
	testutil.Ok(t, err)

	testutil.Equals(t, expected, string(b))
}

func TestManager_Rules(t *testing.T) {
	defer leaktest.CheckTimeout(t, 10*time.Second)()

	dir, err := ioutil.TempDir("", "test_rule_run")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

	curr, err := os.Getwd()
	testutil.Ok(t, err)

	thanosRuleMgr := NewManager(
		context.Background(),
		nil,
		dir,
		rules.ManagerOptions{
			Logger: log.NewLogfmtLogger(os.Stderr),
		},
		func(partialResponseStrategy storepb.PartialResponseStrategy) rules.QueryFunc {
			return func(ctx context.Context, q string, t time.Time) (promql.Vector, error) {
				return nil, nil
			}
		},
		labels.FromStrings("replica", "test1"),
	)
	testutil.Ok(t, thanosRuleMgr.Update(60*time.Second, []string{
		filepath.Join(curr, "../../examples/alerts/alerts.yaml"),
		filepath.Join(curr, "../../examples/alerts/rules.yaml"),
	}))
	defer func() {
		// Update creates go routines. We don't need rules mngrs to run, just to parse things, but let it start and stop
		// at the end to correctly test leaked go routines.
		thanosRuleMgr.Run()
		thanosRuleMgr.Stop()
	}()
	testRulesAgainstExamples(t, filepath.Join(curr, "../../examples/alerts"), thanosRuleMgr)
}

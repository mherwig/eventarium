// Package filter compiles and evaluates the subscription filter expressions of
// section 6.2. The language is CEL, sandboxed with a cost limit. Attribute
// filters run on every event; data filters only on JSON data.
package filter

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"cel.dev/cel-go/cel"
)

// maxCost bounds one filter evaluation. A cost unit is roughly one operation.
const maxCost = 50000

// A compiled filter depends only on its source string, and the same handful
// of expressions gets compiled over and over: registry.Reload recompiles
// every subscription's filters on every poll, in every process. With a few
// thousand subscriptions at a one-second poll that is tens of thousands of
// cel.NewEnv + Compile + Program calls per second across the fleet, for
// expressions that have not changed since the previous tick - measured as
// replay-api at 150% CPU and loadavg 22 on an otherwise idle platform.
// Memoising by source turns all of that into a map lookup.
//
// Failures are memoised too. Compilation is deterministic, so an expression
// that does not compile now never will, and re-proving it every tick is the
// difference between one warning and a permanent load.
//
// cacheMax bounds the map. A deployment has a handful of distinct filter
// strings, so it stays small; the bound exists so a system churning through
// unique strings cannot grow it without limit. Past it, calls compile
// uncached rather than evict - correctness never depends on a hit, only
// speed does.
const cacheMax = 10000

// cacheErr carries a compile failure in the same sync.Map as a success.
type cacheErr struct{ err error }

var (
	attrCache sync.Map // string -> *Compiled | cacheErr
	dataCache sync.Map
	attrSize  atomic.Int64
	dataSize  atomic.Int64
)

func cacheLookup(c *sync.Map, src string) (*Compiled, error, bool) {
	v, ok := c.Load(src)
	if !ok {
		return nil, nil, false
	}
	if e, ok := v.(cacheErr); ok {
		return nil, e.err, true
	}
	return v.(*Compiled), nil, true
}

func cacheStore(c *sync.Map, n *atomic.Int64, src string, prog *Compiled, err error) {
	if n.Load() >= cacheMax {
		return
	}
	if err != nil {
		c.Store(src, cacheErr{err})
	} else {
		c.Store(src, prog)
	}
	n.Add(1)
}

// Compiled is a compiled filter expression.
type Compiled struct {
	Raw string
	// AttrIndex holds the equality predicates of the form
	// attributes["key"] == "value" found in the expression. They let the
	// router skip most filters without evaluating them (section 6.2).
	AttrIndex map[string]string
	prog      cel.Program
}

var indexRe = regexp.MustCompile(`attributes\["([^"]+)"\]\s*==\s*"([^"]*)"`)

// CompileAttribute compiles a filter over CloudEvents attributes.
// Variables: attributes (map[string]string), and the shorthand string
// variables id, source, type, subject.
func CompileAttribute(src string) (*Compiled, error) {
	src = strings.TrimSpace(src)
	if src == "" {
		return nil, errors.New("filter: empty expression")
	}
	if prog, err, ok := cacheLookup(&attrCache, src); ok {
		return prog, err
	}
	prog, err := compileAttribute(src)
	cacheStore(&attrCache, &attrSize, src, prog, err)
	return prog, err
}

// compileAttribute is the uncached body. Callers go through CompileAttribute.
func compileAttribute(src string) (*Compiled, error) {
	src = strings.TrimSpace(src)
	if src == "" {
		return nil, errors.New("filter: empty expression")
	}
	env, err := cel.NewEnv(
		cel.Variable("attributes", cel.MapType(cel.StringType, cel.StringType)),
		cel.Variable("id", cel.StringType),
		cel.Variable("source", cel.StringType),
		cel.Variable("type", cel.StringType),
		cel.Variable("subject", cel.StringType),
	)
	if err != nil {
		return nil, fmt.Errorf("filter: building env: %w", err)
	}
	ast, issues := env.Compile(src)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("filter: %w", issues.Err())
	}
	if ast.OutputType() != cel.BoolType {
		return nil, errors.New("filter: expression must evaluate to bool")
	}
	prog, err := env.Program(ast, cel.CostLimit(maxCost))
	if err != nil {
		return nil, fmt.Errorf("filter: building program: %w", err)
	}
	c := &Compiled{Raw: src, AttrIndex: map[string]string{}, prog: prog}
	for _, m := range indexRe.FindAllStringSubmatch(src, -1) {
		c.AttrIndex[m[1]] = m[2]
	}
	return c, nil
}

// CompileData compiles a filter over the JSON data member.
// Variable: data (dynamic).
func CompileData(src string) (*Compiled, error) {
	src = strings.TrimSpace(src)
	if src == "" {
		return nil, errors.New("filter: empty expression")
	}
	if prog, err, ok := cacheLookup(&dataCache, src); ok {
		return prog, err
	}
	prog, err := compileData(src)
	cacheStore(&dataCache, &dataSize, src, prog, err)
	return prog, err
}

// compileData is the uncached body. Callers go through CompileData.
func compileData(src string) (*Compiled, error) {
	src = strings.TrimSpace(src)
	if src == "" {
		return nil, errors.New("filter: empty expression")
	}
	env, err := cel.NewEnv(
		cel.Variable("data", cel.DynType),
	)
	if err != nil {
		return nil, fmt.Errorf("filter: building env: %w", err)
	}
	ast, issues := env.Compile(src)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("filter: %w", issues.Err())
	}
	if ast.OutputType() != cel.BoolType {
		return nil, errors.New("filter: expression must evaluate to bool")
	}
	prog, err := env.Program(ast, cel.CostLimit(maxCost))
	if err != nil {
		return nil, fmt.Errorf("filter: building program: %w", err)
	}
	return &Compiled{Raw: src, AttrIndex: map[string]string{}, prog: prog}, nil
}

// EvalAttribute evaluates an attribute filter. data is ignored; it exists so
// that both compiled filters share one signature.
func (c *Compiled) EvalAttribute(attrs map[string]string, _ any) (bool, error) {
	out, _, err := c.prog.Eval(map[string]any{
		"attributes": attrs,
		"id":         attrs["id"],
		"source":     attrs["source"],
		"type":       attrs["type"],
		"subject":    attrs["subject"],
	})
	if err != nil {
		return false, fmt.Errorf("filter: evaluating %q: %w", c.Raw, err)
	}
	b, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("filter: %q did not evaluate to bool", c.Raw)
	}
	return b, nil
}

// EvalData evaluates a data filter against the decoded JSON data.
func (c *Compiled) EvalData(data map[string]any, _ any) (bool, error) {
	out, _, err := c.prog.Eval(map[string]any{"data": data})
	if err != nil {
		return false, fmt.Errorf("filter: evaluating %q: %w", c.Raw, err)
	}
	b, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("filter: %q did not evaluate to bool", c.Raw)
	}
	return b, nil
}

// Matches evaluates both filters of a subscription against one event.
// attrs is the CloudEvents attribute map (lower-case keys), data is the decoded
// JSON data member or nil when the event data is not a JSON object.
func Matches(attrFilter, dataFilter *Compiled, attrs map[string]string, data map[string]any) (bool, error) {
	if attrFilter != nil {
		// Fast path: an equality predicate that does not hold means the
		// filter cannot match.
		for k, v := range attrFilter.AttrIndex {
			if attrs[k] != v {
				return false, nil
			}
		}
		ok, err := attrFilter.EvalAttribute(attrs, nil)
		if err != nil || !ok {
			return false, err
		}
	}
	if dataFilter != nil {
		if data == nil {
			return false, nil
		}
		ok, err := dataFilter.EvalData(data, nil)
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

// Package project implements the field allowlist projection of section 6.2:
// only the listed JSON paths of the data member survive, everything else is
// removed. JSON data only.
package project

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Apply keeps only the given dotted paths (for example "user.id") in the data
// member and returns the new JSON document. Paths that are absent are skipped.
func Apply(data []byte, allowlist []string) ([]byte, error) {
	if len(allowlist) == 0 {
		return data, nil
	}
	var root map[string]any
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("project: data is not a JSON object: %w", err)
	}
	out := make(map[string]any, len(allowlist))
	for _, path := range allowlist {
		segs := strings.Split(strings.TrimSpace(path), ".")
		if len(segs) == 0 || segs[0] == "" {
			return nil, errors.New("project: empty path in allowlist")
		}
		var src any = root
		for _, seg := range segs[:len(segs)-1] {
			m, ok := src.(map[string]any)
			if !ok {
				src = nil
				break
			}
			src = m[seg]
		}
		if src == nil {
			continue
		}
		last := segs[len(segs)-1]
		m, ok := src.(map[string]any)
		if !ok {
			continue
		}
		if v, ok := m[last]; ok {
			set(out, segs, v)
		}
	}
	return json.Marshal(out)
}

func set(dst map[string]any, segs []string, v any) {
	if len(segs) == 1 {
		dst[segs[0]] = v
		return
	}
	m, ok := dst[segs[0]].(map[string]any)
	if !ok {
		m = map[string]any{}
		dst[segs[0]] = m
	}
	set(m, segs[1:], v)
}

// Validate checks that every path in the allowlist is well formed.
func Validate(allowlist []string) error {
	for _, p := range allowlist {
		if strings.TrimSpace(p) == "" {
			return errors.New("project: empty path in allowlist")
		}
		for _, seg := range strings.Split(p, ".") {
			if seg == "" {
				return fmt.Errorf("project: malformed path %q", p)
			}
		}
	}
	return nil
}

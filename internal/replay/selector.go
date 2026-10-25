// Package replay implements the Replay API of section 11 of the spec: it
// resolves a selector against the ClickHouse status store of section 9, reads
// the original event records back from the events topic of their retention
// class (partition and offset come from the status record, section 9), and
// writes new deliveries marked with the replay ID through the normal delivery
// path (sections 11 and 12).
//
// The package is split along those seams:
//
//   - selector.go  the request body and its validation (section 11, "at
//     least one" of statuses, a time range by ingestion time, event refs);
//   - jobs.go      the replay_jobs control data in PostgreSQL (migration 002);
//   - resolve.go   the selector query against delivery_status and the live
//     delivered/failed counters of GET /replays/{id};
//   - fetch.go     reading the original event records back from Kafka plus the
//     not_replayable classification (section 11, retention limit);
//   - build.go     turning a fetched event into the delivery and pending
//     status records, mirroring internal/routing (sections 9 and 11);
//   - worker.go    the single-goroutine job worker: claim, batch, produce,
//     honour cancellation between batches, finish (section 11);
//   - api.go       the HTTP surface of section 11.
//
// Everything is owner scoped from the verified JWT (section 6.7): unknown and
// foreign resources answer the same 404 and existence never leaks.
package replay

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"pubsub/internal/status"
)

// Selector is the replay selector of section 11 (spec section 280): statuses,
// a time range by ingestion time and explicit event references. At least one
// of the three must be present, so a replay can never degenerate into
// "everything". Times stay RFC3339 strings exactly as submitted; they were
// validated when the job was created, so the worker can re-parse them without
// a second opinion from the caller.
type Selector struct {
	Statuses  []string `json:"statuses,omitempty"`
	From      string   `json:"from,omitempty"`
	To        string   `json:"to,omitempty"`
	EventRefs []string `json:"event_refs,omitempty"`

	// DryRun resolves the selector and reports matched/not_replayable but
	// never produces a delivery (section 11: dry_run reports the counts up
	// front).
	DryRun bool `json:"dry_run,omitempty"`
	// MaxEvents caps one job (section 11, safeguards). nil means the
	// service default REPLAY_MAX_EVENTS; the pointer keeps an explicitly
	// submitted 0 distinguishable from "not submitted".
	MaxEvents *int `json:"max_events,omitempty"`

	// EventTypes exists only to answer it honestly: status records carry no
	// event type (section 9), so the replay service cannot select on it —
	// the same stance internal/chquery takes for the Status API's
	// event_type filter. A body that carries it is rejected with a 400 that
	// says why; it is never stored.
	EventTypes []string `json:"event_types,omitempty"`
}

// selectableStatuses lists every status of section 9 that a selector may name.
// It is spelled out from the status package constants so a typo is a 400
// instead of a selector that silently matches nothing.
var selectableStatuses = []string{
	status.Pending,
	status.InFlight,
	status.Retrying,
	status.Delivered,
	status.Sent,
	status.Failed,
	status.DeadLettered,
	status.Paused,
	status.Expired,
	status.Dropped,
}

// ParseSelector decodes and validates the body of
// POST /v1/subscriptions/{id}/replays. maxEvents is REPLAY_MAX_EVENTS, the
// service-wide cap of section 11's "maximum number of events per job".
func ParseSelector(body []byte, maxEvents int) (Selector, error) {
	var sel Selector
	if len(bytes.TrimSpace(body)) == 0 {
		return sel, fmt.Errorf("request body is required")
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&sel); err != nil {
		return sel, fmt.Errorf("invalid request body: %w", err)
	}
	// One selector per body: anything after the object is a second value the
	// caller did not intend, so it is rejected instead of silently ignored.
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return sel, fmt.Errorf("invalid request body: unexpected data after the selector")
	}
	if err := sel.validate(maxEvents); err != nil {
		return sel, err
	}
	return sel, nil
}

// validate enforces the selector rules of section 11. The returned error is
// the 400 body, so the messages are written for the caller.
func (s *Selector) validate(maxEvents int) error {
	// Checked first: a body selecting on event types gets the honest reason
	// rather than the "at least one criterion" complaint it would also trip.
	if len(s.EventTypes) > 0 {
		return fmt.Errorf("event_types is not supported: delivery status records carry no event type (section 9)")
	}

	for i, st := range s.Statuses {
		s.Statuses[i] = strings.TrimSpace(st)
		if s.Statuses[i] == "" {
			return fmt.Errorf("statuses must not contain an empty value")
		}
		if !containsStatus(selectableStatuses, s.Statuses[i]) {
			return fmt.Errorf("unknown status %q, must be one of %s",
				s.Statuses[i], strings.Join(selectableStatuses, ", "))
		}
	}
	for _, ref := range s.EventRefs {
		if strings.TrimSpace(ref) == "" {
			return fmt.Errorf("event_refs must not contain an empty value")
		}
	}

	if len(s.Statuses) == 0 && s.From == "" && s.To == "" && len(s.EventRefs) == 0 {
		return fmt.Errorf("selector needs at least one of statuses, from, to or event_refs (section 11)")
	}

	from, to := time.Time{}, time.Time{}
	var err error
	if s.From != "" {
		if from, err = time.Parse(time.RFC3339, s.From); err != nil {
			return fmt.Errorf("from must be an RFC3339 timestamp")
		}
	}
	if s.To != "" {
		if to, err = time.Parse(time.RFC3339, s.To); err != nil {
			return fmt.Errorf("to must be an RFC3339 timestamp")
		}
	}
	if !from.IsZero() && !to.IsZero() && from.After(to) {
		return fmt.Errorf("from is after to")
	}

	if s.MaxEvents != nil && (*s.MaxEvents < 1 || *s.MaxEvents > maxEvents) {
		return fmt.Errorf("max_events must be between 1 and %d", maxEvents)
	}
	return nil
}

// limit returns the effective maximum number of events per job: the submitted
// cap, or the service default when the caller left it out.
func (s Selector) limit(def int) int {
	if s.MaxEvents != nil {
		return *s.MaxEvents
	}
	return def
}

// timeRange parses the validated from/to bounds. Zero values mean "open" on
// that side (section 11: the range is optional, only "at least one" criterion
// is not).
func (s Selector) timeRange() (from, to time.Time, err error) {
	if s.From != "" {
		if from, err = time.Parse(time.RFC3339, s.From); err != nil {
			return from, to, fmt.Errorf("replay: selector from: %w", err)
		}
	}
	if s.To != "" {
		if to, err = time.Parse(time.RFC3339, s.To); err != nil {
			return from, to, fmt.Errorf("replay: selector to: %w", err)
		}
	}
	return from, to, nil
}

func containsStatus(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

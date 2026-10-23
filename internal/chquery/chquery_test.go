package chquery

import (
	"testing"
	"time"
)

func TestCursorRoundTrip(t *testing.T) {
	c := Cursor{
		TS:         time.Date(2026, 10, 23, 12, 34, 56, 123_000_000, time.UTC),
		DeliveryID: "9f4a2c1e-7b3d-4e5f-8a9b-0c1d2e3f4a5b",
	}
	got, err := DecodeCursor(c.Encode())
	if err != nil {
		t.Fatalf("DecodeCursor: %v", err)
	}
	if !got.TS.Equal(c.TS) || got.DeliveryID != c.DeliveryID {
		t.Fatalf("round trip mismatch: got %+v, want %+v", got, c)
	}
}

// A delivery ID may theoretically contain the separator; only the first one
// splits, so the ID survives the round trip.
func TestCursorRoundTripSeparatorInID(t *testing.T) {
	c := Cursor{TS: time.Unix(0, 0).UTC(), DeliveryID: "id|with|pipes"}
	got, err := DecodeCursor(c.Encode())
	if err != nil {
		t.Fatalf("DecodeCursor: %v", err)
	}
	if got.DeliveryID != c.DeliveryID {
		t.Fatalf("delivery ID lost: got %q, want %q", got.DeliveryID, c.DeliveryID)
	}
}

func TestDecodeCursorRejectsGarbage(t *testing.T) {
	for _, raw := range []string{"", "not base64!", "bm90IHBpcGVz", // "not pipes"
		"fDIwMjYtMTAtMjM=", // "|2026-10-23": empty timestamp part
		"bm90LXRpbWV8aWQ",  // "not-time|id"
		"aWQ=",             // "id" without timestamp or separator
	} {
		if _, err := DecodeCursor(raw); err == nil {
			t.Errorf("DecodeCursor(%q) succeeded, want error", raw)
		}
	}
}

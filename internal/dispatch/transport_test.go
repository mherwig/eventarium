package dispatch

import "testing"

// The delivery client originally set no Transport at all, which silently
// selects http.DefaultTransport and its DefaultMaxIdleConnsPerHost of 2 - a
// constant, not a documented product decision, and one that cannot survive
// someone tidying the client literal. Nothing failed when it was missing:
// deliveries kept working, just with a handshake every time the two-slot pool
// drained. So assert the numbers and, more usefully, that the field is set at
// all - the failure this guards against is a client that falls back to a
// default nobody chose.
func TestDeliveryTransportPool(t *testing.T) {
	tr := deliveryTransport()
	if tr == nil {
		t.Fatal("deliveryTransport returned nil: the client would fall back to http.DefaultTransport")
	}
	if tr.MaxIdleConnsPerHost != deliveryMaxIdleConnsPerHost {
		t.Errorf("MaxIdleConnsPerHost = %d, want %d",
			tr.MaxIdleConnsPerHost, deliveryMaxIdleConnsPerHost)
	}
	if tr.MaxIdleConns != deliveryMaxIdleConns {
		t.Errorf("MaxIdleConns = %d, want %d", tr.MaxIdleConns, deliveryMaxIdleConns)
	}
	// The two Go defaults this exists to get away from.
	const goDefaultIdlePerHost = 2
	if tr.MaxIdleConnsPerHost <= goDefaultIdlePerHost {
		t.Errorf("MaxIdleConnsPerHost = %d, which is the Go default of %d - "+
			"the pool would be the one the bug came from",
			tr.MaxIdleConnsPerHost, goDefaultIdlePerHost)
	}
	// Clone keeps what must not be lost when the sizes are overridden.
	if tr.MaxIdleConnsPerHost > tr.MaxIdleConns {
		t.Errorf("MaxIdleConnsPerHost (%d) exceeds MaxIdleConns (%d): the total would cap the per-host",
			tr.MaxIdleConnsPerHost, tr.MaxIdleConns)
	}
	if tr.IdleConnTimeout == 0 {
		t.Error("IdleConnTimeout lost in the clone: idle sockets would never be reclaimed")
	}
}

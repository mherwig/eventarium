package kafkautil

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestTimeoutsDefaults pins the shipped numbers rather than the ones franz-go
// ships. 45s/40s were inherited until the chaos runs measured what they cost
// (46s for a killed replica's partitions to be handed over, against 3s for a
// graceful SIGTERM) and the pair was deliberately lowered; see the constants.
// The point of the test is that a value nobody chose cannot creep back in
// silently - changing it here is a decision, and the rebalance scenario has to
// be re-run when it changes.
func TestTimeoutsDefaults(t *testing.T) {
	t.Setenv("KAFKA_SESSION_TIMEOUT", "")
	t.Setenv("KAFKA_TRANSACTION_TIMEOUT", "")

	session, txn := Timeouts()
	if session != DefaultSessionTimeout {
		t.Errorf("session = %v, want the default %v", session, DefaultSessionTimeout)
	}
	if txn != DefaultTransactionTimeout {
		t.Errorf("txn = %v, want the default %v", txn, DefaultTransactionTimeout)
	}
	if txn >= session {
		t.Errorf("txn (%v) must be below session (%v)", txn, session)
	}
	// The pair has to stay below franz-go's own defaults for the change to be
	// real: a "lowered" default that quietly equals the library's would mean
	// nobody lowered anything.
	if session >= 45*time.Second || txn >= 40*time.Second {
		t.Errorf("session=%v txn=%v - both must be below franz-go's 45s/40s, "+
			"or the measured handover (13s) no longer applies", session, txn)
	}
}

// TestTimeoutsAreConfigurable is why these are environment variables rather
// than constants: the value is still an open decision, and deciding it should
// not require a rebuild.
func TestTimeoutsAreConfigurable(t *testing.T) {
	t.Setenv("KAFKA_SESSION_TIMEOUT", "15s")
	t.Setenv("KAFKA_TRANSACTION_TIMEOUT", "10s")

	session, txn := Timeouts()
	if session != 15*time.Second {
		t.Errorf("session = %v, want 15s", session)
	}
	if txn != 10*time.Second {
		t.Errorf("txn = %v, want 10s", txn)
	}
}

// TestTimeoutsKeepTransactionBelowSession is the clamp that matters. franz-go
// documents "keep the transaction timeout below the session timeout" and
// enforces neither side of it, so an operator raising the session timeout - or
// simply misreading the two - could leave a transaction that outlives its
// group member, which fails silently rather than loudly at join. The pair is
// adjusted instead of rejected: refusing would take a service down over an
// environment variable, and passing it through unclamped would invite exactly
// the failure the rule exists to prevent.
func TestTimeoutsKeepTransactionBelowSession(t *testing.T) {
	for _, tc := range []struct {
		name, session, txn string
	}{
		{"transaction far too long", "10s", "40s"},
		{"transaction equal to session", "30s", "30s"},
		// Below the 5s headroom the clamp cannot subtract, so it halves
		// instead: the rule to keep is txn < session, not a fixed gap.
		{"session smaller than the headroom", "4s", "40s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("KAFKA_SESSION_TIMEOUT", tc.session)
			t.Setenv("KAFKA_TRANSACTION_TIMEOUT", tc.txn)
			session, got := Timeouts()
			if got >= session {
				t.Errorf("session=%v txn=%v - txn must stay below session", session, got)
			}
			if got <= 0 {
				t.Errorf("txn = %v, must be positive", got)
			}
		})
	}
}

// TestNewClientAppliesTheFailoverPair closes the loop the two tests above
// cannot: Timeouts() is only worth anything if the clients actually carry the
// values, and the router - the one client where both halves matter - reads the
// same pair (cmd/router/main.go newSession).
func TestNewClientAppliesTheFailoverPair(t *testing.T) {
	t.Setenv("KAFKA_SESSION_TIMEOUT", "17s")
	t.Setenv("KAFKA_TRANSACTION_TIMEOUT", "9s")

	cl, err := NewClient(kgo.SeedBrokers("127.0.0.1:9092"))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()

	if v := cl.OptValue(kgo.SessionTimeout); v != 17*time.Second {
		t.Errorf("client session timeout = %v (%T), want 17s", v, v)
	}
	if v := cl.OptValue(kgo.TransactionTimeout); v != 9*time.Second {
		t.Errorf("client transaction timeout = %v (%T), want 9s", v, v)
	}
}

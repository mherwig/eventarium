package kafkautil

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestTimeoutsDefaults pins the numbers this codebase is actually measured
// with. They are franz-go's defaults, which is the point of the exercise: they
// were inherited until this, and the chaos runs measured what they cost (43 s
// to hand a killed replica's partitions over, 3 s for a graceful SIGTERM).
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

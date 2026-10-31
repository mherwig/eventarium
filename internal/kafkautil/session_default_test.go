package kafkautil

import (
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The chaos driver's rebalance scenario bounds the failover by
// KAFKA_SESSION_TIMEOUT, and falls back to a hardcoded copy of this package's
// default when the variable is unset - it is shell, it cannot import a Go
// constant. That copy went stale the moment the defaults were lowered: the
// scenario then measured a correct 16s handover and printed it against a 60s
// bound, four times too generous. A wrong bound is worse than none, so the two
// are pinned together here rather than trusted to be edited in step.
//
// This test lives in the Go package on purpose: it is the side that owns the
// value, and a drift in either direction now fails `go test ./...` instead of
// quietly loosening the assertion the scenario makes.
func TestSessionSecondsFallbackMatchesDefault(t *testing.T) {
	const script = "../../scripts/chaos.sh"
	src, err := os.ReadFile(script)
	if err != nil {
		t.Fatalf("reading %s: %v", script, err)
	}

	// The fallback appears in the ${VAR:-15s} default; every other number in
	// the function is derived or is a bound. Require exactly one such default
	// so a second copy cannot hide behind the first.
	re := regexp.MustCompile(`KAFKA_SESSION_TIMEOUT:-([0-9]+)s`)
	m := re.FindAllStringSubmatch(string(src), -1)
	if len(m) != 1 {
		t.Fatalf("%s: want exactly one KAFKA_SESSION_TIMEOUT fallback, found %d", script, len(m))
	}
	got, err := strconv.Atoi(m[0][1])
	if err != nil {
		t.Fatalf("%s: fallback %q is not a number of seconds", script, m[0][1])
	}
	if want := int(DefaultSessionTimeout / time.Second); got != want {
		t.Errorf("%s falls back to %ds, but kafkautil.DefaultSessionTimeout is %ds.\n"+
			"The scenario would print a bound derived from the wrong session timeout: "+
			"the measurement stays right while the number next to it lies.", script, got, want)
	}

	// And the function itself, not only its text: run it with the variable
	// unset and compare what it actually answers. Skipped where bash is not
	// available; the assertion above still holds there.
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not available; text assertion above still applies")
	}
	extract, err := exec.Command("sed", "-n", "/^session_seconds()/,/^}/p", script).Output()
	if err != nil {
		t.Fatalf("extracting session_seconds from %s: %v", script, err)
	}
	out, err := exec.Command(bash, "-c",
		`unset KAFKA_SESSION_TIMEOUT; `+
			`source <(printf '%s\n' "$1"); session_seconds`, "_", string(extract)).Output()
	if err != nil {
		t.Fatalf("running session_seconds: %v", err)
	}
	// session_seconds answers in bare seconds; the newline is the shell's.
	if got := strings.TrimSpace(string(out)); got != m[0][1] {
		t.Errorf("session_seconds with the variable unset answered %q, want %ds",
			got, DefaultSessionTimeout/time.Second)
	}
}

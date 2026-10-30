package offsetgate

import (
	"strconv"
	"testing"
)

// BenchmarkSettleUnderBacklog measures what one Settle costs while the
// partition holds a deep backlog of unsettled records - the shape the
// dispatcher sees when a queue backs up. advance walks the whole pending map
// to find the lowest offset, so a backlog of N turns every delivery into a
// scan of N entries; this is the number that says whether a 10k-deep queue is
// a linear cost per delivery.
//
// Each iteration settles the current low-water record and takes on one more
// record from the poll, so the backlog stays deep instead of draining.
func BenchmarkSettleUnderBacklog(b *testing.B) {
	for _, n := range []int{256, 4096, 16384} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			g := New()
			for i := int64(0); i < int64(n); i++ {
				g.Track(rec(i, 7))
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				g.Settle(topic, 1, int64(i))
				g.Track(rec(int64(n)+int64(i), 7))
			}
		})
	}
}

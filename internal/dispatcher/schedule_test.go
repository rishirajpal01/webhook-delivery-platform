package dispatcher

import (
	"testing"
	"time"
)

func TestRetryDelay(t *testing.T) {
	low := func() float64 { return 0 }
	high := func() float64 { return 0.999999 }

	for i, base := range DefaultSchedule {
		attempt := i + 1
		lo, ok := retryDelay(DefaultSchedule, attempt, DefaultJitter, low)
		if !ok || lo != time.Duration(float64(base)*0.75) {
			t.Errorf("attempt %d low jitter = %v, %v", attempt, lo, ok)
		}
		hi, _ := retryDelay(DefaultSchedule, attempt, DefaultJitter, high)
		if hi <= base || hi > base*5/4 {
			t.Errorf("attempt %d high jitter = %v, want in (%v, %v]", attempt, hi, base, base*5/4)
		}
	}
	if _, ok := retryDelay(DefaultSchedule, len(DefaultSchedule)+1, DefaultJitter, low); ok {
		t.Error("schedule should be used up after the last retry")
	}
	if _, ok := retryDelay(DefaultSchedule, 0, DefaultJitter, low); ok {
		t.Error("attempt 0 is not a failed attempt")
	}
	if d, _ := retryDelay(DefaultSchedule, 1, 0, high); d != DefaultSchedule[0] {
		t.Errorf("no jitter: delay = %v, want %v", d, DefaultSchedule[0])
	}
}

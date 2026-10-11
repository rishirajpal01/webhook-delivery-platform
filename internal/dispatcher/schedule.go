package dispatcher

import "time"

// DefaultSchedule is the wait before each retry: about 22.7 hours in total,
// after which a delivery is dead. Ten attempts in all.
var DefaultSchedule = []time.Duration{
	5 * time.Second,
	30 * time.Second,
	2 * time.Minute,
	10 * time.Minute,
	30 * time.Minute,
	1 * time.Hour,
	3 * time.Hour,
	6 * time.Hour,
	12 * time.Hour,
}

// retryDelay returns how long to wait after the given number of failed
// attempts, or false when the schedule is used up. Each delay is moved by up
// to ±25% at random (rnd returns [0,1)), so retries from one outage spread
// out instead of arriving together, while the total window stays about the
// same as the schedule says.
func retryDelay(schedule []time.Duration, failedAttempts int, rnd func() float64) (time.Duration, bool) {
	if failedAttempts < 1 || failedAttempts > len(schedule) {
		return 0, false
	}
	base := schedule[failedAttempts-1]
	factor := 0.75 + 0.5*rnd()
	return time.Duration(float64(base) * factor), true
}

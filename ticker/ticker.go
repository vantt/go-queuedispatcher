package ticker

import "time"

// InstantTicker like time.Tick() but also fires immediately.
func InstantTicker(t time.Duration, now ...bool) <-chan time.Time {
	c := make(chan time.Time)
	ticker := time.NewTicker(t)

	go func() {
		if len(now) > 0 && now[0] {
			c <- time.Now()
		}

		for t := range ticker.C {
			c <- t
		}
	}()

	return c
}

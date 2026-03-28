package backoff

import (
	"context"
	"math/rand"
	"time"

	"github.com/golang/glog"
)

// Backoff defines an exponential backoff policy for retrying operations.
type Backoff struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	MaxElapsedTime  time.Duration
}

// New returns a Backoff with default values suitable for network operations.
func New() *Backoff {
	return &Backoff{
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     60 * time.Second,
		Multiplier:      2.0,
		MaxElapsedTime:  0, // 0 means no limit
	}
}

// Retry executes the given operation fn. If fn returns an error, it waits according
// to the exponential backoff policy and retries.
func (b *Backoff) Retry(ctx context.Context, name string, fn func() error) error {
	currentInterval := b.InitialInterval
	startTime := time.Now()
	attempt := 1

	for {
		glog.Infof("backoff [%s]: starting attempt %d", name, attempt)
		err := fn()
		if err == nil {
			glog.Infof("backoff [%s]: attempt %d succeeded", name, attempt)
			return nil
		}

		glog.Infof("backoff [%s]: attempt %d failed with error: %v", name, attempt, err)

		if b.MaxElapsedTime > 0 && time.Since(startTime) > b.MaxElapsedTime {
			glog.Infof("backoff [%s]: max elapsed time exceeded after %d attempts", name, attempt)
			return err
		}

		// Calculate next interval with jitter
		jitter := (rand.Float64() * 0.2) - 0.1 // +/- 10% jitter
		sleepDuration := time.Duration(float64(currentInterval) * (1.0 + jitter))

		glog.Infof("backoff [%s]: waiting %v before next attempt", name, sleepDuration)

		select {
		case <-time.After(sleepDuration):
			// Wait completed, proceed to next attempt
		case <-ctx.Done():
			glog.Infof("backoff [%s]: context cancelled during wait", name)
			return ctx.Err()
		}

		currentInterval = time.Duration(float64(currentInterval) * b.Multiplier)
		if currentInterval > b.MaxInterval {
			currentInterval = b.MaxInterval
		}
		attempt++
	}
}

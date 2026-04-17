// SPDX-License-Identifier: Apache-2.0

package backoff

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBackoff_Retry_SuccessOnFirstAttempt(t *testing.T) {
	b := New()
	attempts := 0
	err := b.Retry(context.Background(), "test", func() error {
		attempts++
		return nil
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts)
	}
}

func TestBackoff_Retry_SuccessAfterFailures(t *testing.T) {
	b := New()
	b.InitialInterval = 1 * time.Millisecond // Speed up test
	attempts := 0
	expectedAttempts := 3

	err := b.Retry(context.Background(), "test", func() error {
		attempts++
		if attempts < expectedAttempts {
			return errors.New("failed")
		}
		return nil
	})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if attempts != expectedAttempts {
		t.Fatalf("expected %d attempts, got %d", expectedAttempts, attempts)
	}
}

func TestBackoff_Retry_ContextCancelled(t *testing.T) {
	b := New()
	b.InitialInterval = 500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := b.Retry(ctx, "test", func() error {
		return errors.New("always fail")
	})

	if err != context.DeadlineExceeded {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
}

func TestBackoff_Retry_MaxElapsedTime(t *testing.T) {
	b := New()
	b.InitialInterval = 10 * time.Millisecond
	b.MaxElapsedTime = 50 * time.Millisecond

	startTime := time.Now()
	err := b.Retry(context.Background(), "test", func() error {
		return errors.New("always fail")
	})

	elapsed := time.Since(startTime)
	if err == nil {
		t.Fatalf("expected error due to max elapsed time, got nil")
	}

	if elapsed < b.MaxElapsedTime {
		t.Fatalf("expected elapsed time to be at least %v, got %v", b.MaxElapsedTime, elapsed)
	}
}

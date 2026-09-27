package worker

import (
	"testing"
	"time"
)

func TestShouldRetry(t *testing.T) {
	tests := []struct {
		attempts   int
		maxRetries int
		expected   bool
	}{
		{0, 3, true},
		{1, 3, true},
		{2, 3, true},
		{3, 3, false},
		{4, 3, false},
		{1, 1, false},
	}

	for _, tt := range tests {
		res := ShouldRetry(tt.attempts, tt.maxRetries)
		if res != tt.expected {
			t.Errorf("ShouldRetry(%d, %d) = %v; expected %v", tt.attempts, tt.maxRetries, res, tt.expected)
		}
	}
}

func TestCalculateBackoff(t *testing.T) {
	baseDelay := 10

	// Attempt 1: base = 10s -> with jitter [5s, 15s]
	d1 := CalculateBackoff(1, baseDelay)
	if d1 < 4*time.Second || d1 > 16*time.Second {
		t.Errorf("expected attempt 1 delay around 10s, got %v", d1)
	}

	// Attempt 3: base * 4 = 40s -> with jitter [20s, 60s]
	d3 := CalculateBackoff(3, baseDelay)
	if d3 < 18*time.Second || d3 > 65*time.Second {
		t.Errorf("expected attempt 3 delay around 40s, got %v", d3)
	}
}

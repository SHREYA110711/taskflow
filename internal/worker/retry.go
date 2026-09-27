package worker

import (
	"math"
	"math/rand"
	"time"
)

const (
	MaxBackoffDelaySeconds = 3600 // 1 hour max cap
)

// CalculateBackoff computes exponential backoff with full jitter to avoid thundering herd problems.
// Formula: delay = rand(0, min(MaxCap, BaseDelay * 2^(attempts-1)))
func CalculateBackoff(attempt int, baseDelaySeconds int) time.Duration {
	if baseDelaySeconds <= 0 {
		baseDelaySeconds = 10
	}
	if attempt <= 0 {
		attempt = 1
	}

	// Exponential multiplier: 2^(attempt-1)
	factor := math.Pow(2, float64(attempt-1))
	calculatedDelay := float64(baseDelaySeconds) * factor

	if calculatedDelay > float64(MaxBackoffDelaySeconds) {
		calculatedDelay = float64(MaxBackoffDelaySeconds)
	}

	// Full jitter: uniformly distributed random duration between [0.5 * delay, 1.5 * delay]
	jitterFactor := 0.5 + rand.Float64()
	jitteredDelay := calculatedDelay * jitterFactor

	return time.Duration(jitteredDelay * float64(time.Second))
}

// ShouldRetry checks if a job has remaining retry attempts.
func ShouldRetry(currentAttempts int, maxRetries int) bool {
	if maxRetries <= 0 {
		maxRetries = 3
	}
	return currentAttempts < maxRetries
}

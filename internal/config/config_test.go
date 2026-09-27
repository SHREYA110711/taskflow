package config

import (
	"os"
	"testing"
)

func TestConfigDefaults(t *testing.T) {
	// Clear any overrides for test
	os.Unsetenv("PORT")
	os.Unsetenv("DATABASE_URL")

	cfg := Load()
	if cfg.Port != "8080" {
		t.Errorf("expected default port 8080, got %s", cfg.Port)
	}
	if cfg.WorkerConcurrency != 5 {
		t.Errorf("expected default concurrency 5, got %d", cfg.WorkerConcurrency)
	}
}

func TestConfigEnvOverride(t *testing.T) {
	os.Setenv("PORT", "9090")
	os.Setenv("WORKER_CONCURRENCY", "12")
	defer func() {
		os.Unsetenv("PORT")
		os.Unsetenv("WORKER_CONCURRENCY")
	}()

	cfg := Load()
	if cfg.Port != "9090" {
		t.Errorf("expected overridden port 9090, got %s", cfg.Port)
	}
	if cfg.WorkerConcurrency != 12 {
		t.Errorf("expected overridden concurrency 12, got %d", cfg.WorkerConcurrency)
	}
}

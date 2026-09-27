package config

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Port                     string
	DatabaseURL              string
	RedisAddr                string
	RedisPassword            string
	RedisDB                  int
	WorkerConcurrency        int
	DefaultMaxRetries        int
	DefaultRetryDelaySeconds int
}

// LoadEnv reads .env if present and sets any unset environment variables.
func LoadEnv(filenames ...string) {
	if len(filenames) == 0 {
		filenames = []string{".env"}
	}

	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			continue // .env file is optional in containerized/production environments
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}

			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				key := strings.TrimSpace(parts[0])
				val := strings.TrimSpace(parts[1])
				val = strings.Trim(val, `"'`)

				// Only set if not already present in the OS environment
				if _, exists := os.LookupEnv(key); !exists {
					_ = os.Setenv(key, val)
				}
			}
		}
	}
}

// Load loads application configuration from environment variables.
func Load() *Config {
	LoadEnv()

	port := getEnv("PORT", "8080")
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/taskflow?sslmode=disable")
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	redisPass := getEnv("REDIS_PASSWORD", "")
	redisDB := getEnvAsInt("REDIS_DB", 0)
	concurrency := getEnvAsInt("WORKER_CONCURRENCY", 5)
	maxRetries := getEnvAsInt("DEFAULT_MAX_RETRIES", 3)
	retryDelay := getEnvAsInt("DEFAULT_RETRY_DELAY_SECONDS", 10)

	return &Config{
		Port:                     port,
		DatabaseURL:              dbURL,
		RedisAddr:                redisAddr,
		RedisPassword:            redisPass,
		RedisDB:                  redisDB,
		WorkerConcurrency:        concurrency,
		DefaultMaxRetries:        maxRetries,
		DefaultRetryDelaySeconds: retryDelay,
	}
}

func getEnv(key, defaultVal string) string {
	if val, exists := os.LookupEnv(key); exists && val != "" {
		return val
	}
	return defaultVal
}

func getEnvAsInt(key string, defaultVal int) int {
	valStr := getEnv(key, "")
	if val, err := strconv.Atoi(valStr); err == nil {
		return val
	}
	return defaultVal
}

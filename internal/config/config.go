// Package config provides small helpers for reading configuration from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Str returns the value of the environment variable or def when unset/empty.
func Str(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// MustStr returns the value of the environment variable or panics when unset.
func MustStr(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic(fmt.Sprintf("config: required environment variable %s is not set", key))
	}
	return v
}

// Int returns the int value of the environment variable or def when unset.
func Int(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		panic(fmt.Sprintf("config: %s must be an integer, got %q", key, v))
	}
	return n
}

// Duration returns a duration value from the environment, e.g. "5s", "2m".
func Duration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		panic(fmt.Sprintf("config: %s must be a duration, got %q", key, v))
	}
	return d
}

// Bool returns the bool value of the environment variable or def when unset.
func Bool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		panic(fmt.Sprintf("config: %s must be a boolean, got %q", key, v))
	}
	return b
}

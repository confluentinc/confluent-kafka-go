package testcontainers

import (
	"log"
	"os"
	"testing"
)

// Logger is the default log instance
var Logger Logging = log.New(os.Stderr, "", log.LstdFlags)

// Logging defines the Logger interface
type Logging interface {
	Printf(format string, v ...interface{})
}

// TestLogger returns a Logging implementation for testing.TB
// This way logs from testcontainers are part of the test output of a test suite or test case
func TestLogger(tb testing.TB) Logging {
	tb.Helper()
	return testLogger{TB: tb}
}

// WithLogger is a generic option that implements GenericProviderOption, DockerProviderOption and LocalDockerComposeOption
// It replaces the global Logging implementation with a user defined one e.g. to aggregate logs from testcontainers
// with the logs of specific test case
func WithLogger(logger Logging) LoggerOption {
	return LoggerOption{
		logger: logger,
	}
}

type LoggerOption struct {
	logger Logging
}

func (o LoggerOption) ApplyGenericTo(opts *GenericProviderOptions) {
	opts.Logger = o.logger
}

func (o LoggerOption) ApplyDockerTo(opts *DockerProviderOptions) {
	opts.Logger = o.logger
}

func (o LoggerOption) ApplyToLocalCompose(opts *LocalDockerComposeOptions) {
	opts.Logger = o.logger
}

type testLogger struct {
	testing.TB
}

func (t testLogger) Printf(format string, v ...interface{}) {
	t.Helper()
	t.Logf(format, v...)
}

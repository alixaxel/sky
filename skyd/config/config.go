package config

import (
	"github.com/BurntSushi/toml"
	"io"
	"time"
)

//------------------------------------------------------------------------------
//
// Constants
//
//------------------------------------------------------------------------------

const (
	DefaultPort                 = 8585
	DefaultDataPath             = "/var/lib/sky"
	DefaultPidPath              = "/var/run/skyd.pid"
	DefaultStreamFlushThreshold = 1000
	DefaultParallelism          = 0 // zero means set it to number of CPUs
	DefaultStrictMode           = false
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// The configuration for running Sky.
type Config struct {
	Port                 uint          `toml:"port"`
	DataPath             string        `toml:"data-path"`
	DataExpiration       time.Duration `toml:"data-expiration"`
	PidPath              string        `toml:"pid-path"`
	StreamFlushThreshold uint          `toml:"stream-flush-threshold"`
	Parallelism          uint          `toml:"parallelism"`
	NewRelicKey          string        `toml:"newrelic-key"`
	StrictMode           bool          `toml:"strict-mode"`
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// Creates a new configuration object.
func NewConfig() *Config {
	return &Config{
		Port:                 DefaultPort,
		DataPath:             DefaultDataPath,
		PidPath:              DefaultPidPath,
		StreamFlushThreshold: DefaultStreamFlushThreshold,
		Parallelism:          DefaultParallelism,
		StrictMode:           DefaultStrictMode,
	}
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

// Reads the contents of configuration file and populates the config object.
// Any properties that are not set in the configuration file will default to
// the value of the property before the decode.
func (c *Config) Decode(r io.Reader) error {
	if _, err := toml.DecodeReader(r, &c); err != nil {
		return err
	}
	return nil
}

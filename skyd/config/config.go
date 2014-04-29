package config

import (
	"github.com/BurntSushi/toml"
	"io"
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
	DefaultStreamFlushPeriod    = 60  // seconds
	DefaultStreamFlushThreshold = 1000
	DefaultParallelism 	    = 1
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// The configuration for running Sky.
type Config struct {
	Port                 uint   `toml:"port"`
	DataPath             string `toml:"data-path"`
	PidPath              string `toml:"pid-path"`
	StreamFlushPeriod    uint   `toml:"stream-flush-period"`
	StreamFlushThreshold uint   `toml:"stream-flush-threshold"`
	Parallelism	     uint   `toml:"parallelism"`
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
		StreamFlushPeriod:    DefaultStreamFlushPeriod,
		StreamFlushThreshold: DefaultStreamFlushThreshold,
		Parallelism:	      DefaultParallelism,
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

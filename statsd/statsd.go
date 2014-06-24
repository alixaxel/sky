package statsd

import (
	"github.com/ooyala/go-dogstatsd"
)

const (
	DefaultSampleRate = 1.0
)

// StatsDClient is the global StatsD client, when configured
// performance metrics will be collected under the configured base key.
var StatsDClient *dogstatsd.Client

// BaseKey is the prefix that will be prepended to the subkeys of all collected metrics.
var BaseKey string

func Configure(addr string, base string, tags []string) error {
	var err error
	if StatsDClient, err = dogstatsd.New(addr); err != nil {
		return err
	}
	StatsDClient.Tags = tags
	BaseKey = base + "."
	return nil
}

func Gauge(subkey string, value float64, tags []string) error {
	if StatsDClient == nil {
		return nil
	}
	return StatsDClient.Gauge(BaseKey+subkey, value, tags, DefaultSampleRate)
}

func Count(subkey string, value int64, tags []string) error {
	if StatsDClient == nil {
		return nil
	}
	return StatsDClient.Count(BaseKey+subkey, value, tags, DefaultSampleRate)
}

func Histogram(subkey string, value float64, tags []string) error {
	if StatsDClient == nil {
		return nil
	}
	return StatsDClient.Histogram(BaseKey+subkey, value, tags, DefaultSampleRate)
}

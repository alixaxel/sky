package config

import (
	"bytes"
	"testing"
)

const testConfigFileA = `
port=9000
data-path="/home/data"
pid-path = "/home/pid"
stream-flush-threshold = 500
newrelic-key = "longinscrutablestringofcharacters"
statsd = "127.0.0.1:8125"
statsd-base = "myapp.skydb"
`

// Decode a configuration file.
func TestDecode(t *testing.T) {
	config := NewConfig()
	err := config.Decode(bytes.NewBufferString(testConfigFileA))

	if err != nil {
		t.Fatalf("Unable to decode: %v", err)
	} else if config.Port != 9000 {
		t.Fatalf("Invalid port: %v", config.Port)
	} else if config.DataPath != "/home/data" {
		t.Fatalf("Invalid data path: %v", config.DataPath)
	} else if config.PidPath != "/home/pid" {
		t.Fatalf("Invalid pid path: %v", config.PidPath)
	} else if config.StreamFlushThreshold != 500 {
		t.Fatalf("Invalid no stream flush threshold option: %v", config.StreamFlushThreshold)
	} else if config.NewRelicKey != "longinscrutablestringofcharacters" {
		t.Fatalf("Invalid New Relic key option: %v", config.NewRelicKey)
	} else if config.StatsD != "127.0.0.1:8125" {
		t.Fatalf("Invalid statsd address: %v", config.StatsD)
	} else if config.StatsDBaseKey != "myapp.skydb" {
		t.Fatalf("Invalid statsd address: %v", config.StatsDBaseKey)
	}
}

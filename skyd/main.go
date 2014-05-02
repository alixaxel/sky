package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/skydb/sky/server"
	. "github.com/skydb/sky/skyd/config"

	"github.com/davecheney/profile"
	"github.com/yvasiyarov/gorelic"
)

//------------------------------------------------------------------------------
//
// Variables
//
//------------------------------------------------------------------------------

var config *Config
var configPath string

//------------------------------------------------------------------------------
//
// Functions
//
//------------------------------------------------------------------------------

//--------------------------------------
// Initialization
//--------------------------------------

func init() {
	config = NewConfig()
	flag.UintVar(&config.Port, "port", config.Port, "the port to listen on")
	flag.UintVar(&config.Port, "p", config.Port, "the port to listen on")
	flag.StringVar(&config.DataPath, "data-path", config.DataPath, "the data directory")
	flag.StringVar(&config.PidPath, "pid-path", config.PidPath, "the path to the pid file")
	flag.StringVar(&configPath, "config", "", "the path to the config file")
	flag.UintVar(&config.StreamFlushPeriod, "stream-flush-period", config.StreamFlushPeriod, "time period on which to flush streamed events")
	flag.UintVar(&config.StreamFlushThreshold, "stream-flush-threshold", config.StreamFlushThreshold, "the maximum number of events (per table) in event stream before flush")
	flag.UintVar(&config.Parallelism, "parallelism", config.Parallelism, "the number of cores to use, ie gomaxprocs")
	flag.StringVar(&config.NewRelicKey, "newrelic-key", "", "New Relic license key")
}

//--------------------------------------
// Main
//--------------------------------------

func main() {
	// Parse the command line arguments and load the config file (if specified).
	flag.Parse()
	if configPath != "" {
		file, err := os.Open(configPath)
		if err != nil {
			fmt.Printf("Unable to open config: %v\n", err)
			return
		}
		defer file.Close()
		if err = config.Decode(file); err != nil {
			fmt.Printf("Unable to parse config: %v\n", err)
			os.Exit(1)
		}
	}

	// Hardcore parallelism right here.
	if p := int(config.Parallelism); p == 0 {
		runtime.GOMAXPROCS(runtime.NumCPU())
	} else {
		runtime.GOMAXPROCS(p)
	}

	// Activate New Relic monitoring if the key is present
	if nrKey := config.NewRelicKey; nrKey != "" {
		agent := gorelic.NewAgent()
		agent.Verbose = true
		agent.NewrelicLicense = nrKey
		agent.NewrelicName = "skydb"
		agent.Run()
	}

	// Initialize
	s := server.NewServer(config.Port, config.DataPath)
	s.StreamFlushPeriod = config.StreamFlushPeriod
	s.StreamFlushThreshold = config.StreamFlushThreshold
	writePidFile()
	// setupSignalHandlers(s)
	go handleProfileSignal()

	// Start the server up!
	c := make(chan bool)
	err := s.ListenAndServe(c)
	if err != nil {
		fmt.Printf("%v\n", err)
		cleanup(s)
		return
	}
	<-c
	cleanup(s)
}

//--------------------------------------
// Signals
//--------------------------------------

// Handles signals received from the OS.
func setupSignalHandlers(s *server.Server) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for _ = range c {
			fmt.Fprintln(os.Stderr, "Shutting down...")
			cleanup(s)
			fmt.Fprintln(os.Stderr, "Shutdown complete.")
			os.Exit(1)
		}
	}()
}

// Waits for the SIGUSR1 signal and toggles profiling on and off.
func handleProfileSignal() {
	var p interface {
		Stop()
	}

	// Attach listener for SIGUSR1.
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGUSR1)

	for {
		// Wait for next signal.
		<-c

		// Toggle profiling on and off.
		if p == nil {
			p = profile.Start(&profile.Config{CPUProfile: true, MemProfile: true, BlockProfile: true})
		} else {
			p.Stop()
			p = nil
		}
	}
}

//--------------------------------------
// Utility
//--------------------------------------

// Shuts down the server socket and closes the database.
func cleanup(s *server.Server) {
	if s != nil {
		s.Shutdown()
	}
	deletePidFile()
}

// Writes a file to /var/run that contains the current process id.
func writePidFile() {
	pid := fmt.Sprintf("%d", os.Getpid())
	if err := ioutil.WriteFile(config.PidPath, []byte(pid), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Unable to write pid file: %v\n", err)
	}
}

// Deletes the pid file.
func deletePidFile() {
	if _, err := os.Stat(config.PidPath); !os.IsNotExist(err) {
		if err = os.Remove(config.PidPath); err != nil {
			fmt.Fprintf(os.Stderr, "Unable to remove pid file: %v\n", err)
		}
	}
}

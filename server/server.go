package server

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/skydb/sky"
	"github.com/skydb/sky/db"
	"github.com/skydb/sky/query"
	. "github.com/skydb/sky/skyd/config"
	"github.com/yvasiyarov/gorelic"
)

// The number of servlets created on startup defaults to the number
// of logical cores on a machine.
var defaultServletCount = runtime.NumCPU()

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A Server is the front end that controls access to tables.
type Server struct {
	DB *db.DB

	httpServer           *http.Server
	router               *mux.Router
	logger               *log.Logger
	path                 string
	listener             net.Listener
	tables               map[string]*db.Table
	shutdownChannel      chan bool
	shutdownFinished     chan bool
	mutex                sync.Mutex
	streamFlushThreshold uint
	newRelicAgent        *gorelic.Agent
	strictMode           bool
	expiration           time.Duration
}

//------------------------------------------------------------------------------
//
// Errors
//
//------------------------------------------------------------------------------

//--------------------------------------
// Alternate Content Type
//--------------------------------------

// Hackish way to return plain text for query codegen. Might fix later but
// it's rarely used.
type TextPlainContentTypeError struct {
}

func (e *TextPlainContentTypeError) Error() string {
	return ""
}

//------------------------------------------------------------------------------
//
// Constructors
//
//------------------------------------------------------------------------------

// NewServer returns a new Server.
func NewServer(config *Config) *Server {
	r := mux.NewRouter()
	s := &Server{
		httpServer:           &http.Server{Addr: fmt.Sprintf(":%d", config.Port), Handler: r},
		router:               r,
		logger:               log.New(os.Stdout, "", log.LstdFlags),
		path:                 config.DataPath,
		tables:               make(map[string]*db.Table),
		streamFlushThreshold: config.StreamFlushThreshold,
		strictMode:           config.StrictMode,
		expiration:           config.DataExpiration,
	}

	// Set up New Relic agent if we have a license key
	if nrKey := config.NewRelicKey; nrKey != "" {
		agent := gorelic.NewAgent()
		agent.NewrelicLicense = nrKey
		hn, err := os.Hostname()
		if err == nil {
			agent.NewrelicName = fmt.Sprintf("%s:%d", hn, config.Port)
			s.newRelicAgent = agent
		} else {
			s.logger.Printf("New Relic agent setup error: %s\n", err)
		}
	}

	s.addHandlers()
	s.addTableHandlers()
	s.addPropertyHandlers()
	s.addEventHandlers()
	s.addQueryHandlers()
	s.addDebugHandlers()

	// Activate New Relic monitoring if it is set up.
	if s.newRelicAgent != nil {
		s.newRelicAgent.Run()
	}

	return s
}

//------------------------------------------------------------------------------
//
// Properties
//
//------------------------------------------------------------------------------

// The root server path.
func (s *Server) Path() string {
	return s.path
}

// Generates the path for a table attached to the server.
func (s *Server) TablePath(name string) string {
	return fmt.Sprintf("%v/%v", s.path, name)
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// Lifecycle
//--------------------------------------

// Runs the server.
func (s *Server) ListenAndServe(shutdownChannel chan bool) error {
	s.shutdownChannel = shutdownChannel

	err := s.open()
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		s.close()
		return err
	}
	s.listener = listener

	s.shutdownFinished = make(chan bool)
	go func() {
		s.httpServer.Serve(s.listener)
		s.shutdownFinished <- true
	}()

	s.logger.Printf("Sky v%s is now listening on http://localhost%s\n", sky.Version, s.httpServer.Addr)

	if s.expiration != 0 {
		var tables, err = s.Tables()
		if err != nil {
			s.logger.Printf("Failed to start expiration sweepers: %s", err)
			return
		}
		for _, t := range tables {
			t.EnableExpiration(s.expiration)
		}
	}

	return nil
}

// Stops the server.
func (s *Server) Shutdown() error {
	// Close servlets.
	s.close()

	// Close socket.
	if s.listener != nil {
		// Then stop the server.
		err := s.listener.Close()
		s.listener = nil
		if err != nil {
			return err
		}
	}
	// wait for server goroutine to finish
	<-s.shutdownFinished

	// Notify that the server is shutdown.
	if s.shutdownChannel != nil {
		s.shutdownChannel <- true
	}

	return nil
}

// Checks if the server is listening for new connections.
func (s *Server) Running() bool {
	return (s.listener != nil)
}

// Opens the data directory and servlets.
func (s *Server) open() error {
	s.close()

	// Setup the file system if it doesn't exist.
	err := s.createIfNotExists()
	if err != nil {
		return fmt.Errorf("skyd.Server: Unable to create server folders: %v", err)
	}

	// Initialize and open database.
	s.DB = &db.DB{StrictMode: s.strictMode}
	if err = s.DB.Open(s.path); err != nil {
		s.close()
		return err
	}

	return nil
}

// Closes the database.
func (s *Server) close() {
	if s.DB != nil {
		s.DB.Close()
		s.DB = nil
	}
}

// Creates the appropriate directory structure if one does not exist.
func (s *Server) createIfNotExists() error {
	// Create root directory.
	err := os.MkdirAll(s.path, 0700)
	if err != nil {
		return err
	}

	return nil
}

//--------------------------------------
// Logging
//--------------------------------------

// Silences the log.
func (s *Server) Silence() {
	s.logger = log.New(ioutil.Discard, "", log.LstdFlags)
}

//--------------------------------------
// Routing
//--------------------------------------

// Parses incoming JSON objects and converts outgoing responses to JSON.
func (s *Server) ApiHandleFunc(route string, handlerFunction func(http.ResponseWriter, *http.Request, map[string]interface{}) (interface{}, error)) *mux.Route {
	wrappedFunction := func(w http.ResponseWriter, req *http.Request) {
		// warn("%s \"%s %s %s\"", req.RemoteAddr, req.Method, req.RequestURI, req.Proto)
		t0 := time.Now()

		var ret interface{}
		params, err := s.decodeParams(w, req)
		if err == nil {
			ret, err = handlerFunction(w, req, params)
		}

		// If we're returning plain text then just dump out what's returned.
		if _, ok := err.(*TextPlainContentTypeError); ok {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			if str, ok := ret.(string); ok {
				w.Write([]byte(str))
			}
			return
		}

		// If there is an error then replace the return value.
		if err != nil {
			ret = map[string]interface{}{"message": err.Error()}
		}

		// Write header status.
		w.Header().Set("Content-Type", "application/json")
		var status int
		if err == nil {
			status = http.StatusOK
		} else {
			status = http.StatusInternalServerError
		}
		w.WriteHeader(status)

		// Write to access log.
		s.logger.Printf("%s \"%s %s %s\" %d %0.3f", req.RemoteAddr, req.Method, req.RequestURI, req.Proto, status, time.Since(t0).Seconds())
		if status != http.StatusOK {
			s.logger.Printf("ERROR %v", err)
		}

		// Encode the return value appropriately.
		if ret != nil {
			encoder := json.NewEncoder(w)
			err := encoder.Encode(ConvertToStringKeys(ret))
			if err != nil {
				fmt.Printf("skyd.Server: Encoding error: %v\n", err.Error())
				return
			}
		}
	}

	if s.newRelicAgent != nil {
		wrappedFunction = s.newRelicAgent.WrapHTTPHandlerFunc(wrappedFunction)
	}

	return s.router.HandleFunc(route, wrappedFunction)
}

// Decodes the body of the message into parameters.
func (s *Server) decodeParams(w http.ResponseWriter, req *http.Request) (map[string]interface{}, error) {
	params := make(map[string]interface{})

	// If body starts with an open bracket then assume JSON. Otherwise read as raw string.
	bufReader := bufio.NewReader(req.Body)
	if first, err := bufReader.Peek(1); err == nil && string(first[0]) != "{" {
		raw, _ := ioutil.ReadAll(bufReader)
		params["RAW_POST_DATA"] = string(raw)
	} else {
		decoder := json.NewDecoder(bufReader)
		err := decoder.Decode(&params)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("Malformed json request: %v", err)
		}
	}

	return params, nil
}

//--------------------------------------
// Table Management
//--------------------------------------

// Retrieves a list of all tables in the database but does not open them.
// Do not use these table references for anything but informational purposes!
func (s *Server) Tables() ([]*db.Table, error) {
	// Create a table object for each directory in the tables path.
	infos, err := ioutil.ReadDir(s.path)
	if err != nil {
		return nil, err
	}

	tables := []*db.Table{}
	for _, info := range infos {
		tables = append(tables, db.NewTable(info.Name(), s.TablePath(info.Name())))
	}

	return tables, nil
}

// Opens a table and returns a reference to it.
func (s *Server) OpenTable(name string) (*db.Table, error) {
	return s.DB.OpenTable(name)
}

// Deletes a table.
func (s *Server) DropTable(name string) error {
	return s.DB.DropTable(name)
}

//--------------------------------------
// Query
//--------------------------------------

// Runs a query against a table.
func (s *Server) RunQuery(tx *db.Tx, q *query.Query) (interface{}, error) {

	// Fail if prefix is not provided.
	if q.Prefix == "" {
		return nil, errors.New("Prefix is required on queries.")
	}

	var result interface{}
	result = make(map[interface{}]interface{})
	var engines = make([]*query.ExecutionEngine, 0)

	// Retrieve low-level cursors for iterating.
	buckets := tx.Shards()

	// Create a channel to receive aggregate responses.
	rchannel := make(chan interface{}, len(buckets))

	// Create an engine for merging results.
	rootEngine, err := query.NewExecutionEngine(q)
	if err != nil {
		return nil, err
	}
	defer rootEngine.Destroy()
	//fmt.Println(query.FullAnnotatedSource())

	// Initialize one execution engine for each servlet.
	var data interface{}
	for index, b := range buckets {
		// Create an engine for each cursor. The execution engine is
		// protected by a mutex so it's safe to destroy it at any time.
		subengine, err := query.NewExecutionEngine(q)
		if err != nil {
			return nil, err
		}
		defer subengine.Destroy()

		subengine.SetBucket(b)
		engines = append(engines, subengine)

		// Run initialization once if required.
		if index == 0 && q.RequiresInitialization() {
			if data, err = subengine.Initialize(); err != nil {
				return nil, err
			}
		}
	}

	// Execute servlets asynchronously and retrieve responses outside
	// of the server context.
	for index, _ := range buckets {
		e := engines[index]
		go func() {
			if result, err := e.Aggregate(data); err != nil {
				rchannel <- err
			} else {
				rchannel <- result
			}
		}()
	}

	// Wait for each servlet to complete and then merge the results.
	var servletError error
	for _, _ = range buckets {
		ret := <-rchannel
		if err, ok := ret.(error); ok {
			fmt.Printf("skyd.Server: Aggregate error: %v", err)
			servletError = err
		} else {
			// Defactorize aggregate results.
			if err = q.Defactorize(ret); err != nil {
				return nil, err
			}

			// Merge results.
			if ret != nil {
				if result, err = rootEngine.Merge(result, ret); err != nil {
					fmt.Printf("skyd.Server: Merge error: %v", err)
					servletError = err
				}
			}
		}
	}
	if servletError != nil {
		return nil, servletError
	}

	// Finalize results.
	if err := q.Finalize(result); err != nil {
		return nil, err
	}

	return result, nil
}

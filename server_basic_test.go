// Test package for the tlvserv package.  Set environment variable
// TLV_SERVER_ADDR to "address:port" prior to running "go test"
package tlvserv_test

import (
	"bytes"
	"github.com/pdmorrow/tlvserv"
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

const (
	mtypeHello tlvserv.MTypeID = tlvserv.MTypeIDUserStart
	mtypeEcho  tlvserv.MTypeID = iota
	mtypeReps  tlvserv.MTypeID = iota
)

const testReps = 1000

type handlerStatus struct {
	mtype tlvserv.MTypeID
	err   error
}

var msgServer *tlvserv.TLVServ
var statusChan chan handlerStatus
var test1Reps = 0
var test1Lock sync.RWMutex
var hostAndPort string

// handleHello handles a message type of mtypeHello.  It unblocks the status
// channel when the routine completes.
func handleHello(conn net.Conn, data []byte) {
	var hs handlerStatus

	hs.mtype = mtypeHello
	hs.err = nil
	statusChan <- hs
}

// handleEcho handles a message type of mtypeEcho.  It echos back the data
// received in the data parameter then  unblocks the status channel when the
// routine completes.
func handleEcho(conn net.Conn, data []byte) {
	var hs handlerStatus

	conn.Write(data)
	hs.mtype = mtypeEcho
	hs.err = nil
	statusChan <- hs
}

// handleReps handles a message of type mtypeReps.  Once the number messages
// received of this type reaches testReps then status channel is unblocked.
func handleReps(conn net.Conn, data []byte) {
	test1Lock.Lock()
	test1Reps++

	if test1Reps == testReps {
		var hs handlerStatus

		test1Reps = 0
		hs.mtype = mtypeReps
		hs.err = nil
		statusChan <- hs
	}

	test1Lock.Unlock()
}

// ExampleServer() starts a test server on address:port TLV_SERVER_ADDR.
func ExampleServer() *tlvserv.TLVServ {

	hostAndPort, ok := os.LookupEnv("TLV_SERVER_ADDR")
	if ok == true {
		serv, err := tlvserv.Server(
			// "network"
			"tcp",
			// address:port
			hostAndPort,
			// Read timeout for reading payloads.
			time.Duration(5*time.Second),
			// Map of types to message handler.
			map[tlvserv.MTypeID]func(net.Conn, []byte){
				mtypeHello: handleHello,
				mtypeEcho:  handleEcho,
				mtypeReps:  handleReps,
			})

		if err != nil {
			log.Fatal(err)
		}

		return serv
	} else {
		log.Fatalf("TLV_SERVER_ADDR is not set")
	}

	return nil
}

// writeHdr writes the contents of a message header to conn.
func writeHdr(conn net.Conn, mtype tlvserv.MTypeID, datalen uint16) (int, error) {
	header := make([]byte, 4)

	header[0] = byte(mtype)
	header[1] = byte(mtype >> 8)
	header[2] = byte(datalen)
	header[3] = byte(datalen >> 8)

	return conn.Write(header)
}

// readResponse reads datalen bytes of data from conn and returns the data.
func readResponse(conn net.Conn, datalen uint16) ([]byte, error) {
	rb := 0
	tb := int(datalen)
	data := make([]byte, datalen)

	for rb < tb {
		nbytes, err := conn.Read(data[rb:])
		if err != nil {
			return nil, err
		}

		rb += nbytes
	}

	return data, nil
}

// TestMain is called in the main thread prior to any
func TestMain(m *testing.M) {
	msgServer = ExampleServer()
	if msgServer != nil {
		// Create a channel for signalling test success or failure from
		// message handler routines.
		statusChan = make(chan handlerStatus)
		// Run all the tests.
		code := m.Run()
		// Exit.
		os.Exit(code)
	} else {
		os.Exit(-1)
	}
}

func TestStats(t *testing.T) {
	conn, err := net.Dial("tcp", ":8080")
	if err == nil {
		for i := 0; i < testReps; i++ {
			writeHdr(conn, mtypeHello, 0)
			// Wait for completion of the message.
			select {
			case sm := <-statusChan:
				if sm.err != nil {
					t.Error(sm.err)
				}
			case <-time.After(10 * time.Millisecond):
				t.Errorf("timeout")
			}
		}

		stats := msgServer.GetStats(mtypeHello)
		if stats == nil {
			t.Errorf("couldn't get stats for type mtypeHello(%v)\n",
				mtypeHello)
		} else {
			if stats.NrCalls != testReps {
				t.Errorf("expected %v calls, only got %v\n",
					testReps, stats.NrCalls)
			}
		}

		conn.Close()
	} else {
		t.Errorf("couldn't connect to server: %v\n", err)
	}
}

func TestInvalidDataLen(t *testing.T) {
	conn, err := net.Dial("tcp", ":8080")
	if err == nil {
		// Send a mtypeHello message with a data length of 2, but don't
		// send any data.
		writeHdr(conn, mtypeHello, 2)
		select {
		case sm := <-statusChan:
			if sm.err != nil {
				t.Error(sm.err)
			} else {
				t.Error("handler should not have been invoked")
			}

		case <-time.After(10 * time.Second):
		}

		conn.Close()
	} else {
		t.Errorf("couldn't connect to server: %v\n", err)
	}
}

func Test2Msgs(t *testing.T) {
	conn, err := net.Dial("tcp", ":8080")
	if err == nil {
		writeHdr(conn, mtypeHello, 0)
		select {
		case sm := <-statusChan:
			if sm.err != nil {
				t.Error(sm.err)
			}

		case <-time.After(10 * time.Millisecond):
			t.Errorf("timeout")
		}

		conn.Close()
	} else {
		t.Errorf("couldn't connect to server: %v\n", err)
	}
}

// TestHelloMsg connects to the server and send a hello message.
func TestHelloMsg(t *testing.T) {
	conn, err := net.Dial("tcp", ":8080")
	if err == nil {
		writeHdr(conn, mtypeHello, 0)
		select {
		case sm := <-statusChan:
			if sm.err != nil {
				t.Error(sm.err)
			}

		case <-time.After(10 * time.Millisecond):
			t.Errorf("timeout")
		}

		conn.Close()
	} else {
		t.Errorf("couldn't connect to server: %v\n", err)
	}
}

// TestHelloMsg connects to the server and send a REPS message testReps times.
func TestRepetitionMsg(t *testing.T) {
	conn, err := net.Dial("tcp", ":8080")
	if err == nil {
		// Send testReps messages.
		for rep := 0; rep < testReps; rep++ {
			writeHdr(conn, mtypeReps, 0)
		}

		// Wait till the the handler function for mtypeReps is called
		// testReps times, or timeout after 1 second.
		select {
		case sm := <-statusChan:
			if sm.err != nil {
				t.Error(sm.err)
			} else {
				stats := msgServer.GetStats(mtypeReps)
				if stats == nil {
					t.Errorf("couldn't get stats for type mtypeReps(%v)\n",
						mtypeReps)
				} else {
					if stats.NrCalls != testReps {
						t.Errorf("expected %v calls, only got %v\n",
							testReps, stats.NrCalls)
					}
				}
			}

		case <-time.After(1 * time.Second):
			t.Errorf("timeout")
		}

		conn.Close()
	} else {
		t.Errorf("couldn't connect to server: %v\n", err)
	}
}

// TestEchoMsg connects to the server and send an echo message.
func TestEchoMsg(t *testing.T) {
	conn, err := net.Dial("tcp", ":8080")
	if err == nil {
		// Send the "echo" command.
		data := []byte("echodata")
		datalen := uint16(len(data))
		writeHdr(conn, mtypeEcho, datalen)
		conn.Write(data)

		// Wait 10 milliseconds before declaring an error, within 10ms
		// the handler for mtypeEcho should have been called.
		select {
		case sm := <-statusChan:
			if sm.err != nil {
				t.Error(sm.err)
			} else {
				// Handler has been called, we should be able to read the
				// response now.
				resp, err := readResponse(conn, datalen)
				if err != nil {
					t.Error(err)
				} else {
					if bytes.Compare(data, resp) != 0 {
						t.Errorf("\nresponse was: %v\nexpected: %v\n",
							resp, data)
					}
				}
			}

		case <-time.After(10 * time.Millisecond):
			t.Errorf("timeout")
		}

		conn.Close()
	} else {
		t.Errorf("couldn't connect to server: %v\n", err)
	}
}

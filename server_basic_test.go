package msgserv

import (
	"bytes"
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

const (
	MTYPE_HELLO uint16 = 1
	MTYPE_ECHO  uint16 = 2
	MTYPE_TEST1 uint16 = 3
	MTYPE_TEST2 uint16 = 4
)

const (
	TEST1_REPS = 1000
)

type HandlerStatus struct {
	mtype uint16
	err   error
}

var msgServer *MsgServ
var statusChan chan HandlerStatus
var test1Reps = 0
var test1Lock sync.RWMutex

func handleHello(conn net.Conn, data []byte) {
	var hs HandlerStatus

	hs.mtype = MTYPE_HELLO
	hs.err = nil
	statusChan <- hs
}

func handleEcho(conn net.Conn, data []byte) {
	var hs HandlerStatus

	conn.Write(data)
	hs.mtype = MTYPE_ECHO
	hs.err = nil
	statusChan <- hs
}

func handleTest1(conn net.Conn, data []byte) {
	test1Lock.Lock()
	test1Reps++

	if test1Reps == TEST1_REPS {
		var hs HandlerStatus

		test1Reps = 0
		hs.mtype = MTYPE_TEST1
		hs.err = nil
		statusChan <- hs
	}

	test1Lock.Unlock()
}

func startServer() *MsgServ {

	serv, err := MessageServer(
		"tcp",
		":8080",
		map[uint16]func(net.Conn, []byte){
			MTYPE_HELLO: handleHello,
			MTYPE_ECHO:  handleEcho,
			MTYPE_TEST1: handleTest1,
		})

	if err != nil {
		log.Fatal(err)
	}

	return serv
}

// writeHdr writes the contents of a message header to conn.
func writeHdr(conn net.Conn, mtype uint16, datalen uint16) (int, error) {
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
	msgServer = startServer()
	if msgServer != nil {
		// Create a channel for signalling test success or failure from
		// message handler routines.
		statusChan = make(chan HandlerStatus)
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
		for i := 0; i < 1000; i++ {
			writeHdr(conn, MTYPE_HELLO, 0)
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

		stats := msgServer.GetStats(MTYPE_HELLO)
		if stats == nil {
			t.Errorf("couldn't get stats for type MTYPE_HELLO(%v)\n",
				MTYPE_HELLO)
		} else {
			if stats.calls != 1000 {
				t.Errorf("expected 1000 calls, only got %v\n", stats.calls)
			}
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
		writeHdr(conn, MTYPE_HELLO, 0)
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
func TestRepetitionMsg(t *testing.T) {
	conn, err := net.Dial("tcp", ":8080")
	if err == nil {
		for rep := 0; rep < TEST1_REPS; rep++ {
			writeHdr(conn, MTYPE_TEST1, 0)
		}

		select {
		case sm := <-statusChan:
			if sm.err != nil {
				t.Error(sm.err)
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
		writeHdr(conn, MTYPE_ECHO, datalen)
		conn.Write(data)

		<-statusChan

		resp, err := readResponse(conn, datalen)
		if err != nil {
			t.Error(err)
		} else {
			if bytes.Compare(data, resp) != 0 {
				t.Errorf("\nresponse was: %v\nexpected: %v\n", resp, data)
			}
		}

		conn.Close()
	} else {
		t.Errorf("couldn't connect to server: %v\n", err)
	}
}

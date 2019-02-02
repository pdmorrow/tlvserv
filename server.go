package msgserv

import (
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type MsgStats struct {
	aveRuntime time.Duration
	totRuntime time.Duration
	calls      uint64
}

type MsgServ struct {
	messageHandlersLock sync.RWMutex
	messageHandlers     map[uint16]*MsgHandler
	network             string
	addr                string
}

type MsgHandler struct {
	stats   MsgStats
	handler func(net.Conn, []byte)
}

// All packets contain a 4 byte header:
// mtype uint16 (message type id)
// datalen uint16 (data payload length in bytes)
const HDR_LEN uint16 = 4

// readHeader reads from conn and deserializes a message header.  The
// message type and data length of the payload are returned.
func readHeader(conn net.Conn) (mtype uint16, datalen uint16, err error) {
	var _mtype uint16
	var _datalen uint16

	header, err := readData(conn, HDR_LEN)
	if err != nil {
		return 0, 0, err
	}

	_mtype = uint16(header[0]) | (uint16(header[1]) << 8)
	_datalen = uint16(header[2]) | (uint16(header[3]) << 8)

	return _mtype, _datalen, nil
}

// readData reads datalen bytes from conn.
func readData(conn net.Conn, datalen uint16) ([]byte, error) {
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

// handleRunner runs the message handler and updates the statistics for this
// message type.
func handlerRunner(msgHandler *MsgHandler, conn net.Conn, data []byte) {
	start := time.Now()
	msgHandler.handler(conn, data)
	msgHandler.stats.totRuntime += time.Since(start)
	msgHandler.stats.calls += 1
	msgHandler.stats.aveRuntime = msgHandler.stats.totRuntime / time.Duration(msgHandler.stats.calls)
}

// handleConnection reads messages from conn until the connection is closed.
// Based on the type of the message a registered handler is called in a new
// goroutine.
func handleConnection(ms *MsgServ, conn net.Conn) {
	var msgHandler *MsgHandler
	var msgHandlerFound bool

	defer conn.Close()

	log.Printf("%s connected\n", conn.RemoteAddr())

	for {
		// Block till there is data to read.
		mtype, datalen, readErr := readHeader(conn)
		if readErr != nil {
			if readErr == io.EOF {
				log.Printf("%s disconected\n", conn.RemoteAddr())
			} else {
				log.Println(readErr)
			}

			break
		}

		// datalen might be 0, but that's ok, we'll get back and empty array
		data, err := readData(conn, datalen)
		if err == nil {
			// Get the handler function for this message type (mtype)
			ms.messageHandlersLock.RLock()
			msgHandler, msgHandlerFound = ms.messageHandlers[mtype]
			ms.messageHandlersLock.RUnlock()
			if msgHandlerFound == true {
				go handlerRunner(msgHandler, conn, data)
			} else {
				log.Printf("missing handler for mtype %v\n", mtype)
			}
		} else {
			log.Println(err)
		}
	}
}

func acceptConnections(ms *MsgServ, schan chan error) {
	ln, err := net.Listen(ms.network, ms.addr)
	if err == nil {
		schan <- nil
		for {
			conn, err := ln.Accept()
			if err == nil {
				go handleConnection(ms, conn)
			} else {
				log.Println(err)
			}
		}
	} else {
		schan <- err
	}
}

func (ms *MsgServ) GetStats(mtype uint16) *MsgStats {
	msgInfo, ok := ms.messageHandlers[mtype]
	if ok == true {
		return &msgInfo.stats
	} else {
		return nil
	}
}

// MessageServer creates a new message server listening on the
// address specified on the address family specified in the network argument.
// The handlers map is a map from a message type to a handler function, the
// handler function for a given message type is called when a message of that
// type is received.  This function does not return but continually handles
// messages using non-blocking I/O.
func MessageServer(network string,
	address string,
	handlers map[uint16]func(net.Conn, []byte)) (*MsgServ, error) {

	// Create a map of mtype => MsgHandlers
	messageHandlers := make(map[uint16]*MsgHandler)
	for mtype, handlerFunc := range handlers {
		v := new(MsgHandler)
		v.handler = handlerFunc
		messageHandlers[mtype] = v
	}

	ms := new(MsgServ)
	ms.messageHandlers = messageHandlers
	ms.network = network
	ms.addr = address
	schan := make(chan error)
	go acceptConnections(ms, schan)
	err := <-schan
	if err == nil {
		return ms, nil
	} else {
		return nil, err
	}
}

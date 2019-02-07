// tlvserv is a library which allows you to quickly create a TLV (type length
// value) server.  A user of this library can create a server which listens
// on a TCP port, the server is passed a map of type (MTypeID) to message
// handler functions.  Once a message of a particular type of received the
// registered handler for that type is called.
package tlvserv

import (
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// Represents a message type ID.
type MTypeID uint16

// User message types should start from this base.
const MTypeIDUserStart MTypeID = 32

// Represents a group of statistics which are collected on a per message
// type basis.
type MTypeIDStats struct {
	AveRuntime time.Duration // Average runtime of the handler for this type.
	TotRuntime time.Duration // Total runtime of the handler for this type.
	NrCalls    uint64        // Number of times to handler has been invoked.
}

// Represents the server construct.
type TLVServ struct {
	mtypeHandlersLock sync.RWMutex           // RWLock for the map of handlers.
	mtypeHandlers     map[MTypeID]*mtypeInfo // Map of message handlers.
	network           string                 // IPConn address family.
	addr              string                 // addr:port to listen on.
	readTimeout       time.Duration          // Payload read timeout.
}

// Represents information relating to a single message type.
type mtypeInfo struct {
	stats     MTypeIDStats           // Statistics for this message type.
	handler   func(net.Conn, []byte) // Message handler routine.
	statsLock sync.RWMutex           // RWLock for the statistics element.
}

// All packets contain a 4 byte header, the lower 2 bytes correspond to the
// message type ID (uint16) and the upper 2 bytes (uint16) corresponds to the
// length of the payload which will be sent (a zero value means no data will
// be sent).
const MessageHdrLen uint16 = 4

// readHeader reads from conn and deserializes a message header.  The
// message type and data length of the payload are returned.
func readHeader(conn net.Conn) (mtype MTypeID, datalen uint16, err error) {
	var _mtype MTypeID
	var _datalen uint16

	// Block forever to read the fixed length header.
	header, err := readData(conn, MessageHdrLen, time.Duration(0))
	if err != nil {
		return 0, 0, err
	}

	_mtype = MTypeID(header[0]) | (MTypeID(header[1]) << 8)
	_datalen = uint16(header[2]) | (uint16(header[3]) << 8)

	return _mtype, _datalen, nil
}

// readData reads datalen bytes from conn.
func readData(conn net.Conn,
	datalen uint16,
	timeout time.Duration) ([]byte, error) {
	rb := 0
	tb := int(datalen)
	data := make([]byte, datalen)

	if timeout.Nanoseconds() != 0 {
		// The read operation will eventually timeout.
		conn.SetReadDeadline(time.Now().Add(timeout))
	} else {
		// The read operation will block forever whilst waiting for data.
		conn.SetReadDeadline(time.Time{})
	}

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
func handlerRunner(msgHandler *mtypeInfo, conn net.Conn, data []byte) {
	start := time.Now()
	// Run the handler for this message type.
	msgHandler.handler(conn, data)
	// Update statistics for this message type.
	msgHandler.statsLock.Lock()
	msgHandler.stats.TotRuntime += time.Since(start)
	msgHandler.stats.NrCalls += 1
	msgHandler.stats.AveRuntime = msgHandler.stats.TotRuntime / time.Duration(msgHandler.stats.NrCalls)
	msgHandler.statsLock.Unlock()
}

// handleConnection reads messages from conn until the connection is closed.
// Based on the type of the message a registered handler is called in a new
// goroutine.
func handleConnection(ms *TLVServ, conn net.Conn) {
	var msgHandler *mtypeInfo
	var ok bool

	defer conn.Close()

	log.Printf("%s connected\n", conn.RemoteAddr())

	for {
		// Block till there is data to read.
		mtype, datalen, readErr := readHeader(conn)
		if readErr != nil {
			if readErr == io.EOF {
				log.Printf("%s disconected\n", conn.RemoteAddr())
				break
			} else {
				log.Println(readErr)
			}
		}

		// datalen might be 0, but that's ok, we'll get back and empty array
		data, err := readData(conn, datalen, time.Duration(ms.readTimeout))
		if err == nil {
			// Get the handler function for this message type (mtype)
			ms.mtypeHandlersLock.RLock()
			msgHandler, ok = ms.mtypeHandlers[mtype]
			ms.mtypeHandlersLock.RUnlock()
			if ok == true {
				go handlerRunner(msgHandler, conn, data)
			} else {
				log.Printf("missing handler for mtype %v\n", mtype)
			}
		} else {
			log.Println(err)
		}
	}
}

// acceptConnections listens for new connections on the TLVServ.  If we are
// able to listen on the address specified by ms then unblock the calling
// goroutine by sending nil back via the schan (status channel), otherwise
// an error is sent back via the channel.  Once the routine is listening for
// new connections it blocks until a new connection is detected.  On detecting
// a new connection a new goroutine is spawned to handle all further
// interactions with this client.  The spawned goroutine will exit if the
// client disconnects or if an unrecoverable error occurs.
func acceptConnections(ms *TLVServ, schan chan error) {
	ln, err := net.Listen(ms.network, ms.addr)
	if err == nil {
		// Tell the caller the server is up and running.
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
		// Tell the caller the server is NOT up and running.
		schan <- err
	}
}

// GetStats returns the latest statistics for the message type specified by
// the mtype parameter.
func (ms *TLVServ) GetStats(mtype MTypeID) *MTypeIDStats {
	ms.mtypeHandlersLock.RLock()
	msgInfo, ok := ms.mtypeHandlers[mtype]
	ms.mtypeHandlersLock.RUnlock()
	if ok == true {
		// Return a copy of the statistics object to avoid the caller needing
		// to care about synchronization between themselves and the
		// handlerRunner goroutine which is responsible for updating
		// statistics.
		statsCopy := new(MTypeIDStats)
		msgInfo.statsLock.RLock()
		*statsCopy = msgInfo.stats
		msgInfo.statsLock.RUnlock()
		return statsCopy
	} else {
		// The message type doesn't exist.
		return nil
	}
}

// Server() creates a new message server listening on the address specified
// in the address parameter and on network type specified in the network
// argument.  The handlers map is a map from a message type to a handler
// function, the handler function for a given message type is called when a
// message of that type is received.  This function does not return but
// continually handles messages using non-blocking I/O.
//
// Known networks are "tcp", "tcp4" (IPv4-only), "tcp6" (IPv6-only),
func Server(network string,
	address string,
	readTimeout time.Duration,
	handlers map[MTypeID]func(net.Conn, []byte)) (*TLVServ, error) {

	// Create a map of mtype => mtypeInfos
	mtypeHandlers := make(map[MTypeID]*mtypeInfo)
	for mtype, handlerFunc := range handlers {
		v := new(mtypeInfo)
		v.handler = handlerFunc
		mtypeHandlers[mtype] = v
	}

	ms := new(TLVServ)
	ms.mtypeHandlers = mtypeHandlers
	ms.network = network
	ms.addr = address
	ms.readTimeout = readTimeout
	schan := make(chan error)
	go acceptConnections(ms, schan)
	// Wait till the acceptConnections goroutine is listening for connections.
	err := <-schan
	if err != nil {
		return nil, err
	}

	// All fine, return the TLVServ object.
	return ms, nil
}

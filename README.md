# tlvserv

A really simple TLV based message server written in go.  Message handlers can
be registered for a given message type, a handler is spawned in a new goroutine
if a message with a given type arrives.  Clients for the server can be built
using the tlvclient package.

Set TLV_SERVER_ADDR to "address:port" prior to running "go test"

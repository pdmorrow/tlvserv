# tlvserv

A really simple TLV based message server written in go.  Message handlers can
be registered for a given message type, a handler is spawned in a new goroutine
if a message with a given type arrives.  Clients to the server can be built
using the tlvclient package.

For more information see https://godoc.org/github.com/pdmorrow/tlvserv.

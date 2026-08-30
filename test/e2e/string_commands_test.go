package e2e

import "testing"

func TestBatchAndStringCommands(t *testing.T) {
	c := dial(t)
	a, b := key(t, "a"), key(t, "b")
	sendRecvExact(t, c, cmd("MSET", a, "1", b, "two"), "+OK\r\n")
	sendRecvExact(t, c, cmd("MGET", a, key(t, "missing"), b), "*3\r\n$1\r\n1\r\n$-1\r\n$3\r\ntwo\r\n")
	sendRecvExact(t, c, cmd("INCR", a), ":2\r\n")
	sendRecvExact(t, c, cmd("DECR", a), ":1\r\n")
	sendRecvExact(t, c, cmd("APPEND", b, "!"), ":4\r\n")
	sendRecvExact(t, c, cmd("STRLEN", b), ":4\r\n")
}

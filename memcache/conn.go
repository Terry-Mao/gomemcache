package memcache

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	itime "github.com/Terry-Mao/marmot/time"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	timer = itime.NewTimer(512)
)

// conn is the low-level implementation of Conn
type conn struct {
	// Shared
	mu   sync.Mutex
	err  error
	conn net.Conn
	// Read
	readTimeout time.Duration
	br          *bufio.Reader
	// Write
	writeTimeout time.Duration
	bw           *bufio.Writer
	// Scratch space for formatting integers and floats.
	numScratch [40]byte
	// timer
	timer *itime.Timer
	td    *itime.TimerData
}

// DialOption specifies an option for dialing a Memcache server.
type DialOption struct {
	f func(*dialOptions)
}

type dialOptions struct {
	timer        *itime.Timer
	readTimeout  time.Duration
	writeTimeout time.Duration
	dial         func(network, addr string) (net.Conn, error)
}

// DialReadTimeout specifies the timeout for reading a single command reply.
func DialReadTimeout(d time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		do.readTimeout = d
	}}
}

// DialWriteTimeout specifies the timeout for writing a single command.
func DialWriteTimeout(d time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		do.writeTimeout = d
	}}
}

// DialConnectTimeout specifies the timeout for connecting to the Memcache server.
func DialConnectTimeout(d time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		dialer := net.Dialer{Timeout: d}
		do.dial = dialer.Dial
	}}
}

// DialNetDial specifies a custom dial function for creating TCP
// connections. If this option is left out, then net.Dial is
// used. DialNetDial overrides DialConnectTimeout.
func DialNetDial(dial func(network, addr string) (net.Conn, error)) DialOption {
	return DialOption{func(do *dialOptions) {
		do.dial = dial
	}}
}

// DialTimer specifies a caller's timer.
func DialTimer(timer *itime.Timer) DialOption {
	return DialOption{func(do *dialOptions) {
		do.timer = timer
	}}
}

// Dial connects to the Memcache server at the given network and
// address using the specified options.
func Dial(network, address string, options ...DialOption) (Conn, error) {
	do := dialOptions{
		dial:  net.Dial,
		timer: timer,
	}
	for _, option := range options {
		option.f(&do)
	}

	netConn, err := do.dial(network, address)
	if err != nil {
		return nil, err
	}
	c := &conn{
		conn:         netConn,
		bw:           bufio.NewWriter(netConn),
		br:           bufio.NewReader(netConn),
		readTimeout:  do.readTimeout,
		writeTimeout: do.writeTimeout,
		timer:        do.timer,
	}

	return c, nil
}

// NewConn returns a new gomemcache connection for the given net connection.
func NewConn(netConn net.Conn, readTimeout, writeTimeout time.Duration) Conn {
	return &conn{
		conn:         netConn,
		bw:           bufio.NewWriter(netConn),
		br:           bufio.NewReader(netConn),
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
	}
}

func (c *conn) Close() error {
	c.mu.Lock()
	err := c.err
	if c.err == nil {
		c.err = errors.New("gomemcache: closed")
		err = c.conn.Close()
	}
	c.mu.Unlock()
	return err
}

func (c *conn) fatal(err error) error {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
		// Close connection to force errors on subsequent calls and to unblock
		// other reader or writer.
		c.conn.Close()
	}
	c.mu.Unlock()
	return c.err
}

func (c *conn) Err() error {
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()
	return err
}

func (c *conn) writeStoreCommand(cmd, key string, value []byte, flags uint32, timeout int32, cas uint64) (err error) {
	if len(value) > 1000000 {
		return protocolError("max value size, greate than 1mb")
	}
	// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
	c.bw.WriteString(cmd)
	c.bw.WriteByte(space)

	c.bw.WriteString(key)
	c.bw.WriteByte(space)

	c.bw.Write(strconv.AppendUint(c.numScratch[:0], uint64(flags), 10))
	c.bw.WriteByte(space)

	c.bw.Write(strconv.AppendInt(c.numScratch[:0], int64(timeout), 10))
	c.bw.WriteByte(space)

	c.bw.Write(strconv.AppendInt(c.numScratch[:0], int64(len(value)), 10))
	if cas != 0 {
		c.bw.WriteByte(space)
		c.bw.Write(strconv.AppendUint(c.numScratch[:0], cas, 10))
	}
	c.bw.Write(crlf)
	// <data block>\r\n
	c.bw.Write(value)
	_, err = c.bw.Write(crlf)
	return
}

func (c *conn) writeGetCommand(cmd string, keys []string) (err error) {
	// get(s) <key>*\r\n
	_, err = c.bw.WriteString(cmd)
	for _, key := range keys {
		if err != nil {
			break
		}
		c.bw.WriteByte(space)
		_, err = c.bw.WriteString(key)
	}
	_, err = c.bw.Write(crlf)
	return
}

func (c *conn) writeIncrDecrCommand(cmd, key string, delta uint64) (err error) {
	// incr/decr key delta\r\n
	c.bw.WriteString(cmd)
	c.bw.WriteByte(space)
	c.bw.WriteString(key)
	c.bw.WriteByte(space)
	c.bw.Write(strconv.AppendUint(c.numScratch[:0], delta, 10))
	_, err = c.bw.Write(crlf)
	return
}

func (c *conn) writeDeleteCommand(keys []string) (err error) {
	// delete <key>*\r\n
	c.bw.WriteString("delete")
	err = c.bw.WriteByte(space)
	for _, key := range keys {
		if err != nil {
			break
		}
		c.bw.WriteByte(space)
		_, err = c.bw.WriteString(key)
	}
	_, err = c.bw.Write(crlf)
	return
}

type protocolError string

func (pe protocolError) Error() string {
	return fmt.Sprintf("gomemcache: %s (possible server error or unsupported concurrent read by application)", string(pe))
}

func (c *conn) readLine() ([]byte, error) {
	p, err := c.br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		return nil, protocolError("long response line")
	}
	if err != nil {
		return nil, err
	}
	i := len(p) - 2
	if i < 0 || p[i] != '\r' {
		return nil, protocolError("bad response line terminator")
	}
	line := p[:i]
	if len(line) == 0 {
		return nil, protocolError("short response line")
	}
	return line, nil
}

var (
	crlf           = []byte("\r\n")
	space          = byte(' ')
	spaceStr       = string(" ")
	replyOK        = []byte("OK")
	replyStored    = []byte("STORED")
	replyNotStored = []byte("NOT_STORED")
	replyExists    = []byte("EXISTS")
	replyNotFound  = []byte("NOT_FOUND")
	replyDeleted   = []byte("DELETED")
	replyEnd       = []byte("END")
	replyOk        = []byte("OK")
	replyTouched   = []byte("TOUCHED")
	replyValue     = []byte("VALUE")
	replyValueStr  = "VALUE"

	replyClientErrorPrefix = []byte("CLIENT_ERROR ")
)

func (c *conn) readGetReply(cb func(*Reply)) (err error) {
	var line []byte
	for {
		if line, err = c.readLine(); err != nil {
			return c.fatal(err)
		}
		if bytes.Equal(line, replyEnd) {
			return
		}
		// VALUE <key> <flags> <bytes> [<cas unique>]\r\n
		chunks := strings.Split(string(line), spaceStr)
		if len(chunks) < 4 {
			return protocolError("corrupt get reply")
		}
		if chunks[0] != replyValueStr {
			return protocolError("corrupt get reply, no except VALUE")
		}
		reply := new(Reply)
		reply.Key = chunks[1]
		flags64, err := strconv.ParseUint(chunks[2], 10, 32)
		if err != nil {
			return err
		}
		reply.Flags = uint32(flags64)
		size, err := strconv.ParseUint(chunks[3], 10, 64)
		if err != nil {
			return err
		}
		if len(chunks) > 4 {
			if reply.Cas, err = strconv.ParseUint(chunks[4], 10, 64); err != nil {
				return err
			}
		}
		// <data block>\r\n
		b := make([]byte, size+2)
		if _, err = io.ReadFull(c.br, b); err != nil {
			return c.fatal(err)
		}
		reply.Value = b[:size]
		cb(reply)
	}

	return
}

func (c *conn) readStoreReply() error {
	line, err := c.readLine()
	if err != nil {
		return c.fatal(err)
	}
	switch {
	case bytes.Equal(line, replyStored):
		return nil
	case bytes.Equal(line, replyNotStored):
		return ErrNotStored
	case bytes.Equal(line, replyExists):
		return ErrExists
	case bytes.Equal(line, replyNotFound):
		return ErrNotFound
	}
	return protocolError("unexpected response line")
}

func (c *conn) readIncrDecrReply() (uint64, error) {
	line, err := c.readLine()
	if err != nil {
		return 0, c.fatal(err)
	}
	switch {
	case bytes.Equal(line, replyNotFound):
		return 0, ErrNotFound
	case bytes.HasPrefix(line, replyClientErrorPrefix):
		errMsg := line[len(replyClientErrorPrefix):]
		return 0, protocolError(errMsg)
	}
	if val, err := strconv.ParseUint(string(line), 10, 64); err != nil {
		return 0, err
	} else {
		return val, nil
	}
}

func (c *conn) readDeleteReply() error {
	line, err := c.readLine()
	if err != nil {
		return c.fatal(err)
	}
	switch {
	case bytes.Equal(line, replyOK):
		return nil
	case bytes.Equal(line, replyDeleted):
		return nil
	case bytes.Equal(line, replyNotFound):
		return ErrNotFound
	}
	return protocolError(string(line))
}

func (c *conn) startWriteDeadline() {
	if c.writeTimeout != 0 {
		// c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		c.td = c.timer.Start(c.writeTimeout, func() {
			c.fatal(ErrWriteTimeout)
		})
	}
}

func (c *conn) stopWriteDeadline() {
	if c.writeTimeout != 0 {
		c.td.Stop()
	}
}

func (c *conn) startReadDeadline() {
	if c.readTimeout != 0 {
		// c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		c.td = c.timer.Start(c.readTimeout, func() {
			c.fatal(ErrReadTimeout)
		})
	}
}

func (c *conn) stopReadDeadline() {
	if c.readTimeout != 0 {
		c.td.Stop()
	}
}

func (c *conn) Store(cmd, key string, value []byte, flags uint32, timeout int32, cas uint64) (err error) {
	if cmd == "" {
		return nil
	}

	c.startWriteDeadline()
	if err = c.writeStoreCommand(cmd, key, value, flags, timeout, cas); err == nil {
		err = c.bw.Flush()
	}
	c.stopWriteDeadline()
	if err != nil {
		return c.fatal(err)
	}

	c.startReadDeadline()
	err = c.readStoreReply()
	c.stopReadDeadline()
	return
}

func (c *conn) Get(cmd string, cb func(*Reply), keys ...string) (err error) {
	if cmd == "" {
		return nil
	}

	c.startWriteDeadline()
	if err = c.writeGetCommand(cmd, keys); err == nil {
		err = c.bw.Flush()
	}
	c.stopWriteDeadline()
	if err != nil {
		return c.fatal(err)
	}

	c.startReadDeadline()
	err = c.readGetReply(cb)
	c.stopReadDeadline()
	return
}

func (c *conn) IncrDecr(cmd string, key string, delta uint64) (val uint64, err error) {
	if cmd == "" {
		return 0, nil
	}

	c.startWriteDeadline()
	if err = c.writeIncrDecrCommand(cmd, key, delta); err == nil {
		err = c.bw.Flush()
	}
	c.stopWriteDeadline()
	if err != nil {
		return 0, c.fatal(err)
	}

	c.startReadDeadline()
	val, err = c.readIncrDecrReply()
	c.stopReadDeadline()
	return
}

func (c *conn) Delete(keys ...string) (err error) {
	c.startWriteDeadline()
	if err = c.writeDeleteCommand(keys); err == nil {
		err = c.bw.Flush()
	}
	c.stopWriteDeadline()
	if err != nil {
		return c.fatal(err)
	}

	c.startReadDeadline()
	err = c.readDeleteReply()
	c.stopReadDeadline()
	return
}

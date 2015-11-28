package memcache

import (
	"bytes"
	"testing"
)

func TestPool(t *testing.T) {
	p := NewPool(func() (Conn, error) {
		conn, err := Dial("tcp", "172.16.13.86:11211")
		if err != nil {
			return nil, err
		}
		return conn, nil
	}, 5)
	c := p.Get()
	defer c.Close()
	// set
	if err := c.Store("set", "test", []byte("test"), 0, 60, 0); err != nil {
		t.Errorf("Store() error(%v)", err)
		t.FailNow()
	}
	// get
	if replies, err := c.Get("get", "test"); err != nil {
		t.Errorf("Get() error(%v)", err)
		t.FailNow()
	} else {
		reply := replies[0]
		if reply.Key != "test" || !bytes.Equal(reply.Value, []byte("test")) || reply.Flags != 0 {
			t.Error("Get() error, value")
			t.FailNow()
		}
	}
	c = p.Get()
	defer c.Close()
	// set
	if err := c.Store("set", "test", []byte("test"), 0, 60, 0); err != nil {
		t.Errorf("Store() error(%v)", err)
		t.FailNow()
	}
	// get
	if replies, err := c.Get("get", "test"); err != nil {
		t.Errorf("Get() error(%v)", err)
		t.FailNow()
	} else {
		reply := replies[0]
		if reply.Key != "test" || !bytes.Equal(reply.Value, []byte("test")) || reply.Flags != 0 {
			t.Error("Get() error, value")
			t.FailNow()
		}
	}
}

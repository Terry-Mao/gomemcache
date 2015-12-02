package memcache

import (
	"bytes"
	"testing"
)

func TestConn(t *testing.T) {
	conn, err := Dial("tcp", "172.16.13.86:11211")
	if err != nil {
		t.Errorf("Dial() error(%v)", err)
		t.FailNow()
	}
	conn.Delete("delete", "test", "test1", "test2")
	// set
	if err = conn.Store("set", "test", []byte("test"), 0, 60, 0); err != nil {
		t.Errorf("Store() error(%v)", err)
		t.FailNow()
	}
	// get
	if err = conn.Get("get", func(r *Reply) {
		if r.Key != "test" || !bytes.Equal(r.Value, []byte("test")) || r.Flags != 0 {
			t.Error("Get() error, value")
			t.FailNow()
		}
	}, "test"); err != nil {
		t.Errorf("Get() error(%v)", err)
		t.FailNow()
	}
	// get not exist
	if err = conn.Get("get", func(r *Reply) {
		t.Error("Get() not exist callback")
		t.FailNow()
	}, "not_exist"); err != nil {
		t.Errorf("Get() error(%v)", err)
		t.FailNow()
	}
	// gets
	if err = conn.Get("get", func(r *Reply) {
		if r.Key != "test" || !bytes.Equal(r.Value, []byte("test")) || r.Flags != 0 {
			t.Error("Get() error, value")
			t.FailNow()
		}
	}, "test", "test1"); err != nil {
		t.Errorf("Get() error(%v)", err)
		t.FailNow()
	}
	// set
	if err = conn.Store("set", "test1", []byte("test"), 0, 60, 0); err != nil {
		t.Errorf("Store() error(%v)", err)
		t.FailNow()
	}
	replies := make([]*Reply, 0, 2)
	if err = conn.Get("get", func(r *Reply) {
		replies = append(replies, r)
	}, "test", "test1"); err != nil {
		t.Errorf("Get() error(%v)", err)
		t.FailNow()
	} else {
		if len(replies) != 2 {
			t.Error("Get() error, length")
		}
		reply := replies[0]
		if reply.Key != "test" || !bytes.Equal(reply.Value, []byte("test")) || reply.Flags != 0 {
			t.Error("Get() error, value")
			t.FailNow()
		}
		reply = replies[1]
		if reply.Key != "test1" || !bytes.Equal(reply.Value, []byte("test")) || reply.Flags != 0 {
			t.Error("Get() error, value")
			t.FailNow()
		}
	}
	// set
	if err = conn.Store("set", "test2", []byte("0"), 0, 60, 0); err != nil {
		t.Errorf("Store() error(%v)", err)
		t.FailNow()
	}
	// incr
	if d, err := conn.IncrDecr("incr", "test2", 4); err != nil {
		t.Errorf("Store() error(%v)", err)
		t.FailNow()
	} else {
		if d != 4 {
			t.Error("IncrDecr value error")
			t.FailNow()
		}
	}
	// decr
	if d, err := conn.IncrDecr("decr", "test2", 3); err != nil {
		t.Errorf("Store() error(%v)", err)
		t.FailNow()
	} else {
		if d != 1 {
			t.Error("IncrDecr value error")
			t.FailNow()
		}
	}
	// get
	if err = conn.Get("get", func(r *Reply) {
		if r.Key != "test2" || !bytes.Equal(r.Value, []byte("1")) || r.Flags != 0 {
			t.Error("Get() error, value")
			t.FailNow()
		}
	}, "test2"); err != nil {
		t.Errorf("Get() error(%v)", err)
		t.FailNow()
	}
}

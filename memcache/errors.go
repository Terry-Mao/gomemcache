package memcache

import (
	"errors"
)

var (
	ErrReadTimeout  = errors.New("gomemcache: read timeout")
	ErrWriteTimeout = errors.New("gomemcache: write timeout")
	ErrNotFound     = errors.New("gomemcache: key not found")
	ErrExists       = errors.New("gomemcache: key exists")
	ErrNotStored    = errors.New("gomemcache: key not stored")

	// ErrPoolExhausted is returned from a pool connection method (Store, Get,
	// Delete, IncrDecr, Err) when the maximum number of database connections
	// in the pool has been reached.
	ErrPoolExhausted = errors.New("gomemcache: connection pool exhausted")
	ErrPoolClosed    = errors.New("gomemcache: connection pool closed")
	ErrConnClosed    = errors.New("gomemcache: connection closed")
)

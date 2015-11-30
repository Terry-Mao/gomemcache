package memcache

import (
	"errors"
)

var (
	ErrNotFound  = errors.New("key not found")
	ErrExists    = errors.New("key exists")
	ErrNotStored = errors.New("key not stored")
)

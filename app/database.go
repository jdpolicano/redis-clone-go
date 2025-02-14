package main

import (
	"sync"
	"time"
)

type SharedRWStore[T any] struct {
	lock  sync.RWMutex
	store map[string]T
}

func NewSharedStore[T any]() *SharedRWStore[T] {
	return &SharedRWStore[T]{sync.RWMutex{}, map[string]T{}}
}

type Timestamp struct {
	Created     time.Time
	LastTouched time.Time
	Expiry      time.Time // for our purposes, a zero expiry duration indicates infitinite lifetime
}

func NewKVStore() *SharedRWStore[RespValue] {
	return &SharedRWStore[RespValue]{sync.RWMutex{}, map[string]RespValue{}}
}

func NewExpiryStore() *SharedRWStore[Timestamp] {
	return &SharedRWStore[Timestamp]{sync.RWMutex{}, map[string]Timestamp{}}
}

func NewServerConfig() *SharedRWStore[string] {
	return &SharedRWStore[string]{sync.RWMutex{}, map[string]string{}}
}

func NewTimestamp(ttl time.Duration) Timestamp {
	now := time.Now()
	return Timestamp{now, now, now.Add(ttl)}
}

func NewTimestampFromExpiry(expiry time.Time) Timestamp {
	now := time.Now()
	return Timestamp{now, now, expiry}
}

func (db *SharedRWStore[T]) Set(key string, value T) (T, bool) {
	db.lock.Lock()
	defer db.lock.Unlock()
	v, e := db.store[key]
	db.store[key] = value
	return v, e
}

func (db *SharedRWStore[T]) Get(key string) (T, bool) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	el, exists := db.store[key]
	return el, exists
}

func (db *SharedRWStore[T]) Delete(key string) (T, bool) {
	db.lock.Lock()
	defer db.lock.Unlock()
	v, e := db.store[key]
	delete(db.store, key)
	return v, e
}

func (db *SharedRWStore[T]) Lock(key string) {
	db.lock.Lock()
}

func (db *SharedRWStore[T]) RLock(key string) {
	db.lock.RLock()
}

func (db *SharedRWStore[T]) Unlock(key string) {
	db.lock.Unlock()
}

func (db *SharedRWStore[T]) RUnlock(key string) {
	db.lock.RUnlock()
}

func (db *SharedRWStore[T]) Keys() []string {
	db.lock.RLock()
	defer db.lock.RUnlock()
	keys := make([]string, 0, len(db.store))
	for key := range db.store {
		keys = append(keys, key)
	}
	return keys
}

func (ts Timestamp) Expired() bool {
	if ts.Created.After(ts.Expiry) {
		return true
	}
	return false
}

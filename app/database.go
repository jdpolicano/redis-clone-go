package main

import (
	"sync"
	"time"
)

type Database struct {
	lock  sync.Mutex
	store map[string]*DBEntry
}

func NewDatabase() *Database {
	return &Database{sync.Mutex{}, map[string]*DBEntry{}}
}

type DBEntry struct {
	Value RespValue
	TS    Timestamp
}

func NewDBEntry(v RespValue, ttl time.Duration) *DBEntry {
	return &DBEntry{v, NewTimestamp(ttl)}
}

func (entry *DBEntry) Touch() {
	entry.TS.LastTouched = time.Now()
}

func (entry *DBEntry) SwapInplace(v RespValue, ttl time.Duration) {
	entry.Value = v
	entry.TS = NewTimestamp(ttl)
}

func (entry *DBEntry) Expired() bool {
	if entry.TS.Expiry == 0 {
		return false
	}
	return entry.TS.Created.Add(entry.TS.Expiry).Before(time.Now())
}

type Timestamp struct {
	Created     time.Time
	LastTouched time.Time
	Expiry      time.Duration // for our purposes, a zero expiry duration indicates infitinite lifetime
}

func NewTimestamp(ttl time.Duration) Timestamp {
	now := time.Now()
	return Timestamp{now, now, ttl}
}

func (db *Database) Set(key string, value RespValue, ttl time.Duration) {
	db.lock.Lock()
	defer db.lock.Unlock()
	// update in place to save an alloc hopefully
	if entry, exists := db.store[key]; exists {
		entry.SwapInplace(value, ttl)
		return
	}
	entry := NewDBEntry(value, ttl)
	db.store[key] = entry
	return
}

// This needs to be carefully executed to avoid a lock contention with itself
func (db *Database) Get(key string) (RespValue, bool) {
	var none RespValue
	db.lock.Lock()
	defer db.lock.Unlock()
	el, exists := db.store[key]
	if exists {
		if el.Expired() {
			delete(db.store, key)
			return none, false
		}
		el.Touch()
		return el.Value, exists
	}
	return none, exists
}

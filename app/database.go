package main

import "sync"

type Database struct {
	name  string
	lock  sync.RWMutex
	store map[string]RespValue
}

func NewDatabase(name string) Database {
	return Database{name, sync.RWMutex{}, map[string]RespValue{}}
}

func (db *Database) Set(key string, value RespValue) {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.store[key] = value
}

func (db *Database) Get(key string) (RespValue, bool) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	el, exists := db.store[key]
	return el, exists
}

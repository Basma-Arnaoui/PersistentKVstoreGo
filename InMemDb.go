package main

import (
	"errors"
	"fmt"
	"github.com/elliotchance/orderedmap"
	"io"
	"sync"
)

type Cmd int

const (
	Get Cmd = iota
	Set
	Del
	Ext
	Unk
)

type Error int

func (e Error) Error() string {
	return "Empty command"
}

const (
	Empty Error = iota
)

type operation int

const (
	set operation = iota
	del
)

type entry struct {
	value interface{}
	op    operation
}

type DB interface {
	SetMap(key []byte, value []byte) error
	GetMap(key []byte) ([]byte, error)
	DelMap(key []byte) ([]byte, error)
}

type memDB struct {
	values *orderedmap.OrderedMap
	mu     sync.Mutex
	wal    *walFile
}

func (mem *memDB) SetMap(key, value []byte) error {

	mem.values.Set(string(key), entry{value: value, op: set})

	return nil
}

func (mem *memDB) GetMap(key []byte) ([]byte, error) {

	if v, ok := mem.values.Get(string(key)); ok {
		entry := v.(entry)
		if entry.op == del {
			return nil, errors.New("Key not found")
		}
		fmt.Println(mem.values.Len())
		return entry.value.([]byte), nil
	}

	return nil, errors.New("Key not found")
}

func (mem *memDB) DelMap(key []byte) ([]byte, error) {

	if v, ok := mem.values.Get(string(key)); ok {
		oldEntry := v.(entry)

		// Update the entry with the delete operation
		newEntry := entry{value: oldEntry.value, op: del}
		mem.values.Set(string(key), newEntry)

		return oldEntry.value.([]byte), nil
	}

	return nil, errors.New("Key not found")
}

func (mem *memDB) flushTrigger() {
	mem.mu.Lock()
	defer mem.mu.Unlock()

	// Check if the size of the ordered map exceeds 5
	if mem.values.Len() >= flushThreshold {
		err := mem.flushToSSTFromMap()
		if err != nil {
			fmt.Println("Error flushing to SST:", err)
		}
	}
}

type Repl struct {
	db      DB
	handler handler
	in      io.Reader
	out     io.Writer
}

package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/elliotchance/orderedmap"
	"io"
	"os"
	"strings"
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
	Set(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Del(key []byte) ([]byte, error)
}

type memDB struct {
	values *orderedmap.OrderedMap
	mu     sync.Mutex
	wal    *walFile
}

func (mem *memDB) Set(key, value []byte) error {
	mem.mu.Lock()
	defer mem.mu.Unlock()

	mem.values.Set(string(key), entry{value: value, op: set})
	err := writeWAL(mem.wal.file, byte(Set), key, value)
	if err != nil {
		return err
	}
	fmt.Println("OK")

	return nil
}

func (mem *memDB) Get(key []byte) ([]byte, error) {
	mem.mu.Lock()
	defer mem.mu.Unlock()

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

func (mem *memDB) Del(key []byte) ([]byte, error) {
	mem.mu.Lock()
	defer mem.mu.Unlock()

	if v, ok := mem.values.Get(string(key)); ok {
		oldEntry := v.(entry)

		// Update the entry with the delete operation
		newEntry := entry{value: oldEntry.value, op: del}
		mem.values.Set(string(key), newEntry)

		if oldEntry.op != del {
			err := writeWAL(mem.wal.file, byte(Del), []byte(key), oldEntry.value.([]byte))
			if err != nil {
				return nil, err
			}
		}

		fmt.Println("OK")
		return oldEntry.value.([]byte), nil
	}

	return nil, errors.New("Key not found")
}

func NewInMem(walFileName string) (*memDB, error) {
	walFileInstance, err := os.Create(walFileName)
	if err != nil {
		return nil, err
	}

	return &memDB{
		values: orderedmap.NewOrderedMap(),
		wal:    &walFile{file: walFileInstance},
	}, nil
}

type Repl struct {
	db DB

	in  io.Reader
	out io.Writer
}

func (re *Repl) parseCmd(buf []byte) (Cmd, []string, error) {
	line := string(buf)
	elements := strings.Fields(line)
	if len(elements) < 1 {
		return Unk, nil, Empty
	}

	switch elements[0] {
	case "get":
		return Get, elements[1:], nil
	case "set":
		return Set, elements[1:], nil
	case "del":
		return Del, elements[1:], nil
	case "exit":
		return Ext, nil, nil
	default:
		return Unk, nil, nil
	}
}

func (re *Repl) Start() {
	scanner := bufio.NewScanner(re.in)

	for {
		fmt.Fprint(re.out, "> ")
		if !scanner.Scan() {
			break
		}
		buf := scanner.Bytes()
		cmd, elements, err := re.parseCmd(buf)
		if err != nil {
			fmt.Fprintf(re.out, "%s\n", err.Error())
			continue
		}
		switch cmd {
		case Get:
			if len(elements) != 1 {
				fmt.Fprintf(re.out, "Expected 1 argument, received: %d\n", len(elements))
				continue
			}
			v, err := re.db.Get([]byte(elements[0]))
			if err != nil {
				fmt.Fprintln(re.out, err.Error())
				continue
			}
			fmt.Fprintln(re.out, string(v))
		case Set:
			if len(elements) != 2 {
				fmt.Printf("Expected 2 arguments, received: %d\n", len(elements))
				continue
			}
			err := re.db.Set([]byte(elements[0]), []byte(elements[1]))
			if err != nil {
				fmt.Fprintln(re.out, err.Error())
				continue
			}
		case Del:
			if len(elements) != 1 {
				fmt.Printf("Expected 1 argument, received: %d\n", len(elements))
				continue
			}
			v, err := re.db.Del([]byte(elements[0]))
			if err != nil {
				fmt.Fprintln(re.out, err.Error())
				continue
			}
			fmt.Fprintln(re.out, string(v))
		case Ext:
			fmt.Fprintln(re.out, "Bye!")
			return
		case Unk:
			fmt.Fprintln(re.out, "Unknown command")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(re.out, err.Error())
	} else {
		fmt.Fprintln(re.out, "Bye!")
	}
}

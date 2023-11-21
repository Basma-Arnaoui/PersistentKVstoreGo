package PersistentKVstoreGo

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/elliotchance/orderedmap"
	"io"
	"os"
	"strings"
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

type DB interface {
	Set(key []byte, value []byte) error

	Get(key []byte) ([]byte, error)

	Del(key []byte) ([]byte, error)
}

type memDB struct {
	values *orderedmap.OrderedMap
}

func (mem *memDB) Set(key, value []byte) error {
	mem.values.Set(string(key), value)
	return nil
}

func (mem *memDB) Get(key []byte) ([]byte, error) {
	if v, ok := mem.values.Get(string(key)); ok {
		return v.([]byte), nil
	}

	return nil, errors.New("Key not found")
}

func (mem *memDB) Del(key []byte) ([]byte, error) {
	if v, ok := mem.values.Get(string(key)); ok {
		mem.values.Delete(string(key))
		return v.([]byte), nil
	}
	return nil, errors.New("Key doesn't exist")
}

func NewInMem() *memDB {
	return &memDB{
		values: orderedmap.NewOrderedMap(),
	}
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
				fmt.Fprintf(re.out, "Expected 1 arguments, received: %d\n", len(elements))
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
				fmt.Printf("Expected 1 arguments, received: %d\n", len(elements))
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
			fmt.Fprintln(re.out, "Unkown command")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(re.out, err.Error())
	} else {
		fmt.Fprintln(re.out, "Bye!")
	}
}

func main() {
	db := NewInMem()
	repl := &Repl{
		db:  db,
		in:  os.Stdin,
		out: os.Stdout,
	}
	repl.Start()
}

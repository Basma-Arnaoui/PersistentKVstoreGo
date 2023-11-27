package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/elliotchance/orderedmap"
	"io"
	"os"
	"strings"
)

type handler interface {
	Set(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Del(key []byte) ([]byte, error)
}

func (mem *memDB) Set(key, value []byte) error {
	mem.mu.Lock()
	defer mem.mu.Unlock()
	err := mem.SetMap(key, value)
	if err != nil {
		return err
	}
	err = writeWAL(mem.wal.file, byte(Set), key, value)
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
	//MODIFY THIS TO GET FROM SST FILES

	return nil, errors.New("Key not found")
}
func (mem *memDB) Del(key []byte) ([]byte, error) {
	mem.mu.Lock()
	defer mem.mu.Unlock()

	v, err := mem.DelMap(key)
	if err != nil {
		return nil, errors.New("Key not found")
	}

	err = writeWAL(mem.wal.file, byte(Del), []byte(key), v)
	if err != nil {
		return nil, err
	}

	fmt.Println("OK")

	return v, errors.New("Key not found")
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
			v, err := re.db.GetMap([]byte(elements[0]))
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
			err := re.db.SetMap([]byte(elements[0]), []byte(elements[1]))
			if err != nil {
				fmt.Fprintln(re.out, err.Error())
				continue
			}
		case Del:
			if len(elements) != 1 {
				fmt.Printf("Expected 1 argument, received: %d\n", len(elements))
				continue
			}
			v, err := re.db.DelMap([]byte(elements[0]))
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

func (mem *memDB) flushToSSTFromWatermark(watermark int64) error {
	mem.mu.Lock()
	defer mem.mu.Unlock()

	// Increment and get the current SST file number
	sstFileMutex.Lock()
	sstFileNumber++
	currentSSTFileNumber := sstFileNumber
	sstFileMutex.Unlock()

	// Generate SST file name with the current number
	sstFileName := fmt.Sprintf("sst%d.txt", currentSSTFileNumber)

	sstFile, err := os.Create(sstFileName)
	if err != nil {
		return err
	}
	defer sstFile.Close()

	// Write magic number
	binary.Write(sstFile, binary.LittleEndian, magicNumber)

	// Write entry count
	entryCount := uint32(0)
	_, err = mem.wal.file.Seek(watermark, os.SEEK_SET)
	if err != nil {
		return err
	}

	// Write commands
	for {
		var op byte
		if err := binary.Read(mem.wal.file, binary.LittleEndian, &op); err != nil {
			if err == io.EOF {
				break
			} else {
				fmt.Println("error khayba yarbi matl3ch")
				return err
			}
		}

		var lenKey, lenValue uint32
		binary.Read(mem.wal.file, binary.LittleEndian, &lenKey)
		key := make([]byte, lenKey)
		mem.wal.file.Read(key)

		binary.Read(mem.wal.file, binary.LittleEndian, &lenValue)
		value := make([]byte, lenValue)
		mem.wal.file.Read(value)

		// Write operation byte
		binary.Write(sstFile, binary.LittleEndian, op)
		binary.Write(sstFile, binary.LittleEndian, uint32(len(key)))
		sstFile.Write(key)
		binary.Write(sstFile, binary.LittleEndian, uint32(len(value)))
		sstFile.Write(value)

		mem.wal.watermark, err = mem.wal.file.Seek(0, os.SEEK_CUR)
		if err != nil {
			return err
		}

		entryCount++
	}
	mem.values = orderedmap.NewOrderedMap()

	// Seek back to the beginning to write entry count
	_, err = sstFile.Seek(int64(binary.Size(magicNumber)), os.SEEK_CUR)

	if err != nil {
		return err
	}
	binary.Write(sstFile, binary.LittleEndian, entryCount)

	return nil
}

func (mem *memDB) flushToSST() error {
	return mem.flushToSSTFromWatermark(mem.wal.watermark)
}

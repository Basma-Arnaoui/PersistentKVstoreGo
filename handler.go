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
	"time"
)

const flushInterval = time.Minute / 4

var flushThreshold = 5

const (
	magicNumber = 123456789
)

type handler interface {
	Set(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Del(key []byte) ([]byte, error)
}

func (mem *memDB) Set(key, value []byte) error {
	fmt.Printf("Set called with Key: %s, Value: %s\n", key, value)

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

	// Check if the key is in the in-memory map
	if v, ok := mem.values.Get(string(key)); ok {
		entry := v.(entry)
		if entry.op == del {
			return nil, errors.New("Key not found")
		}
		fmt.Println(mem.values.Len())
		return entry.value.([]byte), nil
	}

	// If not found in in-memory map, attempt to get from SST files
	sstValue, err := GetFromSST(key)
	if err != nil {
		return nil, errors.New("Key not found")
	}

	return sstValue, nil
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

func NewInMem() (*Repl, error) {
	walFileInstance, err := instantiateWal()
	if err != nil {
		return nil, err
	}

	memInstance := &memDB{
		values: orderedmap.NewOrderedMap(),
		wal:    walFileInstance,
	}

	return &Repl{
		handler: memInstance,
		in:      os.Stdin,
		out:     os.Stdout,
	}, nil
}

func (re *Repl) parseCmd(buf []byte) (Cmd, []string, error) {
	line := string(buf)
	elements := strings.Fields(line)
	if len(elements) < 1 {
		return Unk, nil, Empty
	}
	fmt.Println(elements)

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
			v, err := re.handler.Get([]byte(elements[0]))
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
			err := re.handler.Set([]byte(elements[0]), []byte(elements[1]))
			if err != nil {
				fmt.Fprintln(re.out, err.Error())
				continue
			}
		case Del:
			if len(elements) != 1 {
				fmt.Printf("Expected 1 argument, received: %d\n", len(elements))
				continue
			}
			v, err := re.handler.Del([]byte(elements[0]))
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

func countSSTFiles() (int, error) {
	dirPath, err := os.Getwd()
	if err != nil {
		fmt.Println("Error:", err)
		return 0, err
	}

	count := 0
	// Open the directory
	dirEntries, err := os.ReadDir(dirPath + "/SSTFiles")
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return 0, err
	}

	// Iterate over directory entries
	for _, entry := range dirEntries {
		//fmt.Println(entry.Name())
		if entry.IsDir() {
			continue
		}

		// Check if the file starts with "sst"
		if strings.HasPrefix(entry.Name(), "sst") {
			count++
		}
	}

	return count, nil
}

func (mem *memDB) startFlushTimer() {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mem.flushToSSTFromMap()
		}
	}
}

func (mem *memDB) flushToSSTFromMap() error {
	mem.mu.Lock()
	defer mem.mu.Unlock()

	// Count existing SST files
	existingSSTFiles, err := countSSTFiles()
	if err != nil {
		return err
	}

	// Generate SST file name with the count of existing files
	sstFileName := fmt.Sprintf("SSTFiles/sst%d.txt", existingSSTFiles+1)

	sstFile, err := os.Create(sstFileName)
	if err != nil {
		return err
	}
	defer sstFile.Close()

	// Write magic number
	binary.Write(sstFile, binary.LittleEndian, magicNumber)

	// Write smallest key placeholder (to be updated later)
	smallestKeyOffset := int64(binary.Size(magicNumber))
	_, err = sstFile.WriteAt(make([]byte, 8), smallestKeyOffset)
	if err != nil {
		return err
	}

	// Write biggest key placeholder (to be updated later)
	biggestKeyOffset := int64(binary.Size(magicNumber) + 8)
	_, err = sstFile.WriteAt(make([]byte, 8), biggestKeyOffset)
	if err != nil {
		return err
	}

	// Write commands
	var smallestKey, biggestKey []byte
	entryCount := uint32(0)

	// Use Keys function to get a slice of keys
	keys := mem.values.Keys()

	// Manual iteration through the ordered map using the keys slice
	for _, key := range keys {
		value, _ := mem.values.Get(key)
		entry := value.(entry)

		if entryCount == 0 {
			smallestKey = []byte(key.(string))
		}
		biggestKey = []byte(key.(string))

		// Write operation byte
		binary.Write(sstFile, binary.LittleEndian, entry.op)

		// Write key
		keyBytes := []byte(key.(string))
		binary.Write(sstFile, binary.LittleEndian, uint32(len(keyBytes)))
		sstFile.Write(keyBytes)

		// Write value
		valueBytes := entry.value.([]byte)
		binary.Write(sstFile, binary.LittleEndian, uint32(len(valueBytes)))
		sstFile.Write(valueBytes)

		entryCount++
	}

	// Update smallest and biggest key
	_, err = sstFile.WriteAt(smallestKey, smallestKeyOffset)
	if err != nil {
		return err
	}
	_, err = sstFile.WriteAt(biggestKey, biggestKeyOffset)
	if err != nil {
		return err
	}

	// Seek back to the beginning to write entry count
	_, err = sstFile.Seek(int64(binary.Size(magicNumber)), os.SEEK_SET)
	if err != nil {
		return err
	}
	binary.Write(sstFile, binary.LittleEndian, entryCount)

	// Seek to the end of the WAL file
	walFileSize, err := mem.wal.file.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}
	mem.wal.watermark = walFileSize
	// Update the watermark at the top of the WAL file
	binary.Write(mem.wal.file, binary.LittleEndian, walFileSize)

	return nil
}

func UpToDate(wal *walFile) (bool, error) {
	// Get the size of the WAL file
	fileInfo, err := wal.file.Stat()
	if err != nil {
		return false, err
	}
	fileSize := fileInfo.Size()

	// Seek to the beginning to read the stored watermark
	_, err = wal.file.Seek(0, os.SEEK_SET)
	if err != nil {
		return false, err
	}

	// Read the stored watermark
	storedWatermark := make([]byte, 8)
	_, err = wal.file.Read(storedWatermark)
	if err != nil {
		return false, err
	}

	// Compare the stored watermark with the file size
	return fileSize > 8 && uint64(fileSize)-8 == binary.LittleEndian.Uint64(storedWatermark), nil
}

func recoverFromWAL(mem *memDB) error {
	// Check if the WAL is up to date
	upToDate, err := UpToDate(mem.wal)
	if err != nil {
		return err
	}

	if upToDate {
		fmt.Println("WAL is up to date. No recovery needed.")
		return nil
	}

	// Get the current offset before reading commands
	currentOffset, err := mem.wal.file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}

	// Seek to the beginning to read the stored watermark
	_, err = mem.wal.file.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}

	// Read the stored watermark
	storedWatermark := make([]byte, 8)
	_, err = mem.wal.file.Read(storedWatermark)
	if err != nil {
		return err
	}

	// Execute commands below the watermark
	for {
		var op byte
		if err := binary.Read(mem.wal.file, binary.LittleEndian, &op); err != nil {
			if err == io.EOF {
				break
			} else {
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

		switch op {
		case byte(set):
			mem.SetMap(key, value)
		case byte(del):
			mem.DelMap(key)
		default:
			return errors.New("Unknown operation in WAL")
		}
	}

	// Restore the original offset after reading commands
	_, err = mem.wal.file.Seek(currentOffset, os.SEEK_SET)
	if err != nil {
		return err
	}

	return nil
}

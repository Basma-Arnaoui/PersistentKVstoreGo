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

var flushThreshold = 3

type handler interface {
	Set(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Del(key []byte) ([]byte, error)
}

func (mem *memDB) flushToSST() error {
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

	// Write size of ordered map to the SST file
	size := mem.values.Len()
	sizeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBytes, uint32(size))
	if _, err := sstFile.Write(sizeBytes); err != nil {
		return err
	}

	// Write keys to the SST file
	keys := mem.values.Keys()

	// Write smallest key to the SST file
	smallestKey := keys[0].(string)
	smallestKeyLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(smallestKeyLenBytes, uint32(len(smallestKey)))
	if _, err := sstFile.Write(smallestKeyLenBytes); err != nil {
		return err
	}
	if _, err := sstFile.Write([]byte(smallestKey)); err != nil {
		return err
	}

	// Write biggest key to the SST file
	biggestKey := keys[size-1].(string)
	biggestKeyLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(biggestKeyLenBytes, uint32(len(biggestKey)))
	if _, err := sstFile.Write(biggestKeyLenBytes); err != nil {
		return err
	}
	if _, err := sstFile.Write([]byte(biggestKey)); err != nil {
		return err
	}

	// Write keys and values to the SST file
	for _, key := range keys {
		value, _ := mem.values.Get(key)
		entry := value.(entry)

		// Write operation
		opByte := byte(entry.op)
		if _, err := sstFile.Write([]byte{opByte}); err != nil {
			return err
		}

		// Write key
		keyBytes := []byte(key.(string))
		keyLenBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(keyLenBytes, uint32(len(keyBytes)))

		if _, err := sstFile.Write(keyLenBytes); err != nil {
			return err
		}
		if _, err := sstFile.Write(keyBytes); err != nil {
			return err
		}

		// Write value
		valueBytes := entry.value.([]byte)
		valueLenBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(valueLenBytes, uint32(len(valueBytes)))

		if _, err := sstFile.Write(valueLenBytes); err != nil {
			return err
		}
		if _, err := sstFile.Write(valueBytes); err != nil {
			return err
		}
	}

	// Update the watermark in the WAL file
	if err := updateWALWatermark(); err != nil {
		return err
	}

	// Clear the ordered map
	mem.values = orderedmap.NewOrderedMap()

	fmt.Println("Flush to SST completed successfully.")
	return nil
}

func updateWALWatermark() error {
	// Open the WAL file
	walFile, err := os.OpenFile("wal.txt", os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer walFile.Close()

	// Read the watermark value
	var watermark uint32
	if err := binary.Read(walFile, binary.LittleEndian, &watermark); err != nil {
		return err
	}

	// Move the watermark to the end of the file
	if _, err := walFile.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	if err := binary.Write(walFile, binary.LittleEndian, watermark); err != nil {
		return err
	}

	// Update the watermark at the beginning of the file
	if _, err := walFile.Seek(0, 0); err != nil {
		return err
	}
	if err := binary.Write(walFile, binary.LittleEndian, watermark); err != nil {
		return err
	}

	return nil
}

func writeKeyToSSTFile(key []byte, sstFile *os.File) error {
	keyLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(keyLenBytes, uint32(len(key)))

	if _, err := sstFile.Write(keyLenBytes); err != nil {
		return err
	}
	if _, err := sstFile.Write(key); err != nil {
		return err
	}

	return nil
}
func (mem *memDB) flushToSSTFromMap() error {
	// Acquire the lock
	//mem.mu.Lock()
	//defer mem.mu.Unlock()

	return mem.flushToSST()
}

func (mem *memDB) checkSizeAndFlush() {
	// Check if the size of the ordered map exceeds flushThreshold
	if mem.values.Len() >= flushThreshold {
		// Acquire the lock
		//mem.mu.Lock()
		//defer mem.mu.Unlock()

		err := mem.flushToSST()
		if err != nil {
			fmt.Println("Error flushing to SST:", err)
		}
	}
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
	mem.checkSizeAndFlush()

	return nil
}
func (mem *memDB) SetWithNoLock(key, value []byte) error {
	fmt.Printf("Set called with Key: %s, Value: %s\n", key, value)

	err := mem.SetMap(key, value)
	if err != nil {
		return err
	}
	err = writeWAL(mem.wal.file, byte(Set), key, value)
	if err != nil {
		return err
	}
	fmt.Println("OK")
	mem.checkSizeAndFlush()

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
func (mem *memDB) GetWithNoLock(key []byte) ([]byte, error) {

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

	value, er := mem.GetWithNoLock(key)
	if er != nil {
		return nil, errors.New("Key not found")
	}
	er = mem.SetWithNoLock(key, value)
	if er != nil {
		return nil, errors.New("Error setting")
	}
	v, err := mem.DelMap(key)
	if err != nil {
		return nil, errors.New("Key not found")
	}

	err = writeWAL(mem.wal.file, byte(Del), []byte(key), v)
	if err != nil {
		return nil, err
	}

	fmt.Println("OK")
	mem.checkSizeAndFlush()

	return v, nil
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
	currentOffset, err := mem.wal.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	// Seek to the beginning to read the stored watermark
	_, err = mem.wal.file.Seek(0, io.SeekStart)
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
			fmt.Printf("Unknown operation in WAL: %v\n", op)
		}
	}

	// Restore the original offset after reading commands
	_, err = mem.wal.file.Seek(currentOffset, io.SeekStart)
	if err != nil {
		return err
	}

	return nil
}

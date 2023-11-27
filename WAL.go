package main

import (
	"encoding/binary"
	"fmt"
	"github.com/elliotchance/orderedmap"
	"io"
	"os"
	"sync"
	"time"
)

var (
	sstFileNumber int
	sstFileMutex  sync.Mutex
)

const flushInterval = time.Minute / 4

func (mem *memDB) startFlushTimer() {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mem.flushToSST()
		}
	}
}

type walFile struct {
	file      *os.File
	size      int
	watermark int64
}

const (
	magicNumber = 123456789
)

func writeWAL(wal *os.File, op byte, key, value []byte) error {
	lenKey := make([]byte, 4)
	lenValue := make([]byte, 4)

	binary.LittleEndian.PutUint32(lenKey, uint32(len(key)))
	binary.LittleEndian.PutUint32(lenValue, uint32(len(value)))

	if _, err := wal.Write([]byte{op}); err != nil {
		return err
	}
	if _, err := wal.Write(lenKey); err != nil {
		return err
	}
	if _, err := wal.Write(key); err != nil {
		return err
	}
	if _, err := wal.Write(lenValue); err != nil {
		return err
	}
	if _, err := wal.Write(value); err != nil {
		return err
	}

	return nil
}

func (mem *memDB) readCommand(offset int64, wal *walFile) (int64, error) {
	_, err := wal.file.Seek(offset, os.SEEK_SET)
	if err != nil {
		return 0, err
	}

	var op byte
	if err := binary.Read(wal.file, binary.LittleEndian, &op); err != nil {
		return 0, err
	}

	var lenKey, lenValue uint32
	if err := binary.Read(wal.file, binary.LittleEndian, &lenKey); err != nil {
		return 0, err
	}

	key := make([]byte, lenKey)
	if _, err := wal.file.Read(key); err != nil {
		return 0, err
	}

	if err := binary.Read(wal.file, binary.LittleEndian, &lenValue); err != nil {
		return 0, err
	}

	value := make([]byte, lenValue)
	if _, err := wal.file.Read(value); err != nil {
		return 0, err
	}

	endOffset, err := wal.file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return 0, err
	}

	return endOffset, nil
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
		sstFile.Write(value)
		binary.Write(sstFile, binary.LittleEndian, uint32(len(value)))
		sstFile.Write(key)
		binary.Write(sstFile, binary.LittleEndian, uint32(len(key)))

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

	fmt.Println("wslna lkher")
	return nil
}

func (mem *memDB) flushToSST() error {
	return mem.flushToSSTFromWatermark(mem.wal.watermark)
}

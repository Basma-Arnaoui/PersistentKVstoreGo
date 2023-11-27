package main

import (
	"encoding/binary"
	"fmt"
	"github.com/elliotchance/orderedmap"
	"io"
	"os"
)

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

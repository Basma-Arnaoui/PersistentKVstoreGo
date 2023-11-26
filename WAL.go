package main

import (
	"encoding/binary"
	"os"
)

type walFile struct {
	file      *os.File
	size      int
	watermark int64
}

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

func (mem *memDB) flushToSST(sstFileName string) error {
	mem.mu.Lock()
	defer mem.mu.Unlock()

	sstFile, err := os.Create(sstFileName)
	if err != nil {
		return err
	}
	defer sstFile.Close()

	_, err = mem.wal.file.Seek(mem.wal.watermark, os.SEEK_SET)
	if err != nil {
		return err
	}

	for {
		var op byte
		if err := binary.Read(mem.wal.file, binary.LittleEndian, &op); err != nil {
			break
		}

		var lenKey, lenValue uint32
		binary.Read(mem.wal.file, binary.LittleEndian, &lenKey)
		key := make([]byte, lenKey)
		mem.wal.file.Read(key)

		binary.Read(mem.wal.file, binary.LittleEndian, &lenValue)
		value := make([]byte, lenValue)
		mem.wal.file.Read(value)

		binary.Write(sstFile, binary.LittleEndian, lenKey)
		sstFile.Write(key)
		binary.Write(sstFile, binary.LittleEndian, lenValue)
		sstFile.Write(value)

		mem.wal.watermark, err = mem.wal.file.Seek(0, os.SEEK_CUR)
		if err != nil {
			return err
		}
	}

	return nil
}

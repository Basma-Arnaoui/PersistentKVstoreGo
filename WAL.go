package main

import (
	"encoding/binary"
	"os"
	"sync"
)

var (
	sstFileNumber int
	sstFileMutex  sync.Mutex
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

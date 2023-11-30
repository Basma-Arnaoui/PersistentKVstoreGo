package main

import (
	"encoding/binary"
	"fmt"
	"os"
)

type walFile struct {
	file      *os.File
	size      int
	watermark int64
}

func writeWAL(wal *os.File, op byte, key, value []byte) error {
	//write in the wal file
	fmt.Printf("Writing to WAL. Op: %d, Key: %s, Value: %s\n", op, key, value)

	lenKey := make([]byte, 4)
	lenValue := make([]byte, 4)

	binary.LittleEndian.PutUint32(lenKey, uint32(len(key)))
	binary.LittleEndian.PutUint32(lenValue, uint32(len(value)))

	// Write operation byte
	if _, err := wal.Write([]byte{op}); err != nil {
		return err
	}

	// Write key length and key
	if _, err := wal.Write(lenKey); err != nil {
		return err
	}
	if _, err := wal.Write(key); err != nil {
		return err
	}

	// Write value length and value
	if _, err := wal.Write(lenValue); err != nil {
		return err
	}
	if _, err := wal.Write(value); err != nil {
		return err
	}

	return nil
}

func instantiateWal() (*walFile, error) {
	file, err := os.OpenFile("wal.txt", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// Write the initial watermark (0) at the top of the file
	binary.Write(file, binary.LittleEndian, int64(0))

	return &walFile{file: file}, nil
}

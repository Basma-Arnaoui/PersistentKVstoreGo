package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

func compareKeys(key1, key2 []byte) int {
	return bytes.Compare(key1, key2)
}

func GetFromSST(key []byte) ([]byte, error) {
	// Get the number of SST files
	fileCount, err := countSSTFiles()
	if err != nil {
		return nil, err
	}

	// Variable to store the found value
	var foundValue []byte

	// Iterate through SST files in reverse order
	for i := fileCount; i > 0; i-- {
		sstFileName := fmt.Sprintf("SSTFiles/sst%d.txt", i)
		sstFile, err := os.Open(sstFileName)
		if err != nil {
			return nil, err
		}
		defer sstFile.Close()

		// Read magic number
		var fileMagicNumber uint32
		if err := binary.Read(sstFile, binary.LittleEndian, &fileMagicNumber); err != nil {
			return nil, err
		}

		if fileMagicNumber != magicNumber {
			return nil, errors.New("Invalid SST file format")
		}

		// Read smallest key
		var smallestKeyLen uint32
		binary.Read(sstFile, binary.LittleEndian, &smallestKeyLen)
		smallestKey := make([]byte, smallestKeyLen)
		sstFile.Read(smallestKey)

		// Read biggest key
		var biggestKeyLen uint32
		binary.Read(sstFile, binary.LittleEndian, &biggestKeyLen)
		biggestKey := make([]byte, biggestKeyLen)
		sstFile.Read(biggestKey)

		// Check if the key is within the range
		if compareKeys(key, smallestKey) >= 0 && compareKeys(key, biggestKey) <= 0 {
			// Iterate through entries in the SST file
			for {
				var op byte
				if err := binary.Read(sstFile, binary.LittleEndian, &op); err != nil {
					break // End of file
				}

				var lenKey, lenValue uint32
				binary.Read(sstFile, binary.LittleEndian, &lenKey)
				readKey := make([]byte, lenKey)
				sstFile.Read(readKey)

				binary.Read(sstFile, binary.LittleEndian, &lenValue)
				readValue := make([]byte, lenValue)
				sstFile.Read(readValue)

				// Check if the key matches
				if compareKeys(key, readKey) == 0 {
					// If the operation is a deletion, set foundValue to nil
					if op == byte(0x01) {
						foundValue = nil
					} else {
						foundValue = readValue
					}
					// Continue iterating to find the latest value
				}
			}
		}
	}

	if foundValue != nil {
		return foundValue, nil
	}

	// Key not found in any SST file
	return nil, errors.New("Key not found")
}

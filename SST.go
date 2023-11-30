package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

// compareKeys compares two byte slices to determine their order, so i
// can know if a key is in the sst before iterating through the keys.
func compareKeys(key1, key2 []byte) int {
	cmp := bytes.Compare(key1, key2)
	switch {
	case cmp < 0:
		return -1
	case cmp > 0:
		return 1
	default:
		return 0
	}
}

// isEqual checks if two byte slices are equal.
func isEqual(slice1, slice2 []byte) bool {
	if len(slice1) != len(slice2) {
		return false
	}

	for i := 0; i < len(slice1); i++ {
		if slice1[i] != slice2[i] {
			return false
		}
	}

	return true
}

// GetFromSST retrieves a value from SST files based on the given key.
func GetFromSST(key []byte) ([]byte, error) {
	// Count the number of SST files
	fileCount, err := countSSTFiles()
	fmt.Println("count sst ", fileCount)
	if err != nil {
		return nil, err
	}

	var foundValue []byte

	// Iterate through SST files
	for i := fileCount; i > 0; i-- {
		sstFileName := fmt.Sprintf("SSTFiles/sst%d.txt", i)
		sstFile, err := os.Open(sstFileName)
		if err != nil {
			return nil, err
		}
		defer sstFile.Close()

		// Read entry count
		var entryCount uint32
		if err := binary.Read(sstFile, binary.LittleEndian, &entryCount); err != nil {
			return nil, err
		}

		var smallestKey, biggestKey []byte
		// Read smallest key length
		var smallestKeyLen uint32
		binary.Read(sstFile, binary.LittleEndian, &smallestKeyLen)

		smallestKey = make([]byte, smallestKeyLen)
		sstFile.Read(smallestKey)

		// Read biggest key length
		var biggestKeyLen uint32
		binary.Read(sstFile, binary.LittleEndian, &biggestKeyLen)
		biggestKey = make([]byte, biggestKeyLen)
		sstFile.Read(biggestKey)

		foundInFile := false

		// Check if the key is within the range of smallest and biggest keys
		if (compareKeys(key, smallestKey) >= 0 && compareKeys(key, biggestKey) <= 0) || isEqual(key, smallestKey) || isEqual(key, biggestKey) {

			// Iterate through entries in the SST file
			for j := 0; j < int(entryCount); j++ {

				var op byte
				if err := binary.Read(sstFile, binary.LittleEndian, &op); err != nil {
					break // End of file
				}

				var lenKey, lenValue uint32
				// Read the key length
				if err := binary.Read(sstFile, binary.LittleEndian, &lenKey); err != nil {
					fmt.Printf("Error reading key length: %v\n", err)
					break
				}

				// Read the key
				readKey := make([]byte, lenKey)
				n, err := sstFile.Read(readKey)
				fmt.Printf("Read key (%d): %v, err: %v\n", n, readKey, err)

				// Read the value length
				if err := binary.Read(sstFile, binary.LittleEndian, &lenValue); err != nil {
					fmt.Printf("Error reading value length: %v\n", err)
					break
				}

				// Read the value
				readValue := make([]byte, lenValue)
				n, err = sstFile.Read(readValue)

				// Check if the key matches
				if compareKeys(key, readKey) == 0 {
					foundInFile = true
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

		if foundInFile {
			if foundValue != nil {
				return foundValue, nil
			}
			return nil, errors.New("Key not found")
		}
	}

	if foundValue != nil {
		return foundValue, nil
	}

	// Key not found in any SST file
	return nil, errors.New("Key not found")
}

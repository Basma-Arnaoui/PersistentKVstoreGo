package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
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

		// Read entry count
		var entryCount uint32
		if err := binary.Read(sstFile, binary.LittleEndian, &entryCount); err != nil {
			return nil, err
		}
		fmt.Println("entry ", entryCount)

		// Read smallest key
		var smallestKeyLen uint32
		binary.Read(sstFile, binary.LittleEndian, &smallestKeyLen)
		smallestKey := make([]byte, smallestKeyLen)
		sstFile.Read(smallestKey)
		fmt.Println("samllest : ", smallestKey)
		// Read biggest key
		var biggestKeyLen uint32
		binary.Read(sstFile, binary.LittleEndian, &biggestKeyLen)
		biggestKey := make([]byte, biggestKeyLen)
		sstFile.Read(biggestKey)
		fmt.Println("biggest ", biggestKey)
		// Check if the key is within the range
		if compareKeys(key, smallestKey) >= 0 && compareKeys(key, biggestKey) <= 0 {
			// Iterate through entries in the SST file
			for j := uint32(0); j < entryCount; j++ {
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
					foundValue = readValue
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

func findSmallestBiggestKey(fileName string, offset int64) ([]byte, []byte) {
	// Open the SST file
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Seek to the offset
	if _, err := file.Seek(offset, 0); err != nil {
		log.Fatal(err)
	}

	// Read entry count
	var entryCount uint32
	if err := binary.Read(file, binary.LittleEndian, &entryCount); err != nil {
		log.Fatal(err)
	}

	// Read smallest key
	var smallestKeyLen uint32
	binary.Read(file, binary.LittleEndian, &smallestKeyLen)
	smallestKey := make([]byte, smallestKeyLen)
	file.Read(smallestKey)
	fmt.Println("samllest ", smallestKey)
	// Read biggest key
	var biggestKeyLen uint32
	binary.Read(file, binary.LittleEndian, &biggestKeyLen)
	biggestKey := make([]byte, biggestKeyLen)
	file.Read(biggestKey)
	fmt.Println("biggest : ", biggestKey)

	return smallestKey, biggestKey
}

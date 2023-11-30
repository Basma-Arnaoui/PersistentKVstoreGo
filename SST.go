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
	fileCount, err := countSSTFiles()
	if err != nil {
		return nil, err
	}

	var foundValue []byte

	for i := fileCount; i > 0; i-- {
		sstFileName := fmt.Sprintf("SSTFiles/sst%d.txt", i)
		sstFile, err := os.Open(sstFileName)
		if err != nil {
			return nil, err
		}
		defer sstFile.Close()

		// Skip magic number

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
		fmt.Println("smallest:", smallestKey)
		// Read biggest key length
		var biggestKeyLen uint32
		binary.Read(sstFile, binary.LittleEndian, &biggestKeyLen)
		biggestKey = make([]byte, biggestKeyLen)
		sstFile.Read(biggestKey)
		fmt.Println("biggest:", biggestKey)

		if compareKeys(key, smallestKey) >= 0 && compareKeys(key, biggestKey) <= 0 {
			fmt.Println("dkhlna hna")
			// Iterate through entries in the SST file
			for j := 0; j < int(entryCount); j++ {
				var op byte
				if err := binary.Read(sstFile, binary.LittleEndian, &op); err != nil {
					break // End of file
				}
				fmt.Println("krina op", op)

				var lenKey, lenValue uint32
				// Read the key length
				if err := binary.Read(sstFile, binary.LittleEndian, &lenKey); err != nil {
					fmt.Printf("Error reading key length: %v\n", err)
					break
				}
				fmt.Println("krina keylen ", lenKey)

				// Read the key
				readKey := make([]byte, lenKey)
				n, err := sstFile.Read(readKey)
				fmt.Printf("Read key (%d): %v, err: %v\n", n, readKey, err)

				// Read the value length
				if err := binary.Read(sstFile, binary.LittleEndian, &lenValue); err != nil {
					fmt.Printf("Error reading value length: %v\n", err)
					break
				}
				fmt.Println("krina len value", lenValue)

				// Read the value
				readValue := make([]byte, lenValue)
				n, err = sstFile.Read(readValue)
				fmt.Printf("Read value (%d): %v, err: %v\n", n, readValue, err)

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
				fmt.Println("salina iteration")
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

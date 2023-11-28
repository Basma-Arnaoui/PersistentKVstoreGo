package main

import (
	"encoding/binary"
	"fmt"
	"os"
)

func readSSTFile(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Get the file size
	//fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	//fileSize := fileInfo.Size()

	// Seek to the end of the file
	offset, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}

	// Read and print entries in reverse order
	for {
		// Move the offset back by the size of an entry
		offset -= 4 // size of uint32 (entryCount)
		_, err = file.Seek(offset, os.SEEK_SET)
		if err != nil {
			return err
		}

		// Read the operation byte
		var op byte
		if err := binary.Read(file, binary.LittleEndian, &op); err != nil {
			return err
		}

		// Read the value length
		var lenValue uint32
		if err := binary.Read(file, binary.LittleEndian, &lenValue); err != nil {
			return err
		}

		// Move the offset back by the length of value
		offset -= int64(lenValue)
		_, err = file.Seek(offset, os.SEEK_SET)
		if err != nil {
			return err
		}

		value := make([]byte, lenValue)
		if _, err := file.Read(value); err != nil {
			return err
		}

		// Read the key length
		var lenKey uint32
		if err := binary.Read(file, binary.LittleEndian, &lenKey); err != nil {
			return err
		}

		// Move the offset back by the length of key
		offset -= int64(lenKey)
		_, err = file.Seek(offset, os.SEEK_SET)
		if err != nil {
			return err
		}

		key := make([]byte, lenKey)
		if _, err := file.Read(key); err != nil {
			return err
		}

		fmt.Printf("Op: %d, Key: %s, Value: %s\n", op, key, value)
	}

	return nil
}

/*
	func main() {
		db, err := NewInMem("wal.txt")
		instantiateWal(db)

		if err != nil {
			fmt.Println("Error creating in-memory DB:", err)
			return
		}
		defer db.wal.file.Close()
		go db.startFlushTimer()

		repl := Repl{
			db:  db,
			in:  os.Stdin,
			out: os.Stdout,
		}

		repl.Start()
		err = readSSTFile("sst1.txt")
		if err != nil {
			fmt.Println("Error reading SST file:", err)
		}
		//select{}
	}
*/
func main() {
	num, err := countSSTFiles()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(num)
}

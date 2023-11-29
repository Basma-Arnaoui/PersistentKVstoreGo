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
func main() {
	repl, err := NewInMem()
	if err != nil {
		fmt.Println("Error creating REPL:", err)
		return
	}

	// Perform recovery from WAL
	err = recoverFromWAL(repl.handler.(*memDB))
	if err != nil {
		fmt.Println("Error recovering from WAL:", err)
		return
	}

	// Start the REPL
	repl.Start()

	// Ensure the WAL file is closed when the program exits
	defer func() {
		if err := repl.handler.(*memDB).wal.file.Close(); err != nil {
			fmt.Println("Error closing WAL file:", err)
		}
	}()
}

/*
func main() {
	// Create a memDB instance
	memInstance := &memDB{
		values: orderedmap.NewOrderedMap(),
		wal:    &walFile{}, // You need to initialize walFile properly
	}

	// Create a Repl instance
	repl := &Repl{
		db:      memInstance,
		handler: memInstance,
		in:      os.Stdin,
		out:     os.Stdout,
	}

	// Start the flush trigger goroutine
	go memInstance.flushTrigger()

	// Start the REPL
	repl.Start()
}*/
/*
func main() {
	re, err := NewInMem("wal.txt")

	if err != nil {
		fmt.Println("Error creating in-memory DB:", err)
		return
	}
	defer db.
	go db.()

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

/*func main() {
	num, err := countSSTFiles()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(num)
}*/

package main

import (
	"fmt"
	"os"
)

func main() {
	db, err := NewInMem("wal.txt")
	if err != nil {
		fmt.Println("Error creating in-memory DB:", err)
		return
	}
	defer db.wal.file.Close()
	// Test Set
	fmt.Println("Testing Set:")
	err = db.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		fmt.Println("Error:", err)
	}

	// Test Get
	fmt.Println("Testing Get:")
	val, err := db.Get([]byte("key1"))
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("Value:", string(val))
	}

	// Test Del
	fmt.Println("Testing Del:")
	delVal, err := db.Del([]byte("key1"))
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("Deleted Value:", string(delVal))
	}

	// Test Get after deletion
	fmt.Println("Testing Get after deletion:")
	valAfterDel, err := db.Get([]byte("key1"))
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("Value after deletion:", string(valAfterDel))
	}

	// Test unknown command
	fmt.Println("Testing Unknown Command:")
	err = db.Set([]byte("key2"), []byte("value2"))
	if err != nil {
		fmt.Println("Error:", err)
	}

	// Test Exit
	fmt.Println("Testing Exit:")
	repl := Repl{
		db:  db,
		in:  os.Stdin,
		out: os.Stdout,
	}

	repl.Start()
}

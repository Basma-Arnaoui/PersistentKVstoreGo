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

	repl := Repl{
		db:  db,
		in:  os.Stdin,
		out: os.Stdout,
	}

	repl.Start()
}

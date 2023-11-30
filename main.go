package main

import (
	"fmt"
	"net/http"
	"sync"
)

// memDB is my in-memory database implementation
var mem *memDB
var memMutex sync.Mutex

func main() {
	// New memdb
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

	// API
	http.HandleFunc("/get", GetHandler)
	http.HandleFunc("/set", SetHandler)
	http.HandleFunc("/del", DelHandler)

	// Start the REPL
	repl.Start()

	// Ensure the WAL file is closed when the program exits
	defer func() {
		if err := repl.handler.(*memDB).wal.file.Close(); err != nil {
			fmt.Println("Error closing WAL file:", err)
		}
	}()

	// Specify the port and start the server
	port := 8080
	fmt.Printf("Server is running on http://localhost:%d\n", port)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}

func GetHandler(w http.ResponseWriter, r *http.Request) {
	//Handles get requests
	key := r.URL.Query().Get("key")

	memMutex.Lock()
	defer memMutex.Unlock()

	result, err := mem.Get([]byte(key))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Write(result)
}

func SetHandler(w http.ResponseWriter, r *http.Request) {
	//Handles set requests
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")

	if key == "" {
		http.Error(w, "Key not provided", http.StatusBadRequest)
		return
	}

	memMutex.Lock()
	defer memMutex.Unlock()

	err := mem.Set([]byte(key), []byte(value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte("OK"))
}

func DelHandler(w http.ResponseWriter, r *http.Request) {
	//handles del requests
	key := r.URL.Query().Get("key")

	memMutex.Lock()
	defer memMutex.Unlock()

	value, err := mem.Del([]byte(key))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Write(value)
}

//THIS IS ANOTHER MAIN WHERE THERE IS NOT API

/*
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
*/

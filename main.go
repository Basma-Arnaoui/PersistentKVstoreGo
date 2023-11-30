package main

import (
	"fmt"
	"net/http"
	"sync"
)

// memDB is assumed to be your in-memory database implementation
var mem *memDB
var memMutex sync.Mutex

func main() {
	repl, _ := NewInMem()
	mem = repl.handler.(*memDB)

	http.HandleFunc("/get", GetHandler)
	http.HandleFunc("/set", SetHandler)
	http.HandleFunc("/del", DelHandler)

	port := 8080
	fmt.Printf("Server is running on http://localhost:%d\n", port)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}

func GetHandler(w http.ResponseWriter, r *http.Request) {
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

/*func main() {
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

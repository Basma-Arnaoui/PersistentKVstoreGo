# Persistent KV Store

This project is a simple key-value store implementation with persistence using Go. It offers basic functionality, allowing users to set a key-value pair, retrieve the value associated with a key, and delete a key. The key-value store ensures data persistence, even across application restarts. The provided API supports GET, POST, and DELETE requests for interacting with the key-value store. To get started, clone the repository, build, and run the application using `go run main.go`. The server will be accessible at [http://localhost:8080](http://localhost:8080). Usage examples, including cURL commands, are provided for setting values, getting values, and deleting keys. The README also includes a TODO section for future improvements, such as implementing compaction for SST files since I didn't have enough time to do it. 

## TODO

- [ ] Implement compaction for SST files
- [ ] Additional features or optimizations (optional)


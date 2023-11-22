package PersistentKVstoreGo

import (
	"bufio"
	"fmt"
	"github.com/elliotchance/orderedmap"
	"os"
)

type walfile interface {
	Set(key []byte, value []byte) (bool, error)

	Del(key []byte) (bool, error)

	flush() error

	getSize() (int64, error)
}
type walFile struct {
	values *orderedmap.OrderedMap
}

func (wal *walFile) Set(key []byte, value []byte) (bool, error) {
	file, err := os.OpenFile("wal.log", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return false, err
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	toWrite := "s "
	toWrite += string(key)
	toWrite += "="
	toWrite += string(value)
	data := []byte(toWrite)
	_, err = writer.Write(data)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return false, err
	}
	err = writer.Flush()
	if err != nil {
		fmt.Println("Error flushing buffer:", err)
		return false, err
	}
	fmt.Println("OK")
	return true, nil
}

func main() {

}

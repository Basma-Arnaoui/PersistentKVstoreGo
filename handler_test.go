package main

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestMemDB(t *testing.T) {
	mem, err := NewInMem()
	if err != nil {
		t.Fatalf("Error creating in-memory database: %v", err)
	}

	// Test Set
	err = mem.handler.Set([]byte("test_key"), []byte("test_value"))
	if err != nil {
		t.Fatalf("Error setting key: %v", err)
	}

	// Test Get
	result, err := mem.handler.Get([]byte("test_key"))
	if err != nil {
		t.Fatalf("Error getting key: %v", err)
	}

	expected := []byte("test_value")
	if !bytes.Equal(result, expected) {
		t.Fatalf("Expected %v, got %v", expected, result)
	}

	// Test Del
	deletedValue, err := mem.handler.Del([]byte("test_key"))
	if err != nil {
		t.Fatalf("Error deleting key: %v", err)
	}

	if !bytes.Equal(deletedValue, expected) {
		t.Fatalf("Expected deleted value %v, got %v", expected, deletedValue)
	}

	// Test that the key is not present after deletion
	_, err = mem.handler.Get([]byte("test_key"))
	if err == nil {
		t.Fatalf("Expected key to be deleted, but it still exists")
	}
}

func TestMemDBWithConcurrency(t *testing.T) {
	mem, err := NewInMem()
	if err != nil {
		t.Fatalf("Error creating in-memory database: %v", err)
	}

	threshHold := 1000

	// Test concurrency with Set and Del
	for i := 0; i < 3*threshHold; i++ {
		time.Sleep(time.Second)

		go func(idx int) {
			key := []byte(fmt.Sprintf("key%d", idx))
			value := []byte(fmt.Sprintf("value%d", idx))

			err := mem.handler.Set(key, value)
			if err != nil {
				t.Fatalf("Error setting key: %v", err)
			}

			result, err := mem.handler.Get(key)
			if err != nil {
				t.Fatalf("Error getting key: %v", err)
			}

			if !bytes.Equal(result, value) {
				t.Fatalf("Expected %v, got %v", value, result)
			}

			deletedValue, err := mem.handler.Del(key)
			if err != nil {
				t.Fatalf("Error deleting key: %v", err)
			}

			if !bytes.Equal(deletedValue, value) {
				t.Fatalf("Expected deleted value %v, got %v", value, deletedValue)
			}

			// Test that the key is not present after deletion
			_, err = mem.handler.Get(key)
			if err == nil {
				t.Fatalf("Expected key to be deleted, but it still exists")
			}
		}(i)

	}

	// Wait for goroutines to finish
	time.Sleep(2 * time.Second)
}

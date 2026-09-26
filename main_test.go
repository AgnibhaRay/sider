package main

import (
	"net"
	"os"
	"testing"
	"time"
)

var originalWalContent []byte

func TestMain(m *testing.M) {
	if data, err := os.ReadFile(WALFile); err == nil {
		originalWalContent = data
	}
	code := m.Run()
	os.RemoveAll(DataDir)
	if originalWalContent != nil {
		_ = os.WriteFile(WALFile, originalWalContent, 0644)
	} else {
		_ = os.Remove(WALFile)
	}
	os.Exit(code)
}

func cleanupTestFiles() {
	os.Remove(WALFile)
	os.RemoveAll(DataDir)
}

func TestBloomFilter(t *testing.T) {
	bf := NewBloomFilter()
	bf.Add("apple")
	bf.Add("banana")

	if !bf.MayContain("apple") {
		t.Errorf("expected bloom filter to contain 'apple'")
	}
	if !bf.MayContain("banana") {
		t.Errorf("expected bloom filter to contain 'banana'")
	}
	if bf.MayContain("cherry") {
		t.Logf("note: false positive on cherry")
	}
}

func TestEngineBasicCRUD(t *testing.T) {
	cleanupTestFiles()
	defer cleanupTestFiles()

	engine := NewEngine()

	// Put and Get
	engine.Put("k1", "v1")
	if val := engine.Get("k1"); val != "v1" {
		t.Fatalf("expected v1, got %s", val)
	}

	// Overwrite
	engine.Put("k1", "v2")
	if val := engine.Get("k1"); val != "v2" {
		t.Fatalf("expected v2, got %s", val)
	}

	// Delete
	engine.Delete("k1")
	if val := engine.Get("k1"); val != "(nil)" {
		t.Fatalf("expected (nil) after delete, got %s", val)
	}
}

func TestEngineTTL(t *testing.T) {
	cleanupTestFiles()
	defer cleanupTestFiles()

	engine := NewEngine()

	// Put with TTL
	engine.PutWithTTL("tempKey", "tempVal", 50*time.Millisecond)
	if val := engine.Get("tempKey"); val != "tempVal" {
		t.Fatalf("expected tempVal, got %s", val)
	}

	ttl := engine.TTL("tempKey")
	if ttl < 0 {
		t.Fatalf("expected positive ttl, got %d", ttl)
	}

	// Wait for expiration
	time.Sleep(70 * time.Millisecond)
	if val := engine.Get("tempKey"); val != "(nil)" {
		t.Fatalf("expected expired key to return (nil), got %s", val)
	}
	if ttl := engine.TTL("tempKey"); ttl != -2 {
		t.Fatalf("expected ttl -2 for expired key, got %d", ttl)
	}

	// Persistent key has TTL -1
	engine.Put("permKey", "permVal")
	if ttl := engine.TTL("permKey"); ttl != -1 {
		t.Fatalf("expected ttl -1 for persistent key, got %d", ttl)
	}

	// Expire existing key
	ok := engine.Expire("permKey", 50*time.Millisecond)
	if !ok {
		t.Fatalf("expected Expire to succeed")
	}
	time.Sleep(70 * time.Millisecond)
	if val := engine.Get("permKey"); val != "(nil)" {
		t.Fatalf("expected permKey to expire, got %s", val)
	}
}

func TestEngineClearPrefix(t *testing.T) {
	cleanupTestFiles()
	defer cleanupTestFiles()

	engine := NewEngine()

	engine.Put("cache:user:1", "alice")
	engine.Put("cache:user:2", "bob")
	engine.Put("cache:order:100", "order_data")

	cleared := engine.ClearPrefix("cache:user:")
	if cleared < 2 {
		t.Fatalf("expected at least 2 keys cleared, got %d", cleared)
	}

	if val := engine.Get("cache:user:1"); val != "(nil)" {
		t.Fatalf("expected cache:user:1 to be cleared, got %s", val)
	}
	if val := engine.Get("cache:user:2"); val != "(nil)" {
		t.Fatalf("expected cache:user:2 to be cleared, got %s", val)
	}
	if val := engine.Get("cache:order:100"); val != "order_data" {
		t.Fatalf("expected cache:order:100 to remain, got %s", val)
	}
}

func TestBrokerPubSub(t *testing.T) {
	broker := NewBroker()
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	client := &Client{conn: serverConn}
	broker.Subscribe(client, "test-channel")

	go func() {
		delivered := broker.Publish("test-channel", "hello-world")
		if delivered != 1 {
			t.Errorf("expected 1 delivered, got %d", delivered)
		}
	}()

	buf := make([]byte, 64)
	n, err := clientConn.Read(buf)
	if err != nil {
		t.Fatalf("failed to read message: %v", err)
	}
	msg := string(buf[:n])
	if msg != "MESSAGE test-channel hello-world\n" {
		t.Fatalf("unexpected message: %s", msg)
	}

	broker.Unsubscribe(client, "test-channel")
	delivered := broker.Publish("test-channel", "dropped")
	if delivered != 0 {
		t.Fatalf("expected 0 delivered after unsubscribe, got %d", delivered)
	}
}

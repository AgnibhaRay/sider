package main

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
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

func TestEngineAuth(t *testing.T) {
	testDir := "data_test_auth"
	testWal := "wal_test_auth.wal"
	defer os.RemoveAll(testDir)
	defer os.Remove(testWal)

	engine := NewEngineWithConfig(testDir, testWal, "secret-pass-123")
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	client := &Client{
		conn:          serverConn,
		authenticated: false,
		remoteAddr:    "127.0.0.1:9999",
	}

	// 1. Without auth, PUT should be rejected
	resp := executeCommand(engine, client, "PUT secret-key value")
	if resp != "ERR NOAUTH Authentication required" {
		t.Fatalf("expected NOAUTH error, got %s", resp)
	}

	// 2. PING should be allowed
	resp = executeCommand(engine, client, "PING")
	if resp != "PONG" {
		t.Fatalf("expected PONG, got %s", resp)
	}

	// 3. Wrong password should fail
	resp = executeCommand(engine, client, "AUTH wrong-password")
	if resp != "ERR invalid password" {
		t.Fatalf("expected invalid password error, got %s", resp)
	}
	if client.authenticated {
		t.Fatalf("client should not be authenticated")
	}

	// 4. Correct password should succeed
	resp = executeCommand(engine, client, "AUTH secret-pass-123")
	if resp != "OK" {
		t.Fatalf("expected OK, got %s", resp)
	}
	if !client.authenticated {
		t.Fatalf("client should now be authenticated")
	}

	// 5. Subsequent commands should succeed
	resp = executeCommand(engine, client, "PUT auth-k1 auth-v1")
	if resp != "OK" {
		t.Fatalf("expected OK after auth, got %s", resp)
	}
	resp = executeCommand(engine, client, "GET auth-k1")
	if resp != "auth-v1" {
		t.Fatalf("expected auth-v1, got %s", resp)
	}
}

func TestEngineStatsAndKeyListing(t *testing.T) {
	testDir := "data_test_stats"
	testWal := "wal_test_stats.wal"
	defer os.RemoveAll(testDir)
	defer os.Remove(testWal)

	engine := NewEngineWithConfig(testDir, testWal, "")
	engine.Put("users:1", "Alice")
	engine.Put("users:2", "Bob")
	engine.PutWithTTL("session:xyz", "active", 60*time.Second)

	keys := engine.ListKeys("")
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}

	stats := engine.Stats()
	if stats["status"] != "online" {
		t.Fatalf("expected status online, got %v", stats["status"])
	}
	if stats["memtable_entries"].(int) != 3 {
		t.Fatalf("expected 3 memtable entries, got %v", stats["memtable_entries"])
	}
}

func TestHTTPExecAPI(t *testing.T) {
	testDir := "data_test_http"
	testWal := "wal_test_http.wal"
	defer os.RemoveAll(testDir)
	defer os.Remove(testWal)

	engine := NewEngineWithConfig(testDir, testWal, "http-token")

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Command string `json:"command"`
			Token   string `json:"token"`
		}
		json.NewDecoder(r.Body).Decode(&payload)

		authenticated := engine.AuthToken == "" || payload.Token == engine.AuthToken
		virtualClient := &Client{
			authenticated: authenticated,
			remoteAddr:    "127.0.0.1:8888",
		}

		resp := executeCommand(engine, virtualClient, payload.Command)
		json.NewEncoder(w).Encode(map[string]interface{}{"response": resp})
	})

	// Test with valid token
	body, _ := json.Marshal(map[string]string{
		"command": "PUT cloud:key cloud:val",
		"token":   "http-token",
	})
	req := httptest.NewRequest(http.MethodPost, "/api/exec", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	var res map[string]interface{}
	json.NewDecoder(rec.Body).Decode(&res)
	if res["response"] != "OK" {
		t.Fatalf("expected OK from HTTP exec, got %v", res["response"])
	}

	// Verify key was set in engine
	if val := engine.Get("cloud:key"); val != "cloud:val" {
		t.Fatalf("expected cloud:val, got %s", val)
	}
}

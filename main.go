package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ==========================================
// CONFIGURATION
// ==========================================
const (
	Port            = ":4000"
	WALFile         = "sider.wal"
	DataDir         = "data"
	MemtableLimit   = 100 // Increased for server usage
	MaxLevel        = 16
	Probability     = 0.5
	CmdPut          = byte(0)
	CmdDel          = byte(1)
	CmdPutTTL       = byte(2)
	BloomFilterSize = 1024
)

// Expiring values are stored as an 8-byte Unix-nanosecond timestamp followed
// by the original value. This keeps the existing WAL/SSTable record format
// intact while making TTL state durable across restarts and flushes.
func encodeExpiringValue(value string, expiresAt time.Time) string {
	encoded := make([]byte, 8+len(value))
	binary.LittleEndian.PutUint64(encoded[:8], uint64(expiresAt.UnixNano()))
	copy(encoded[8:], value)
	return string(encoded)
}

func decodeExpiringValue(value string) (string, time.Time, bool) {
	if len(value) < 8 {
		return "", time.Time{}, false
	}
	expiresAt := time.Unix(0, int64(binary.LittleEndian.Uint64([]byte(value[:8]))))
	return value[8:], expiresAt, true
}

func isExpired(kind byte, value string, now time.Time) bool {
	if kind != CmdPutTTL {
		return false
	}
	_, expiresAt, valid := decodeExpiringValue(value)
	return !valid || !now.Before(expiresAt)
}

func visibleValue(value string, kind byte, now time.Time) (string, bool) {
	if kind == CmdDel || isExpired(kind, value, now) {
		return "", false
	}
	if kind == CmdPutTTL {
		decoded, _, valid := decodeExpiringValue(value)
		if !valid {
			return "", false
		}
		return decoded, true
	}
	return value, true
}

// ==========================================
// MEMTABLE (SKIP LIST IMPLEMENTATION)
// ==========================================

type Node struct {
	Key   string
	Value string
	Kind  byte
	Next  []*Node
}

type SkipList struct {
	Head  *Node
	Level int
	Size  int
}

func NewSkipList() *SkipList {
	return &SkipList{
		Head:  &Node{Next: make([]*Node, MaxLevel)},
		Level: 1,
		Size:  0,
	}
}

func (sl *SkipList) Put(key, value string, kind byte) {
	update := make([]*Node, MaxLevel)
	current := sl.Head
	for i := sl.Level - 1; i >= 0; i-- {
		for current.Next[i] != nil && current.Next[i].Key < key {
			current = current.Next[i]
		}
		update[i] = current
	}
	if current.Next[0] != nil && current.Next[0].Key == key {
		current.Next[0].Value = value
		current.Next[0].Kind = kind
		return
	}
	lvl := sl.randomLevel()
	if lvl > sl.Level {
		for i := sl.Level; i < lvl; i++ {
			update[i] = sl.Head
		}
		sl.Level = lvl
	}
	newNode := &Node{Key: key, Value: value, Kind: kind, Next: make([]*Node, lvl)}
	for i := 0; i < lvl; i++ {
		newNode.Next[i] = update[i].Next[i]
		update[i].Next[i] = newNode
	}
	sl.Size++
}

func (sl *SkipList) Get(key string) (string, bool, byte) {
	current := sl.Head
	for i := sl.Level - 1; i >= 0; i-- {
		for current.Next[i] != nil && current.Next[i].Key < key {
			current = current.Next[i]
		}
	}
	current = current.Next[0]
	if current != nil && current.Key == key {
		return current.Value, true, current.Kind
	}
	return "", false, 0
}

func (sl *SkipList) Iterator() []*Node {
	var nodes []*Node
	current := sl.Head.Next[0]
	for current != nil {
		nodes = append(nodes, current)
		current = current.Next[0]
	}
	return nodes
}

func (sl *SkipList) randomLevel() int {
	lvl := 1
	for rand.Float64() < Probability && lvl < MaxLevel {
		lvl++
	}
	return lvl
}

// ==========================================
// BLOOM FILTER & HASHING
// ==========================================

type BloomFilter struct{ BitSet []byte }

func NewBloomFilter() *BloomFilter { return &BloomFilter{BitSet: make([]byte, BloomFilterSize)} }
func (bf *BloomFilter) Add(key string) {
	h1, h2, h3 := hashKey(key)
	bf.setBit(h1)
	bf.setBit(h2)
	bf.setBit(h3)
}
func (bf *BloomFilter) MayContain(key string) bool {
	h1, h2, h3 := hashKey(key)
	return bf.checkBit(h1) && bf.checkBit(h2) && bf.checkBit(h3)
}
func (bf *BloomFilter) setBit(pos uint32) {
	bf.BitSet[(pos/8)%uint32(BloomFilterSize)] |= (1 << (pos % 8))
}
func (bf *BloomFilter) checkBit(pos uint32) bool {
	return (bf.BitSet[(pos/8)%uint32(BloomFilterSize)] & (1 << (pos % 8))) != 0
}
func hashKey(key string) (uint32, uint32, uint32) {
	h := fnv.New32a()
	h.Write([]byte(key))
	v1 := h.Sum32()
	return v1, v1 * 16777619, v1 * 16777619 * 16777619
}

// ==========================================
// WAL & SSTABLE
// ==========================================

type WAL struct{ file *os.File }

func OpenWAL() (*WAL, error) {
	f, err := os.OpenFile(WALFile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	return &WAL{file: f}, err
}
func (w *WAL) WriteEntry(key, value string, kind byte) error {
	buf := new(bytes.Buffer)
	buf.WriteByte(kind)
	binary.Write(buf, binary.LittleEndian, int32(len(key)))
	binary.Write(buf, binary.LittleEndian, int32(len(value)))
	buf.WriteString(key)
	buf.WriteString(value)
	_, err := w.file.Write(buf.Bytes())
	return err
}
func (w *WAL) Clear() { w.file.Close(); os.Truncate(WALFile, 0) }
func (w *WAL) Recover(sl *SkipList) {
	w.file.Seek(0, 0)
	r := bufio.NewReader(w.file)
	for {
		kind, err := r.ReadByte()
		if err != nil {
			break
		}
		var kLen, vLen int32
		binary.Read(r, binary.LittleEndian, &kLen)
		binary.Read(r, binary.LittleEndian, &vLen)
		kBytes := make([]byte, kLen)
		vBytes := make([]byte, vLen)
		io.ReadFull(r, kBytes)
		io.ReadFull(r, vBytes)
		sl.Put(string(kBytes), string(vBytes), kind)
	}
}

func FlushMemTable(sl *SkipList) {
	if _, err := os.Stat(DataDir); os.IsNotExist(err) {
		os.Mkdir(DataDir, 0755)
	}
	f, _ := os.Create(fmt.Sprintf("%s/sstable_%d.db", DataDir, time.Now().UnixNano()))
	defer f.Close()
	bf := NewBloomFilter()
	for _, n := range sl.Iterator() {
		bf.Add(n.Key)
		f.Write([]byte{n.Kind})
		binary.Write(f, binary.LittleEndian, int32(len(n.Key)))
		binary.Write(f, binary.LittleEndian, int32(len(n.Value)))
		f.WriteString(n.Key)
		f.WriteString(n.Value)
	}
	offset, _ := f.Seek(0, io.SeekCurrent)
	f.Write(bf.BitSet)
	binary.Write(f, binary.LittleEndian, int64(offset))
}

func SearchSSTables(key string) (string, bool, byte) {
	files, _ := os.ReadDir(DataDir)
	for i := len(files) - 1; i >= 0; i-- {
		if strings.HasPrefix(files[i].Name(), "temp_") {
			continue
		}
		path := filepath.Join(DataDir, files[i].Name())
		if v, found, k := searchFile(path, key); found {
			return v, true, k
		}
	}
	return "", false, 0
}

func collectKeysWithPrefixFromFile(path, prefix string, keys map[string]struct{}) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil || stat.Size() < 8 {
		return
	}
	f.Seek(stat.Size()-8, 0)
	var limit int64
	if binary.Read(f, binary.LittleEndian, &limit) != nil || limit < 0 || limit > stat.Size()-8 {
		return
	}
	f.Seek(0, 0)
	r := bufio.NewReader(f)
	current := int64(0)
	for current < limit {
		_, err := r.ReadByte()
		if err != nil {
			return
		}
		current++
		var keyLen, valueLen int32
		if binary.Read(r, binary.LittleEndian, &keyLen) != nil || binary.Read(r, binary.LittleEndian, &valueLen) != nil {
			return
		}
		current += 8
		if keyLen < 0 || valueLen < 0 {
			return
		}
		keyBytes := make([]byte, keyLen)
		valueBytes := make([]byte, valueLen)
		if _, err := io.ReadFull(r, keyBytes); err != nil {
			return
		}
		if _, err := io.ReadFull(r, valueBytes); err != nil {
			return
		}
		current += int64(keyLen + valueLen)
		if strings.HasPrefix(string(keyBytes), prefix) {
			keys[string(keyBytes)] = struct{}{}
		}
	}
}

func searchFile(path, key string) (string, bool, byte) {
	f, err := os.Open(path)
	if err != nil {
		return "", false, 0
	}
	defer f.Close()

	stat, _ := f.Stat()
	if stat.Size() < 8 {
		return "", false, 0
	}
	f.Seek(stat.Size()-8, 0)
	var bfOffset int64
	binary.Read(f, binary.LittleEndian, &bfOffset)

	f.Seek(bfOffset, 0)
	bfBytes := make([]byte, BloomFilterSize)
	io.ReadFull(f, bfBytes)
	if !(&BloomFilter{BitSet: bfBytes}).MayContain(key) {
		return "", false, 0
	}

	f.Seek(0, 0)
	r := bufio.NewReader(f)
	readBytes := int64(0)
	for readBytes < bfOffset {
		kind, err := r.ReadByte()
		if err != nil {
			break
		}
		readBytes++
		var kLen, vLen int32
		binary.Read(r, binary.LittleEndian, &kLen)
		binary.Read(r, binary.LittleEndian, &vLen)
		readBytes += 8
		kBytes := make([]byte, kLen)
		vBytes := make([]byte, vLen)
		io.ReadFull(r, kBytes)
		io.ReadFull(r, vBytes)
		readBytes += int64(kLen + vLen)
		if string(kBytes) == key {
			return string(vBytes), true, kind
		}
	}
	return "", false, 0
}

// ==========================================
// COMPACTION
// ==========================================

// Simplified compaction: merges all files into one.
func Compact(e *Engine) {
	e.mu.Lock()
	defer e.mu.Unlock()

	fmt.Println(">> Compaction Started")
	files, _ := os.ReadDir(DataDir)
	var paths []string
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".db") {
			paths = append(paths, filepath.Join(DataDir, f.Name()))
		}
	}
	sort.Strings(paths)

	// In a real DB, we would use K-Way Merge Sort here with iterators.
	// For this snippet, we load keys into a map to dedup (memory heavy but simple for demo)
	type record struct {
		value string
		kind  byte
	}
	merged := make(map[string]record)

	// Read oldest to newest
	for _, p := range paths {
		f, _ := os.Open(p)
		r := bufio.NewReader(f)

		stat, _ := f.Stat()
		size := stat.Size()
		// Get Footer
		f.Seek(size-8, 0)
		var limit int64
		binary.Read(f, binary.LittleEndian, &limit)
		f.Seek(0, 0)

		current := int64(0)
		for current < limit {
			kind, _ := r.ReadByte()
			current++
			var kl, vl int32
			binary.Read(r, binary.LittleEndian, &kl)
			binary.Read(r, binary.LittleEndian, &vl)
			current += 8
			k := make([]byte, kl)
			v := make([]byte, vl)
			io.ReadFull(r, k)
			io.ReadFull(r, v)
			current += int64(kl + vl)

			key := string(k)
			if kind == CmdDel || isExpired(kind, string(v), time.Now()) {
				delete(merged, key)
			} else {
				merged[key] = record{value: string(v), kind: kind}
			}
		}
		f.Close()
	}

	// Write new file
	newFile := fmt.Sprintf("%s/sstable_%d_compacted.db", DataDir, time.Now().UnixNano())
	f, _ := os.Create(newFile)
	bf := NewBloomFilter()

	// Write map to file
	for k, record := range merged {
		bf.Add(k)
		f.Write([]byte{record.kind})
		binary.Write(f, binary.LittleEndian, int32(len(k)))
		binary.Write(f, binary.LittleEndian, int32(len(record.value)))
		f.WriteString(k)
		f.WriteString(record.value)
	}
	off, _ := f.Seek(0, io.SeekCurrent)
	f.Write(bf.BitSet)
	binary.Write(f, binary.LittleEndian, int64(off))
	f.Close()

	// Remove old files
	for _, p := range paths {
		os.Remove(p)
	}
	fmt.Println(">> Compaction Done")
}

// ==========================================
// ENGINE & NETWORK SERVER
// ==========================================

type Engine struct {
	MemTable *SkipList
	Wal      *WAL
	mu       sync.RWMutex
}

func NewEngine() *Engine {
	sl := NewSkipList()
	wal, _ := OpenWAL()
	wal.Recover(sl)
	return &Engine{MemTable: sl, Wal: wal}
}

func (e *Engine) putLocked(key, value string, kind byte) {
	if err := e.Wal.WriteEntry(key, value, kind); err != nil {
		log.Printf("WAL write failed for key %q: %v", key, err)
		return
	}
	e.MemTable.Put(key, value, kind)
	if e.MemTable.Size >= MemtableLimit {
		fmt.Println(">> MemTable full. Flushing...")
		FlushMemTable(e.MemTable)
		e.MemTable = NewSkipList()
		e.Wal.Clear()
		e.Wal, _ = OpenWAL()
	}
}

func (e *Engine) Put(key, value string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.putLocked(key, value, CmdPut)
}

func (e *Engine) PutWithTTL(key, value string, ttl time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	expiresAt := time.Now().Add(ttl)
	e.putLocked(key, encodeExpiringValue(value, expiresAt), CmdPutTTL)
}

func (e *Engine) Delete(key string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.putLocked(key, "", CmdDel)
}

func (e *Engine) lookupLocked(key string) (string, bool, byte) {
	if value, found, kind := e.MemTable.Get(key); found {
		return value, true, kind
	}
	return SearchSSTables(key)
}

func (e *Engine) Get(key string) string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if value, found, kind := e.lookupLocked(key); found {
		if visible, ok := visibleValue(value, kind, time.Now()); ok {
			return visible
		}
	}
	return "(nil)"
}

func (e *Engine) Expire(key string, ttl time.Duration) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	value, found, kind := e.lookupLocked(key)
	current, visible := visibleValue(value, kind, time.Now())
	if !found || !visible {
		return false
	}
	e.putLocked(key, encodeExpiringValue(current, time.Now().Add(ttl)), CmdPutTTL)
	return true
}

func (e *Engine) TTL(key string) int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	value, found, kind := e.lookupLocked(key)
	if !found || kind == CmdDel || isExpired(kind, value, time.Now()) {
		return -2
	}
	if kind != CmdPutTTL {
		return -1
	}
	_, expiresAt, valid := decodeExpiringValue(value)
	if !valid {
		return -2
	}
	remaining := int64(time.Until(expiresAt) / time.Second)
	if remaining < 0 {
		return -2
	}
	return remaining
}

func (e *Engine) ClearPrefix(prefix string) int {
	e.mu.Lock()
	defer e.mu.Unlock()

	keys := make(map[string]struct{})
	for _, node := range e.MemTable.Iterator() {
		if strings.HasPrefix(node.Key, prefix) {
			keys[node.Key] = struct{}{}
		}
	}
	if files, err := os.ReadDir(DataDir); err == nil {
		for _, file := range files {
			if strings.HasSuffix(file.Name(), ".db") {
				collectKeysWithPrefixFromFile(filepath.Join(DataDir, file.Name()), prefix, keys)
			}
		}
	}

	for key := range keys {
		e.putLocked(key, "", CmdDel)
	}
	return len(keys)
}

type Client struct {
	conn    net.Conn
	writeMu sync.Mutex
}

func (c *Client) writeLine(line string) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err := c.conn.Write([]byte(line + "\n"))
	return err
}

type Broker struct {
	mu       sync.RWMutex
	channels map[string]map[*Client]struct{}
}

func NewBroker() *Broker {
	return &Broker{channels: make(map[string]map[*Client]struct{})}
}

func (b *Broker) Subscribe(client *Client, channel string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.channels[channel] == nil {
		b.channels[channel] = make(map[*Client]struct{})
	}
	b.channels[channel][client] = struct{}{}
}

func (b *Broker) Unsubscribe(client *Client, channel string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if subscribers, ok := b.channels[channel]; ok {
		delete(subscribers, client)
		if len(subscribers) == 0 {
			delete(b.channels, channel)
		}
	}
}

func (b *Broker) RemoveClient(client *Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for channel, subscribers := range b.channels {
		delete(subscribers, client)
		if len(subscribers) == 0 {
			delete(b.channels, channel)
		}
	}
}

func (b *Broker) Publish(channel, message string) int {
	b.mu.RLock()
	subscribers := make([]*Client, 0, len(b.channels[channel]))
	for client := range b.channels[channel] {
		subscribers = append(subscribers, client)
	}
	b.mu.RUnlock()

	delivered := 0
	for _, client := range subscribers {
		if err := client.writeLine("MESSAGE " + channel + " " + message); err != nil {
			b.RemoveClient(client)
			continue
		}
		delivered++
	}
	return delivered
}

func handleConnection(conn net.Conn, e *Engine) {
	defer conn.Close()
	client := &Client{conn: conn}
	defer broker.RemoveClient(client)
	reader := bufio.NewReader(conn)

	for {
		// Read command line
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		} // Client disconnected

		line = strings.TrimSpace(line)
		parts := strings.SplitN(line, " ", 3)
		if len(parts) == 0 || parts[0] == "" {
			continue
		}

		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "PUT":
			if len(parts) < 3 {
				client.writeLine("ERR Usage: PUT <key> <val>")
				continue
			}
			e.Put(parts[1], parts[2])
			client.writeLine("OK")

		case "PUTEX":
			putParts := strings.SplitN(line, " ", 4)
			if len(putParts) < 4 {
				client.writeLine("ERR Usage: PUTEX <key> <ttl-seconds> <val>")
				continue
			}
			seconds, err := strconv.ParseInt(putParts[2], 10, 64)
			if err != nil || seconds <= 0 {
				client.writeLine("ERR TTL must be a positive integer")
				continue
			}
			e.PutWithTTL(putParts[1], putParts[3], time.Duration(seconds)*time.Second)
			client.writeLine("OK")

		case "GET":
			if len(parts) < 2 {
				client.writeLine("ERR Usage: GET <key>")
				continue
			}
			val := e.Get(parts[1])
			client.writeLine(val)

		case "DEL":
			if len(parts) < 2 {
				client.writeLine("ERR Usage: DEL <key>")
				continue
			}
			e.Delete(parts[1])
			client.writeLine("OK")

		case "EXPIRE":
			if len(parts) < 3 {
				client.writeLine("ERR Usage: EXPIRE <key> <ttl-seconds>")
				continue
			}
			seconds, err := strconv.ParseInt(parts[2], 10, 64)
			if err != nil || seconds <= 0 {
				client.writeLine("ERR TTL must be a positive integer")
				continue
			}
			if e.Expire(parts[1], time.Duration(seconds)*time.Second) {
				client.writeLine("1")
			} else {
				client.writeLine("0")
			}

		case "TTL":
			if len(parts) < 2 {
				client.writeLine("ERR Usage: TTL <key>")
				continue
			}
			client.writeLine(strconv.FormatInt(e.TTL(parts[1]), 10))

		case "CLEAR":
			if len(parts) < 2 {
				client.writeLine("ERR Usage: CLEAR <key-prefix>")
				continue
			}
			client.writeLine("CLEARED " + strconv.Itoa(e.ClearPrefix(parts[1])))

		case "SUBSCRIBE":
			if len(parts) < 2 {
				client.writeLine("ERR Usage: SUBSCRIBE <channel>")
				continue
			}
			broker.Subscribe(client, parts[1])
			client.writeLine("SUBSCRIBED " + parts[1])

		case "UNSUBSCRIBE":
			if len(parts) < 2 {
				client.writeLine("ERR Usage: UNSUBSCRIBE <channel>")
				continue
			}
			broker.Unsubscribe(client, parts[1])
			client.writeLine("UNSUBSCRIBED " + parts[1])

		case "PUBLISH":
			if len(parts) < 3 {
				client.writeLine("ERR Usage: PUBLISH <channel> <message>")
				continue
			}
			delivered := broker.Publish(parts[1], parts[2])
			client.writeLine("PUBLISHED " + strconv.Itoa(delivered))

		case "COMPACT":
			go Compact(e) // Run in background
			client.writeLine("OK Compact Started")

		default:
			client.writeLine("ERR Unknown Command")
		}
	}
}

var broker = NewBroker()

func main() {
	rand.Seed(time.Now().UnixNano())
	engine := NewEngine()

	listener, err := net.Listen("tcp", Port)
	if err != nil {
		log.Fatal("Error starting server:", err)
	}
	defer listener.Close()

	fmt.Println("========================================")
	fmt.Printf("   SIDER SERVER LISTENING ON %s   \n", Port)
	fmt.Println("   Version: 1.0.1                      ")
	fmt.Println("   Author:  AgnibhaRay                 ")
	fmt.Println("========================================")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Connection error:", err)
			continue
		}
		go handleConnection(conn, engine)
	}
}

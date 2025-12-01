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
	BloomFilterSize = 1024
)

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
	bf.setBit(h1); bf.setBit(h2); bf.setBit(h3)
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
	h := fnv.New32a(); h.Write([]byte(key)); v1 := h.Sum32()
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
		if err != nil { break }
		var kLen, vLen int32
		binary.Read(r, binary.LittleEndian, &kLen)
		binary.Read(r, binary.LittleEndian, &vLen)
		kBytes := make([]byte, kLen); vBytes := make([]byte, vLen)
		io.ReadFull(r, kBytes); io.ReadFull(r, vBytes)
		sl.Put(string(kBytes), string(vBytes), kind)
	}
}

func FlushMemTable(sl *SkipList) {
	if _, err := os.Stat(DataDir); os.IsNotExist(err) { os.Mkdir(DataDir, 0755) }
	f, _ := os.Create(fmt.Sprintf("%s/sstable_%d.db", DataDir, time.Now().UnixNano()))
	defer f.Close()
	bf := NewBloomFilter()
	for _, n := range sl.Iterator() {
		bf.Add(n.Key)
		f.Write([]byte{n.Kind})
		binary.Write(f, binary.LittleEndian, int32(len(n.Key)))
		binary.Write(f, binary.LittleEndian, int32(len(n.Value)))
		f.WriteString(n.Key); f.WriteString(n.Value)
	}
	offset, _ := f.Seek(0, io.SeekCurrent)
	f.Write(bf.BitSet)
	binary.Write(f, binary.LittleEndian, int64(offset))
}

func SearchSSTables(key string) (string, bool, byte) {
	files, _ := os.ReadDir(DataDir)
	for i := len(files) - 1; i >= 0; i-- {
		if strings.HasPrefix(files[i].Name(), "temp_") { continue }
		path := filepath.Join(DataDir, files[i].Name())
		if v, found, k := searchFile(path, key); found { return v, true, k }
	}
	return "", false, 0
}

func searchFile(path, key string) (string, bool, byte) {
	f, err := os.Open(path)
	if err != nil { return "", false, 0 }
	defer f.Close()
	
	stat, _ := f.Stat()
	if stat.Size() < 8 { return "", false, 0 }
	f.Seek(stat.Size()-8, 0)
	var bfOffset int64
	binary.Read(f, binary.LittleEndian, &bfOffset)
	
	f.Seek(bfOffset, 0)
	bfBytes := make([]byte, BloomFilterSize)
	io.ReadFull(f, bfBytes)
	if !(&BloomFilter{BitSet: bfBytes}).MayContain(key) { return "", false, 0 }

	f.Seek(0, 0)
	r := bufio.NewReader(f)
	readBytes := int64(0)
	for readBytes < bfOffset {
		kind, err := r.ReadByte()
		if err != nil { break }
		readBytes++
		var kLen, vLen int32
		binary.Read(r, binary.LittleEndian, &kLen)
		binary.Read(r, binary.LittleEndian, &vLen)
		readBytes += 8
		kBytes := make([]byte, kLen); vBytes := make([]byte, vLen)
		io.ReadFull(r, kBytes); io.ReadFull(r, vBytes)
		readBytes += int64(kLen + vLen)
		if string(kBytes) == key { return string(vBytes), true, kind }
	}
	return "", false, 0
}

// ==========================================
// COMPACTION
// ==========================================

// Simplified compaction: merges all files into one.
func Compact(e *Engine) {
	fmt.Println(">> Compaction Started")
	files, _ := os.ReadDir(DataDir)
	var paths []string
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".db") { paths = append(paths, filepath.Join(DataDir, f.Name())) }
	}
	sort.Strings(paths)
	
	// In a real DB, we would use K-Way Merge Sort here with iterators.
	// For this snippet, we load keys into a map to dedup (memory heavy but simple for demo)
	merged := make(map[string]string)
	
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
			current+=8
			k := make([]byte, kl); v := make([]byte, vl)
			io.ReadFull(r, k); io.ReadFull(r, v)
			current += int64(kl + vl)
			
			if kind == CmdDel {
				delete(merged, string(k))
			} else {
				merged[string(k)] = string(v)
			}
		}
		f.Close()
	}

	// Write new file
	newFile := fmt.Sprintf("%s/sstable_%d_compacted.db", DataDir, time.Now().UnixNano())
	f, _ := os.Create(newFile)
	bf := NewBloomFilter()
	
	// Write map to file
	for k, v := range merged {
		bf.Add(k)
		f.Write([]byte{CmdPut})
		binary.Write(f, binary.LittleEndian, int32(len(k)))
		binary.Write(f, binary.LittleEndian, int32(len(v)))
		f.WriteString(k); f.WriteString(v)
	}
	off, _ := f.Seek(0, io.SeekCurrent)
	f.Write(bf.BitSet)
	binary.Write(f, binary.LittleEndian, int64(off))
	f.Close()

	// Remove old files
	for _, p := range paths { os.Remove(p) }
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

func (e *Engine) Put(key, value string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Wal.WriteEntry(key, value, CmdPut)
	e.MemTable.Put(key, value, CmdPut)
	if e.MemTable.Size >= MemtableLimit {
		fmt.Println(">> MemTable full. Flushing...")
		FlushMemTable(e.MemTable)
		e.MemTable = NewSkipList()
		e.Wal.Clear()
		e.Wal, _ = OpenWAL()
	}
}

func (e *Engine) Get(key string) string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	// MemTable
	if v, found, k := e.MemTable.Get(key); found {
		if k == CmdDel { return "(nil)" }
		return v
	}
	// SSTables
	if v, found, k := SearchSSTables(key); found && k == CmdPut {
		return v
	}
	return "(nil)"
}

func handleConnection(conn net.Conn, e *Engine) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		// Read command line
		line, err := reader.ReadString('\n')
		if err != nil { break } // Client disconnected
		
		line = strings.TrimSpace(line)
		parts := strings.SplitN(line, " ", 3)
		if len(parts) == 0 { continue }

		cmd := strings.ToUpper(parts[0])
		
		switch cmd {
		case "PUT":
			if len(parts) < 3 {
				conn.Write([]byte("ERR Usage: PUT <key> <val>\n"))
				continue
			}
			e.Put(parts[1], parts[2])
			conn.Write([]byte("OK\n"))
		
		case "GET":
			if len(parts) < 2 {
				conn.Write([]byte("ERR Usage: GET <key>\n"))
				continue
			}
			val := e.Get(parts[1])
			conn.Write([]byte(val + "\n"))
			
		case "DEL":
			if len(parts) < 2 {
				conn.Write([]byte("ERR Usage: DEL <key>\n"))
				continue
			}
			e.mu.Lock()
			e.Wal.WriteEntry(parts[1], "", CmdDel)
			e.MemTable.Put(parts[1], "", CmdDel)
			e.mu.Unlock()
			conn.Write([]byte("OK\n"))

		case "COMPACT":
			go Compact(e) // Run in background
			conn.Write([]byte("OK Compact Started\n"))

		default:
			conn.Write([]byte("ERR Unknown Command\n"))
		}
	}
}

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
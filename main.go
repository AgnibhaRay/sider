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
	WALFile         = "sider.wal"
	DataDir         = "data"
	MemtableLimit   = 10 // Entries before flushing to disk
	MaxLevel        = 16 // Max level for SkipList
	Probability     = 0.5
	CmdPut          = byte(0) // Marker for Put operation
	CmdDel          = byte(1) // Marker for Delete (Tombstone)
	BloomFilterSize = 1024    // Size of bitset in bytes
)

// ==========================================
// MEMTABLE (SKIP LIST IMPLEMENTATION)
// ==========================================

// Node represents a node in the Skip List
type Node struct {
	Key   string
	Value string
	Kind  byte // Put or Delete
	Next  []*Node
}

// SkipList is our in-memory sorted structure
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

	newNode := &Node{
		Key:   key,
		Value: value,
		Kind:  kind,
		Next:  make([]*Node, lvl),
	}

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
// BLOOM FILTER
// ==========================================

type BloomFilter struct {
	BitSet []byte
}

func NewBloomFilter() *BloomFilter {
	return &BloomFilter{
		BitSet: make([]byte, BloomFilterSize),
	}
}

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
	idx := (pos / 8) % uint32(BloomFilterSize)
	bit := uint32(pos % 8)
	bf.BitSet[idx] |= (1 << bit)
}

func (bf *BloomFilter) checkBit(pos uint32) bool {
	idx := (pos / 8) % uint32(BloomFilterSize)
	bit := uint32(pos % 8)
	return (bf.BitSet[idx] & (1 << bit)) != 0
}

func hashKey(key string) (uint32, uint32, uint32) {
	h := fnv.New32a()
	h.Write([]byte(key))
	v1 := h.Sum32()

	// Simple mixing for demo purposes to get 3 hash variants
	v2 := v1 * 16777619
	v3 := v2 * 16777619
	return v1, v2, v3
}

// ==========================================
// WRITE AHEAD LOG (WAL)
// ==========================================

type WAL struct {
	file *os.File
}

func OpenWAL() (*WAL, error) {
	f, err := os.OpenFile(WALFile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f}, nil
}

// Format: [Kind(1)][KeyLen(4)][ValLen(4)][Key][Value]
func (w *WAL) WriteEntry(key, value string, kind byte) error {
	kBytes := []byte(key)
	vBytes := []byte(value)

	buf := new(bytes.Buffer)
	buf.WriteByte(kind)
	binary.Write(buf, binary.LittleEndian, int32(len(kBytes)))
	binary.Write(buf, binary.LittleEndian, int32(len(vBytes)))
	buf.Write(kBytes)
	buf.Write(vBytes)

	_, err := w.file.Write(buf.Bytes())
	return err
}

func (w *WAL) Close() error {
	return w.file.Close()
}

func (w *WAL) Clear() error {
	w.file.Close()
	return os.Truncate(WALFile, 0)
}

func (w *WAL) Recover(sl *SkipList) error {
	_, err := w.file.Seek(0, 0)
	if err != nil {
		return err
	}

	reader := bufio.NewReader(w.file)
	for {
		kind, err := reader.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		var kLen, vLen int32
		binary.Read(reader, binary.LittleEndian, &kLen)
		binary.Read(reader, binary.LittleEndian, &vLen)

		kBytes := make([]byte, kLen)
		vBytes := make([]byte, vLen)

		io.ReadFull(reader, kBytes)
		io.ReadFull(reader, vBytes)

		sl.Put(string(kBytes), string(vBytes), kind)
	}
	return nil
}

// ==========================================
// SSTABLE ITERATOR (For Compaction)
// ==========================================

type SSTIterator struct {
	file      *os.File
	reader    *bufio.Reader
	limit     int64
	bytesRead int64
	currentK  string
	currentV  string
	currentOp byte
	done      bool
}

func NewSSTIterator(path string) (*SSTIterator, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// Read footer to find limit (data end)
	stat, _ := f.Stat()
	if stat.Size() < 8 {
		f.Close()
		return nil, fmt.Errorf("invalid sstable")
	}
	f.Seek(stat.Size()-8, 0)
	var bfOffset int64
	binary.Read(f, binary.LittleEndian, &bfOffset)

	f.Seek(0, 0)

	it := &SSTIterator{
		file:   f,
		reader: bufio.NewReader(f),
		limit:  bfOffset,
		done:   false,
	}
	it.Next() // Prime the first value
	return it, nil
}

func (it *SSTIterator) Next() {
	if it.bytesRead >= it.limit {
		it.done = true
		return
	}

	kind, err := it.reader.ReadByte()
	if err == io.EOF {
		it.done = true
		return
	}
	it.bytesRead++

	var kLen, vLen int32
	binary.Read(it.reader, binary.LittleEndian, &kLen)
	binary.Read(it.reader, binary.LittleEndian, &vLen)
	it.bytesRead += 8

	kBytes := make([]byte, kLen)
	vBytes := make([]byte, vLen)
	io.ReadFull(it.reader, kBytes)
	io.ReadFull(it.reader, vBytes)
	it.bytesRead += int64(kLen + vLen)

	it.currentK = string(kBytes)
	it.currentV = string(vBytes)
	it.currentOp = kind
}

func (it *SSTIterator) Close() {
	it.file.Close()
}

// ==========================================
// SSTABLE (DISK STORAGE)
// ==========================================

// FlushMemTable creates a file with Data Block + Bloom Filter + Footer
func FlushMemTable(sl *SkipList) error {
	if _, err := os.Stat(DataDir); os.IsNotExist(err) {
		os.Mkdir(DataDir, 0755)
	}

	filename := fmt.Sprintf("%s/sstable_%d.db", DataDir, time.Now().UnixNano())
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	bf := NewBloomFilter()
	nodes := sl.Iterator()

	// 1. Write Data Blocks
	for _, node := range nodes {
		bf.Add(node.Key) // Add to Bloom Filter

		kBytes := []byte(node.Key)
		vBytes := []byte(node.Value)

		f.Write([]byte{node.Kind})
		binary.Write(f, binary.LittleEndian, int32(len(kBytes)))
		binary.Write(f, binary.LittleEndian, int32(len(vBytes)))
		f.Write(kBytes)
		f.Write(vBytes)
	}

	// 2. Write Bloom Filter (Fixed Size)
	bfOffset, _ := f.Seek(0, io.SeekCurrent)
	f.Write(bf.BitSet)

	// 3. Write Footer (Offset of Bloom Filter)
	binary.Write(f, binary.LittleEndian, int64(bfOffset))

	return nil
}

func SearchSSTables(key string) (string, bool, byte) {
	files, _ := os.ReadDir(DataDir)
	// Search in reverse order (newest files first)
	for i := len(files) - 1; i >= 0; i-- {
		// Ignore temporary merge files
		if strings.HasPrefix(files[i].Name(), "temp_") {
			continue
		}
		path := filepath.Join(DataDir, files[i].Name())
		val, found, kind := searchFile(path, key)
		if found {
			return val, true, kind
		}
	}
	return "", false, 0
}

func searchFile(path, searchKey string) (string, bool, byte) {
	f, err := os.Open(path)
	if err != nil {
		return "", false, 0
	}
	defer f.Close()

	// 1. Read Footer (Last 8 bytes) to find Bloom Filter
	stat, _ := f.Stat()
	fileSize := stat.Size()
	if fileSize < 8 {
		return "", false, 0
	}

	f.Seek(fileSize-8, 0)
	var bfOffset int64
	binary.Read(f, binary.LittleEndian, &bfOffset)

	// 2. Read Bloom Filter
	f.Seek(bfOffset, 0)
	bfBytes := make([]byte, BloomFilterSize)
	io.ReadFull(f, bfBytes)

	bf := &BloomFilter{BitSet: bfBytes}
	if !bf.MayContain(searchKey) {
		return "", false, 0
	}

	// 3. Scan Data Blocks
	f.Seek(0, 0)
	reader := bufio.NewReader(f)

	bytesRead := int64(0)
	for bytesRead < bfOffset {
		kind, err := reader.ReadByte()
		if err == io.EOF {
			break
		}
		bytesRead++

		var kLen, vLen int32
		binary.Read(reader, binary.LittleEndian, &kLen)
		binary.Read(reader, binary.LittleEndian, &vLen)
		bytesRead += 8

		kBytes := make([]byte, kLen)
		vBytes := make([]byte, vLen)
		io.ReadFull(reader, kBytes)
		io.ReadFull(reader, vBytes)
		bytesRead += int64(kLen + vLen)

		if string(kBytes) == searchKey {
			return string(vBytes), true, kind
		}
	}
	return "", false, 0
}

// ==========================================
// DATABASE ENGINE
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
	e.write(key, value, CmdPut)
}

func (e *Engine) Delete(key string) {
	e.write(key, "", CmdDel)
}

func (e *Engine) write(key, value string, kind byte) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.Wal.WriteEntry(key, value, kind); err != nil {
		log.Println("Error writing to WAL:", err)
		return
	}

	e.MemTable.Put(key, value, kind)

	if e.MemTable.Size >= MemtableLimit {
		fmt.Println(">> MemTable full! Flushing to SSTable...")
		FlushMemTable(e.MemTable)
		e.MemTable = NewSkipList()
		e.Wal.Clear()
		e.Wal.file.Close()
		e.Wal, _ = OpenWAL()
	}
}

func (e *Engine) Get(key string) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 1. Check MemTable
	if val, found, kind := e.MemTable.Get(key); found {
		if kind == CmdDel {
			return "", false
		}
		return val, true
	}

	// 2. Check SSTables (Disk)
	val, found, kind := SearchSSTables(key)
	if found && kind == CmdPut {
		return val, true
	}
	return "", false
}

// Compact merges ALL existing sstables into a single new one
func (e *Engine) Compact() {
	e.mu.Lock()
	defer e.mu.Unlock()
	fmt.Println(">> Starting Compaction...")

	files, _ := os.ReadDir(DataDir)
	var sstFiles []string
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "sstable_") && strings.HasSuffix(f.Name(), ".db") {
			sstFiles = append(sstFiles, filepath.Join(DataDir, f.Name()))
		}
	}

	if len(sstFiles) < 2 {
		fmt.Println(">> Not enough files to compact.")
		return
	}

	// Sort files by time (implicit in filename)
	sort.Strings(sstFiles)

	// Open iterators
	var iterators []*SSTIterator
	for _, path := range sstFiles {
		it, err := NewSSTIterator(path)
		if err == nil {
			iterators = append(iterators, it)
		}
	}

	// Create Output File
	outFilename := fmt.Sprintf("%s/sstable_%d_compacted.db", DataDir, time.Now().UnixNano())
	outFile, _ := os.Create(outFilename)
	bf := NewBloomFilter()

	// Merge Loop
	lastKey := ""
	for {
		// Find iterator with smallest key
		minKey := ""

		activeCount := 0
		for _, it := range iterators {
			if it.done {
				continue
			}
			activeCount++
			if minKey == "" || it.currentK < minKey {
				minKey = it.currentK
			} else if it.currentK == minKey {
				// Duplicate key!
				// Since we iterate files Oldest -> Newest (by sort),
				// the 'it' that comes LATER in the list is newer.
				// We want to keep the NEWER version and discard the OLDER 'minIt'.
				// So we set minIt = it (update winner) and advance the old minIt.
				// Actually, we need to advance ALL iterators that have this key,
				// but only write the value from the newest one.
			}
		}

		if activeCount == 0 {
			break
		}

		// Resolution phase: Find the "Winner" for this minKey
		// The winner is the iterator with this key that appears LAST in the `iterators` list
		var winner *SSTIterator
		for _, it := range iterators {
			if !it.done && it.currentK == minKey {
				winner = it // Update winner to the latest one found
			}
		}

		// Advance ALL iterators that had this key
		for _, it := range iterators {
			if !it.done && it.currentK == minKey {
				it.Next()
			}
		}

		// Write Winner to disk (if not a deleted tombstone)
		// NOTE: In a full compaction, we can drop tombstones!
		if winner.currentOp != CmdDel {
			// Basic dedup check: don't write same key twice if logic failed
			if minKey != lastKey {
				bf.Add(minKey)
				kBytes := []byte(minKey)
				vBytes := []byte(winner.currentV)

				outFile.Write([]byte{winner.currentOp})
				binary.Write(outFile, binary.LittleEndian, int32(len(kBytes)))
				binary.Write(outFile, binary.LittleEndian, int32(len(vBytes)))
				outFile.Write(kBytes)
				outFile.Write(vBytes)
				lastKey = minKey
			}
		}
	}

	// Finalize New File
	bfOffset, _ := outFile.Seek(0, io.SeekCurrent)
	outFile.Write(bf.BitSet)
	binary.Write(outFile, binary.LittleEndian, int64(bfOffset))
	outFile.Close()

	// Cleanup Old Files & Iterators
	for _, it := range iterators {
		it.Close()
	}
	for _, path := range sstFiles {
		os.Remove(path)
	}

	fmt.Printf(">> Compaction Complete. Merged %d files into 1.\n", len(sstFiles))
}

func (e *Engine) Close() {
	e.Wal.Close()
}

// ==========================================
// CLI / MAIN
// ==========================================

func main() {
	rand.Seed(time.Now().UnixNano())
	engine := NewEngine()
	defer engine.Close()

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("========================================")
	fmt.Println("   SIDER DB (Advanced LSM Engine)      ")
	fmt.Println("   Version: 1.0.0                      ")
	fmt.Println("   Author:  AgnibhaRay                 ")
	fmt.Println("   Now with Compaction!                ")
	fmt.Println("========================================")
	fmt.Println("Commands: put <k> <v> | get <k> | del <k> | compact | exit")

	for {
		fmt.Print("sider> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		parts := strings.SplitN(input, " ", 3)

		if len(parts) == 0 {
			continue
		}
		cmd := parts[0]

		switch cmd {
		case "put":
			if len(parts) < 3 {
				fmt.Println("Usage: put <key> <value>")
				continue
			}
			engine.Put(parts[1], parts[2])
			fmt.Println("OK")

		case "del":
			if len(parts) < 2 {
				fmt.Println("Usage: del <key>")
				continue
			}
			engine.Delete(parts[1])
			fmt.Println("OK (Tombstone written)")

		case "get":
			if len(parts) < 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			val, found := engine.Get(parts[1])
			if found {
				fmt.Printf("\"%s\"\n", val)
			} else {
				fmt.Println("(nil)")
			}

		case "compact":
			engine.Compact()

		case "exit":
			fmt.Println("Bye!")
			return

		default:
			fmt.Println("Unknown command")
		}
	}
}

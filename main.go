package main

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ==========================================
// CONFIGURATION & GLOBAL DEFAULTS
// ==========================================
const (
	DefaultPort     = ":4000"
	DefaultHTTPPort = ":4001"
	DefaultWALFile  = "sider.wal"
	DefaultDataDir  = "data"
	MaxLevel        = 16
	Probability     = 0.5
	CmdPut          = byte(0)
	CmdDel          = byte(1)
	CmdPutTTL       = byte(2)
	BloomFilterSize = 1024
)

var (
	Port          = DefaultPort
	HTTPPort      = DefaultHTTPPort
	WALFile       = DefaultWALFile
	DataDir       = DefaultDataDir
	MemtableLimit = 100 // Threshold for flushing to SSTable
	AuthToken     = ""
	InstanceName  = "sider-primary"
	StartTime     = time.Now()
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
	target := current.Next[0]
	if target != nil && target.Key == key {
		return target.Value, true, target.Kind
	}
	return "", false, 0
}

func (sl *SkipList) randomLevel() int {
	lvl := 1
	for rand.Float64() < Probability && lvl < MaxLevel {
		lvl++
	}
	return lvl
}

func (sl *SkipList) Iterator() []*Node {
	var nodes []*Node
	curr := sl.Head.Next[0]
	for curr != nil {
		nodes = append(nodes, curr)
		curr = curr.Next[0]
	}
	return nodes
}

func (sl *SkipList) ByteSize() int64 {
	var bytes int64
	curr := sl.Head.Next[0]
	for curr != nil {
		bytes += int64(len(curr.Key) + len(curr.Value) + 16)
		curr = curr.Next[0]
	}
	return bytes
}

// ==========================================
// BLOOM FILTER (1024-BYTE BITSET)
// ==========================================

type BloomFilter struct {
	BitSet []byte
}

func NewBloomFilter() *BloomFilter {
	return &BloomFilter{BitSet: make([]byte, BloomFilterSize)}
}

func (bf *BloomFilter) hash1(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func (bf *BloomFilter) hash2(key string) uint32 {
	h := fnv.New32()
	h.Write([]byte(key))
	return h.Sum32()
}

func (bf *BloomFilter) Add(key string) {
	totalBits := uint32(BloomFilterSize * 8)
	idx1 := bf.hash1(key) % totalBits
	idx2 := bf.hash2(key) % totalBits

	bf.BitSet[idx1/8] |= 1 << (idx1 % 8)
	bf.BitSet[idx2/8] |= 1 << (idx2 % 8)
}

func (bf *BloomFilter) MayContain(key string) bool {
	totalBits := uint32(BloomFilterSize * 8)
	idx1 := bf.hash1(key) % totalBits
	idx2 := bf.hash2(key) % totalBits

	if (bf.BitSet[idx1/8] & (1 << (idx1 % 8))) == 0 {
		return false
	}
	if (bf.BitSet[idx2/8] & (1 << (idx2 % 8))) == 0 {
		return false
	}
	return true
}

// ==========================================
// WRITE-AHEAD LOG (WAL)
// ==========================================

type WAL struct {
	file *os.File
	mu   sync.Mutex
	path string
}

func OpenWAL(path string) (*WAL, error) {
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		os.MkdirAll(dir, 0755)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f, path: path}, nil
}

func (w *WAL) WriteEntry(key, value string, kind byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	buf := new(bytes.Buffer)
	buf.WriteByte(kind)
	binary.Write(buf, binary.LittleEndian, int32(len(key)))
	binary.Write(buf, binary.LittleEndian, int32(len(value)))
	buf.WriteString(key)
	buf.WriteString(value)

	_, err := w.file.Write(buf.Bytes())
	if err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL) Clear() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.file.Close()
	os.Truncate(w.path, 0)
}

func (w *WAL) Recover(sl *SkipList) {
	w.mu.Lock()
	defer w.mu.Unlock()
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

// ==========================================
// SSTABLE DISK PERSISTENCE & COMPACTION
// ==========================================

func FlushMemTable(sl *SkipList, dataDir string) {
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		os.MkdirAll(dataDir, 0755)
	}
	filePath := filepath.Join(dataDir, fmt.Sprintf("sstable_%d.db", time.Now().UnixNano()))
	f, err := os.Create(filePath)
	if err != nil {
		log.Printf("Error creating SSTable %s: %v", filePath, err)
		return
	}
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

func SearchSSTables(key string, dataDir string) (string, bool, byte) {
	files, err := os.ReadDir(dataDir)
	if err != nil {
		return "", false, 0
	}
	for i := len(files) - 1; i >= 0; i-- {
		if strings.HasPrefix(files[i].Name(), "temp_") || !strings.HasSuffix(files[i].Name(), ".db") {
			continue
		}
		path := filepath.Join(dataDir, files[i].Name())
		if v, found, k := searchFile(path, key); found {
			return v, true, k
		}
	}
	return "", false, 0
}

func searchFile(path, key string) (string, bool, byte) {
	f, err := os.Open(path)
	if err != nil {
		return "", false, 0
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return "", false, 0
	}
	size := stat.Size()
	if size < 8+BloomFilterSize {
		return "", false, 0
	}

	f.Seek(size-8, 0)
	var dataEndOffset int64
	binary.Read(f, binary.LittleEndian, &dataEndOffset)

	f.Seek(dataEndOffset, 0)
	bfBytes := make([]byte, BloomFilterSize)
	io.ReadFull(f, bfBytes)

	bf := &BloomFilter{BitSet: bfBytes}
	if !bf.MayContain(key) {
		return "", false, 0
	}

	f.Seek(0, 0)
	r := bufio.NewReader(f)
	currentOffset := int64(0)

	for currentOffset < dataEndOffset {
		kind, err := r.ReadByte()
		if err != nil {
			break
		}
		currentOffset++

		var kl, vl int32
		binary.Read(r, binary.LittleEndian, &kl)
		binary.Read(r, binary.LittleEndian, &vl)
		currentOffset += 8

		kBytes := make([]byte, kl)
		vBytes := make([]byte, vl)
		io.ReadFull(r, kBytes)
		io.ReadFull(r, vBytes)
		currentOffset += int64(kl + vl)

		if string(kBytes) == key {
			return string(vBytes), true, kind
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
	if err != nil || stat.Size() < 8+BloomFilterSize {
		return
	}
	size := stat.Size()

	f.Seek(size-8, 0)
	var dataEndOffset int64
	binary.Read(f, binary.LittleEndian, &dataEndOffset)

	f.Seek(0, 0)
	r := bufio.NewReader(f)
	currentOffset := int64(0)

	for currentOffset < dataEndOffset {
		kind, err := r.ReadByte()
		if err != nil {
			break
		}
		currentOffset++

		var kl, vl int32
		binary.Read(r, binary.LittleEndian, &kl)
		binary.Read(r, binary.LittleEndian, &vl)
		currentOffset += 8

		kBytes := make([]byte, kl)
		vBytes := make([]byte, vl)
		io.ReadFull(r, kBytes)
		io.ReadFull(r, vBytes)
		currentOffset += int64(kl + vl)

		key := string(kBytes)
		if strings.HasPrefix(key, prefix) {
			if kind == CmdDel || isExpired(kind, string(vBytes), time.Now()) {
				delete(keys, key)
			} else {
				keys[key] = struct{}{}
			}
		}
	}
}

func Compact(e *Engine) {
	e.mu.Lock()
	defer e.mu.Unlock()

	dataDir := e.DataDir
	files, err := os.ReadDir(dataDir)
	if err != nil {
		return
	}
	var paths []string
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".db") && !strings.HasPrefix(f.Name(), "temp_") {
			paths = append(paths, filepath.Join(dataDir, f.Name()))
		}
	}
	if len(paths) <= 1 {
		return // Nothing to compact
	}
	sort.Strings(paths)

	type record struct {
		value string
		kind  byte
	}
	merged := make(map[string]record)

	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			continue
		}
		stat, _ := f.Stat()
		size := stat.Size()
		if size < 8+BloomFilterSize {
			f.Close()
			continue
		}

		f.Seek(size-8, 0)
		var limit int64
		binary.Read(f, binary.LittleEndian, &limit)
		f.Seek(0, 0)

		r := bufio.NewReader(f)
		current := int64(0)
		for current < limit {
			kind, err := r.ReadByte()
			if err != nil {
				break
			}
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

	newFile := filepath.Join(dataDir, fmt.Sprintf("sstable_%d_compacted.db", time.Now().UnixNano()))
	f, err := os.Create(newFile)
	if err != nil {
		return
	}
	bf := NewBloomFilter()

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

	for _, p := range paths {
		os.Remove(p)
	}
}

// ==========================================
// ENGINE
// ==========================================

type Engine struct {
	MemTable      *SkipList
	Wal           *WAL
	DataDir       string
	WALPath       string
	AuthToken     string
	TotalOps      int64
	OpsThisSecond int64
	OpsPerSec     float64
	mu            sync.RWMutex
}

func NewEngine() *Engine {
	return NewEngineWithConfig(DataDir, WALFile, AuthToken)
}

func NewEngineWithConfig(dataDir, walPath, authToken string) *Engine {
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		os.MkdirAll(dataDir, 0755)
	}
	sl := NewSkipList()
	wal, _ := OpenWAL(walPath)
	if wal != nil {
		wal.Recover(sl)
	}
	e := &Engine{
		MemTable:  sl,
		Wal:       wal,
		DataDir:   dataDir,
		WALPath:   walPath,
		AuthToken: authToken,
	}

	// Rolling ops/sec counter ticker
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			ops := atomic.SwapInt64(&e.OpsThisSecond, 0)
			e.mu.Lock()
			e.OpsPerSec = float64(ops)
			e.mu.Unlock()
		}
	}()

	return e
}

func (e *Engine) putLocked(key, value string, kind byte) {
	if e.Wal != nil {
		if err := e.Wal.WriteEntry(key, value, kind); err != nil {
			log.Printf("WAL write failed for key %q: %v", key, err)
			return
		}
	}
	e.MemTable.Put(key, value, kind)
	if e.MemTable.Size >= MemtableLimit {
		FlushMemTable(e.MemTable, e.DataDir)
		e.MemTable = NewSkipList()
		if e.Wal != nil {
			e.Wal.Clear()
			e.Wal, _ = OpenWAL(e.WALPath)
		}
	}
}

func (e *Engine) Put(key, value string) {
	atomic.AddInt64(&e.TotalOps, 1)
	atomic.AddInt64(&e.OpsThisSecond, 1)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.putLocked(key, value, CmdPut)
}

func (e *Engine) PutWithTTL(key, value string, ttl time.Duration) {
	atomic.AddInt64(&e.TotalOps, 1)
	atomic.AddInt64(&e.OpsThisSecond, 1)
	e.mu.Lock()
	defer e.mu.Unlock()
	expiresAt := time.Now().Add(ttl)
	e.putLocked(key, encodeExpiringValue(value, expiresAt), CmdPutTTL)
}

func (e *Engine) Delete(key string) {
	atomic.AddInt64(&e.TotalOps, 1)
	atomic.AddInt64(&e.OpsThisSecond, 1)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.putLocked(key, "", CmdDel)
}

func (e *Engine) lookupLocked(key string) (string, bool, byte, string) {
	if value, found, kind := e.MemTable.Get(key); found {
		return value, true, kind, "memtable"
	}
	v, found, k := SearchSSTables(key, e.DataDir)
	return v, found, k, "sstable"
}

func (e *Engine) Get(key string) string {
	val, _ := e.GetWithTier(key)
	return val
}

func (e *Engine) GetWithTier(key string) (string, string) {
	atomic.AddInt64(&e.TotalOps, 1)
	atomic.AddInt64(&e.OpsThisSecond, 1)
	e.mu.RLock()
	defer e.mu.RUnlock()
	if value, found, kind, tier := e.lookupLocked(key); found {
		if visible, ok := visibleValue(value, kind, time.Now()); ok {
			return visible, tier
		}
	}
	return "(nil)", "none"
}

func (e *Engine) Expire(key string, ttl time.Duration) bool {
	atomic.AddInt64(&e.TotalOps, 1)
	atomic.AddInt64(&e.OpsThisSecond, 1)
	e.mu.Lock()
	defer e.mu.Unlock()

	value, found, kind, _ := e.lookupLocked(key)
	current, visible := visibleValue(value, kind, time.Now())
	if !found || !visible {
		return false
	}
	e.putLocked(key, encodeExpiringValue(current, time.Now().Add(ttl)), CmdPutTTL)
	return true
}

func (e *Engine) TTL(key string) int64 {
	atomic.AddInt64(&e.TotalOps, 1)
	atomic.AddInt64(&e.OpsThisSecond, 1)
	e.mu.RLock()
	defer e.mu.RUnlock()

	value, found, kind, _ := e.lookupLocked(key)
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
	atomic.AddInt64(&e.TotalOps, 1)
	atomic.AddInt64(&e.OpsThisSecond, 1)
	e.mu.Lock()
	defer e.mu.Unlock()

	keys := make(map[string]struct{})
	for _, node := range e.MemTable.Iterator() {
		if strings.HasPrefix(node.Key, prefix) {
			keys[node.Key] = struct{}{}
		}
	}
	if files, err := os.ReadDir(e.DataDir); err == nil {
		for _, file := range files {
			if strings.HasSuffix(file.Name(), ".db") {
				collectKeysWithPrefixFromFile(filepath.Join(e.DataDir, file.Name()), prefix, keys)
			}
		}
	}

	for key := range keys {
		e.putLocked(key, "", CmdDel)
	}
	return len(keys)
}

type KeyInfo struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	TTL     int64  `json:"ttl"`
	Tier    string `json:"tier"`
	Expires string `json:"expires,omitempty"`
}

func (e *Engine) ListKeys(pattern string) []KeyInfo {
	e.mu.RLock()
	defer e.mu.RUnlock()

	activeMap := make(map[string]KeyInfo)

	// Collect from SSTables first
	if files, err := os.ReadDir(e.DataDir); err == nil {
		for _, file := range files {
			if strings.HasSuffix(file.Name(), ".db") {
				filePath := filepath.Join(e.DataDir, file.Name())
				f, err := os.Open(filePath)
				if err != nil {
					continue
				}
				stat, _ := f.Stat()
				if stat.Size() >= 8+BloomFilterSize {
					f.Seek(stat.Size()-8, 0)
					var limit int64
					binary.Read(f, binary.LittleEndian, &limit)
					f.Seek(0, 0)
					r := bufio.NewReader(f)
					cur := int64(0)
					for cur < limit {
						kind, err := r.ReadByte()
						if err != nil {
							break
						}
						cur++
						var kl, vl int32
						binary.Read(r, binary.LittleEndian, &kl)
						binary.Read(r, binary.LittleEndian, &vl)
						cur += 8
						k := make([]byte, kl)
						v := make([]byte, vl)
						io.ReadFull(r, k)
						io.ReadFull(r, v)
						cur += int64(kl + vl)

						kStr := string(k)
						if pattern != "" && !strings.Contains(kStr, pattern) {
							continue
						}
						if kind == CmdDel || isExpired(kind, string(v), time.Now()) {
							delete(activeMap, kStr)
						} else {
							val, _ := visibleValue(string(v), kind, time.Now())
							activeMap[kStr] = KeyInfo{
								Key:   kStr,
								Value: val,
								TTL:   -1,
								Tier:  "sstable",
							}
						}
					}
				}
				f.Close()
			}
		}
	}

	// Overlay with MemTable
	now := time.Now()
	for _, node := range e.MemTable.Iterator() {
		if pattern != "" && !strings.Contains(node.Key, pattern) {
			continue
		}
		if node.Kind == CmdDel || isExpired(node.Kind, node.Value, now) {
			delete(activeMap, node.Key)
		} else {
			val, _ := visibleValue(node.Value, node.Kind, now)
			ttlSec := int64(-1)
			expiresStr := ""
			if node.Kind == CmdPutTTL {
				if _, expAt, ok := decodeExpiringValue(node.Value); ok {
					ttlSec = int64(time.Until(expAt) / time.Second)
					expiresStr = expAt.Format(time.RFC3339)
				}
			}
			activeMap[node.Key] = KeyInfo{
				Key:     node.Key,
				Value:   val,
				TTL:     ttlSec,
				Tier:    "memtable",
				Expires: expiresStr,
			}
		}
	}

	res := make([]KeyInfo, 0, len(activeMap))
	for _, ki := range activeMap {
		res = append(res, ki)
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Key < res[j].Key
	})
	return res
}

func (e *Engine) Stats() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var sstablesCount int
	var sstablesBytes int64
	if files, err := os.ReadDir(e.DataDir); err == nil {
		for _, f := range files {
			if strings.HasSuffix(f.Name(), ".db") {
				sstablesCount++
				if info, err := f.Info(); err == nil {
					sstablesBytes += info.Size()
				}
			}
		}
	}

	var walBytes int64
	if e.Wal != nil && e.Wal.file != nil {
		if stat, err := e.Wal.file.Stat(); err == nil {
			walBytes = stat.Size()
		}
	}

	return map[string]interface{}{
		"status":            "online",
		"name":              InstanceName,
		"version":           "2.1.0",
		"port":              Port,
		"http_port":         HTTPPort,
		"uptime_seconds":    int64(time.Since(StartTime).Seconds()),
		"memtable_entries":  e.MemTable.Size,
		"memtable_bytes":    e.MemTable.ByteSize(),
		"memtable_limit":    MemtableLimit,
		"sstables_count":    sstablesCount,
		"sstables_bytes":    sstablesBytes,
		"wal_bytes":         walBytes,
		"total_ops":         atomic.LoadInt64(&e.TotalOps),
		"ops_per_sec":       e.OpsPerSec,
		"connected_clients": atomic.LoadInt64(&activeClientsCount),
		"auth_required":     e.AuthToken != "",
	}
}

// ==========================================
// REAL-TIME TELEMETRY & EVENT BROADCAST BUS
// ==========================================

type OperationEvent struct {
	Timestamp   int64    `json:"timestamp"`
	ClientAddr  string   `json:"client_addr"`
	Command     string   `json:"command"`
	Key         string   `json:"key,omitempty"`
	Args        []string `json:"args,omitempty"`
	LatencyUs   int64    `json:"latency_us"`
	Status      string   `json:"status"`
	StorageTier string   `json:"storage_tier,omitempty"`
}

type TelemetryHub struct {
	subscribers map[chan OperationEvent]struct{}
	mu          sync.RWMutex
}

var telemetryHub = &TelemetryHub{
	subscribers: make(map[chan OperationEvent]struct{}),
}

func (h *TelemetryHub) Subscribe() chan OperationEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch := make(chan OperationEvent, 128)
	h.subscribers[ch] = struct{}{}
	return ch
}

func (h *TelemetryHub) Unsubscribe(ch chan OperationEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.subscribers, ch)
	close(ch)
}

func (h *TelemetryHub) Broadcast(event OperationEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for ch := range h.subscribers {
		select {
		case ch <- event:
		default:
			// Non-blocking drop if consumer is too slow
		}
	}
}

// ==========================================
// PUB/SUB BROKER
// ==========================================

type Client struct {
	conn          net.Conn
	writeMu       sync.Mutex
	authenticated bool
	remoteAddr    string
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

var broker = NewBroker()
var activeClientsCount int64

// ==========================================
// TCP COMMAND EXECUTION & NETWORK LOOP
// ==========================================

func executeCommand(e *Engine, client *Client, line string) string {
	start := time.Now()
	line = strings.TrimSpace(line)
	parts := strings.SplitN(line, " ", 3)
	if len(parts) == 0 || parts[0] == "" {
		return ""
	}

	cmd := strings.ToUpper(parts[0])
	resp := ""
	status := "OK"
	targetKey := ""
	tier := ""

	// Check authentication if required
	if e.AuthToken != "" && !client.authenticated {
		if cmd != "AUTH" && cmd != "PING" {
			latency := time.Since(start).Microseconds()
			telemetryHub.Broadcast(OperationEvent{
				Timestamp:  time.Now().UnixMilli(),
				ClientAddr: client.remoteAddr,
				Command:    cmd,
				LatencyUs:  latency,
				Status:     "ERR NOAUTH",
			})
			return "ERR NOAUTH Authentication required"
		}
	}

	switch cmd {
	case "AUTH":
		if len(parts) < 2 {
			resp = "ERR Usage: AUTH <password>"
			status = "ERR"
		} else if e.AuthToken == "" || parts[1] == e.AuthToken {
			client.authenticated = true
			resp = "OK"
			status = "AUTH OK"
		} else {
			resp = "ERR invalid password"
			status = "ERR AUTH"
		}

	case "PING":
		if len(parts) > 1 {
			resp = parts[1]
		} else {
			resp = "PONG"
		}

	case "PUT":
		if len(parts) < 3 {
			resp = "ERR Usage: PUT <key> <val>"
			status = "ERR"
		} else {
			targetKey = parts[1]
			e.Put(parts[1], parts[2])
			resp = "OK"
			tier = "memtable"
		}

	case "PUTEX":
		putParts := strings.SplitN(line, " ", 4)
		if len(putParts) < 4 {
			resp = "ERR Usage: PUTEX <key> <ttl-seconds> <val>"
			status = "ERR"
		} else {
			seconds, err := strconv.ParseInt(putParts[2], 10, 64)
			if err != nil || seconds <= 0 {
				resp = "ERR TTL must be a positive integer"
				status = "ERR"
			} else {
				targetKey = putParts[1]
				e.PutWithTTL(putParts[1], putParts[3], time.Duration(seconds)*time.Second)
				resp = "OK"
				tier = "memtable"
			}
		}

	case "GET":
		if len(parts) < 2 {
			resp = "ERR Usage: GET <key>"
			status = "ERR"
		} else {
			targetKey = parts[1]
			val, t := e.GetWithTier(parts[1])
			resp = val
			tier = t
		}

	case "DEL":
		if len(parts) < 2 {
			resp = "ERR Usage: DEL <key>"
			status = "ERR"
		} else {
			targetKey = parts[1]
			e.Delete(parts[1])
			resp = "OK"
			tier = "memtable"
		}

	case "EXPIRE":
		if len(parts) < 3 {
			resp = "ERR Usage: EXPIRE <key> <ttl-seconds>"
			status = "ERR"
		} else {
			seconds, err := strconv.ParseInt(parts[2], 10, 64)
			if err != nil || seconds <= 0 {
				resp = "ERR TTL must be a positive integer"
				status = "ERR"
			} else {
				targetKey = parts[1]
				if e.Expire(parts[1], time.Duration(seconds)*time.Second) {
					resp = "1"
				} else {
					resp = "0"
				}
				tier = "memtable"
			}
		}

	case "TTL":
		if len(parts) < 2 {
			resp = "ERR Usage: TTL <key>"
			status = "ERR"
		} else {
			targetKey = parts[1]
			resp = strconv.FormatInt(e.TTL(parts[1]), 10)
		}

	case "CLEAR":
		if len(parts) < 2 {
			resp = "ERR Usage: CLEAR <key-prefix>"
			status = "ERR"
		} else {
			targetKey = parts[1]
			count := e.ClearPrefix(parts[1])
			resp = "CLEARED " + strconv.Itoa(count)
		}

	case "SUBSCRIBE":
		if len(parts) < 2 {
			resp = "ERR Usage: SUBSCRIBE <channel>"
			status = "ERR"
		} else {
			targetKey = parts[1]
			broker.Subscribe(client, parts[1])
			resp = "SUBSCRIBED " + parts[1]
		}

	case "UNSUBSCRIBE":
		if len(parts) < 2 {
			resp = "ERR Usage: UNSUBSCRIBE <channel>"
			status = "ERR"
		} else {
			targetKey = parts[1]
			broker.Unsubscribe(client, parts[1])
			resp = "UNSUBSCRIBED " + parts[1]
		}

	case "PUBLISH":
		if len(parts) < 3 {
			resp = "ERR Usage: PUBLISH <channel> <message>"
			status = "ERR"
		} else {
			targetKey = parts[1]
			delivered := broker.Publish(parts[1], parts[2])
			resp = "PUBLISHED " + strconv.Itoa(delivered)
		}

	case "COMPACT":
		go Compact(e)
		resp = "OK Compact Started"

	case "INFO":
		stats := e.Stats()
		var b strings.Builder
		b.WriteString("# Sider Server\r\n")
		for k, v := range stats {
			b.WriteString(fmt.Sprintf("%s:%v\r\n", k, v))
		}
		resp = b.String()

	default:
		resp = "ERR Unknown Command"
		status = "ERR"
	}

	latencyUs := time.Since(start).Microseconds()
	telemetryHub.Broadcast(OperationEvent{
		Timestamp:   time.Now().UnixMilli(),
		ClientAddr:  client.remoteAddr,
		Command:     cmd,
		Key:         targetKey,
		LatencyUs:   latencyUs,
		Status:      status,
		StorageTier: tier,
	})

	return resp
}

func handleConnection(conn net.Conn, e *Engine) {
	defer conn.Close()
	atomic.AddInt64(&activeClientsCount, 1)
	defer atomic.AddInt64(&activeClientsCount, -1)

	client := &Client{
		conn:          conn,
		remoteAddr:    conn.RemoteAddr().String(),
		authenticated: e.AuthToken == "", // Auto-authenticated if no token set
	}
	defer broker.RemoveClient(client)

	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		resp := executeCommand(e, client, line)
		if resp != "" {
			client.writeLine(resp)
		}
	}
}

// ==========================================
// HTTP & WEBSOCKET/SSE TELEMETRY GATEWAY
// ==========================================

func enableCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With")
}

func startHTTPGateway(port string, engine *Engine) {
	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "ok",
			"name":    InstanceName,
			"version": "2.1.0",
			"time":    time.Now().Unix(),
		})
	})

	// Real-time telemetry stats
	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(engine.Stats())
	})

	// Keys explorer & search
	mux.HandleFunc("/api/keys", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		pattern := r.URL.Query().Get("q")
		keys := engine.ListKeys(pattern)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"count": len(keys),
			"keys":  keys,
		})
	})

	// Web terminal command executor
	mux.HandleFunc("/api/exec", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var payload struct {
			Command string `json:"command"`
			Token   string `json:"token"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
			return
		}

		// Virtual HTTP client
		authenticated := engine.AuthToken == "" || payload.Token == engine.AuthToken
		virtualClient := &Client{
			authenticated: authenticated,
			remoteAddr:    r.RemoteAddr + " (HTTP)",
		}

		resp := executeCommand(engine, virtualClient, payload.Command)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"command":  payload.Command,
			"response": resp,
		})
	})

	// Server-Sent Events (SSE) Live Operation Stream
	mux.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
			return
		}

		ch := telemetryHub.Subscribe()
		defer telemetryHub.Unsubscribe(ch)

		// Send initial heartbeat
		fmt.Fprintf(w, "event: connected\ndata: {\"status\":\"connected\",\"name\":\"%s\"}\n\n", InstanceName)
		flusher.Flush()

		ctx := r.Context()
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-ch:
				if !ok {
					return
				}
				data, _ := json.Marshal(event)
				fmt.Fprintf(w, "data: %s\n\n", string(data))
				flusher.Flush()
			}
		}
	})

	// Pure Standard Library RFC 6455 WebSocket Monitor
	mux.HandleFunc("/ws/monitor", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			http.Error(w, "Expected WebSocket Upgrade", http.StatusBadRequest)
			return
		}

		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "Hijacking unsupported", http.StatusInternalServerError)
			return
		}

		conn, bufrw, err := hj.Hijack()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer conn.Close()

		// Perform WebSocket handshake
		key := r.Header.Get("Sec-WebSocket-Key")
		h := sha1.New()
		h.Write([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
		acceptKey := base64.StdEncoding.EncodeToString(h.Sum(nil))

		bufrw.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
		bufrw.WriteString("Upgrade: websocket\r\n")
		bufrw.WriteString("Connection: Upgrade\r\n")
		bufrw.WriteString("Sec-WebSocket-Accept: " + acceptKey + "\r\n")
		bufrw.WriteString("Access-Control-Allow-Origin: *\r\n\r\n")
		bufrw.Flush()

		ch := telemetryHub.Subscribe()
		defer telemetryHub.Unsubscribe(ch)

		for event := range ch {
			data, _ := json.Marshal(event)
			// Frame: 0x81 (text frame) + payload length + payload
			var frame bytes.Buffer
			frame.WriteByte(0x81)
			payloadLen := len(data)
			if payloadLen < 126 {
				frame.WriteByte(byte(payloadLen))
			} else if payloadLen <= 65535 {
				frame.WriteByte(126)
				binary.Write(&frame, binary.BigEndian, uint16(payloadLen))
			} else {
				frame.WriteByte(127)
				binary.Write(&frame, binary.BigEndian, uint64(payloadLen))
			}
			frame.Write(data)

			if _, err := bufrw.Write(frame.Bytes()); err != nil {
				break
			}
			bufrw.Flush()
		}
	})

	server := &http.Server{
		Addr:    port,
		Handler: mux,
	}

	fmt.Printf("   HTTP & TELEMETRY GATEWAY ON %s\n", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("HTTP Gateway error: %v", err)
	}
}

// ==========================================
// MAIN ENTRYPOINT & CLI FLAGS
// ==========================================

func main() {
	portFlag := flag.String("port", DefaultPort, "TCP port to listen on (e.g. :4000)")
	httpPortFlag := flag.String("http-port", DefaultHTTPPort, "HTTP/WebSocket telemetry gateway port (e.g. :4001, empty to disable)")
	dataDirFlag := flag.String("data-dir", DefaultDataDir, "Directory path for SSTable storage")
	walFileFlag := flag.String("wal-file", DefaultWALFile, "Path to write-ahead log file")
	authFlag := flag.String("auth-token", "", "Optional password/token for client authentication")
	nameFlag := flag.String("name", "sider-primary", "Database instance identifier name")
	limitFlag := flag.Int("memtable-limit", 100, "Number of keys before flushing MemTable to SSTable")
	flag.Parse()

	Port = *portFlag
	if !strings.HasPrefix(Port, ":") && !strings.Contains(Port, ":") {
		Port = ":" + Port
	}

	HTTPPort = *httpPortFlag
	if HTTPPort != "" && !strings.HasPrefix(HTTPPort, ":") && !strings.Contains(HTTPPort, ":") {
		HTTPPort = ":" + HTTPPort
	}

	DataDir = *dataDirFlag
	WALFile = *walFileFlag
	AuthToken = *authFlag
	InstanceName = *nameFlag
	MemtableLimit = *limitFlag

	rand.Seed(time.Now().UnixNano())
	engine := NewEngineWithConfig(DataDir, WALFile, AuthToken)

	listener, err := net.Listen("tcp", Port)
	if err != nil {
		log.Fatal("Error starting TCP server:", err)
	}
	defer listener.Close()

	fmt.Println("==================================================")
	fmt.Printf("   ⚡ SIDER DB ENGINE LISTENING ON %s\n", Port)
	fmt.Printf("   Instance Name: %s\n", InstanceName)
	fmt.Printf("   Data Storage:  %s | WAL: %s\n", DataDir, WALFile)
	if AuthToken != "" {
		fmt.Println("   Security:      AUTH REQUIRED (Token Enabled)")
	} else {
		fmt.Println("   Security:      OPEN ACCESS (No Auth)")
	}
	fmt.Println("   Version:       2.1.0 Enterprise Ready")
	fmt.Println("==================================================")

	// Start HTTP / SSE / WebSocket Gateway in background
	if HTTPPort != "" {
		go startHTTPGateway(HTTPPort, engine)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Connection error:", err)
			continue
		}
		go handleConnection(conn, engine)
	}
}

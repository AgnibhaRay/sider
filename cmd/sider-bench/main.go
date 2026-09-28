package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type BenchConfig struct {
	Host        string
	TCPPort     int
	HTTPPort    int
	AuthToken   string
	Concurrency int
	KeyPrefix   string
	ValSize     int
}

type BenchResult struct {
	Name        string
	TotalOps    int
	Concurrency int
	Duration    time.Duration
	OpsPerSec   float64
	MinLat      time.Duration
	AvgLat      time.Duration
	P50Lat      time.Duration
	P90Lat      time.Duration
	P95Lat      time.Duration
	P99Lat      time.Duration
	MaxLat      time.Duration
	Errors      int64
}

func printResult(r BenchResult) {
	fmt.Printf("\n======================================================================\n")
	fmt.Printf(" 📊 BENCHMARK: %s\n", r.Name)
	fmt.Printf("======================================================================\n")
	fmt.Printf("  • Total Completed Ops: %d ops\n", r.TotalOps)
	fmt.Printf("  • Client Concurrency:  %d concurrent workers\n", r.Concurrency)
	fmt.Printf("  • Total Wall Time:     %v\n", r.Duration.Round(time.Millisecond))
	fmt.Printf("  • Throughput:          %.2f ops/sec\n", r.OpsPerSec)
	fmt.Printf("  • Latency Distribution:\n")
	fmt.Printf("      - Min:             %v\n", r.MinLat)
	fmt.Printf("      - Median (P50):    %v\n", r.P50Lat)
	fmt.Printf("      - 90th Percentile: %v\n", r.P90Lat)
	fmt.Printf("      - 95th Percentile: %v\n", r.P95Lat)
	fmt.Printf("      - 99th Percentile: %v\n", r.P99Lat)
	fmt.Printf("      - Max:             %v\n", r.MaxLat)
	fmt.Printf("      - Arithmetic Mean: %v\n", r.AvgLat)
	fmt.Printf("  • Error Count:         %d\n", r.Errors)
	fmt.Printf("======================================================================\n")
}

func calculateResult(name string, latencies []time.Duration, totalDuration time.Duration, concurrency int, errors int64) BenchResult {
	if len(latencies) == 0 {
		return BenchResult{Name: name, Duration: totalDuration, Concurrency: concurrency, Errors: errors}
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	var totalLat time.Duration
	for _, l := range latencies {
		totalLat += l
	}

	n := len(latencies)
	p50 := latencies[int(float64(n)*0.50)]
	p90 := latencies[int(float64(n)*0.90)]
	p95 := latencies[int(float64(n)*0.95)]
	p99 := latencies[int(float64(n)*0.99)]

	return BenchResult{
		Name:        name,
		TotalOps:    n,
		Concurrency: concurrency,
		Duration:    totalDuration,
		OpsPerSec:   float64(n) / totalDuration.Seconds(),
		MinLat:      latencies[0],
		AvgLat:      totalLat / time.Duration(n),
		P50Lat:      p50,
		P90Lat:      p90,
		P95Lat:      p95,
		P99Lat:      p99,
		MaxLat:      latencies[n-1],
		Errors:      errors,
	}
}

func connectTCP(cfg BenchConfig) (net.Conn, *bufio.Reader, error) {
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.TCPPort)
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, nil, err
	}

	reader := bufio.NewReader(conn)
	if cfg.AuthToken != "" {
		if _, err := conn.Write([]byte(fmt.Sprintf("AUTH %s\r\n", cfg.AuthToken))); err != nil {
			conn.Close()
			return nil, nil, err
		}
		resp, err := reader.ReadString('\n')
		if err != nil || !strings.Contains(resp, "OK") {
			conn.Close()
			return nil, nil, fmt.Errorf("auth failed: %v, resp: %s", err, resp)
		}
	}

	return conn, reader, nil
}

// 1. PING Baseline Benchmark (Network Stack & Protocol Loop)
func RunPingBench(cfg BenchConfig, count int) BenchResult {
	opsPerWorker := count / cfg.Concurrency
	var wg sync.WaitGroup
	var errCount int64

	allLatencies := make([][]time.Duration, cfg.Concurrency)

	start := time.Now()
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			conn, reader, err := connectTCP(cfg)
			if err != nil {
				atomic.AddInt64(&errCount, int64(opsPerWorker))
				return
			}
			defer conn.Close()

			lats := make([]time.Duration, 0, opsPerWorker)
			pingCmd := []byte("PING\r\n")

			for j := 0; j < opsPerWorker; j++ {
				t0 := time.Now()
				if _, err := conn.Write(pingCmd); err != nil {
					atomic.AddInt64(&errCount, 1)
					continue
				}
				if _, err := reader.ReadString('\n'); err != nil {
					atomic.AddInt64(&errCount, 1)
					continue
				}
				lats = append(lats, time.Since(t0))
			}
			allLatencies[workerID] = lats
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	var flattened []time.Duration
	for _, l := range allLatencies {
		flattened = append(flattened, l...)
	}

	return calculateResult("PING (Loopback Network Stack & Protocol Parsing)", flattened, duration, cfg.Concurrency, errCount)
}

// 2. PUT Benchmark (Writes -> WAL Fsync + SkipList MemTable)
func RunPutBench(cfg BenchConfig, count int) BenchResult {
	opsPerWorker := count / cfg.Concurrency
	var wg sync.WaitGroup
	var errCount int64

	valPayload := strings.Repeat("x", cfg.ValSize)
	allLatencies := make([][]time.Duration, cfg.Concurrency)

	start := time.Now()
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			conn, reader, err := connectTCP(cfg)
			if err != nil {
				atomic.AddInt64(&errCount, int64(opsPerWorker))
				return
			}
			defer conn.Close()

			lats := make([]time.Duration, 0, opsPerWorker)
			for j := 0; j < opsPerWorker; j++ {
				key := fmt.Sprintf("%s_%d_%d", cfg.KeyPrefix, workerID, j)
				cmd := fmt.Sprintf("PUT %s %s\r\n", key, valPayload)

				t0 := time.Now()
				if _, err := conn.Write([]byte(cmd)); err != nil {
					atomic.AddInt64(&errCount, 1)
					continue
				}
				resp, err := reader.ReadString('\n')
				if err != nil || !strings.Contains(resp, "OK") {
					atomic.AddInt64(&errCount, 1)
					continue
				}
				lats = append(lats, time.Since(t0))
			}
			allLatencies[workerID] = lats
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	var flattened []time.Duration
	for _, l := range allLatencies {
		flattened = append(flattened, l...)
	}

	return calculateResult("PUT (LSM Sequential WAL fsync & SkipList Insert)", flattened, duration, cfg.Concurrency, errCount)
}

// 3. GET Benchmark (Concurrent In-Memory SkipList Reads)
func RunGetBench(cfg BenchConfig, count int) BenchResult {
	opsPerWorker := count / cfg.Concurrency
	var wg sync.WaitGroup
	var errCount int64

	// First ensure keys exist in MemTable
	prepConn, prepReader, err := connectTCP(cfg)
	if err == nil {
		for i := 0; i < 50; i++ {
			prepConn.Write([]byte(fmt.Sprintf("PUT cached_key_%d val_%d\r\n", i, i)))
			prepReader.ReadString('\n')
		}
		prepConn.Close()
	}

	allLatencies := make([][]time.Duration, cfg.Concurrency)

	start := time.Now()
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			conn, reader, err := connectTCP(cfg)
			if err != nil {
				atomic.AddInt64(&errCount, int64(opsPerWorker))
				return
			}
			defer conn.Close()

			lats := make([]time.Duration, 0, opsPerWorker)
			for j := 0; j < opsPerWorker; j++ {
				key := fmt.Sprintf("cached_key_%d", j%50)
				cmd := fmt.Sprintf("GET %s\r\n", key)

				t0 := time.Now()
				if _, err := conn.Write([]byte(cmd)); err != nil {
					atomic.AddInt64(&errCount, 1)
					continue
				}
				_, err := reader.ReadString('\n')
				if err != nil {
					atomic.AddInt64(&errCount, 1)
					continue
				}
				lats = append(lats, time.Since(t0))
			}
			allLatencies[workerID] = lats
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	var flattened []time.Duration
	for _, l := range allLatencies {
		flattened = append(flattened, l...)
	}

	return calculateResult("GET (In-Memory SkipList Cache Hit Concurrency)", flattened, duration, cfg.Concurrency, errCount)
}

// 4. MIXED 80/20 Workload Benchmark (80% GET, 20% PUT)
func RunMixedBench(cfg BenchConfig, count int) BenchResult {
	opsPerWorker := count / cfg.Concurrency
	var wg sync.WaitGroup
	var errCount int64

	valPayload := strings.Repeat("m", cfg.ValSize)
	allLatencies := make([][]time.Duration, cfg.Concurrency)

	start := time.Now()
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			conn, reader, err := connectTCP(cfg)
			if err != nil {
				atomic.AddInt64(&errCount, int64(opsPerWorker))
				return
			}
			defer conn.Close()

			lats := make([]time.Duration, 0, opsPerWorker)
			for j := 0; j < opsPerWorker; j++ {
				key := fmt.Sprintf("mix_key_%d", j%100)
				var cmd string

				// 80% Read / 20% Write
				if r.Float32() < 0.80 {
					cmd = fmt.Sprintf("GET %s\r\n", key)
				} else {
					cmd = fmt.Sprintf("PUT %s %s\r\n", key, valPayload)
				}

				t0 := time.Now()
				if _, err := conn.Write([]byte(cmd)); err != nil {
					atomic.AddInt64(&errCount, 1)
					continue
				}
				_, err := reader.ReadString('\n')
				if err != nil {
					atomic.AddInt64(&errCount, 1)
					continue
				}
				lats = append(lats, time.Since(t0))
			}
			allLatencies[workerID] = lats
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	var flattened []time.Duration
	for _, l := range allLatencies {
		flattened = append(flattened, l...)
	}

	return calculateResult("MIXED (80% Concurrent Reads / 20% Concurrent Writes)", flattened, duration, cfg.Concurrency, errCount)
}

// 5. BLOOM FILTER MISS Benchmark (Querying Non-Existent Keys)
func RunBloomMissBench(cfg BenchConfig, count int) BenchResult {
	opsPerWorker := count / cfg.Concurrency
	var wg sync.WaitGroup
	var errCount int64

	allLatencies := make([][]time.Duration, cfg.Concurrency)

	start := time.Now()
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			conn, reader, err := connectTCP(cfg)
			if err != nil {
				atomic.AddInt64(&errCount, int64(opsPerWorker))
				return
			}
			defer conn.Close()

			lats := make([]time.Duration, 0, opsPerWorker)
			for j := 0; j < opsPerWorker; j++ {
				key := fmt.Sprintf("non_existent_random_key_%d_%d", workerID, j)
				cmd := fmt.Sprintf("GET %s\r\n", key)

				t0 := time.Now()
				if _, err := conn.Write([]byte(cmd)); err != nil {
					atomic.AddInt64(&errCount, 1)
					continue
				}
				_, err := reader.ReadString('\n')
				if err != nil {
					atomic.AddInt64(&errCount, 1)
					continue
				}
				lats = append(lats, time.Since(t0))
			}
			allLatencies[workerID] = lats
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	var flattened []time.Duration
	for _, l := range allLatencies {
		flattened = append(flattened, l...)
	}

	return calculateResult("BLOOM FILTER MISS (Rejection Speed for Non-Existent Keys)", flattened, duration, cfg.Concurrency, errCount)
}

// 6. HTTP REST Gateway Benchmark
func RunHTTPGatewayBench(cfg BenchConfig, count int) BenchResult {
	opsPerWorker := count / cfg.Concurrency
	if opsPerWorker < 5 {
		opsPerWorker = 5
	}
	var wg sync.WaitGroup
	var errCount int64

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        cfg.Concurrency * 2,
			MaxIdleConnsPerHost: cfg.Concurrency * 2,
			IdleConnTimeout:     30 * time.Second,
		},
		Timeout: 5 * time.Second,
	}

	targetURL := fmt.Sprintf("http://%s:%d/api/exec", cfg.Host, cfg.HTTPPort)
	allLatencies := make([][]time.Duration, cfg.Concurrency)

	start := time.Now()
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			lats := make([]time.Duration, 0, opsPerWorker)
			for j := 0; j < opsPerWorker; j++ {
				body := fmt.Sprintf(`{"command":"GET cached_key_0","token":"%s"}`, cfg.AuthToken)
				t0 := time.Now()
				req, err := http.NewRequest("POST", targetURL, bytes.NewBufferString(body))
				if err != nil {
					atomic.AddInt64(&errCount, 1)
					continue
				}
				req.Header.Set("Content-Type", "application/json")

				resp, err := client.Do(req)
				if err != nil {
					atomic.AddInt64(&errCount, 1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				lats = append(lats, time.Since(t0))
			}
			allLatencies[workerID] = lats
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	var flattened []time.Duration
	for _, l := range allLatencies {
		flattened = append(flattened, l...)
	}

	return calculateResult("HTTP REST GATEWAY (/api/exec JSON over HTTP/1.1)", flattened, duration, cfg.Concurrency, errCount)
}

func main() {
	host := flag.String("host", "127.0.0.1", "Target Sider host")
	tcpPort := flag.Int("port", 4100, "TCP port")
	httpPort := flag.Int("http-port", 5100, "HTTP port")
	token := flag.String("token", "sdr_live_793855e4ec0e70901bef05344264ba98", "Auth token")
	concurrency := flag.Int("c", 100, "Concurrent clients")
	valSize := flag.Int("valsize", 128, "Value payload size in bytes")
	flag.Parse()

	cfg := BenchConfig{
		Host:        *host,
		TCPPort:     *tcpPort,
		HTTPPort:    *httpPort,
		AuthToken:   *token,
		Concurrency: *concurrency,
		KeyPrefix:   "bench_key",
		ValSize:     *valSize,
	}

	fmt.Printf("\n🚀 SIDER CLOUD BARE-METAL LOAD & CONCURRENCY BENCHMARK SUITE\n")
	fmt.Printf("----------------------------------------------------------------------\n")
	fmt.Printf(" Target Engine:       %s:%d (TCP) / :%d (HTTP)\n", cfg.Host, cfg.TCPPort, cfg.HTTPPort)
	fmt.Printf(" Client Concurrency:  %d concurrent worker goroutines\n", cfg.Concurrency)
	fmt.Printf(" Value Payload Size:  %d bytes\n", cfg.ValSize)
	fmt.Printf("----------------------------------------------------------------------\n")

	// Verify connectivity
	testConn, _, err := connectTCP(cfg)
	if err != nil {
		fmt.Printf("❌ Failed to connect to Sider at %s:%d: %v\n", cfg.Host, cfg.TCPPort, err)
		os.Exit(1)
	}
	testConn.Close()
	fmt.Printf("✅ Connected successfully to Sider engine on bare-metal host. Starting tests...\n")

	// Suite 1: PING baseline (50,000 ops)
	rPing := RunPingBench(cfg, 50000)
	printResult(rPing)

	// Suite 2: GET reads (50,000 ops)
	rGet := RunGetBench(cfg, 50000)
	printResult(rGet)

	// Suite 3: BLOOM MISS (50,000 ops)
	rMiss := RunBloomMissBench(cfg, 50000)
	printResult(rMiss)

	// Suite 4: MIXED 80/20 (5,000 ops)
	rMixed := RunMixedBench(cfg, 5000)
	printResult(rMixed)

	// Suite 5: PUT writes with NVMe WAL sync (1,000 ops)
	rPut := RunPutBench(cfg, 1000)
	printResult(rPut)

	// Suite 6: HTTP REST Gateway (2,000 ops)
	rHTTP := RunHTTPGatewayBench(cfg, 2000)
	printResult(rHTTP)

	fmt.Printf("\n🏆 ALL BARE-METAL BENCHMARK SUITES COMPLETED SUCCESSFULLY.\n")
}

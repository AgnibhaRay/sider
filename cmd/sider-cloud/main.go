package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ==========================================
// CONFIGURATION & STRUCTURES
// ==========================================

type DatabaseInstance struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	Region        string    `json:"region"`
	TCPPort       int       `json:"tcp_port"`
	HTTPPort      int       `json:"http_port"`
	Token         string    `json:"token"`
	Status        string    `json:"status"` // "running", "stopped", "error"
	DataDir       string    `json:"data_dir"`
	WALPath       string    `json:"wal_path"`
	CreatedAt     time.Time `json:"created_at"`
	ConnectionURI string    `json:"connection_uri"`
	HTTPEndpoint  string    `json:"http_endpoint"`

	// Runtime process pointer (not serialized)
	cmd *exec.Cmd `json:"-"`
}

type Supervisor struct {
	mu           sync.RWMutex
	databases    map[string]*DatabaseInstance
	registryPath string
	baseDataDir  string
	siderBin     string
	publicHost   string
	startTCPPort int
	startHTTPPort int
}

func randomHex(bytesLen int) string {
	b := make([]byte, bytesLen)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func isPortAvailable(port int) bool {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return false
	}
	ln.Close()
	return true
}

func (s *Supervisor) findNextFreePorts() (int, int, error) {
	tcp := s.startTCPPort
	httpP := s.startHTTPPort

	for i := 0; i < 500; i++ {
		candidateTCP := tcp + i
		candidateHTTP := httpP + i

		// Check if already assigned in memory
		inUse := false
		for _, db := range s.databases {
			if db.TCPPort == candidateTCP || db.HTTPPort == candidateHTTP {
				inUse = true
				break
			}
		}
		if inUse {
			continue
		}

		if isPortAvailable(candidateTCP) && isPortAvailable(candidateHTTP) {
			return candidateTCP, candidateHTTP, nil
		}
	}
	return 0, 0, fmt.Errorf("no available ports found in range")
}

func (s *Supervisor) saveRegistry() error {
	dir := filepath.Dir(s.registryPath)
	os.MkdirAll(dir, 0755)

	data, err := json.MarshalIndent(s.databases, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.registryPath, data, 0644)
}

func (s *Supervisor) loadRegistry() {
	data, err := os.ReadFile(s.registryPath)
	if err != nil {
		return // File doesn't exist yet, clean start
	}
	var loaded map[string]*DatabaseInstance
	if err := json.Unmarshal(data, &loaded); err == nil {
		s.databases = loaded
		// Auto-relaunch existing instances
		for _, db := range s.databases {
			s.launchDatabaseProcess(db)
		}
	}
}

func (s *Supervisor) launchDatabaseProcess(db *DatabaseInstance) error {
	os.MkdirAll(db.DataDir, 0755)

	cmd := exec.Command(s.siderBin,
		"--port", fmt.Sprintf(":%d", db.TCPPort),
		"--http-port", fmt.Sprintf(":%d", db.HTTPPort),
		"--data-dir", db.DataDir,
		"--wal-file", db.WALPath,
		"--auth-token", db.Token,
		"--name", db.Name,
	)

	// Redirect stdout/stderr to instance log
	logFile := filepath.Join(db.DataDir, "instance.log")
	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		cmd.Stdout = f
		cmd.Stderr = f
	}

	if err := cmd.Start(); err != nil {
		db.Status = "error"
		return err
	}

	db.cmd = cmd
	db.Status = "running"

	// Monitor child in background
	go func(inst *DatabaseInstance) {
		cmd.Wait()
		s.mu.Lock()
		if inst.cmd == cmd {
			inst.Status = "stopped"
		}
		s.mu.Unlock()
	}(db)

	return nil
}

func (s *Supervisor) CreateDatabase(name, region string) (*DatabaseInstance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if name == "" {
		name = fmt.Sprintf("sider-%s", randomHex(3))
	}
	if region == "" {
		region = "Home Cloud (Arch Linux • i5-9600)"
	}

	tcpPort, httpPort, err := s.findNextFreePorts()
	if err != nil {
		return nil, err
	}

	id := fmt.Sprintf("sdr-db-%s", randomHex(4))
	token := fmt.Sprintf("sdr_live_%s", randomHex(16))
	dbDir := filepath.Join(s.baseDataDir, id)
	dataDir := filepath.Join(dbDir, "data")
	walPath := filepath.Join(dbDir, "sider.wal")

	host := s.publicHost
	if host == "" {
		host = "localhost"
	}

	inst := &DatabaseInstance{
		ID:            id,
		Name:          name,
		Region:        region,
		TCPPort:       tcpPort,
		HTTPPort:      httpPort,
		Token:         token,
		Status:        "starting",
		DataDir:       dataDir,
		WALPath:       walPath,
		CreatedAt:     time.Now(),
		ConnectionURI: fmt.Sprintf("sider://default:%s@%s:%d", token, host, tcpPort),
		HTTPEndpoint:  fmt.Sprintf("http://%s:%d", host, httpPort),
	}

	if err := s.launchDatabaseProcess(inst); err != nil {
		return nil, fmt.Errorf("failed to start database process: %w", err)
	}

	s.databases[id] = inst
	s.saveRegistry()

	return inst, nil
}

func (s *Supervisor) DeleteDatabase(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	inst, exists := s.databases[id]
	if !exists {
		return fmt.Errorf("database not found")
	}

	if inst.cmd != nil && inst.cmd.Process != nil {
		_ = inst.cmd.Process.Kill()
	}

	delete(s.databases, id)
	s.saveRegistry()

	// Clean up files in background
	go os.RemoveAll(filepath.Join(s.baseDataDir, id))

	return nil
}

func (s *Supervisor) ListDatabases() []*DatabaseInstance {
	s.mu.RLock()
	defer s.mu.RUnlock()

	list := make([]*DatabaseInstance, 0, len(s.databases))
	for _, db := range s.databases {
		// Update connection URI with latest publicHost dynamically
		host := s.publicHost
		if host == "" {
			host = "localhost"
		}
		db.ConnectionURI = fmt.Sprintf("sider://default:%s@%s:%d", db.Token, host, db.TCPPort)
		db.HTTPEndpoint = fmt.Sprintf("http://%s:%d", host, db.HTTPPort)
		list = append(list, db)
	}
	return list
}

func (s *Supervisor) GetDatabase(id string) (*DatabaseInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	inst, exists := s.databases[id]
	if !exists {
		return nil, fmt.Errorf("database not found")
	}
	return inst, nil
}

// ==========================================
// HTTP HANDLERS & ROUTER
// ==========================================

func enableCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With")
}

func (s *Supervisor) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":            "ok",
			"service":           "sider-cloud-supervisor",
			"version":           "1.0.0",
			"active_databases": len(s.databases),
			"public_host":       s.publicHost,
		})
	})

	mux.HandleFunc("/api/databases", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"databases": s.ListDatabases(),
			})

		case http.MethodPost:
			var req struct {
				Name   string `json:"name"`
				Region string `json:"region"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)

			db, err := s.CreateDatabase(req.Name, req.Region)
			if err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(db)

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/databases/", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) < 3 {
			http.Error(w, "Invalid database URL", http.StatusBadRequest)
			return
		}
		dbID := parts[2]

		if len(parts) == 3 {
			switch r.Method {
			case http.MethodGet:
				db, err := s.GetDatabase(dbID)
				if err != nil {
					http.Error(w, err.Error(), http.StatusNotFound)
					return
				}

				// Generate driver code snippets
				host := s.publicHost
				if host == "" {
					host = "localhost"
				}

				pythonSnippet := fmt.Sprintf(`import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("%s", %d))
s.sendall(b"AUTH %s\n")
print(s.recv(1024).decode()) # OK

s.sendall(b"PUT user:101 '{\"name\": \"Alice\", \"role\": \"admin\"}'\n")
print(s.recv(1024).decode()) # OK

s.sendall(b"GET user:101\n")
print(s.recv(1024).decode())
s.close()`, host, db.TCPPort, db.Token)

				nodeSnippet := fmt.Sprintf(`const net = require('net');

const client = net.createConnection({ host: '%s', port: %d }, () => {
  client.write('AUTH %s\n');
  client.write('PUT cache:greeting "Hello from Node.js"\n');
  client.write('GET cache:greeting\n');
});

client.on('data', (data) => {
  console.log('Sider DB Response:', data.toString().trim());
});`, host, db.TCPPort, db.Token)

				springSnippet := fmt.Sprintf(`// In your Spring Boot application.properties:
sider.host=%s
sider.port=%d
sider.token=%s

// Using Java Socket or PulseNode client:
Socket socket = new Socket("%s", %d);
PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

out.println("AUTH %s");
out.println("PUT sensor:temp 98.6");
out.println("GET sensor:temp");`, host, db.TCPPort, db.Token, host, db.TCPPort, db.Token)

				netcatSnippet := fmt.Sprintf(`printf "AUTH %s\nPUT mykey myval\nGET mykey\n" | nc %s %d`,
					db.Token, host, db.TCPPort)

				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]interface{}{
					"database": db,
					"snippets": map[string]string{
						"python":     pythonSnippet,
						"node":       nodeSnippet,
						"springboot": springSnippet,
						"netcat":     netcatSnippet,
					},
				})

			case http.MethodDelete:
				if err := s.DeleteDatabase(dbID); err != nil {
					http.Error(w, err.Error(), http.StatusNotFound)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]string{"status": "deleted", "id": dbID})

			default:
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
			return
		}

		// Proxy stats/keys directly from the instance's HTTP gateway
		if len(parts) == 4 && parts[3] == "stats" {
			db, err := s.GetDatabase(dbID)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			// Fetch from child instance
			resp, err := http.Get(fmt.Sprintf("http://localhost:%d/api/stats", db.HTTPPort))
			if err != nil {
				http.Error(w, "Instance unavailable", http.StatusBadGateway)
				return
			}
			defer resp.Body.Close()
			w.Header().Set("Content-Type", "application/json")
			io.Copy(w, resp.Body)
			return
		}
	})
}

// ==========================================
// MAIN ENTRYPOINT
// ==========================================

func main() {
	portFlag := flag.String("port", ":8080", "Supervisor HTTP port")
	hostFlag := flag.String("host", "", "Public IP / Hostname for client connection strings (e.g. 100.95.206.7 or tailnet DNS)")
	siderBinFlag := flag.String("sider-bin", "./sider", "Path to sider engine executable")
	dataDirFlag := flag.String("data-dir", filepath.Join(os.Getenv("HOME"), ".sider-cloud", "dbs"), "Base directory for instances")
	registryFlag := flag.String("registry", filepath.Join(os.Getenv("HOME"), ".sider-cloud", "registry.json"), "Path to registry file")
	tcpStartFlag := flag.Int("start-tcp-port", 4100, "Starting port for tenant TCP instances")
	httpStartFlag := flag.Int("start-http-port", 5100, "Starting port for tenant HTTP telemetry gateways")
	flag.Parse()

	// Default host to Tailscale IP or localhost if empty
	host := *hostFlag
	if host == "" {
		host = "100.95.206.7" // Default to Tailscale node IP
	}

	supervisor := &Supervisor{
		databases:     make(map[string]*DatabaseInstance),
		registryPath:  *registryFlag,
		baseDataDir:   *dataDirFlag,
		siderBin:      *siderBinFlag,
		publicHost:    host,
		startTCPPort:  *tcpStartFlag,
		startHTTPPort: *httpStartFlag,
	}

	supervisor.loadRegistry()

	mux := http.NewServeMux()
	supervisor.RegisterRoutes(mux)

	fmt.Println("==================================================")
	fmt.Printf("   ☁️  SIDER CLOUD SUPERVISOR LISTENING ON %s\n", *portFlag)
	fmt.Printf("   Public Host:      %s\n", host)
	fmt.Printf("   Database Data:    %s\n", *dataDirFlag)
	fmt.Printf("   TCP Port Range:   %d+\n", *tcpStartFlag)
	fmt.Printf("   HTTP Port Range:  %d+\n", *httpStartFlag)
	fmt.Printf("   Sider Executable: %s\n", *siderBinFlag)
	fmt.Println("==================================================")

	if err := http.ListenAndServe(*portFlag, mux); err != nil {
		log.Fatalf("Supervisor failed: %v", err)
	}
}

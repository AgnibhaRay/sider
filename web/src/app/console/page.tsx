"use client";

import React, { useState, useEffect, useRef } from "react";
import Link from "next/link";
import { SiderLogo } from "@/components/SiderLogo";
import { ThreeDarkCanvas } from "@/components/ThreeDarkCanvas";
import { useUser, UserButton } from "@clerk/nextjs";

interface DatabaseInstance {
  id: string;
  name: string;
  region: string;
  tcp_port: number;
  http_port: number;
  token: string;
  status: string;
  connection_uri: string;
  http_endpoint: string;
  created_at: string;
}

interface OperationEvent {
  timestamp: number;
  client_addr: string;
  command: string;
  key?: string;
  latency_us: number;
  status: string;
  storage_tier?: string;
}

interface KeyRecord {
  key: string;
  value: string;
  ttl: number;
  tier: string;
  expires?: string;
}

interface InstanceStats {
  name?: string;
  port?: string;
  http_port?: string;
  version?: string;
  status?: string;
  uptime_seconds?: number;
  memtable_entries?: number;
  memtable_bytes?: number;
  memtable_limit?: number;
  sstables_count?: number;
  sstables_bytes?: number;
  wal_bytes?: number;
  total_ops?: number;
  ops_per_sec?: number;
  connected_clients?: number;
  auth_required?: boolean;
}

// Canonical default instance
const DEFAULT_INSTANCE: DatabaseInstance = {
  id: "sdr-db-fe7b9912",
  name: "alpha-production",
  region: "ind-tbn-1",
  tcp_port: 4100,
  http_port: 5100,
  token: "sdr_live_793855e4ec0e70901bef05344264ba98",
  status: "running",
  connection_uri: "sider://default:sdr_live_793855e4ec0e70901bef05344264ba98@100.95.206.7:4100",
  http_endpoint: "http://100.95.206.7:5100",
  created_at: "2026-09-28T15:05:56Z"
};

type LanguageId = "python" | "node" | "go" | "rust" | "java" | "csharp" | "curl" | "netcat";

export default function SiderConsolePage() {
  const { user, isSignedIn, isLoaded } = useUser();
  const [supervisorHost] = useState<string>("http://100.95.206.7:8080");
  const [isDemoMode, setIsDemoMode] = useState<boolean>(false);
  const [databases, setDatabases] = useState<DatabaseInstance[]>([DEFAULT_INSTANCE]);
  const [selectedDbId, setSelectedDbId] = useState<string>(DEFAULT_INSTANCE.id);
  const [activeTab, setActiveTab] = useState<"overview" | "metrics" | "browser" | "monitor" | "cli">("overview");
  const [activeLang, setActiveLang] = useState<LanguageId>("python");
  
  const [, setLoading] = useState<boolean>(false);
  const [supervisorOnline, setSupervisorOnline] = useState<boolean | null>(null);
  const [showNewDbModal, setShowNewDbModal] = useState<boolean>(false);
  const [newDbName, setNewDbName] = useState<string>("");
  const [newDbRegion, setNewDbRegion] = useState<string>("ind-tbn-1");

  // Derive stable selected database instance
  const selectedDb = databases.find((d) => d.id === selectedDbId) || databases[0] || DEFAULT_INSTANCE;

  // Live Monitor Stream State
  const [events, setEvents] = useState<OperationEvent[]>([]);
  const [isMonitorPaused, setIsMonitorPaused] = useState<boolean>(false);
  const [filterCmd, setFilterCmd] = useState<string>("ALL");
  const monitorEndRef = useRef<HTMLDivElement>(null);

  // Key-Value Explorer State
  const [keys, setKeys] = useState<KeyRecord[]>([]);
  const [searchQuery, setSearchQuery] = useState<string>("");
  const [newKey, setNewKey] = useState<string>("");
  const [newVal, setNewVal] = useState<string>("");
  const [newTTL, setNewTTL] = useState<string>("");
  const [keyFilterTier, setKeyFilterTier] = useState<"ALL" | "memtable" | "sstable">("ALL");

  // CLI State
  const [cliInput, setCliInput] = useState<string>("");
  const [cliHistory, setCliHistory] = useState<{ cmd: string; resp: string; time: string }[]>([
    { cmd: "AUTH " + selectedDb.token, resp: "OK", time: "0.12ms" },
    { cmd: "INFO", resp: "# Sider Server v2.1.0\r\nstatus:online\r\nengine:LSM-Tree\r\nwal_bytes:51\r\nmemtable_entries:1\r\nsstables_count:0", time: "0.24ms" }
  ]);
  const cliBottomRef = useRef<HTMLDivElement>(null);

  // Stats State
  const [stats, setStats] = useState<InstanceStats>({
    memtable_entries: 1,
    memtable_bytes: 58,
    memtable_limit: 100,
    sstables_count: 0,
    sstables_bytes: 0,
    wal_bytes: 51,
    total_ops: 2,
    ops_per_sec: 140,
    connected_clients: 1,
    auth_required: true,
    version: "2.1.0"
  });

  const [copiedKey, setCopiedKey] = useState<string | null>(null);

  const copyToClipboard = (text: string, keyName: string) => {
    navigator.clipboard.writeText(text);
    setCopiedKey(keyName);
    setTimeout(() => setCopiedKey(null), 2000);
  };

  // 1. Fetch Supervisor Databases
  const fetchDatabases = async () => {
    try {
      const res = await fetch(`${supervisorHost}/api/databases`, {
        signal: AbortSignal.timeout(3500)
      });
      if (res.ok) {
        const data = await res.json();
        if (data && Array.isArray(data.databases) && data.databases.length > 0) {
          const sorted = [...data.databases].sort((a, b) => a.tcp_port - b.tcp_port);
          setDatabases(sorted);
          setSelectedDbId((prev) => {
            const exists = sorted.some((d) => d.id === prev);
            return exists ? prev : sorted[0].id;
          });
          setSupervisorOnline(true);
          return;
        }
      }
      setSupervisorOnline(false);
    } catch {
      setSupervisorOnline(false);
    }
  };

  // 2. Fetch Instance Real-Time Telemetry Stats
  const fetchStats = async () => {
    if (isDemoMode) {
      setStats((prev) => ({
        ...prev,
        ops_per_sec: Math.floor(180 + Math.random() * 80),
        total_ops: (prev.total_ops || 0) + 1,
        memtable_bytes: 58 + Math.floor(Math.random() * 20)
      }));
      return;
    }

    try {
      const res = await fetch(`${selectedDb.http_endpoint}/api/stats`, {
        signal: AbortSignal.timeout(2500)
      });
      if (res.ok) {
        const data = await res.json();
        setStats(data);
      }
    } catch {
      // Keep previous stats
    }
  };

  // 3. Fetch Keys
  const fetchKeys = async () => {
    if (isDemoMode) {
      if (keys.length === 0) {
        setKeys([
          { key: "session:user_1001", value: '{"id":1001,"role":"admin","status":"active"}', ttl: -1, tier: "memtable" },
          { key: "rate_limit:ip:10.0.4.12", value: "24", ttl: 58, tier: "memtable", expires: "in 58s" },
          { key: "cache:catalog:trending", value: '["p_89","p_12","p_44"]', ttl: 3590, tier: "sstable", expires: "in 59m" }
        ]);
      }
      return;
    }

    try {
      const res = await fetch(`${selectedDb.http_endpoint}/api/keys`, {
        signal: AbortSignal.timeout(3000)
      });
      if (res.ok) {
        const data = await res.json();
        if (data && Array.isArray(data.keys)) {
          setKeys(data.keys);
        }
      }
    } catch {
      // Fallback
    }
  };

  // Polling loops
  useEffect(() => {
    fetchDatabases();
    const interval = setInterval(fetchDatabases, 6000);
    return () => clearInterval(interval);
  }, [supervisorHost]);

  useEffect(() => {
    fetchStats();
    fetchKeys();
    const statTimer = setInterval(fetchStats, 2500);
    return () => clearInterval(statTimer);
  }, [selectedDb, isDemoMode]);

  // SSE Monitor Stream
  useEffect(() => {
    if (isDemoMode) {
      const demoInterval = setInterval(() => {
        if (isMonitorPaused) return;
        const cmds = ["GET", "PUT", "DEL", "EXPIRE", "KEYS", "AUTH"];
        const randCmd = cmds[Math.floor(Math.random() * cmds.length)];
        const randKey = ["user:101", "session:tok", "item:99", "alpha:lock", "stats:views"][Math.floor(Math.random() * 5)];
        const newEv: OperationEvent = {
          timestamp: Date.now(),
          client_addr: `100.95.206.7:${Math.floor(30000 + Math.random() * 20000)}`,
          command: randCmd,
          key: randCmd === "AUTH" ? undefined : randKey,
          latency_us: Math.floor(40 + Math.random() * 180),
          status: "OK",
          storage_tier: Math.random() > 0.3 ? "memtable" : "sstable"
        };
        setEvents((prev) => [...prev.slice(-99), newEv]);
      }, 1400);
      return () => clearInterval(demoInterval);
    }

    let es: EventSource | null = null;
    try {
      es = new EventSource(`${selectedDb.http_endpoint}/api/monitor`);
      es.onmessage = (e) => {
        if (isMonitorPaused) return;
        try {
          const parsed = JSON.parse(e.data);
          setEvents((prev) => [...prev.slice(-99), parsed]);
        } catch {
          // ignore
        }
      };
    } catch {
      // Fallback
    }

    return () => {
      if (es) es.close();
    };
  }, [selectedDb, isDemoMode, isMonitorPaused]);

  // Execute Command via HTTP REST or Simulation
  const handleExecuteCli = async (e: React.FormEvent) => {
    e.preventDefault();
    const raw = cliInput.trim();
    if (!raw) return;

    setCliInput("");
    const startTime = performance.now();

    if (isDemoMode) {
      const parts = raw.split(" ");
      const verb = parts[0].toUpperCase();
      let simulatedResp = "ERR unknown command";

      if (verb === "AUTH") {
        simulatedResp = "OK";
      } else if (verb === "PING") {
        simulatedResp = "PONG";
      } else if (verb === "PUT" && parts.length >= 3) {
        simulatedResp = "OK";
        setKeys((prev) => [
          ...prev.filter((k) => k.key !== parts[1]),
          { key: parts[1], value: parts.slice(2).join(" "), ttl: -1, tier: "memtable" }
        ]);
      } else if (verb === "GET" && parts.length >= 2) {
        const found = keys.find((k) => k.key === parts[1]);
        simulatedResp = found ? `"${found.value}"` : "(nil)";
      } else if (verb === "DEL" && parts.length >= 2) {
        setKeys((prev) => prev.filter((k) => k.key !== parts[1]));
        simulatedResp = "1";
      } else if (verb === "KEYS") {
        simulatedResp = keys.map((k, i) => `${i + 1}) "${k.key}"`).join("\r\n") || "(empty list)";
      } else if (verb === "INFO") {
        simulatedResp = `# Sider Server v2.1.0\r\nstatus:online\r\nengine:LSM-Tree\r\nregion:ind-tbn-1\r\nmemtable_entries:${keys.length}\r\nwal_bytes:128`;
      }

      const elapsed = (performance.now() - startTime).toFixed(2);
      setCliHistory((prev) => [...prev, { cmd: raw, resp: simulatedResp, time: `${elapsed}ms` }]);
      setTimeout(() => cliBottomRef.current?.scrollIntoView({ behavior: "smooth" }), 50);
      return;
    }

    try {
      const res = await fetch(`${selectedDb.http_endpoint}/api/exec`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          command: raw,
          token: selectedDb.token
        })
      });
      const data = await res.json();
      const elapsed = (performance.now() - startTime).toFixed(2);
      setCliHistory((prev) => [
        ...prev,
        { cmd: raw, resp: data.output || data.error || "OK", time: `${elapsed}ms` }
      ]);
      fetchKeys();
      fetchStats();
    } catch {
      const elapsed = (performance.now() - startTime).toFixed(2);
      setCliHistory((prev) => [
        ...prev,
        { cmd: raw, resp: "ERR connection failed to node endpoint", time: `${elapsed}ms` }
      ]);
    }

    setTimeout(() => cliBottomRef.current?.scrollIntoView({ behavior: "smooth" }), 50);
  };

  // Add Key in Browser
  const handleAddKey = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newKey.trim()) return;

    const cmd = newTTL
      ? `PUT ${newKey.trim()} ${newVal || '""'} ${newTTL}`
      : `PUT ${newKey.trim()} ${newVal || '""'}`;

    if (isDemoMode) {
      setKeys((prev) => [
        ...prev.filter((k) => k.key !== newKey.trim()),
        {
          key: newKey.trim(),
          value: newVal || '""',
          ttl: newTTL ? parseInt(newTTL) : -1,
          tier: "memtable"
        }
      ]);
      setNewKey("");
      setNewVal("");
      setNewTTL("");
      return;
    }

    try {
      await fetch(`${selectedDb.http_endpoint}/api/exec`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ command: cmd, token: selectedDb.token })
      });
      setNewKey("");
      setNewVal("");
      setNewTTL("");
      fetchKeys();
      fetchStats();
    } catch {
      // Error handling
    }
  };

  // Delete Key
  const handleDeleteKey = async (targetKey: string) => {
    if (isDemoMode) {
      setKeys((prev) => prev.filter((k) => k.key !== targetKey));
      return;
    }
    try {
      await fetch(`${selectedDb.http_endpoint}/api/exec`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ command: `DEL ${targetKey}`, token: selectedDb.token })
      });
      fetchKeys();
      fetchStats();
    } catch {
      // Error handling
    }
  };

  // Create Database Instance
  const handleCreateDatabase = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newDbName.trim()) return;
    setLoading(true);

    if (isDemoMode || !supervisorOnline) {
      const newInst: DatabaseInstance = {
        id: `sdr-db-${Math.random().toString(16).slice(2, 10)}`,
        name: newDbName.trim().toLowerCase().replace(/\s+/g, "-"),
        region: newDbRegion,
        tcp_port: 4100 + databases.length,
        http_port: 5100 + databases.length,
        token: `sdr_live_${Math.random().toString(16).slice(2, 34)}`,
        status: "running",
        connection_uri: `sider://default:token@100.95.206.7:${4100 + databases.length}`,
        http_endpoint: `http://100.95.206.7:${5100 + databases.length}`,
        created_at: new Date().toISOString()
      };
      const updated = [...databases, newInst].sort((a, b) => a.tcp_port - b.tcp_port);
      setDatabases(updated);
      setSelectedDbId(newInst.id);
      setLoading(false);
      setShowNewDbModal(false);
      setNewDbName("");
      return;
    }

    try {
      const res = await fetch(`${supervisorHost}/api/databases`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: newDbName.trim(),
          region: newDbRegion
        })
      });
      if (res.ok) {
        const created = await res.json();
        const updated = [...databases.filter((d) => d.id !== created.id), created].sort(
          (a, b) => a.tcp_port - b.tcp_port
        );
        setDatabases(updated);
        setSelectedDbId(created.id);
        setShowNewDbModal(false);
        setNewDbName("");
      }
    } catch {
      // Error handling
    } finally {
      setLoading(false);
    }
  };

  // Delete Database
  const handleDeleteDatabase = async (id: string) => {
    if (databases.length <= 1) {
      alert("At least one database instance must be retained.");
      return;
    }
    if (!confirm(`Are you sure you want to terminate database instance '${selectedDb.name}'?`)) {
      return;
    }

    if (isDemoMode || !supervisorOnline) {
      const filtered = databases.filter((d) => d.id !== id);
      setDatabases(filtered);
      setSelectedDbId(filtered[0]?.id || DEFAULT_INSTANCE.id);
      return;
    }

    try {
      const res = await fetch(`${supervisorHost}/api/databases/${id}`, { method: "DELETE" });
      if (res.ok) {
        const filtered = databases.filter((d) => d.id !== id);
        setDatabases(filtered);
        setSelectedDbId(filtered[0]?.id || DEFAULT_INSTANCE.id);
      }
    } catch {
      // Error handling
    }
  };

  // Filtered keys
  const filteredKeys = keys.filter((k) => {
    const matchesSearch = k.key.toLowerCase().includes(searchQuery.toLowerCase());
    const matchesTier = keyFilterTier === "ALL" || k.tier === keyFilterTier;
    return matchesSearch && matchesTier;
  });

  // Filtered events
  const filteredEvents = events.filter((ev) => {
    if (filterCmd === "ALL") return true;
    return ev.command.toUpperCase() === filterCmd;
  });

  // Multi-Language SDK Snippets
  const getSnippet = (lang: LanguageId) => {
    const host = "100.95.206.7";
    const port = selectedDb.tcp_port;
    const httpPort = selectedDb.http_port;
    const token = selectedDb.token;

    switch (lang) {
      case "python":
        return `# Sider Python Client for ind-tbn-1
import socket

class SiderClient:
    def __init__(self, host="${host}", port=${port}, token="${token}"):
        self.sock = socket.create_connection((host, port))
        self.send(f"AUTH {token}")

    def send(self, cmd: str) -> str:
        self.sock.sendall(f"{cmd}\\r\\n".encode())
        return self.sock.recv(4096).decode().strip()

    def put(self, key: str, value: str, ttl_sec: int = 0) -> str:
        return self.send(f"PUT {key} {value} {ttl_sec}" if ttl_sec > 0 else f"PUT {key} {value}")

    def get(self, key: str) -> str:
        return self.send(f"GET {key}")

client = SiderClient()
print("Write:", client.put("user:session_99", '{"status":"active"}'))
print("Read:", client.get("user:session_99"))`;

      case "node":
        return `// Sider Node.js / TypeScript Driver
import net from "net";

export class SiderClient {
  private client: net.Socket;

  constructor(host = "${host}", port = ${port}, token = "${token}") {
    this.client = net.createConnection({ host, port }, () => {
      this.client.write(\`AUTH \${token}\\r\\n\`);
    });
  }

  async execute(cmd: string): Promise<string> {
    return new Promise((resolve) => {
      this.client.once("data", (data) => resolve(data.toString().trim()));
      this.client.write(\`\${cmd}\\r\\n\`);
    });
  }
}

const db = new SiderClient();
await db.execute('PUT session:active "token_live_123"');
const res = await db.execute('GET session:active');
console.log("Sider Result:", res);`;

      case "go":
        return `// Sider Native Go Client
package main

import (
	"bufio"
	"fmt"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "${host}:${port}")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// 1. Authenticate
	fmt.Fprintf(conn, "AUTH ${token}\\r\\n")
	resp, _ := reader.ReadString('\\n')
	fmt.Printf("Auth: %s", resp)

	// 2. Put key
	fmt.Fprintf(conn, "PUT telemetry:temperature 23.4\\r\\n")
	resp, _ = reader.ReadString('\\n')
	fmt.Printf("Put: %s", resp)

	// 3. Get key
	fmt.Fprintf(conn, "GET telemetry:temperature\\r\\n")
	resp, _ = reader.ReadString('\\n')
	fmt.Printf("Value: %s", resp)
}`;

      case "rust":
        return `// Sider Rust Driver
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;

fn main() -> std::io::Result<()> {
    let mut stream = TcpStream::connect("${host}:${port}")?;
    let mut reader = BufReader::new(stream.try_clone()?);

    // 1. Authenticate
    writeln!(stream, "AUTH ${token}")?;
    let mut line = String::new();
    reader.read_line(&mut line)?;
    println!("Auth: {}", line.trim());

    // 2. Write key
    writeln!(stream, "PUT cache:region 'ind-tbn-1'")?;
    line.clear();
    reader.read_line(&mut line)?;
    println!("Put: {}", line.trim());

    // 3. Read key
    writeln!(stream, "GET cache:region")?;
    line.clear();
    reader.read_line(&mut line)?;
    println!("Val: {}", line.trim());

    Ok(())
}`;

      case "java":
        return `// Sider Java / Spring Boot Driver
import java.io.*;
import java.net.*;

public class SiderQuickstart {
    public static void main(String[] args) throws IOException {
        try (Socket socket = new Socket("${host}", ${port});
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // 1. Authenticate
            out.println("AUTH ${token}");
            System.out.println("Auth: " + in.readLine());

            // 2. Write key
            out.println("PUT order:9482 '{\\"total\\":45.99}'");
            System.out.println("Put: " + in.readLine());

            // 3. Read key
            out.println("GET order:9482");
            System.out.println("Value: " + in.readLine());
        }
    }
}`;

      case "csharp":
        return `// Sider .NET / C# Client
using System;
using System.IO;
using System.Net.Sockets;

class Program {
    static void Main() {
        using var client = new TcpClient("${host}", ${port});
        using var stream = client.GetStream();
        using var reader = new StreamReader(stream);
        using var writer = new StreamWriter(stream) { AutoFlush = true };

        // 1. Authenticate
        writer.WriteLine("AUTH ${token}");
        Console.WriteLine($"Auth: {reader.ReadLine()}");

        // 2. Write
        writer.WriteLine("PUT session:token 'active_abc'");
        Console.WriteLine($"Put: {reader.ReadLine()}");

        // 3. Read
        writer.WriteLine("GET session:token");
        Console.WriteLine($"Value: {reader.ReadLine()}");
    }
}`;

      case "curl":
        return `# 1. Execute query via HTTP REST gateway
curl -X POST "http://${host}:${httpPort}/api/exec" \\
  -H "Content-Type: application/json" \\
  -d '{"command": "PUT api:token secret", "token": "${token}"}'

# 2. Inspect real-time database stats
curl -s "http://${host}:${httpPort}/api/stats"

# 3. Query all keys
curl -s "http://${host}:${httpPort}/api/keys"`;

      case "netcat":
        return `# Raw TCP Pipeline over netcat
printf "AUTH ${token}\\nPUT alpha:ping pong\\nGET alpha:ping\\nINFO\\n" | nc ${host} ${port}`;
    }
  };

  return (
    <div
      className="min-h-screen text-[#ffffff] flex flex-col font-sans select-none antialiased relative overflow-x-hidden"
      style={{ backgroundColor: "#0e0e0e", fontFamily: "var(--font-inter), sans-serif" }}
    >
      {/* Ambient 3D Neon Constellation Grid */}
      <div className="fixed inset-0 w-full h-full pointer-events-none opacity-30 z-0">
        <ThreeDarkCanvas />
      </div>
      {/* 1. TOP SIGNAL STRIP (VIOLET GLOW ACCENT) */}
      <div
        className="w-full h-10 px-4 text-white text-[12px] font-medium flex items-center justify-center gap-2 border-b border-[#414042]/50 z-50 sticky top-0"
        style={{
          background: "linear-gradient(179deg, rgba(64,91,255,0.25) 1.06%, rgba(112,132,255,0.06) 123.42%)"
        }}
      >
        <span className="w-1.5 h-1.5 rounded-full bg-[#405bff] animate-pulse" />
        <span className="font-mono text-[#7084ff] uppercase font-bold tracking-wider text-[10px] px-2 py-0.5 rounded-[30px] bg-[#405bff]/20 border border-[#405bff]/40">
          PUBLIC ALPHA v0.2.1
        </span>
        <span className="text-[#d1d3d4]">
          <strong>Sider Cloud Cockpit</strong> — Zero-dependency LSM storage with native SkipList MemTable on region <code className="text-[#7084ff] font-mono">ind-tbn-1</code>.
        </span>
        <Link href="/" className="text-[#7084ff] hover:underline font-semibold ml-1 hidden sm:inline">
          Explore Architecture &rarr;
        </Link>
      </div>

      {/* 2. FLOATING COCKPIT HEADER BAR (60px radius pill) */}
      <div className="w-full max-w-[1360px] mx-auto pt-4 px-4 sticky top-10 z-40">
        <header className="w-full bg-[#191919] border border-white/10 rounded-[60px] px-6 h-14 flex items-center justify-between shadow-[0_4px_20px_rgba(0,0,0,0.45)] backdrop-blur-md">
          <div className="flex items-center gap-4">
            <Link href="/" className="flex items-center gap-3 group">
              <SiderLogo size={24} />
              <div className="flex items-center gap-2">
                <span className="text-[15px] font-medium text-white tracking-[-0.02em]">
                  Sider Cloud
                </span>
                <span className="text-[9px] font-mono uppercase bg-[#405bff]/20 text-[#7084ff] border border-[#405bff]/40 px-2 py-0.5 rounded-[30px]">
                  ALPHA
                </span>
              </div>
            </Link>

            <div className="hidden lg:flex items-center gap-2 pl-4 border-l border-[#414042]">
              <span className="text-[12px] text-[#6d6e71]">Node:</span>
              <span className="text-[11px] font-mono text-[#7084ff] bg-[#0e0e0e] px-2 py-0.5 rounded-[30px] border border-[#414042]">
                100.95.206.7 (ind-tbn-1)
              </span>
              <div className="flex items-center gap-1.5 ml-1">
                <span
                  className={`w-2 h-2 rounded-full ${
                    isDemoMode ? "bg-[#eab308]" : supervisorOnline ? "bg-[#405bff] shadow-[0_0_8px_rgba(64,91,255,0.8)]" : "bg-[#ef4444]"
                  }`}
                />
                <span className="text-[11px] text-[#a7a9ac]">
                  {isDemoMode ? "Sandbox" : supervisorOnline ? "Live Mesh" : "Connecting"}
                </span>
              </div>
            </div>
          </div>

          {/* Right Nav Actions with Dynamic Clerk Auth */}
          <div className="flex items-center gap-3">
            <button
              onClick={() => setIsDemoMode((prev) => !prev)}
              className="px-3.5 py-1 rounded-[30px] text-[12px] font-medium border border-[#414042] bg-[#0e0e0e] text-[#d1d3d4] hover:text-white hover:border-[#405bff] transition-colors cursor-pointer"
            >
              {isDemoMode ? "🟡 Sandbox Simulated" : "🟢 Live Bare-Metal"}
            </button>

            {/* Dynamic Clerk Status: Keeps user profile and stays signed in */}
            {isLoaded && isSignedIn && (
              <div className="flex items-center gap-2 pl-2 border-l border-[#414042]">
                <span className="text-[12px] font-mono text-[#a7a9ac] hidden md:inline">
                  {user?.primaryEmailAddress?.emailAddress || user?.firstName || "Developer"}
                </span>
                <UserButton />
              </div>
            )}
            {isLoaded && !isSignedIn && (
              <Link
                href="/sign-in"
                className="px-4 py-1 rounded-[30px] border border-[#414042] bg-[#191919] text-[#d1d3d4] hover:text-white hover:border-[#405bff] text-[12px] font-medium transition-colors"
              >
                Sign in
              </Link>
            )}

            <button
              onClick={() => setShowNewDbModal(true)}
              className="px-4 py-1.5 rounded-[30px] text-[13px] font-medium text-white bg-[#405bff] hover:bg-[#344bd6] transition-all shadow-[0_0_20px_rgba(64,91,255,0.4)] flex items-center gap-1.5 cursor-pointer"
            >
              <span className="text-base leading-none">+</span>
              <span>New Database</span>
            </button>
          </div>
        </header>
      </div>

      {/* 3. MAIN WORKSPACE (LaunchDarkly Neon Control Cockpit) */}
      <div className="flex-1 w-full max-w-[1360px] mx-auto p-4 sm:p-6 lg:p-8 flex flex-col lg:flex-row gap-6 mt-2">
        
        {/* LEFT COLUMN: INSTANCE SELECTOR & HARDWARE SPECS (280px) */}
        <aside className="w-full lg:w-72 flex flex-col gap-4 shrink-0">
          <div className="bg-[#191919] rounded-[30px] border border-[#414042] p-5 shadow-[0_0_20px_rgba(0,0,0,0.5)]">
            <div className="flex items-center justify-between mb-3">
              <span className="text-[11px] font-semibold text-[#a7a9ac] uppercase tracking-wider font-mono">
                INSTANCES ({databases.length})
              </span>
              <button
                onClick={fetchDatabases}
                className="text-[11px] text-[#7084ff] hover:underline cursor-pointer"
              >
                Refresh
              </button>
            </div>

            {/* Stable Deterministic Database List */}
            <div className="space-y-2">
              {databases.map((db) => {
                const isSelected = selectedDbId === db.id;
                return (
                  <div
                    key={db.id}
                    onClick={() => setSelectedDbId(db.id)}
                    className={`p-3.5 rounded-[20px] border cursor-pointer transition-all ${
                      isSelected
                        ? "bg-[#0e0e0e] border-[#405bff] shadow-[0_0_20px_rgba(64,91,255,0.3)] ring-1 ring-[#405bff]"
                        : "bg-[#191919] border-[#414042] hover:border-[#58595b]"
                    }`}
                  >
                    <div className="flex items-center justify-between">
                      <span className="text-[14px] font-medium text-white">
                        {db.name}
                      </span>
                      <span className={`w-2 h-2 rounded-full ${isSelected ? "bg-[#405bff] animate-pulse" : "bg-[#19a05f]"}`} />
                    </div>
                    <div className="mt-1 flex items-center justify-between text-[11px] font-mono text-[#a7a9ac]">
                      <span>PORT :{db.tcp_port}</span>
                      <span className="text-[10px] text-[#6d6e71]">{db.id.slice(0, 10)}</span>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* HARDWARE BLUEPRINT CARD — CLOUD SPECIFICATIONS ONLY */}
          <div className="bg-[#191919] rounded-[30px] border border-[#414042] p-5 text-[12px] space-y-2.5 shadow-[0_0_20px_rgba(0,0,0,0.5)]">
            <div className="text-[11px] font-semibold text-[#7084ff] uppercase tracking-wider font-mono">
              CLOUD NODE SPECIFICATIONS
            </div>
            <div className="flex items-center justify-between pt-1">
              <span className="text-[#a7a9ac]">Architecture</span>
              <span className="font-medium text-white font-mono">x86_64 Bare-Metal</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[#a7a9ac]">Processor (CPU)</span>
              <span className="font-mono text-white">Intel i5-9600 (6 Cores)</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[#a7a9ac]">Memory (RAM)</span>
              <span className="font-mono text-white">16GB DDR4</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[#a7a9ac]">GPU Accelerator</span>
              <span className="font-mono text-[#7084ff] font-medium">NVIDIA GTX 1660 Ti</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[#a7a9ac]">Storage (NVMe)</span>
              <span className="font-mono text-white">480GB High-IOPS SSD</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[#a7a9ac]">Region</span>
              <span className="font-mono text-[#3dd6f5]">ind-tbn-1</span>
            </div>
            <div className="pt-2 border-t border-[#414042] flex items-center justify-between text-[11px]">
              <span className="text-[#6d6e71]">Storage Engine</span>
              <span className="text-[#7084ff] font-medium font-mono">SkipList + LSM v2.1.0</span>
            </div>
          </div>
        </aside>

        {/* RIGHT COLUMN: ACTIVE DATABASE WORKSPACE */}
        <main className="flex-1 flex flex-col gap-6 min-w-0">
          
          {/* HEADER HERO CARD */}
          <div className="bg-[#191919] rounded-[30px] border border-[#414042] p-6 lg:p-7 shadow-[0_0_30px_rgba(0,0,0,0.5)]">
            <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 pb-6 border-b border-[#414042]">
              <div>
                <div className="flex items-center gap-3">
                  <h1 className="text-[28px] sm:text-[36px] font-medium text-white tracking-tight">
                    {selectedDb.name}
                  </h1>
                  <span className="px-3 py-0.5 rounded-[30px] text-[11px] font-medium bg-[#405bff]/20 text-[#7084ff] border border-[#405bff]/40 font-mono">
                    ● ACTIVE
                  </span>
                  <span className="px-2.5 py-0.5 rounded-[30px] text-[10px] font-mono bg-[#0e0e0e] text-[#3dd6f5] border border-[#414042]">
                    ind-tbn-1
                  </span>
                </div>
                <p className="text-[14px] text-[#a7a9ac] mt-1">
                  Single-tenant LSM instance isolated with dedicated WAL log and SSTable storage level.
                </p>
              </div>

              {/* Quick Specs Badges */}
              <div className="flex flex-wrap items-center gap-2 font-mono text-[12px]">
                <div className="bg-[#0e0e0e] px-3.5 py-1.5 rounded-[30px] border border-[#414042] flex items-center gap-1.5">
                  <span className="text-[#6d6e71]">TCP:</span>
                  <strong className="text-white">:{selectedDb.tcp_port}</strong>
                </div>
                <div className="bg-[#0e0e0e] px-3.5 py-1.5 rounded-[30px] border border-[#414042] flex items-center gap-1.5">
                  <span className="text-[#6d6e71]">HTTP:</span>
                  <strong className="text-[#7084ff]">:{selectedDb.http_port}</strong>
                </div>
              </div>
            </div>

            {/* TAB SELECTOR (LaunchDarkly Segmented Control) */}
            <div className="flex flex-wrap items-center gap-2 pt-4">
              {[
                { id: "overview", label: "Overview & SDK Drivers" },
                { id: "metrics", label: "LSM Telemetry & Storage" },
                { id: "browser", label: `Data Browser (${keys.length})` },
                { id: "monitor", label: `Live Monitor (${events.length})` },
                { id: "cli", label: "Interactive Web CLI" }
              ].map((tab) => (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id as any)}
                  className={`px-4 py-2 rounded-[30px] text-[13px] font-medium transition-all cursor-pointer flex items-center gap-2 ${
                    activeTab === tab.id
                      ? "bg-[#405bff] text-white shadow-[0_0_20px_rgba(64,91,255,0.4)]"
                      : "bg-[#0e0e0e] text-[#d1d3d4] hover:text-white hover:bg-[#2c2c2c] border border-[#414042]"
                  }`}
                >
                  {activeTab === tab.id && <span className="w-1.5 h-1.5 rounded-full bg-white animate-pulse" />}
                  <span>{tab.label}</span>
                </button>
              ))}
            </div>
          </div>

          {/* TAB 1: OVERVIEW & MULTI-LANGUAGE SDK DRIVERS */}
          {activeTab === "overview" && (
            <div className="space-y-6">
              
              {/* Connection Endpoint Card */}
              <div className="bg-[#191919] rounded-[30px] border border-[#414042] p-6 shadow-[0_0_20px_rgba(0,0,0,0.5)]">
                <span className="text-[11px] font-semibold text-[#7084ff] uppercase tracking-wider font-mono">
                  CONNECTION ENDPOINT (URI)
                </span>
                <div className="mt-2.5 flex items-center justify-between gap-3 bg-[#0e0e0e] p-3.5 rounded-[12px] border border-[#414042]">
                  <code className="text-white font-mono text-[13px] truncate">
                    {selectedDb.connection_uri}
                  </code>
                  <button
                    onClick={() => copyToClipboard(selectedDb.connection_uri, "uri")}
                    className="px-4 py-1.5 rounded-[30px] bg-[#405bff] text-white text-[12px] font-medium hover:bg-[#344bd6] transition-colors shrink-0 shadow-[0_0_12px_rgba(64,91,255,0.4)] cursor-pointer"
                  >
                    {copiedKey === "uri" ? "✓ Copied" : "Copy URI"}
                  </button>
                </div>

                <div className="grid grid-cols-1 sm:grid-cols-4 gap-4 mt-5 pt-5 border-t border-[#414042] text-[13px]">
                  <div>
                    <span className="text-[11px] text-[#6d6e71] uppercase font-semibold">Host Node</span>
                    <div className="font-mono text-white mt-0.5">100.95.206.7</div>
                  </div>
                  <div>
                    <span className="text-[11px] text-[#6d6e71] uppercase font-semibold">TCP Port</span>
                    <div className="font-mono text-white mt-0.5">{selectedDb.tcp_port}</div>
                  </div>
                  <div>
                    <span className="text-[11px] text-[#6d6e71] uppercase font-semibold">HTTP Gateway</span>
                    <div className="font-mono text-[#7084ff] mt-0.5">:{selectedDb.http_port}</div>
                  </div>
                  <div>
                    <span className="text-[11px] text-[#6d6e71] uppercase font-semibold">Auth Token</span>
                    <div className="font-mono text-white mt-0.5 truncate">{selectedDb.token}</div>
                  </div>
                </div>
              </div>

              {/* MULTI-LANGUAGE SDK DRIVERS CARD */}
              <div className="bg-[#191919] rounded-[30px] border border-[#414042] p-6 shadow-[0_0_20px_rgba(0,0,0,0.5)]">
                <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 mb-4">
                  <div>
                    <h2 className="text-[18px] font-medium text-white tracking-tight">
                      Production Driver Quickstart
                    </h2>
                    <p className="text-[13px] text-[#a7a9ac]">
                      Plug-and-play drivers with native protocol formatting and automatic token authentication.
                    </p>
                  </div>
                  <button
                    onClick={() => copyToClipboard(getSnippet(activeLang), "snippet")}
                    className="px-4 py-1.5 rounded-[30px] bg-[#405bff] text-white text-[12px] font-medium hover:bg-[#344bd6] transition-all shrink-0 shadow-[0_0_15px_rgba(64,91,255,0.4)] cursor-pointer"
                  >
                    {copiedKey === "snippet" ? "✓ Copied Code" : "Copy Snippet"}
                  </button>
                </div>

                {/* Language Pill Selector */}
                <div className="flex flex-wrap items-center gap-1.5 p-1 bg-[#0e0e0e] rounded-[30px] border border-[#414042] mb-4">
                  {(
                    [
                      { id: "python", label: "Python" },
                      { id: "node", label: "TypeScript / Node" },
                      { id: "go", label: "Go" },
                      { id: "rust", label: "Rust" },
                      { id: "java", label: "Java (Spring)" },
                      { id: "csharp", label: "C# (.NET)" },
                      { id: "curl", label: "cURL / REST" },
                      { id: "netcat", label: "Netcat / TCP" }
                    ] as const
                  ).map((lang) => (
                    <button
                      key={lang.id}
                      onClick={() => setActiveLang(lang.id)}
                      className={`px-3 py-1 rounded-[30px] text-[12px] font-mono transition-colors cursor-pointer ${
                        activeLang === lang.id
                          ? "bg-[#405bff] text-white font-medium shadow-[0_0_10px_rgba(64,91,255,0.5)]"
                          : "text-[#d1d3d4] hover:text-white hover:bg-[#191919]"
                      }`}
                    >
                      {lang.label}
                    </button>
                  ))}
                </div>

                {/* Code Window with Dracula Syntax */}
                <div className="rounded-[16px] bg-[#0e0e0e] p-5 font-mono text-[13px] border border-[#414042] overflow-x-auto text-[#f8f8f2] leading-relaxed">
                  <pre>
                    <code>{getSnippet(activeLang)}</code>
                  </pre>
                </div>
              </div>
            </div>
          )}

          {/* TAB 2: LSM TELEMETRY & STORAGE */}
          {activeTab === "metrics" && (
            <div className="space-y-6">
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                <div className="bg-[#191919] p-5 rounded-[24px] border border-[#414042] shadow-[0_0_20px_rgba(0,0,0,0.5)]">
                  <span className="text-[11px] font-semibold text-[#a7a9ac] uppercase font-mono">
                    MemTable Entries
                  </span>
                  <div className="text-[28px] font-semibold font-mono text-white mt-1">
                    {stats.memtable_entries ?? 1}
                  </div>
                  <div className="text-[11px] text-[#405bff] mt-1 font-mono">
                    {stats.memtable_bytes ?? 58} bytes in SkipList
                  </div>
                </div>

                <div className="bg-[#191919] p-5 rounded-[24px] border border-[#414042] shadow-[0_0_20px_rgba(0,0,0,0.5)]">
                  <span className="text-[11px] font-semibold text-[#a7a9ac] uppercase font-mono">
                    SSTable Count
                  </span>
                  <div className="text-[28px] font-semibold font-mono text-white mt-1">
                    {stats.sstables_count ?? 0}
                  </div>
                  <div className="text-[11px] text-[#3dd6f5] mt-1 font-mono">
                    {stats.sstables_bytes ?? 0} bytes on NVMe
                  </div>
                </div>

                <div className="bg-[#191919] p-5 rounded-[24px] border border-[#414042] shadow-[0_0_20px_rgba(0,0,0,0.5)]">
                  <span className="text-[11px] font-semibold text-[#a7a9ac] uppercase font-mono">
                    Write-Ahead Log (WAL)
                  </span>
                  <div className="text-[28px] font-semibold font-mono text-[#7084ff] mt-1">
                    {stats.wal_bytes ?? 51} B
                  </div>
                  <div className="text-[11px] text-[#a7a9ac] mt-1">
                    Crash Recovery: Synced
                  </div>
                </div>

                <div className="bg-[#191919] p-5 rounded-[24px] border border-[#414042] shadow-[0_0_20px_rgba(0,0,0,0.5)]">
                  <span className="text-[11px] font-semibold text-[#a7a9ac] uppercase font-mono">
                    Throughput Rate
                  </span>
                  <div className="text-[28px] font-semibold font-mono text-white mt-1">
                    {stats.ops_per_sec ?? 140}
                  </div>
                  <div className="text-[11px] text-[#19a05f] mt-1 font-mono">
                    ops / second (active)
                  </div>
                </div>
              </div>

              {/* Engine Architecture Flow */}
              <div className="bg-[#191919] rounded-[30px] border border-[#414042] p-6 shadow-[0_0_20px_rgba(0,0,0,0.5)]">
                <h3 className="text-[16px] font-medium text-white mb-2">
                  LSM Compaction & Durability Blueprint
                </h3>
                <p className="text-[13px] text-[#a7a9ac] mb-6">
                  Every mutation is sequentially committed to disk via Write-Ahead Log while remaining queryable in memory via lock-free SkipList.
                </p>

                <div className="grid grid-cols-1 md:grid-cols-3 gap-4 font-mono text-[12px]">
                  <div className="p-4 rounded-[20px] bg-[#0e0e0e] border border-[#414042]">
                    <div className="text-[#7084ff] font-semibold mb-1">1. MEMTABLE TIER</div>
                    <div className="text-[#d1d3d4]">Lock-Free SkipList in RAM</div>
                    <div className="text-[11px] text-[#6d6e71] mt-2">Latency: &lt; 0.15ms</div>
                  </div>
                  <div className="p-4 rounded-[20px] bg-[#0e0e0e] border border-[#414042]">
                    <div className="text-[#405bff] font-semibold mb-1">2. WAL PERSISTENCE</div>
                    <div className="text-[#d1d3d4]">Append-Only Log + CRC32</div>
                    <div className="text-[11px] text-[#6d6e71] mt-2">Durability: Crash Proof</div>
                  </div>
                  <div className="p-4 rounded-[20px] bg-[#0e0e0e] border border-[#414042]">
                    <div className="text-[#3dd6f5] font-semibold mb-1">3. SSTABLE STORAGE</div>
                    <div className="text-[#d1d3d4]">Bloom Filter + Sparse Block</div>
                    <div className="text-[11px] text-[#6d6e71] mt-2">Storage: 480GB NVMe</div>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* TAB 3: DATA BROWSER */}
          {activeTab === "browser" && (
            <div className="space-y-6">
              {/* Insert Form */}
              <div className="bg-[#191919] rounded-[30px] border border-[#414042] p-6 shadow-[0_0_20px_rgba(0,0,0,0.5)]">
                <h3 className="text-[16px] font-medium text-white mb-4">
                  Insert or Update Key
                </h3>
                <form onSubmit={handleAddKey} className="grid grid-cols-1 sm:grid-cols-12 gap-3">
                  <div className="sm:col-span-4">
                    <input
                      type="text"
                      placeholder="key (e.g. user:profile_101)"
                      value={newKey}
                      onChange={(e) => setNewKey(e.target.value)}
                      className="w-full bg-[#0e0e0e] border border-[#58595b] px-3.5 py-2 rounded-[10px] text-[13px] font-mono text-white focus:outline-none focus:border-[#405bff]"
                    />
                  </div>
                  <div className="sm:col-span-5">
                    <input
                      type="text"
                      placeholder='value (string or JSON payload)'
                      value={newVal}
                      onChange={(e) => setNewVal(e.target.value)}
                      className="w-full bg-[#0e0e0e] border border-[#58595b] px-3.5 py-2 rounded-[10px] text-[13px] font-mono text-white focus:outline-none focus:border-[#405bff]"
                    />
                  </div>
                  <div className="sm:col-span-1">
                    <input
                      type="number"
                      placeholder="TTL (s)"
                      value={newTTL}
                      onChange={(e) => setNewTTL(e.target.value)}
                      className="w-full bg-[#0e0e0e] border border-[#58595b] px-2 py-2 rounded-[10px] text-[13px] font-mono text-white focus:outline-none focus:border-[#405bff]"
                    />
                  </div>
                  <div className="sm:col-span-2">
                    <button
                      type="submit"
                      className="w-full h-full py-2 rounded-[30px] bg-[#405bff] hover:bg-[#344bd6] text-white text-[13px] font-medium transition-all shadow-[0_0_15px_rgba(64,91,255,0.4)] cursor-pointer"
                    >
                      PUT Key
                    </button>
                  </div>
                </form>
              </div>

              {/* Key Records Table */}
              <div className="bg-[#191919] rounded-[30px] border border-[#414042] p-6 shadow-[0_0_20px_rgba(0,0,0,0.5)]">
                <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 mb-4">
                  <div className="flex items-center gap-2 flex-1 max-w-sm">
                    <input
                      type="text"
                      placeholder="Filter keys..."
                      value={searchQuery}
                      onChange={(e) => setSearchQuery(e.target.value)}
                      className="w-full bg-[#0e0e0e] border border-[#58595b] px-3 py-1.5 rounded-[10px] text-[12px] font-mono text-white focus:outline-none focus:border-[#405bff]"
                    />
                  </div>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => setKeyFilterTier("ALL")}
                      className={`px-3 py-1 rounded-[30px] text-[11px] font-mono cursor-pointer ${
                        keyFilterTier === "ALL" ? "bg-[#405bff] text-white" : "bg-[#0e0e0e] text-[#a7a9ac] border border-[#414042]"
                      }`}
                    >
                      ALL
                    </button>
                    <button
                      onClick={() => setKeyFilterTier("memtable")}
                      className={`px-3 py-1 rounded-[30px] text-[11px] font-mono cursor-pointer ${
                        keyFilterTier === "memtable" ? "bg-[#405bff] text-white" : "bg-[#0e0e0e] text-[#a7a9ac] border border-[#414042]"
                      }`}
                    >
                      MEMTABLE
                    </button>
                    <button
                      onClick={() => setKeyFilterTier("sstable")}
                      className={`px-3 py-1 rounded-[30px] text-[11px] font-mono cursor-pointer ${
                        keyFilterTier === "sstable" ? "bg-[#405bff] text-white" : "bg-[#0e0e0e] text-[#a7a9ac] border border-[#414042]"
                      }`}
                    >
                      SSTABLE
                    </button>
                  </div>
                </div>

                <div className="overflow-x-auto">
                  <table className="w-full text-left font-mono text-[12px]">
                    <thead>
                      <tr className="border-b border-[#414042] text-[#6d6e71]">
                        <th className="pb-2.5">KEY</th>
                        <th className="pb-2.5">VALUE</th>
                        <th className="pb-2.5">TIER</th>
                        <th className="pb-2.5">TTL</th>
                        <th className="pb-2.5 text-right">ACTION</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-[#2c2c2c]">
                      {filteredKeys.length === 0 ? (
                        <tr>
                          <td colSpan={5} className="py-8 text-center text-[#6d6e71]">
                            No keys found matching query
                          </td>
                        </tr>
                      ) : (
                        filteredKeys.map((k) => (
                          <tr key={k.key} className="hover:bg-[#0e0e0e]/50">
                            <td className="py-2.5 font-semibold text-white">{k.key}</td>
                            <td className="py-2.5 text-[#d1d3d4] max-w-xs truncate">{k.value}</td>
                            <td className="py-2.5">
                              <span className={`px-2 py-0.5 rounded-[30px] text-[10px] ${k.tier === "memtable" ? "bg-[#405bff]/20 text-[#7084ff]" : "bg-[#3dd6f5]/20 text-[#3dd6f5]"}`}>
                                {k.tier}
                              </span>
                            </td>
                            <td className="py-2.5 text-[#a7a9ac]">{k.ttl === -1 ? "persist" : `${k.ttl}s`}</td>
                            <td className="py-2.5 text-right">
                              <button
                                onClick={() => handleDeleteKey(k.key)}
                                className="text-[#ef4444] hover:underline cursor-pointer"
                              >
                                Delete
                              </button>
                            </td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          )}

          {/* TAB 4: LIVE MONITOR */}
          {activeTab === "monitor" && (
            <div className="bg-[#191919] rounded-[30px] border border-[#414042] p-6 shadow-[0_0_20px_rgba(0,0,0,0.5)]">
              <div className="flex items-center justify-between pb-4 border-b border-[#414042] mb-4">
                <div className="flex items-center gap-2">
                  <span className="w-2.5 h-2.5 rounded-full bg-[#405bff] animate-pulse" />
                  <span className="text-[14px] font-medium text-white">Live Real-Time SSE Monitor</span>
                </div>
                <div className="flex items-center gap-2">
                  <button
                    onClick={() => setIsMonitorPaused((prev) => !prev)}
                    className="px-3.5 py-1 rounded-[30px] text-[12px] font-mono border border-[#414042] bg-[#0e0e0e] text-[#d1d3d4] hover:text-white cursor-pointer"
                  >
                    {isMonitorPaused ? "▶ Resume" : "⏸ Pause"}
                  </button>
                  <button
                    onClick={() => setEvents([])}
                    className="px-3.5 py-1 rounded-[30px] text-[12px] font-mono border border-[#414042] bg-[#0e0e0e] text-[#d1d3d4] hover:text-white cursor-pointer"
                  >
                    Clear
                  </button>
                </div>
              </div>

              <div className="h-80 overflow-y-auto font-mono text-[12px] space-y-1.5 p-3 rounded-[16px] bg-[#0e0e0e] border border-[#414042]">
                {filteredEvents.map((ev, i) => (
                  <div key={i} className="flex items-center justify-between py-1 border-b border-[#2c2c2c] text-[#d1d3d4]">
                    <div className="flex items-center gap-2">
                      <span className="text-[#6d6e71]">{new Date(ev.timestamp).toLocaleTimeString()}</span>
                      <span className="px-2 py-0.5 rounded-[30px] text-[10px] font-bold bg-[#405bff]/20 text-[#7084ff]">
                        {ev.command}
                      </span>
                      <span className="text-white">{ev.key || "—"}</span>
                    </div>
                    <div className="flex items-center gap-3">
                      <span className="text-[#19a05f]">{ev.latency_us}µs</span>
                      <span className="text-[10px] text-[#6d6e71]">{ev.client_addr}</span>
                    </div>
                  </div>
                ))}
                <div ref={monitorEndRef} />
              </div>
            </div>
          )}

          {/* TAB 5: INTERACTIVE CLI */}
          {activeTab === "cli" && (
            <div className="bg-[#191919] rounded-[30px] border border-[#414042] p-6 shadow-[0_0_20px_rgba(0,0,0,0.5)]">
              <div className="flex items-center justify-between pb-3 border-b border-[#414042] mb-3">
                <span className="text-[12px] font-mono text-[#a7a9ac]">
                  sider:{selectedDb.name}@100.95.206.7:{selectedDb.tcp_port}
                </span>
                <span className="text-[11px] font-mono text-[#7084ff]">PROTOCOL v2.1</span>
              </div>

              <div className="h-72 overflow-y-auto font-mono text-[12.5px] p-4 rounded-[16px] bg-[#0e0e0e] border border-[#414042] space-y-3">
                {cliHistory.map((item, idx) => (
                  <div key={idx} className="space-y-1">
                    <div className="flex items-center gap-2 text-[#405bff]">
                      <span>&gt;</span>
                      <span className="text-white">{item.cmd}</span>
                      <span className="text-[10px] text-[#6d6e71] ml-auto">{item.time}</span>
                    </div>
                    <pre className="text-[#a7a9ac] pl-4 whitespace-pre-wrap">{item.resp}</pre>
                  </div>
                ))}
                <div ref={cliBottomRef} />
              </div>

              <form onSubmit={handleExecuteCli} className="mt-3 flex items-center gap-2">
                <input
                  type="text"
                  placeholder="Enter Sider command (e.g. PUT user:name 'Alice', GET user:name, INFO)"
                  value={cliInput}
                  onChange={(e) => setCliInput(e.target.value)}
                  className="flex-1 bg-[#0e0e0e] border border-[#58595b] px-4 py-2.5 rounded-[10px] text-[13px] font-mono text-white focus:outline-none focus:border-[#405bff]"
                />
                <button
                  type="submit"
                  className="px-6 py-2.5 rounded-[30px] bg-[#405bff] hover:bg-[#344bd6] text-white text-[13px] font-medium transition-all shadow-[0_0_15px_rgba(64,91,255,0.4)] cursor-pointer"
                >
                  Send
                </button>
              </form>
            </div>
          )}
        </main>
      </div>

      {/* NEW DATABASE MODAL */}
      {showNewDbModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/80 backdrop-blur-sm">
          <div className="w-full max-w-md bg-[#191919] rounded-[30px] border border-[#414042] p-6 sm:p-7 shadow-[0_0_40px_rgba(64,91,255,0.3)]">
            <div className="flex items-center justify-between pb-4 border-b border-[#414042] mb-5">
              <h3 className="text-[17px] font-medium text-white">Provision New Database Instance</h3>
              <button
                onClick={() => setShowNewDbModal(false)}
                className="text-[#6d6e71] hover:text-white text-lg cursor-pointer"
              >
                ✕
              </button>
            </div>

            <form onSubmit={handleCreateDatabase} className="space-y-4">
              <div>
                <label className="block text-[11px] font-semibold text-[#a7a9ac] uppercase mb-1">
                  Database Instance Name
                </label>
                <input
                  type="text"
                  required
                  placeholder="e.g. beta-staging or analytics-db"
                  value={newDbName}
                  onChange={(e) => setNewDbName(e.target.value)}
                  className="w-full bg-[#0e0e0e] border border-[#58595b] px-3.5 py-2.5 rounded-[10px] text-[13px] font-mono text-white focus:outline-none focus:border-[#405bff]"
                />
              </div>

              <div>
                <label className="block text-[11px] font-semibold text-[#a7a9ac] uppercase mb-1">
                  Cloud Node Region
                </label>
                <input
                  type="text"
                  disabled
                  value={newDbRegion}
                  className="w-full bg-[#0e0e0e]/50 border border-[#414042] px-3.5 py-2.5 rounded-[10px] text-[13px] font-mono text-[#7084ff]"
                />
                <span className="text-[11px] text-[#6d6e71] mt-1 block">
                  Region ind-tbn-1: Intel i5-9600, 16GB DDR4, NVIDIA GTX 1660 Ti, 480GB NVMe SSD.
                </span>
              </div>

              <div className="pt-3 border-t border-[#414042] flex items-center justify-end gap-2.5">
                <button
                  type="button"
                  onClick={() => setShowNewDbModal(false)}
                  className="px-4 py-2 rounded-[30px] border border-[#414042] text-[13px] text-[#a7a9ac] hover:text-white cursor-pointer"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  className="px-5 py-2 rounded-[30px] bg-[#405bff] hover:bg-[#344bd6] text-white text-[13px] font-medium shadow-[0_0_15px_rgba(64,91,255,0.4)] transition-all cursor-pointer"
                >
                  Provision Database &rarr;
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}

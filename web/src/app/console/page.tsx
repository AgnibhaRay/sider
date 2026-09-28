"use client";

import React, { useState, useEffect, useRef } from "react";
import Link from "next/link";
import { SiderLogo } from "@/components/SiderLogo";

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

// Default instance on the home desktop host
const DEFAULT_INSTANCE: DatabaseInstance = {
  id: "sdr-db-fe7b9912",
  name: "alpha-production",
  region: "Home Desktop (i5-9600 • Arch Linux)",
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
  const [supervisorHost, setSupervisorHost] = useState<string>("http://100.95.206.7:8080");
  const [isDemoMode, setIsDemoMode] = useState<boolean>(false);
  const [databases, setDatabases] = useState<DatabaseInstance[]>([DEFAULT_INSTANCE]);
  const [selectedDb, setSelectedDb] = useState<DatabaseInstance>(DEFAULT_INSTANCE);
  const [activeTab, setActiveTab] = useState<"overview" | "metrics" | "browser" | "monitor" | "cli">("overview");
  const [activeLang, setActiveLang] = useState<LanguageId>("python");
  
  const [loading, setLoading] = useState<boolean>(false);
  const [supervisorOnline, setSupervisorOnline] = useState<boolean | null>(null);
  const [showNewDbModal, setShowNewDbModal] = useState<boolean>(false);
  const [newDbName, setNewDbName] = useState<string>("");
  const [newDbRegion, setNewDbRegion] = useState<string>("Home Desktop (i5-9600 • Arch Linux)");

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
    ops_per_sec: 0,
    connected_clients: 1,
    status: "online",
    version: "2.1.0"
  });

  const [copiedKey, setCopiedKey] = useState<string | null>(null);

  const copyToClipboard = (text: string, id: string) => {
    navigator.clipboard.writeText(text);
    setCopiedKey(id);
    setTimeout(() => setCopiedKey(null), 2000);
  };

  // 1. Fetch Databases from Supervisor
  const fetchDatabases = async () => {
    if (isDemoMode) return;
    try {
      setLoading(true);
      const res = await fetch(`${supervisorHost}/api/databases`, { method: "GET" });
      if (res.ok) {
        const data = await res.json();
        setSupervisorOnline(true);
        if (data.databases && data.databases.length > 0) {
          setDatabases(data.databases);
          if (!selectedDb || !data.databases.find((d: DatabaseInstance) => d.id === selectedDb.id)) {
            setSelectedDb(data.databases[0]);
          }
        }
      } else {
        setSupervisorOnline(false);
      }
    } catch {
      setSupervisorOnline(false);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchDatabases();
    const interval = setInterval(fetchDatabases, 6000);
    return () => clearInterval(interval);
  }, [supervisorHost, isDemoMode]);

  // 2. Fetch Keys for Active Instance
  const fetchKeys = async () => {
    if (isDemoMode) {
      setKeys([
        { key: "user:1001", value: '{"name": "Agnibha", "role": "admin"}', ttl: -1, tier: "memtable" },
        { key: "users:session:9a7f", value: "active_user_sess_xyz", ttl: 245, tier: "memtable" },
        { key: "cache:products:featured", value: '["prod_1", "prod_2", "prod_9"]', ttl: 1180, tier: "sstable" },
        { key: "analytics:pageviews:home", value: "19402", ttl: -1, tier: "sstable" },
        { key: "config:rate_limit:burst", value: "500", ttl: -1, tier: "memtable" }
      ]);
      return;
    }

    try {
      const targetUrl = `${selectedDb.http_endpoint}/api/keys?q=${encodeURIComponent(searchQuery)}`;
      const res = await fetch(targetUrl);
      if (res.ok) {
        const data = await res.json();
        setKeys(data.keys || []);
      }
    } catch {
      // Offline fallback
    }
  };

  // 3. Fetch Stats for Active Instance
  const fetchStats = async () => {
    if (isDemoMode) {
      setStats((prev) => ({
        ...prev,
        ops_per_sec: Math.floor(Math.random() * 220) + 120,
        total_ops: (prev.total_ops || 1000) + 3
      }));
      return;
    }
    try {
      const res = await fetch(`${selectedDb.http_endpoint}/api/stats`);
      if (res.ok) {
        const data = await res.json();
        setStats(data);
      }
    } catch {
      // Offline fallback
    }
  };

  useEffect(() => {
    fetchKeys();
    fetchStats();
    const t = setInterval(() => {
      fetchKeys();
      fetchStats();
    }, 4000);
    return () => clearInterval(t);
  }, [selectedDb, searchQuery, isDemoMode]);

  // 4. Live Command Monitor (SSE Stream or Simulation)
  useEffect(() => {
    if (isDemoMode) {
      const commands = ["PUT", "GET", "PUTEX", "PUBLISH", "TTL", "DEL"];
      const keysSample = ["user:1001", "session:tok_8a", "cache:hero", "order:991", "telemetry:ping"];
      const interval = setInterval(() => {
        if (isMonitorPaused) return;
        const cmd = commands[Math.floor(Math.random() * commands.length)];
        const k = keysSample[Math.floor(Math.random() * keysSample.length)];
        const newEvt: OperationEvent = {
          timestamp: Date.now(),
          client_addr: "100.95.206.7:" + (49152 + Math.floor(Math.random() * 1000)),
          command: cmd,
          key: k,
          latency_us: Math.floor(Math.random() * 190) + 35,
          status: "OK",
          storage_tier: Math.random() > 0.35 ? "memtable" : "sstable"
        };
        setEvents((prev) => [...prev.slice(-150), newEvt]);
      }, 1400);
      return () => clearInterval(interval);
    }

    let eventSource: EventSource | null = null;
    try {
      eventSource = new EventSource(`${selectedDb.http_endpoint}/api/events`);
      eventSource.onmessage = (e) => {
        if (isMonitorPaused) return;
        try {
          const parsed = JSON.parse(e.data);
          if (parsed.command) {
            setEvents((prev) => [...prev.slice(-150), parsed]);
          }
        } catch {}
      };
    } catch {}

    return () => {
      if (eventSource) eventSource.close();
    };
  }, [selectedDb, isDemoMode, isMonitorPaused]);

  useEffect(() => {
    if (activeTab === "monitor" && !isMonitorPaused) {
      monitorEndRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [events, activeTab, isMonitorPaused]);

  useEffect(() => {
    cliBottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [cliHistory]);

  // Create Database Handler
  const handleCreateDatabase = async (e: React.FormEvent) => {
    e.preventDefault();
    if (isDemoMode) {
      const created: DatabaseInstance = {
        id: "sdr-db-" + Math.random().toString(36).substring(2, 8),
        name: newDbName || "new-database",
        region: newDbRegion,
        tcp_port: 4100 + databases.length,
        http_port: 5100 + databases.length,
        token: "sdr_live_" + Math.random().toString(36).substring(2, 18),
        status: "running",
        connection_uri: `sider://default:sdr_live_demo@100.95.206.7:${4100 + databases.length}`,
        http_endpoint: `http://100.95.206.7:${5100 + databases.length}`,
        created_at: new Date().toISOString()
      };
      setDatabases((prev) => [...prev, created]);
      setSelectedDb(created);
      setShowNewDbModal(false);
      setNewDbName("");
      return;
    }

    try {
      const res = await fetch(`${supervisorHost}/api/databases`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: newDbName, region: newDbRegion })
      });
      if (res.ok) {
        const created = await res.json();
        setDatabases((prev) => [...prev, created]);
        setSelectedDb(created);
        setShowNewDbModal(false);
        setNewDbName("");
      }
    } catch (err) {
      alert("Error creating database: " + err);
    }
  };

  // Execute Web CLI Command
  const handleCliSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!cliInput.trim()) return;
    const cmd = cliInput.trim();
    setCliInput("");
    const startTime = performance.now();

    if (isDemoMode) {
      let resp = "OK";
      if (cmd.startsWith("GET")) resp = '"demo_value_payload"';
      else if (cmd.startsWith("INFO")) resp = "# Sider Server v2.1.0\r\nstatus:online\r\nmemtable_entries:14\r\nengine:LSM-Tree";
      else if (cmd.startsWith("PING")) resp = "PONG";
      else if (cmd.startsWith("TTL")) resp = "240";
      const timeMs = (performance.now() - startTime).toFixed(2) + "ms";
      setCliHistory((prev) => [...prev, { cmd, resp, time: timeMs }]);
      return;
    }

    try {
      const res = await fetch(`${selectedDb.http_endpoint}/api/exec`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ command: cmd, token: selectedDb.token })
      });
      const data = await res.json();
      const timeMs = (performance.now() - startTime).toFixed(2) + "ms";
      setCliHistory((prev) => [...prev, { cmd, resp: data.response, time: timeMs }]);
      fetchKeys();
      fetchStats();
    } catch {
      const timeMs = (performance.now() - startTime).toFixed(2) + "ms";
      setCliHistory((prev) => [...prev, { cmd, resp: "ERR connection failed to instance HTTP gateway", time: timeMs }]);
    }
  };

  // Add Key Action
  const handleAddKey = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newKey || !newVal) return;

    let cmd = `PUT ${newKey} ${newVal}`;
    if (newTTL && parseInt(newTTL) > 0) {
      cmd = `PUTEX ${newKey} ${newTTL} ${newVal}`;
    }

    if (isDemoMode) {
      setKeys((prev) => [
        { key: newKey, value: newVal, ttl: newTTL ? parseInt(newTTL) : -1, tier: "memtable" },
        ...prev
      ]);
      setNewKey("");
      setNewVal("");
      setNewTTL("");
      return;
    }

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
  };

  // Delete Key Action
  const handleDeleteKey = async (key: string) => {
    if (isDemoMode) {
      setKeys((prev) => prev.filter((k) => k.key !== key));
      return;
    }
    await fetch(`${selectedDb.http_endpoint}/api/exec`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ command: `DEL ${key}`, token: selectedDb.token })
    });
    fetchKeys();
    fetchStats();
  };

  // Filtered keys
  const filteredKeys = keys.filter((k) => {
    if (keyFilterTier !== "ALL" && k.tier !== keyFilterTier) return false;
    if (searchQuery && !k.key.toLowerCase().includes(searchQuery.toLowerCase())) return false;
    return true;
  });

  // Filtered events
  const filteredEvents = events.filter((e) => {
    if (filterCmd !== "ALL" && e.command !== filterCmd) return false;
    return true;
  });

  // Code snippet generators
  const getSnippet = (lang: LanguageId): string => {
    const host = "100.95.206.7";
    const port = selectedDb.tcp_port;
    const token = selectedDb.token;
    const httpPort = selectedDb.http_port;

    switch (lang) {
      case "python":
        return `import socket

class SiderClient:
    def __init__(self, host="${host}", port=${port}, token="${token}"):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))
        if token:
            self._send(f"AUTH {token}")

    def _send(self, cmd: str) -> str:
        self.sock.sendall((cmd + "\\n").encode("utf-8"))
        return self.sock.recv(4096).decode("utf-8").strip()

    def set(self, key: str, value: str) -> str:
        return self._send(f"PUT {key} {value}")

    def get(self, key: str) -> str:
        return self._send(f"GET {key}")

# Usage:
db = SiderClient()
db.set("user:101", '{"name":"Alice","role":"admin"}')
print("Value:", db.get("user:101"))`;

      case "node":
        return `import net from "node:net";

class SiderClient {
  private client = new net.Socket();

  async connect(host = "${host}", port = ${port}, token = "${token}"): Promise<void> {
    return new Promise((resolve) => {
      this.client.connect(port, host, () => {
        this.command(\`AUTH \${token}\`).then(() => resolve());
      });
    });
  }

  command(cmd: string): Promise<string> {
    return new Promise((resolve) => {
      this.client.once("data", (data) => resolve(data.toString().trim()));
      this.client.write(\`\${cmd}\\n\`);
    });
  }
}

// Usage:
const db = new SiderClient();
await db.connect();
await db.command('PUT user:101 "Alice"');
console.log(await db.command('GET user:101'));`;

      case "go":
        return `package main

import (
\t"bufio"
\t"fmt"
\t"net"
)

func main() {
\tconn, err := net.Dial("tcp", "${host}:${port}")
\tif err != nil {
\t\tpanic(err)
\t}
\tdefer conn.Close()

\treader := bufio.NewReader(conn)

\t// 1. Authenticate with instance token
\tfmt.Fprintf(conn, "AUTH ${token}\\n")
\tresp, _ := reader.ReadString('\\n')
\tfmt.Println("Auth:", resp)

\t// 2. Set key-value
\tfmt.Fprintf(conn, "PUT cluster:status healthy\\n")
\tresp, _ = reader.ReadString('\\n')

\t// 3. Query key
\tfmt.Fprintf(conn, "GET cluster:status\\n")
\tval, _ := reader.ReadString('\\n')
\tfmt.Println("Status:", val)
}`;

      case "rust":
        return `use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;

fn main() -> std::io::Result<()> {
    let mut stream = TcpStream::connect("${host}:${port}")?;
    let mut reader = BufReader::new(stream.try_clone()?);

    // 1. Authenticate
    writeln!(stream, "AUTH {}", "${token}")?;
    let mut auth_resp = String::new();
    reader.read_line(&mut auth_resp)?;

    // 2. Put key
    writeln!(stream, "PUT metrics:latency 42us")?;
    let mut put_resp = String::new();
    reader.read_line(&mut put_resp)?;

    // 3. Get key
    writeln!(stream, "GET metrics:latency")?;
    let mut get_resp = String::new();
    reader.read_line(&mut get_resp)?;
    println!("Response: {}", get_resp.trim());

    Ok(())
}`;

      case "java":
        return `import java.io.*;
import java.net.Socket;

public class SiderQuickstart {
    public static void main(String[] args) throws Exception {
        try (Socket socket = new Socket("${host}", ${port});
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // 1. Authenticate
            out.println("AUTH ${token}");
            System.out.println("Auth: " + in.readLine());

            // 2. Write key
            out.println("PUT order:2049 '{\\"items\\": 4}'");
            System.out.println("Put: " + in.readLine());

            // 3. Read key
            out.println("GET order:2049");
            System.out.println("Value: " + in.readLine());
        }
    }
}`;

      case "csharp":
        return `using System;
using System.IO;
using System.Net.Sockets;

class SiderClient {
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
      className="min-h-screen text-[#1b1b1b] flex flex-col font-sans select-none antialiased"
      style={{ backgroundColor: "#eaeaea", fontFamily: "var(--font-inter), sans-serif" }}
    >
      {/* 1. TOP ANNOUNCEMENT BAR (Grafbase Forest-to-Deep-Teal Gradient) */}
      <div
        className="w-full h-10 px-4 text-white text-[13px] font-medium flex items-center justify-center gap-2 shadow-sm z-50 sticky top-0"
        style={{
          background: "linear-gradient(89.97deg, rgb(25, 160, 95) 0.02%, rgb(13, 127, 140) 123.85%)"
        }}
      >
        <span>
          <strong>Sider Cloud v2.1.0</strong>: Zero-dependency LSM storage with native SkipList MemTable, WAL durability & SSTable tiering.
        </span>
        <Link href="/" className="underline hover:opacity-85 font-medium ml-1">
          Explore Architecture &rarr;
        </Link>
      </div>

      {/* 2. CLINICAL MARBLE NAVIGATION BAR */}
      <header className="w-full bg-[#ffffff] border-b border-[#e0e1e6] px-6 h-16 flex items-center justify-between sticky top-10 z-40">
        <div className="flex items-center gap-6">
          <Link href="/" className="flex items-center gap-3 group">
            <SiderLogo size={28} />
            <div className="flex flex-col">
              <span
                className="text-[15px] font-semibold text-[#1b1b1b] tracking-[-0.03em]"
                style={{ letterSpacing: "-0.5px" }}
              >
                Sider Cloud
              </span>
              <span className="text-[11px] text-[#60646c] font-normal leading-tight">
                LSM-Tree Console
              </span>
            </div>
          </Link>

          <div className="hidden lg:flex items-center gap-2 pl-4 border-l border-[#e0e1e6]">
            <span className="text-[12px] text-[#60646c]">Control Plane:</span>
            <span className="text-[12px] font-mono text-[#1b1b1b] bg-[#eaeaea] px-2 py-0.5 rounded-[4px] border border-[#e0e1e6]">
              {supervisorHost}
            </span>
            <div className="flex items-center gap-1.5 ml-1">
              <span
                className={`w-2 h-2 rounded-full ${
                  isDemoMode ? "bg-[#eab308]" : supervisorOnline ? "bg-[#0d7f8c]" : "bg-[#ef4444]"
                }`}
              />
              <span className="text-[11px] text-[#7c7c7c]">
                {isDemoMode ? "Sandbox" : supervisorOnline ? "Live" : "Connecting"}
              </span>
            </div>
          </div>
        </div>

        {/* Right Nav Actions */}
        <div className="flex items-center gap-3">
          <button
            onClick={() => setIsDemoMode((prev) => !prev)}
            className="px-3.5 py-1.5 rounded-[40px] text-[13px] font-medium border border-[#e0e1e6] bg-[#ffffff] text-[#1b1b1b] hover:bg-[#eaeaea]/50 transition-colors"
          >
            {isDemoMode ? "🟡 Sandbox Simulated" : "🟢 Desktop Host (Tailscale)"}
          </button>

          <button
            onClick={() => setShowNewDbModal(true)}
            className="px-4 py-2 rounded-[6px] text-[13px] font-medium text-white bg-[#1b1b1b] hover:opacity-90 transition-all shadow-[0px_4px_20px_0px_rgba(0,0,0,0.15)] flex items-center gap-1.5"
          >
            <span className="text-base leading-none">+</span>
            <span>New Database</span>
          </button>
        </div>
      </header>

      {/* 3. MAIN WORKSPACE (Max 1280px Blueprint Container) */}
      <div className="flex-1 w-full max-w-[1360px] mx-auto p-4 sm:p-6 lg:p-8 flex flex-col lg:flex-row gap-6">
        
        {/* LEFT COLUMN: ARCHITECTURAL INSTANCE NAV (280px) */}
        <aside className="w-full lg:w-72 flex flex-col gap-4 shrink-0">
          <div className="bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] p-5 shadow-sm">
            <div className="flex items-center justify-between mb-3">
              <span className="text-[12px] font-semibold text-[#7c7c7c] uppercase tracking-wider">
                Instances ({databases.length})
              </span>
              <button
                onClick={fetchDatabases}
                className="text-[11px] text-[#60646c] hover:text-[#1b1b1b] underline"
              >
                Refresh
              </button>
            </div>

            <div className="space-y-2">
              {databases.map((db) => {
                const isSelected = selectedDb?.id === db.id;
                return (
                  <div
                    key={db.id}
                    onClick={() => setSelectedDb(db)}
                    className={`p-3.5 rounded-[12px] border cursor-pointer transition-all ${
                      isSelected
                        ? "bg-[#eaeaea]/60 border-[#1b1b1b] shadow-sm"
                        : "bg-[#ffffff] border-[#e0e1e6] hover:border-[#b0b3ba]"
                    }`}
                  >
                    <div className="flex items-center justify-between">
                      <span className="text-[14px] font-semibold text-[#1b1b1b]">
                        {db.name}
                      </span>
                      <span className="w-2 h-2 rounded-full bg-[#19a05f]" />
                    </div>
                    <div className="mt-1 flex items-center justify-between text-[11px] font-mono text-[#60646c]">
                      <span>PORT :{db.tcp_port}</span>
                      <span className="text-[10px] text-[#7c7c7c]">{db.id.slice(0, 10)}</span>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* HARDWARE BLUEPRINT CARD */}
          <div className="bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] p-5 text-[12px] space-y-2.5 shadow-sm">
            <div className="text-[11px] font-semibold text-[#7c7c7c] uppercase tracking-wider">
              Host Environment
            </div>
            <div className="flex items-center justify-between pt-1">
              <span className="text-[#60646c]">Host Machine</span>
              <span className="font-medium text-[#1b1b1b]">agnirockz</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[#60646c]">Processor</span>
              <span className="font-mono text-[#1b1b1b]">Intel i5-9600</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[#60646c]">Memory</span>
              <span className="font-mono text-[#1b1b1b]">16GB DDR4</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[#60646c]">Operating System</span>
              <span className="font-mono text-[#1b1b1b]">Arch Linux</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[#60646c]">Tailscale IP</span>
              <span className="font-mono text-[#0d7f8c] font-medium">100.95.206.7</span>
            </div>
            <div className="pt-2 border-t border-[#e0e1e6] flex items-center justify-between text-[11px]">
              <span className="text-[#7c7c7c]">Storage Engine</span>
              <span className="text-[#19a05f] font-medium">SkipList + LSM</span>
            </div>
          </div>
        </aside>

        {/* RIGHT COLUMN: ACTIVE DATABASE BLUEPRINT WORKSPACE */}
        <main className="flex-1 flex flex-col gap-6 min-w-0">
          
          {/* HEADER HERO CARD */}
          <div className="bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] p-6 lg:p-7 shadow-sm">
            <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 pb-6 border-b border-[#e0e1e6]">
              <div>
                <div className="flex items-center gap-3">
                  <h1
                    className="text-[28px] sm:text-[36px] font-semibold text-[#1b1b1b]"
                    style={{ letterSpacing: "-1px" }}
                  >
                    {selectedDb.name}
                  </h1>
                  <span className="px-2.5 py-0.5 rounded-[40px] text-[11px] font-medium bg-[#19a05f]/15 text-[#19a05f] border border-[#19a05f]/30">
                    ONLINE
                  </span>
                </div>
                <p className="text-[14px] text-[#60646c] mt-1">
                  Single-tenant LSM instance isolated with dedicated WAL log and SSTable storage level.
                </p>
              </div>

              {/* Quick Specs Badges */}
              <div className="flex flex-wrap items-center gap-2 font-mono text-[12px]">
                <div className="bg-[#eaeaea] px-3 py-1.5 rounded-[6px] border border-[#e0e1e6] flex items-center gap-1.5">
                  <span className="text-[#7c7c7c]">TCP:</span>
                  <strong className="text-[#1b1b1b]">:{selectedDb.tcp_port}</strong>
                </div>
                <div className="bg-[#eaeaea] px-3 py-1.5 rounded-[6px] border border-[#e0e1e6] flex items-center gap-1.5">
                  <span className="text-[#7c7c7c]">HTTP:</span>
                  <strong className="text-[#0d7f8c]">:{selectedDb.http_port}</strong>
                </div>
              </div>
            </div>

            {/* TAB SELECTOR (Clinical Segmented Control) */}
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
                  className={`px-4 py-2 rounded-[6px] text-[13px] font-medium transition-all ${
                    activeTab === tab.id
                      ? "bg-[#1b1b1b] text-white shadow-sm"
                      : "bg-[#eaeaea]/60 text-[#60646c] hover:text-[#1b1b1b] hover:bg-[#eaeaea]"
                  }`}
                >
                  {tab.label}
                </button>
              ))}
            </div>
          </div>

          {/* TAB 1: OVERVIEW & MULTI-LANGUAGE SDK DRIVERS */}
          {activeTab === "overview" && (
            <div className="space-y-6">
              
              {/* Connection Blueprint Card */}
              <div className="bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] p-6 shadow-sm">
                <span className="text-[12px] font-semibold text-[#7c7c7c] uppercase tracking-wider">
                  Connection Endpoint (URI)
                </span>
                <div className="mt-2.5 flex items-center justify-between gap-3 bg-[#eaeaea]/60 p-3.5 rounded-[8px] border border-[#e0e1e6]">
                  <code className="text-[#1b1b1b] font-mono text-[13px] truncate font-medium">
                    {selectedDb.connection_uri}
                  </code>
                  <button
                    onClick={() => copyToClipboard(selectedDb.connection_uri, "uri")}
                    className="px-3.5 py-1.5 rounded-[6px] bg-[#ffffff] border border-[#e0e1e6] text-[12px] font-medium text-[#1b1b1b] hover:bg-[#eaeaea] shrink-0"
                  >
                    {copiedKey === "uri" ? "✓ Copied" : "Copy URI"}
                  </button>
                </div>

                <div className="grid grid-cols-1 sm:grid-cols-4 gap-4 mt-5 pt-5 border-t border-[#e0e1e6] text-[13px]">
                  <div>
                    <span className="text-[11px] text-[#7c7c7c] uppercase font-semibold">Host</span>
                    <div className="font-mono text-[#1b1b1b] mt-0.5">100.95.206.7</div>
                  </div>
                  <div>
                    <span className="text-[11px] text-[#7c7c7c] uppercase font-semibold">TCP Port</span>
                    <div className="font-mono text-[#1b1b1b] mt-0.5">{selectedDb.tcp_port}</div>
                  </div>
                  <div>
                    <span className="text-[11px] text-[#7c7c7c] uppercase font-semibold">HTTP Gateway</span>
                    <div className="font-mono text-[#0d7f8c] mt-0.5">:{selectedDb.http_port}</div>
                  </div>
                  <div>
                    <span className="text-[11px] text-[#7c7c7c] uppercase font-semibold">Auth Token</span>
                    <div className="font-mono text-[#1b1b1b] mt-0.5 truncate">{selectedDb.token}</div>
                  </div>
                </div>
              </div>

              {/* MULTI-LANGUAGE SDK DRIVERS CARD */}
              <div className="bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] p-6 shadow-sm">
                <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 mb-4">
                  <div>
                    <h2 className="text-[18px] font-semibold text-[#1b1b1b]" style={{ letterSpacing: "-0.5px" }}>
                      Production Driver Quickstart
                    </h2>
                    <p className="text-[13px] text-[#60646c]">
                      Plug-and-play drivers with native protocol formatting and automatic token authentication.
                    </p>
                  </div>
                  <button
                    onClick={() => copyToClipboard(getSnippet(activeLang), "snippet")}
                    className="px-3.5 py-1.5 rounded-[6px] bg-[#1b1b1b] text-white text-[12px] font-medium hover:opacity-90 transition-opacity shrink-0"
                  >
                    {copiedKey === "snippet" ? "✓ Copied Code" : "Copy Snippet"}
                  </button>
                </div>

                {/* Language Switcher Tabs */}
                <div className="flex flex-wrap gap-1.5 p-1 bg-[#eaeaea] rounded-[8px] border border-[#e0e1e6] mb-4">
                  {[
                    { id: "python", label: "Python" },
                    { id: "node", label: "TypeScript / Node" },
                    { id: "go", label: "Go" },
                    { id: "rust", label: "Rust" },
                    { id: "java", label: "Java (Spring)" },
                    { id: "csharp", label: "C# (.NET)" },
                    { id: "curl", label: "cURL / REST" },
                    { id: "netcat", label: "Netcat / Bash" }
                  ].map((lang) => (
                    <button
                      key={lang.id}
                      onClick={() => setActiveLang(lang.id as LanguageId)}
                      className={`px-3 py-1.5 rounded-[6px] text-[12px] font-medium transition-all ${
                        activeLang === lang.id
                          ? "bg-[#ffffff] text-[#1b1b1b] shadow-sm font-semibold"
                          : "text-[#60646c] hover:text-[#1b1b1b]"
                      }`}
                    >
                      {lang.label}
                    </button>
                  ))}
                </div>

                {/* Code Viewer */}
                <div className="relative rounded-[12px] border border-[#e0e1e6] bg-[#1b1b1b] p-4 text-[#eaeaea] font-mono text-[12.5px] overflow-x-auto leading-relaxed">
                  <pre>
                    <code>{getSnippet(activeLang)}</code>
                  </pre>
                </div>
              </div>
            </div>
          )}

          {/* TAB 2: LSM METRICS & STORAGE TELEMETRY */}
          {activeTab === "metrics" && (
            <div className="space-y-6">
              
              {/* Telemetry Metric Cards */}
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                <div className="bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] p-5 shadow-sm">
                  <span className="text-[11px] font-semibold text-[#7c7c7c] uppercase tracking-wider">
                    MemTable RAM Keys
                  </span>
                  <div className="text-[32px] font-semibold text-[#1b1b1b] mt-1" style={{ letterSpacing: "-1px" }}>
                    {stats.memtable_entries || 0}
                  </div>
                  <div className="text-[12px] text-[#19a05f] font-medium mt-1">
                    SkipList $O(\log N)$ in Memory
                  </div>
                </div>

                <div className="bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] p-5 shadow-sm">
                  <span className="text-[11px] font-semibold text-[#7c7c7c] uppercase tracking-wider">
                    Immutable SSTables
                  </span>
                  <div className="text-[32px] font-semibold text-[#1b1b1b] mt-1" style={{ letterSpacing: "-1px" }}>
                    {stats.sstables_count || 0} <span className="text-[16px] text-[#7c7c7c] font-normal">files</span>
                  </div>
                  <div className="text-[12px] text-[#0d7f8c] font-medium mt-1">
                    Sorted Disk String Tables
                  </div>
                </div>

                <div className="bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] p-5 shadow-sm">
                  <span className="text-[11px] font-semibold text-[#7c7c7c] uppercase tracking-wider">
                    Write-Ahead Log (WAL)
                  </span>
                  <div className="text-[32px] font-semibold text-[#1b1b1b] mt-1" style={{ letterSpacing: "-1px" }}>
                    {stats.wal_bytes || 0} <span className="text-[16px] text-[#7c7c7c] font-normal">bytes</span>
                  </div>
                  <div className="text-[12px] text-[#60646c] font-medium mt-1">
                    Zero Data-Loss Durability
                  </div>
                </div>

                <div className="bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] p-5 shadow-sm">
                  <span className="text-[11px] font-semibold text-[#7c7c7c] uppercase tracking-wider">
                    Total Operations
                  </span>
                  <div className="text-[32px] font-semibold text-[#1b1b1b] mt-1" style={{ letterSpacing: "-1px" }}>
                    {stats.total_ops || 0}
                  </div>
                  <div className="text-[12px] text-[#19a05f] font-medium mt-1">
                    Sub-millisecond writes
                  </div>
                </div>
              </div>

              {/* Informative Architectural Breakdown */}
              <div className="bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] p-6 shadow-sm space-y-4">
                <h3 className="text-[16px] font-semibold text-[#1b1b1b]">
                  LSM Storage Engine Architecture
                </h3>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4 pt-2">
                  <div className="p-4 rounded-[12px] border border-[#e0e1e6] bg-[#eaeaea]/40">
                    <div className="text-[13px] font-semibold text-[#1b1b1b] flex items-center justify-between">
                      <span>1. Write-Ahead Log</span>
                      <span className="text-[10px] font-mono bg-[#ffffff] px-1.5 py-0.5 rounded border border-[#e0e1e6]">Disk</span>
                    </div>
                    <p className="text-[12px] text-[#60646c] mt-2 leading-relaxed">
                      Every incoming write (`PUT`, `PUTEX`, `DEL`) is appended sequentially to disk with an CRC32 checksum before memory insertion.
                    </p>
                  </div>

                  <div className="p-4 rounded-[12px] border border-[#e0e1e6] bg-[#eaeaea]/40">
                    <div className="text-[13px] font-semibold text-[#1b1b1b] flex items-center justify-between">
                      <span>2. Active MemTable</span>
                      <span className="text-[10px] font-mono bg-[#19a05f]/20 text-[#19a05f] px-1.5 py-0.5 rounded">RAM</span>
                    </div>
                    <p className="text-[12px] text-[#60646c] mt-2 leading-relaxed">
                      Lock-free concurrent SkipList maintaining lexicographically sorted keys. When capacity reaches threshold ({stats.memtable_limit || 100} keys), flush occurs.
                    </p>
                  </div>

                  <div className="p-4 rounded-[12px] border border-[#e0e1e6] bg-[#eaeaea]/40">
                    <div className="text-[13px] font-semibold text-[#1b1b1b] flex items-center justify-between">
                      <span>3. SSTable + Bloom Filter</span>
                      <span className="text-[10px] font-mono bg-[#0d7f8c]/20 text-[#0d7f8c] px-1.5 py-0.5 rounded">NVMe</span>
                    </div>
                    <p className="text-[12px] text-[#60646c] mt-2 leading-relaxed">
                      Flushed files are written with sparse indexes and Murmur3 Bloom Filters, preventing unnecessary disk I/O on misses.
                    </p>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* TAB 3: DATA BROWSER / KEY EXPLORER */}
          {activeTab === "browser" && (
            <div className="space-y-6">
              
              {/* Insert / Update Key Form */}
              <form
                onSubmit={handleAddKey}
                className="bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] p-5 shadow-sm flex flex-col sm:flex-row items-center gap-3"
              >
                <input
                  type="text"
                  placeholder="Key (e.g. user:profile:102)"
                  value={newKey}
                  onChange={(e) => setNewKey(e.target.value)}
                  className="flex-1 bg-[#eaeaea]/60 border border-[#e0e1e6] px-3.5 py-2 rounded-[6px] font-mono text-[13px] text-[#1b1b1b] focus:outline-none focus:border-[#1b1b1b]"
                  required
                />
                <input
                  type="text"
                  placeholder="Value string or JSON payload"
                  value={newVal}
                  onChange={(e) => setNewVal(e.target.value)}
                  className="flex-1 bg-[#eaeaea]/60 border border-[#e0e1e6] px-3.5 py-2 rounded-[6px] font-mono text-[13px] text-[#1b1b1b] focus:outline-none focus:border-[#1b1b1b]"
                  required
                />
                <input
                  type="number"
                  placeholder="TTL (sec)"
                  value={newTTL}
                  onChange={(e) => setNewTTL(e.target.value)}
                  className="w-28 bg-[#eaeaea]/60 border border-[#e0e1e6] px-3.5 py-2 rounded-[6px] font-mono text-[13px] text-[#1b1b1b] focus:outline-none focus:border-[#1b1b1b]"
                />
                <button
                  type="submit"
                  className="px-5 py-2 rounded-[6px] bg-[#1b1b1b] text-white font-medium text-[13px] hover:opacity-90 shadow-sm shrink-0"
                >
                  Set Key
                </button>
              </form>

              {/* Stored Keys Table */}
              <div className="bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] shadow-sm overflow-hidden">
                <div className="p-4 border-b border-[#e0e1e6] flex flex-col sm:flex-row sm:items-center justify-between gap-3">
                  <div className="flex items-center gap-2">
                    <span className="text-[13px] font-semibold text-[#1b1b1b]">
                      Stored Keys ({filteredKeys.length})
                    </span>
                    <div className="flex items-center gap-1 ml-3">
                      {(["ALL", "memtable", "sstable"] as const).map((tier) => (
                        <button
                          key={tier}
                          onClick={() => setKeyFilterTier(tier)}
                          className={`px-2 py-0.5 rounded text-[11px] font-medium border ${
                            keyFilterTier === tier
                              ? "bg-[#1b1b1b] text-white border-[#1b1b1b]"
                              : "bg-[#eaeaea] text-[#60646c] border-[#e0e1e6]"
                          }`}
                        >
                          {tier === "ALL" ? "All Tiers" : tier === "memtable" ? "RAM (MemTable)" : "Disk (SSTable)"}
                        </button>
                      ))}
                    </div>
                  </div>

                  <input
                    type="text"
                    placeholder="Filter keys..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="bg-[#eaeaea]/60 border border-[#e0e1e6] px-3 py-1 rounded-[6px] text-[12px] font-mono text-[#1b1b1b] w-48 focus:outline-none focus:border-[#1b1b1b]"
                  />
                </div>

                <div className="divide-y divide-[#e0e1e6]">
                  {filteredKeys.length === 0 ? (
                    <div className="p-12 text-center text-[#7c7c7c] text-[13px]">
                      No keys found matching query. Add keys above or send commands from an SDK!
                    </div>
                  ) : (
                    filteredKeys.map((k) => (
                      <div
                        key={k.key}
                        className="p-4 flex flex-col sm:flex-row sm:items-center justify-between gap-3 hover:bg-[#eaeaea]/30 transition-colors"
                      >
                        <div className="flex items-center gap-3 min-w-0">
                          <span
                            className={`text-[10px] font-mono uppercase px-2 py-0.5 rounded-[4px] font-semibold ${
                              k.tier === "memtable"
                                ? "bg-[#19a05f]/15 text-[#19a05f] border border-[#19a05f]/30"
                                : "bg-[#0d7f8c]/15 text-[#0d7f8c] border border-[#0d7f8c]/30"
                            }`}
                          >
                            {k.tier === "memtable" ? "RAM" : "SSTABLE"}
                          </span>
                          <span className="font-mono text-[13px] font-medium text-[#1b1b1b]">
                            {k.key}
                          </span>
                          <span className="text-[12px] font-mono text-[#60646c] truncate max-w-md">
                            {k.value}
                          </span>
                        </div>

                        <div className="flex items-center gap-4 text-[12px] font-mono shrink-0">
                          <span className="text-[#7c7c7c]">
                            {k.ttl === -1 ? "PERSISTENT" : `TTL: ${k.ttl}s`}
                          </span>
                          <button
                            onClick={() => handleDeleteKey(k.key)}
                            className="text-[#ef4444] hover:underline"
                          >
                            Delete
                          </button>
                        </div>
                      </div>
                    ))
                  )}
                </div>
              </div>
            </div>
          )}

          {/* TAB 4: LIVE MONITOR STREAM */}
          {activeTab === "monitor" && (
            <div className="bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] p-6 shadow-sm flex flex-col h-[580px]">
              <div className="flex flex-wrap items-center justify-between pb-4 border-b border-[#e0e1e6] gap-3">
                <div className="flex items-center gap-3">
                  <div className="w-2.5 h-2.5 rounded-full bg-[#19a05f] animate-pulse" />
                  <span className="text-[14px] font-semibold text-[#1b1b1b]">
                    Live Telemetry Stream ({filteredEvents.length} events)
                  </span>
                </div>

                <div className="flex items-center gap-2">
                  <select
                    value={filterCmd}
                    onChange={(e) => setFilterCmd(e.target.value)}
                    className="bg-[#eaeaea] border border-[#e0e1e6] rounded-[6px] px-2.5 py-1 text-[12px] font-mono text-[#1b1b1b]"
                  >
                    <option value="ALL">All Commands</option>
                    <option value="PUT">PUT</option>
                    <option value="GET">GET</option>
                    <option value="PUTEX">PUTEX</option>
                    <option value="DEL">DEL</option>
                    <option value="TTL">TTL</option>
                  </select>
                  <button
                    onClick={() => setIsMonitorPaused((p) => !p)}
                    className="px-3 py-1 rounded-[6px] border border-[#e0e1e6] bg-[#eaeaea] text-[12px] font-medium text-[#1b1b1b] hover:bg-[#e0e1e6]"
                  >
                    {isMonitorPaused ? "▶ Resume" : "⏸ Pause"}
                  </button>
                  <button
                    onClick={() => setEvents([])}
                    className="px-3 py-1 rounded-[6px] border border-[#e0e1e6] bg-[#eaeaea] text-[12px] font-medium text-[#1b1b1b] hover:bg-[#e0e1e6]"
                  >
                    Clear
                  </button>
                </div>
              </div>

              {/* Streaming Terminal Window */}
              <div className="flex-1 bg-[#1b1b1b] rounded-[12px] p-4 font-mono text-[12px] overflow-y-auto mt-4 space-y-2 text-[#eaeaea]">
                {filteredEvents.length === 0 ? (
                  <div className="text-center py-20 text-[#7c7c7c]">
                    Waiting for real-time operations... Connect an SDK driver or web terminal to view events!
                  </div>
                ) : (
                  filteredEvents.map((evt, idx) => (
                    <div
                      key={idx}
                      className="flex flex-wrap items-center gap-3 py-1 border-b border-[#2d2d38] text-[12px]"
                    >
                      <span className="text-[#7c7c7c]">
                        {new Date(evt.timestamp).toLocaleTimeString()}
                      </span>
                      <span className="text-[#00f2e6] font-semibold">{evt.command}</span>
                      <span className="text-white truncate max-w-xs">{evt.key || "-"}</span>
                      <span className="ml-auto text-[#8dc63f] font-semibold">{evt.latency_us} µs</span>
                      <span
                        className={`text-[10px] px-1.5 py-0.5 rounded ${
                          evt.storage_tier === "memtable"
                            ? "bg-[#19a05f]/20 text-[#8dc63f]"
                            : "bg-[#00b9f1]/20 text-[#00b9f1]"
                        }`}
                      >
                        {evt.storage_tier || "RAM"}
                      </span>
                    </div>
                  ))
                )}
                <div ref={monitorEndRef} />
              </div>
            </div>
          )}

          {/* TAB 5: INTERACTIVE WEB CLI TERMINAL */}
          {activeTab === "cli" && (
            <div className="bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] p-6 shadow-sm flex flex-col h-[580px]">
              <div className="flex items-center justify-between pb-3 border-b border-[#e0e1e6]">
                <span className="text-[14px] font-semibold text-[#1b1b1b]">
                  Interactive Query Shell (Sider REPL)
                </span>
                <span className="text-[12px] font-mono text-[#60646c]">
                  Connected to {selectedDb.name}
                </span>
              </div>

              {/* Shell Display */}
              <div className="flex-1 bg-[#1b1b1b] rounded-[12px] p-4 font-mono text-[13px] flex flex-col justify-between mt-4 overflow-hidden text-[#eaeaea]">
                <div className="space-y-3 overflow-y-auto mb-3 pr-2">
                  {cliHistory.map((item, idx) => (
                    <div key={idx} className="space-y-1">
                      <div className="flex items-center justify-between text-white">
                        <div className="flex items-center gap-2">
                          <span className="text-[#00f2e6]">&gt;</span>
                          <span className="font-semibold">{item.cmd}</span>
                        </div>
                        <span className="text-[10px] text-[#7c7c7c]">{item.time}</span>
                      </div>
                      <div className="text-[#b0b3ba] whitespace-pre-wrap pl-4 text-[12px] font-mono">
                        {item.resp}
                      </div>
                    </div>
                  ))}
                  <div ref={cliBottomRef} />
                </div>

                {/* Input prompt */}
                <form
                  onSubmit={handleCliSubmit}
                  className="flex items-center gap-2 pt-3 border-t border-[#2d2d38]"
                >
                  <span className="text-[#00f2e6] font-bold">&gt;</span>
                  <input
                    type="text"
                    value={cliInput}
                    onChange={(e) => setCliInput(e.target.value)}
                    placeholder="Enter command (e.g. PUT user:1 'John', GET user:1, INFO, TTL user:1)..."
                    className="flex-1 bg-transparent text-white focus:outline-none font-mono text-[13px]"
                    autoFocus
                  />
                  <button
                    type="submit"
                    className="text-[11px] font-mono text-[#7c7c7c] hover:text-white px-2 py-1"
                  >
                    Execute &crarr;
                  </button>
                </form>
              </div>
            </div>
          )}
        </main>
      </div>

      {/* NEW DATABASE MODAL */}
      {showNewDbModal && (
        <div className="fixed inset-0 z-50 bg-[#1b1b1b]/40 backdrop-blur-sm flex items-center justify-center p-4">
          <div className="w-full max-w-md bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] p-6 shadow-2xl">
            <h2 className="text-[20px] font-semibold text-[#1b1b1b] mb-1" style={{ letterSpacing: "-0.5px" }}>
              Provision New Database
            </h2>
            <p className="text-[13px] text-[#60646c] mb-5">
              Spawns an isolated Sider LSM-Tree process with dedicated TCP/HTTP ports and credentials.
            </p>
            <form onSubmit={handleCreateDatabase} className="space-y-4">
              <div>
                <label className="block text-[12px] font-semibold text-[#7c7c7c] uppercase mb-1">
                  Database Name
                </label>
                <input
                  type="text"
                  placeholder="e.g. redis-cache-production"
                  value={newDbName}
                  onChange={(e) => setNewDbName(e.target.value)}
                  className="w-full bg-[#eaeaea]/60 border border-[#e0e1e6] px-3.5 py-2.5 rounded-[6px] text-[13px] font-mono text-[#1b1b1b] focus:outline-none focus:border-[#1b1b1b]"
                  required
                />
              </div>

              <div>
                <label className="block text-[12px] font-semibold text-[#7c7c7c] uppercase mb-1">
                  Target Host & Node Hardware
                </label>
                <select
                  value={newDbRegion}
                  onChange={(e) => setNewDbRegion(e.target.value)}
                  className="w-full bg-[#eaeaea]/60 border border-[#e0e1e6] px-3 py-2 rounded-[6px] text-[13px] text-[#1b1b1b] focus:outline-none"
                >
                  <option value="Home Desktop (i5-9600 • Arch Linux)">
                    Home Desktop (Intel i5-9600 • Arch Linux)
                  </option>
                  <option value="Local Sandbox Instance">Local Developer Sandbox</option>
                </select>
              </div>

              <div className="flex items-center justify-end gap-3 pt-4 border-t border-[#e0e1e6]">
                <button
                  type="button"
                  onClick={() => setShowNewDbModal(false)}
                  className="px-4 py-2 rounded-[6px] text-[13px] text-[#60646c] hover:text-[#1b1b1b]"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  className="px-5 py-2 rounded-[6px] bg-[#1b1b1b] text-white text-[13px] font-medium hover:opacity-90 transition-opacity shadow-[0px_4px_20px_0px_rgba(0,0,0,0.15)]"
                >
                  Provision Database
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}

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
  memtable_entries?: number;
  memtable_bytes?: number;
  sstables_count?: number;
  sstables_bytes?: number;
  wal_bytes?: number;
  total_ops?: number;
  ops_per_sec?: number;
  connected_clients?: number;
}

// Built-in Demo instance for immediate sandbox testing without Tailscale
const DEMO_INSTANCE: DatabaseInstance = {
  id: "sdr-db-demo99",
  name: "demo-production-store",
  region: "Home Desktop (i5-9600 • Arch Linux)",
  tcp_port: 4100,
  http_port: 5100,
  token: "sdr_live_9a8bc43d0e2f1837c7",
  status: "running",
  connection_uri: "sider://default:sdr_live_9a8bc43d0e2f1837c7@100.95.206.7:4100",
  http_endpoint: "http://100.95.206.7:5100",
  created_at: new Date().toISOString()
};

export default function SiderConsolePage() {
  const [supervisorHost, setSupervisorHost] = useState<string>("http://100.95.206.7:8080");
  const [isDemoMode, setIsDemoMode] = useState<boolean>(false);
  const [databases, setDatabases] = useState<DatabaseInstance[]>([DEMO_INSTANCE]);
  const [selectedDb, setSelectedDb] = useState<DatabaseInstance>(DEMO_INSTANCE);
  const [activeTab, setActiveTab] = useState<"overview" | "monitor" | "browser" | "cli" | "metrics">("overview");
  
  const [loading, setLoading] = useState<boolean>(false);
  const [supervisorOnline, setSupervisorOnline] = useState<boolean | null>(null);
  const [showNewDbModal, setShowNewDbModal] = useState<boolean>(false);
  const [newDbName, setNewDbName] = useState<string>("");
  const [newDbRegion, setNewDbRegion] = useState<string>("Home Desktop (i5-9600 • Arch Linux)");

  // Live Monitor Stream State
  const [events, setEvents] = useState<OperationEvent[]>([]);
  const [isMonitorPaused, setIsMonitorPaused] = useState<boolean>(false);
  const monitorEndRef = useRef<HTMLDivElement>(null);

  // Key-Value Explorer State
  const [keys, setKeys] = useState<KeyRecord[]>([]);
  const [searchQuery, setSearchQuery] = useState<string>("");
  const [newKey, setNewKey] = useState<string>("");
  const [newVal, setNewVal] = useState<string>("");
  const [newTTL, setNewTTL] = useState<string>("");

  // CLI State
  const [cliInput, setCliInput] = useState<string>("");
  const [cliHistory, setCliHistory] = useState<{ cmd: string; resp: string }[]>([
    { cmd: "AUTH " + selectedDb.token, resp: "OK" },
    { cmd: "INFO", resp: "# Sider Server v2.1.0\r\nstatus:online\r\nengine:LSM-Tree\r\n" }
  ]);
  const cliBottomRef = useRef<HTMLDivElement>(null);

  // Stats State
  const [stats, setStats] = useState<InstanceStats>({
    memtable_entries: 14,
    memtable_bytes: 4280,
    sstables_count: 2,
    sstables_bytes: 184000,
    wal_bytes: 1240,
    total_ops: 8940,
    ops_per_sec: 142.5,
    connected_clients: 3
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
        } else {
          setDatabases([]);
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
        { key: "users:101:profile", value: '{"name": "Alice", "role": "admin"}', ttl: -1, tier: "memtable" },
        { key: "users:102:profile", value: '{"name": "Bob", "role": "engineer"}', ttl: -1, tier: "memtable" },
        { key: "session:token:9a7f", value: "active_user_sess_xyz", ttl: 245, tier: "memtable" },
        { key: "cache:products:featured", value: '["prod_1", "prod_2", "prod_9"]', ttl: 1180, tier: "sstable" },
        { key: "analytics:pageviews:home", value: "19402", ttl: -1, tier: "sstable" }
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
      // Fallback
    }
  };

  // 3. Fetch Stats for Active Instance
  const fetchStats = async () => {
    if (isDemoMode) return;
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
      // Simulate live incoming traffic in Demo mode
      const commands = ["PUT", "GET", "PUTEX", "PUBLISH", "TTL"];
      const keysSample = ["user:profile:104", "session:tok_8a", "alerts:hospital", "cache:orders:991"];
      const interval = setInterval(() => {
        if (isMonitorPaused) return;
        const cmd = commands[Math.floor(Math.random() * commands.length)];
        const k = keysSample[Math.floor(Math.random() * keysSample.length)];
        const newEvt: OperationEvent = {
          timestamp: Date.now(),
          client_addr: "100.95.206.7:" + (49152 + Math.floor(Math.random() * 1000)),
          command: cmd,
          key: k,
          latency_us: Math.floor(Math.random() * 250) + 45,
          status: "OK",
          storage_tier: Math.random() > 0.3 ? "memtable" : "sstable"
        };
        setEvents((prev) => [...prev.slice(-100), newEvt]);
      }, 1600);
      return () => clearInterval(interval);
    }

    // Connect to real SSE endpoint
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

    if (isDemoMode) {
      let resp = "OK";
      if (cmd.startsWith("GET")) resp = '"demo_value_response"';
      else if (cmd.startsWith("INFO")) resp = "# Sider Server v2.1.0\r\nstatus:online\r\nmemtable_entries:14\r\n";
      else if (cmd.startsWith("PING")) resp = "PONG";
      setCliHistory((prev) => [...prev, { cmd, resp }]);
      return;
    }

    try {
      const res = await fetch(`${selectedDb.http_endpoint}/api/exec`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ command: cmd, token: selectedDb.token })
      });
      const data = await res.json();
      setCliHistory((prev) => [...prev, { cmd, resp: data.response }]);
      fetchKeys();
      fetchStats();
    } catch {
      setCliHistory((prev) => [...prev, { cmd, resp: "ERR connection failed" }]);
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
  };

  return (
    <div className="min-h-screen bg-[#07070a] text-white flex flex-col font-sans select-none">
      {/* Top Studio Header */}
      <header className="w-full bg-[#0c0c12] border-b border-[#1f1f2e] px-6 h-16 flex items-center justify-between sticky top-0 z-50">
        <div className="flex items-center gap-4">
          <Link href="/" className="flex items-center gap-2.5 group">
            <SiderLogo size={32} />
            <div className="flex flex-col">
              <span className="font-mono text-[14px] font-semibold tracking-wider text-white">
                SIDER CLOUD
              </span>
              <span className="text-[10px] text-[#9ca3af] font-mono">
                LSM-Tree Database Console
              </span>
            </div>
          </Link>
          <span className="text-[#27273a] hidden sm:inline">|</span>
          <div className="hidden sm:flex items-center gap-2 font-mono text-[11px] bg-[#14141e] px-2.5 py-1 rounded-[6px] border border-[#27273a]">
            <span className="text-[#6b7280]">SUPERVISOR:</span>
            <span className="text-[#38bdf8] truncate max-w-[200px]">{supervisorHost}</span>
            <span
              className={`w-2 h-2 rounded-full ${
                isDemoMode ? "bg-[#eab308]" : supervisorOnline ? "bg-[#10b981]" : "bg-[#ef4444]"
              }`}
            />
          </div>
        </div>

        {/* Right Actions: Mode Toggle & Docs */}
        <div className="flex items-center gap-3">
          <button
            onClick={() => setIsDemoMode((prev) => !prev)}
            className={`px-3 py-1.5 rounded-[6px] text-[11px] font-mono border transition-all ${
              isDemoMode
                ? "bg-[#eab308]/20 text-[#eab308] border-[#eab308]/50"
                : "bg-[#14141e] text-[#9ca3af] border-[#27273a] hover:text-white"
            }`}
          >
            {isDemoMode ? "🟡 Sandbox Demo Mode" : "🟢 Desktop Cloud (Live)"}
          </button>
          <Link
            href="/"
            className="text-[12px] font-mono text-[#9ca3af] hover:text-white transition-colors"
          >
            &larr; Landing Page
          </Link>
        </div>
      </header>

      {/* Main Workspace Layout */}
      <div className="flex-1 flex flex-col md:flex-row overflow-hidden">
        {/* Left Sidebar: Databases List */}
        <aside className="w-full md:w-72 bg-[#09090e] border-r border-[#1f1f2e] p-4 flex flex-col justify-between shrink-0">
          <div>
            <div className="flex items-center justify-between mb-4">
              <span className="text-[11px] font-mono uppercase tracking-wider text-[#6b7280]">
                My Databases ({databases.length})
              </span>
              <button
                onClick={() => setShowNewDbModal(true)}
                className="px-2.5 py-1 rounded-[5px] bg-white text-black font-mono text-[11px] font-semibold hover:bg-neutral-200 transition-colors"
              >
                + New DB
              </button>
            </div>

            <div className="space-y-2">
              {databases.map((db) => {
                const isSelected = selectedDb?.id === db.id;
                return (
                  <div
                    key={db.id}
                    onClick={() => setSelectedDb(db)}
                    className={`p-3 rounded-[8px] border cursor-pointer transition-all ${
                      isSelected
                        ? "bg-[#141420] border-[#38bdf8] shadow-[0_0_15px_rgba(56,189,248,0.15)]"
                        : "bg-[#0e0e16] border-[#1f1f2e] hover:border-[#374151]"
                    }`}
                  >
                    <div className="flex items-center justify-between">
                      <span className="font-mono text-[13px] font-semibold text-white">
                        {db.name}
                      </span>
                      <span className="w-2 h-2 rounded-full bg-[#10b981]" />
                    </div>
                    <div className="flex items-center justify-between mt-1 text-[11px] font-mono text-[#9ca3af]">
                      <span>PORT :{db.tcp_port}</span>
                      <span className="text-[10px] text-[#6b7280]">{db.id.slice(0, 10)}</span>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Supervisor Health Footer */}
          <div className="pt-4 border-t border-[#1f1f2e] mt-4 font-mono text-[11px] text-[#6b7280]">
            <div>HOST: Arch Linux (i5-9600)</div>
            <div>STATUS: {isDemoMode ? "Simulated" : supervisorOnline ? "Connected" : "Reconnecting..."}</div>
          </div>
        </aside>

        {/* Center / Right Content: Database Workspace */}
        <main className="flex-1 flex flex-col bg-[#07070a] overflow-y-auto">
          {/* Active Database Header Banner */}
          <div className="p-6 bg-[#0a0a10] border-b border-[#1f1f2e] flex flex-col lg:flex-row items-start lg:items-center justify-between gap-4">
            <div>
              <div className="flex items-center gap-2 mb-1">
                <h1 className="text-[22px] font-semibold text-white font-mono">
                  {selectedDb.name}
                </h1>
                <span className="px-2 py-0.5 rounded-[4px] bg-[#10b981]/20 text-[#10b981] border border-[#10b981]/40 text-[10px] font-mono uppercase">
                  {selectedDb.status}
                </span>
                <span className="px-2 py-0.5 rounded-[4px] bg-[#14141e] text-[#9ca3af] border border-[#27273a] text-[10px] font-mono">
                  {selectedDb.region}
                </span>
              </div>
              <div className="text-[12px] font-mono text-[#9ca3af] flex items-center gap-3">
                <span>TCP PORT: <strong className="text-white">:{selectedDb.tcp_port}</strong></span>
                <span>&bull;</span>
                <span>HTTP GATEWAY: <strong className="text-[#38bdf8]">:{selectedDb.http_port}</strong></span>
                <span>&bull;</span>
                <span>ID: {selectedDb.id}</span>
              </div>
            </div>

            {/* Navigation Tabs */}
            <div className="flex flex-wrap items-center gap-1.5 bg-[#101018] p-1 rounded-[8px] border border-[#1f1f2e]">
              {[
                { id: "overview", label: "Overview & Connect" },
                { id: "monitor", label: "Live Monitor (Stream)" },
                { id: "browser", label: "Data Browser (Keys)" },
                { id: "cli", label: "Web CLI Terminal" },
                { id: "metrics", label: "LSM Telemetry" }
              ].map((tab) => (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id as any)}
                  className={`px-3 py-1.5 rounded-[6px] text-[12px] font-mono transition-all ${
                    activeTab === tab.id
                      ? "bg-white text-black font-semibold shadow"
                      : "text-[#9ca3af] hover:text-white"
                  }`}
                >
                  {tab.label}
                </button>
              ))}
            </div>
          </div>

          {/* TAB 1: OVERVIEW & CONNECT */}
          {activeTab === "overview" && (
            <div className="p-6 max-w-5xl space-y-6">
              {/* Connection Strings Card */}
              <div className="bg-[#0e0e16] border border-[#1f1f2e] rounded-[12px] p-5">
                <span className="text-[11px] font-mono uppercase tracking-wider text-[#6b7280]">
                  Connection URI (TCP Protocol)
                </span>
                <div className="mt-2 flex items-center justify-between gap-3 bg-[#050508] p-3 rounded-[8px] border border-[#27273a]">
                  <code className="text-[#38bdf8] font-mono text-[13px] truncate">
                    {selectedDb.connection_uri}
                  </code>
                  <button
                    onClick={() => copyToClipboard(selectedDb.connection_uri, "uri")}
                    className="px-3 py-1 rounded-[5px] bg-[#14141e] border border-[#27273a] text-[11px] font-mono text-[#9ca3af] hover:text-white shrink-0"
                  >
                    {copiedKey === "uri" ? "✓ Copied" : "Copy URI"}
                  </button>
                </div>

                <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 mt-4 pt-4 border-t border-[#1f1f2e]">
                  <div>
                    <span className="text-[10px] font-mono text-[#6b7280]">HOST</span>
                    <div className="font-mono text-[12px] text-white">100.95.206.7</div>
                  </div>
                  <div>
                    <span className="text-[10px] font-mono text-[#6b7280]">PORT</span>
                    <div className="font-mono text-[12px] text-white">{selectedDb.tcp_port}</div>
                  </div>
                  <div>
                    <span className="text-[10px] font-mono text-[#6b7280]">AUTH TOKEN</span>
                    <div className="font-mono text-[12px] text-[#f59e0b] truncate">{selectedDb.token}</div>
                  </div>
                </div>
              </div>

              {/* Ready-to-use Code Snippets */}
              <div className="bg-[#0e0e16] border border-[#1f1f2e] rounded-[12px] p-5">
                <span className="text-[11px] font-mono uppercase tracking-wider text-[#6b7280]">
                  Quickstart Driver Snippets
                </span>

                <div className="mt-4 space-y-4">
                  {/* Python */}
                  <div>
                    <div className="flex items-center justify-between mb-1">
                      <span className="text-[12px] font-mono text-[#38bdf8]">🐍 Python (Zero Dependencies)</span>
                      <button
                        onClick={() =>
                          copyToClipboard(
                            `import socket\ns = socket.socket()\ns.connect(("100.95.206.7", ${selectedDb.tcp_port}))\ns.sendall(b"AUTH ${selectedDb.token}\\n")\ns.sendall(b"PUT user:101 '{\\"name\\": \\"Alice\\"}'\\n")\nprint(s.recv(1024).decode())`,
                            "py"
                          )
                        }
                        className="text-[11px] font-mono text-[#6b7280] hover:text-white"
                      >
                        {copiedKey === "py" ? "✓ Copied" : "Copy"}
                      </button>
                    </div>
                    <pre className="bg-[#050508] p-3 rounded-[6px] text-[12px] font-mono text-[#9ca3af] border border-[#1f1f2e]">
                      <code>{`import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("100.95.206.7", ${selectedDb.tcp_port}))
s.sendall(b"AUTH ${selectedDb.token}\\n")
s.sendall(b"PUT user:101 '{\\"name\\": \\"Alice\\"}'\\n")
print(s.recv(1024).decode()) # OK`}</code>
                    </pre>
                  </div>

                  {/* Netcat CLI */}
                  <div>
                    <div className="flex items-center justify-between mb-1">
                      <span className="text-[12px] font-mono text-[#10b981]">💻 Terminal / Netcat (nc)</span>
                      <button
                        onClick={() =>
                          copyToClipboard(
                            `printf "AUTH ${selectedDb.token}\\nPUT foo bar\\nGET foo\\n" | nc 100.95.206.7 ${selectedDb.tcp_port}`,
                            "nc"
                          )
                        }
                        className="text-[11px] font-mono text-[#6b7280] hover:text-white"
                      >
                        {copiedKey === "nc" ? "✓ Copied" : "Copy"}
                      </button>
                    </div>
                    <pre className="bg-[#050508] p-3 rounded-[6px] text-[12px] font-mono text-[#9ca3af] border border-[#1f1f2e]">
                      <code>{`printf "AUTH ${selectedDb.token}\\nPUT foo bar\\nGET foo\\n" | nc 100.95.206.7 ${selectedDb.tcp_port}`}</code>
                    </pre>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* TAB 2: LIVE COMMAND MONITOR */}
          {activeTab === "monitor" && (
            <div className="p-6 flex-1 flex flex-col">
              <div className="flex items-center justify-between pb-4 border-b border-[#1f1f2e] mb-4">
                <div className="flex items-center gap-3">
                  <div className="w-2.5 h-2.5 rounded-full bg-[#10b981] animate-pulse" />
                  <span className="font-mono text-[12px] text-white">
                    LIVE OPERATION FEED ({events.length} EVENTS)
                  </span>
                </div>
                <div className="flex items-center gap-2">
                  <button
                    onClick={() => setIsMonitorPaused((p) => !p)}
                    className="px-3 py-1 rounded-[5px] bg-[#14141e] border border-[#27273a] text-[11px] font-mono text-[#9ca3af] hover:text-white"
                  >
                    {isMonitorPaused ? "▶ Resume Feed" : "⏸ Pause Feed"}
                  </button>
                  <button
                    onClick={() => setEvents([])}
                    className="px-3 py-1 rounded-[5px] bg-[#14141e] border border-[#27273a] text-[11px] font-mono text-[#9ca3af] hover:text-white"
                  >
                    Clear Stream
                  </button>
                </div>
              </div>

              {/* Virtualized Terminal Feed */}
              <div className="flex-1 bg-[#050508] border border-[#1f1f2e] rounded-[10px] p-4 font-mono text-[12px] overflow-y-auto space-y-2 min-h-[420px]">
                {events.length === 0 ? (
                  <div className="text-center py-16 text-[#6b7280]">
                    Listening for database commands... Connect a client to see live operations!
                  </div>
                ) : (
                  events.map((evt, idx) => (
                    <div
                      key={idx}
                      className="flex flex-wrap items-center gap-3 py-1 border-b border-[#14141e] text-[12px]"
                    >
                      <span className="text-[#6b7280]">
                        {new Date(evt.timestamp).toLocaleTimeString()}
                      </span>
                      <span className="text-[#38bdf8] font-semibold">{evt.command}</span>
                      <span className="text-white truncate max-w-xs">{evt.key || "-"}</span>
                      <span className="ml-auto text-[#10b981] font-semibold">{evt.latency_us} µs</span>
                      <span
                        className={`text-[10px] px-1.5 py-0.5 rounded ${
                          evt.storage_tier === "memtable"
                            ? "bg-[#10b981]/20 text-[#10b981]"
                            : "bg-[#a855f7]/20 text-[#a855f7]"
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

          {/* TAB 3: DATA BROWSER */}
          {activeTab === "browser" && (
            <div className="p-6 space-y-6">
              {/* Insert Key Form */}
              <form
                onSubmit={handleAddKey}
                className="bg-[#0e0e16] border border-[#1f1f2e] rounded-[12px] p-4 flex flex-col md:flex-row items-center gap-3"
              >
                <input
                  type="text"
                  placeholder="Key (e.g. users:103:profile)"
                  value={newKey}
                  onChange={(e) => setNewKey(e.target.value)}
                  className="flex-1 bg-[#050508] border border-[#27273a] px-3 py-2 rounded-[6px] font-mono text-[12px] text-white"
                />
                <input
                  type="text"
                  placeholder="Value string or JSON"
                  value={newVal}
                  onChange={(e) => setNewVal(e.target.value)}
                  className="flex-1 bg-[#050508] border border-[#27273a] px-3 py-2 rounded-[6px] font-mono text-[12px] text-white"
                />
                <input
                  type="number"
                  placeholder="TTL (sec, opt)"
                  value={newTTL}
                  onChange={(e) => setNewTTL(e.target.value)}
                  className="w-24 bg-[#050508] border border-[#27273a] px-3 py-2 rounded-[6px] font-mono text-[12px] text-white"
                />
                <button
                  type="submit"
                  className="px-4 py-2 bg-white text-black font-mono text-[12px] font-semibold rounded-[6px] hover:bg-neutral-200 transition-colors"
                >
                  Set Key
                </button>
              </form>

              {/* Keys Table */}
              <div className="bg-[#0e0e16] border border-[#1f1f2e] rounded-[12px] overflow-hidden">
                <div className="p-4 border-b border-[#1f1f2e] flex items-center justify-between">
                  <span className="font-mono text-[12px] uppercase text-[#6b7280]">
                    Stored Keys ({keys.length})
                  </span>
                  <input
                    type="text"
                    placeholder="Search keys..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="bg-[#050508] border border-[#27273a] px-3 py-1 rounded-[6px] font-mono text-[11px] text-white w-48"
                  />
                </div>

                <div className="divide-y divide-[#1f1f2e]">
                  {keys.map((k) => (
                    <div
                      key={k.key}
                      className="p-3 flex items-center justify-between hover:bg-[#12121c] font-mono text-[12px]"
                    >
                      <div className="flex items-center gap-3">
                        <span
                          className={`text-[9px] px-1.5 py-0.5 rounded uppercase font-semibold ${
                            k.tier === "memtable"
                              ? "bg-[#10b981]/20 text-[#10b981]"
                              : "bg-[#a855f7]/20 text-[#a855f7]"
                          }`}
                        >
                          {k.tier}
                        </span>
                        <span className="text-[#38bdf8] font-medium">{k.key}</span>
                        <span className="text-[#9ca3af] truncate max-w-md">{k.value}</span>
                      </div>
                      <div className="flex items-center gap-3">
                        <span className="text-[11px] text-[#6b7280]">
                          TTL: {k.ttl === -1 ? "Persistent" : `${k.ttl}s`}
                        </span>
                        <button
                          onClick={() => handleDeleteKey(k.key)}
                          className="text-[#ef4444] hover:underline text-[11px]"
                        >
                          Delete
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* TAB 4: WEB CLI TERMINAL */}
          {activeTab === "cli" && (
            <div className="p-6 flex-1 flex flex-col">
              <div className="flex-1 bg-[#050508] border border-[#1f1f2e] rounded-[10px] p-4 font-mono text-[13px] flex flex-col justify-between min-h-[460px]">
                <div className="space-y-3 overflow-y-auto mb-4">
                  {cliHistory.map((item, idx) => (
                    <div key={idx} className="space-y-1">
                      <div className="flex items-center gap-2 text-white">
                        <span className="text-[#10b981]">&gt;</span>
                        <span>{item.cmd}</span>
                      </div>
                      <div className="text-[#9ca3af] whitespace-pre-wrap pl-4 font-mono text-[12px]">
                        {item.resp}
                      </div>
                    </div>
                  ))}
                  <div ref={cliBottomRef} />
                </div>

                <form onSubmit={handleCliSubmit} className="flex items-center gap-2 pt-2 border-t border-[#1f1f2e]">
                  <span className="text-[#10b981] font-bold">&gt;</span>
                  <input
                    type="text"
                    value={cliInput}
                    onChange={(e) => setCliInput(e.target.value)}
                    placeholder="Enter Sider command (e.g. PUT key val, GET key, INFO)..."
                    className="flex-1 bg-transparent text-white focus:outline-none font-mono text-[13px]"
                    autoFocus
                  />
                  <button type="submit" className="text-[11px] text-[#6b7280] hover:text-white font-mono">
                    Run &crarr;
                  </button>
                </form>
              </div>
            </div>
          )}

          {/* TAB 5: LSM METRICS & STORAGE */}
          {activeTab === "metrics" && (
            <div className="p-6 max-w-5xl space-y-6 font-mono">
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                <div className="bg-[#0e0e16] border border-[#1f1f2e] rounded-[10px] p-4">
                  <span className="text-[10px] text-[#6b7280] uppercase">MemTable RAM Keys</span>
                  <div className="text-[24px] font-bold text-white mt-1">
                    {stats.memtable_entries || 0}
                  </div>
                  <span className="text-[11px] text-[#10b981]">Fast $O(\log N)$ In-Memory</span>
                </div>
                <div className="bg-[#0e0e16] border border-[#1f1f2e] rounded-[10px] p-4">
                  <span className="text-[10px] text-[#6b7280] uppercase">Immutable SSTables on Disk</span>
                  <div className="text-[24px] font-bold text-white mt-1">
                    {stats.sstables_count || 0} .db files
                  </div>
                  <span className="text-[11px] text-[#a855f7]">Persistent Storage</span>
                </div>
                <div className="bg-[#0e0e16] border border-[#1f1f2e] rounded-[10px] p-4">
                  <span className="text-[10px] text-[#6b7280] uppercase">Total Operations Executed</span>
                  <div className="text-[24px] font-bold text-white mt-1">
                    {stats.total_ops || 0}
                  </div>
                  <span className="text-[11px] text-[#38bdf8]">Sub-millisecond writes</span>
                </div>
              </div>
            </div>
          )}
        </main>
      </div>

      {/* New Database Modal */}
      {showNewDbModal && (
        <div className="fixed inset-0 z-50 bg-black/80 backdrop-blur-sm flex items-center justify-center p-4">
          <div className="w-full max-w-md bg-[#0e0e16] border border-[#27273a] rounded-[14px] p-6 shadow-2xl">
            <h2 className="text-[18px] font-semibold text-white font-mono mb-4">
              Provision New Sider Database
            </h2>
            <form onSubmit={handleCreateDatabase} className="space-y-4">
              <div>
                <label className="block text-[11px] font-mono text-[#9ca3af] uppercase mb-1">
                  Database Name
                </label>
                <input
                  type="text"
                  placeholder="e.g. production-cache"
                  value={newDbName}
                  onChange={(e) => setNewDbName(e.target.value)}
                  className="w-full bg-[#050508] border border-[#27273a] px-3 py-2 rounded-[6px] font-mono text-[13px] text-white"
                  required
                />
              </div>
              <div>
                <label className="block text-[11px] font-mono text-[#9ca3af] uppercase mb-1">
                  Host & Engine Hardware
                </label>
                <select
                  value={newDbRegion}
                  onChange={(e) => setNewDbRegion(e.target.value)}
                  className="w-full bg-[#050508] border border-[#27273a] px-3 py-2 rounded-[6px] font-mono text-[13px] text-white"
                >
                  <option value="Home Desktop (i5-9600 • Arch Linux)">
                    Home Desktop (i5-9600 • Arch Linux)
                  </option>
                  <option value="Local Developer Sandbox">Local Developer Sandbox</option>
                </select>
              </div>

              <div className="flex items-center justify-end gap-3 pt-4 border-t border-[#1f1f2e]">
                <button
                  type="button"
                  onClick={() => setShowNewDbModal(false)}
                  className="px-4 py-2 font-mono text-[12px] text-[#9ca3af] hover:text-white"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  className="px-4 py-2 rounded-[6px] bg-white text-black font-mono text-[12px] font-semibold hover:bg-neutral-200 transition-colors"
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

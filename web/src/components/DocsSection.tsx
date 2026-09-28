"use client";

import React, { useState } from "react";
import Link from "next/link";
import { ScrollReveal } from "./ScrollReveal";
import { LaserBorderCard } from "./LaserBorderCard";

type DocTab = "protocol" | "rest" | "lsm" | "cluster";

export function DocsSection() {
  const [activeTab, setActiveTab] = useState<DocTab>("protocol");
  const [copiedCode, setCopiedCode] = useState<string | null>(null);

  const copySnippet = (code: string, id: string) => {
    navigator.clipboard.writeText(code);
    setCopiedCode(id);
    setTimeout(() => setCopiedCode(null), 2000);
  };

  return (
    <section id="docs" className="w-full max-w-[1200px] mx-auto px-4 sm:px-6 py-20 scroll-mt-24 border-t border-[#414042]/50">
      {/* Eyebrow & Headline */}
      <ScrollReveal variant="drop" delayMs={50}>
        <div className="text-center mb-12">
          <div className="inline-flex items-center gap-2 px-4 py-1 rounded-[30px] bg-[#191919] border border-[#7084ff]/40 text-[11px] text-[#7084ff] font-mono font-semibold uppercase tracking-wider mb-3 shadow-[0_0_20px_rgba(112,132,255,0.2)]">
            <span className="w-2 h-2 rounded-full bg-[#7084ff] animate-pulse" />
            <span>TECHNICAL CODEX // SYSTEM ARCHITECTURE & REFERENCE</span>
          </div>
          <h2 className="text-[28px] sm:text-[44px] font-medium text-white tracking-tight leading-tight px-2">
            Complete Technical Specification.
            <br />
            <span className="bg-gradient-to-r from-[#7084ff] via-[#3dd6f5] to-[#405bff] bg-clip-text text-transparent">
              Engine internals, wire protocol, and APIs.
            </span>
          </h2>
          <p className="text-[13px] sm:text-[16px] text-[#d1d3d4] max-w-2xl mx-auto mt-3 px-2">
            Everything you need to integrate natively with Sider Cloud: raw TCP sockets, HTTP REST gateway, LSM-Tree mechanics, and cluster node specifications.
          </p>
        </div>
      </ScrollReveal>

      {/* Tab Navigation Pill */}
      <div className="flex justify-start sm:justify-center mb-8 overflow-x-auto no-scrollbar -mx-4 px-4 sm:mx-0 sm:px-0">
        <div className="inline-flex items-center gap-1 p-1 sm:p-1.5 rounded-[60px] bg-[#191919] border border-[#414042] backdrop-blur-md shrink-0">
          {[
            { id: "protocol", label: "Wire Protocol (TCP)", icon: "⚡" },
            { id: "rest", label: "REST Gateway API", icon: "🌐" },
            { id: "lsm", label: "LSM-Tree Internals", icon: "💾" },
            { id: "cluster", label: "Cluster Spec (ind-tbn-1)", icon: "🖥️" },
          ].map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id as DocTab)}
              className={`px-3.5 sm:px-5 py-1.5 sm:py-2 rounded-[30px] text-[11px] sm:text-[13px] shrink-0 font-medium transition-all flex items-center gap-2 whitespace-nowrap cursor-pointer active:scale-95 ${
                activeTab === tab.id
                  ? "bg-[#405bff] text-white shadow-[0_0_20px_rgba(64,91,255,0.4)]"
                  : "text-[#a7a9ac] hover:text-white hover:bg-white/5"
              }`}
            >
              <span>{tab.icon}</span>
              <span>{tab.label}</span>
            </button>
          ))}
        </div>
      </div>

      {/* Main Documentation Card */}
      <ScrollReveal variant="swoop-up" delayMs={80} enableTilt={true}>
        <LaserBorderCard glowColor="#7084ff">
          <div className="p-4 sm:p-8 font-sans">
            
            {/* TAB 1: WIRE PROTOCOL SPEC */}
            {activeTab === "protocol" && (
              <div className="space-y-6">
                <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-5 border-b border-[#414042] gap-3">
                  <div>
                    <span className="text-[11px] font-mono text-[#7084ff] uppercase font-semibold">
                      PORT 4100 // RAW TCP WIRE PROTOCOL
                    </span>
                    <h3 className="text-[22px] font-semibold text-white tracking-tight mt-0.5">
                      Framed ASCII Command Specification
                    </h3>
                  </div>
                  <button
                    onClick={() =>
                      copySnippet(
                        `printf "AUTH sdr_live_793855e4ec0e70901bef05344264ba98\\nPUT session:token 'active_123' 3600\\nGET session:token\\nINFO\\n" | nc 100.95.206.7 4100`,
                        "netcat-spec"
                      )
                    }
                    className="px-4 py-1.5 rounded-[30px] bg-[#0e0e0e] hover:bg-[#2c2c2c] border border-[#414042] text-[12px] font-mono text-[#d1d3d4] hover:text-white transition-colors flex items-center gap-1.5 cursor-pointer shrink-0"
                  >
                    <span>{copiedCode === "netcat-spec" ? "✓ Copied" : "Copy Netcat Pipeline"}</span>
                  </button>
                </div>

                <p className="text-[14px] text-[#d1d3d4] leading-relaxed">
                  Sider uses a lightweight, human-readable line protocol framed with newline (<code className="text-[#00f0ff] font-mono">\r\n</code> or <code className="text-[#00f0ff] font-mono">\n</code>). All commands are processed synchronously on dedicated TCP connections with keep-alive support.
                </p>

                <div className="overflow-x-auto w-full -mx-4 sm:mx-0 px-4 sm:px-0">
                  <table className="min-w-[620px] w-full text-left font-mono text-[12px]">
                    <thead>
                      <tr className="border-b border-[#414042] text-[#6d6e71]">
                        <th className="pb-3">COMMAND</th>
                        <th className="pb-3">SYNTAX</th>
                        <th className="pb-3">SUCCESS RESPONSE</th>
                        <th className="pb-3">ERROR RESPONSE</th>
                        <th className="pb-3">DESCRIPTION</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-[#2c2c2c] text-[#d1d3d4]">
                      <tr>
                        <td className="py-3 font-semibold text-white">AUTH</td>
                        <td className="py-3 text-[#7084ff]">AUTH &lt;token&gt;</td>
                        <td className="py-3 text-[#19a05f]">OK</td>
                        <td className="py-3 text-[#ef4444]">ERR invalid token</td>
                        <td className="py-3 text-[#a7a9ac]">Authenticates client session with instance bearer token.</td>
                      </tr>
                      <tr>
                        <td className="py-3 font-semibold text-white">PUT</td>
                        <td className="py-3 text-[#7084ff]">PUT &lt;key&gt; &lt;val&gt; [ttl]</td>
                        <td className="py-3 text-[#19a05f]">OK</td>
                        <td className="py-3 text-[#ef4444]">ERR missing arguments</td>
                        <td className="py-3 text-[#a7a9ac]">Appends to WAL & inserts into SkipList MemTable with optional TTL (s).</td>
                      </tr>
                      <tr>
                        <td className="py-3 font-semibold text-white">GET</td>
                        <td className="py-3 text-[#7084ff]">GET &lt;key&gt;</td>
                        <td className="py-3 text-[#19a05f]">&lt;value&gt;</td>
                        <td className="py-3 text-[#ef4444]">ERR key not found</td>
                        <td className="py-3 text-[#a7a9ac]">Queries MemTable, checks Bloom filter, then searches SSTables.</td>
                      </tr>
                      <tr>
                        <td className="py-3 font-semibold text-white">DEL</td>
                        <td className="py-3 text-[#7084ff]">DEL &lt;key&gt;</td>
                        <td className="py-3 text-[#19a05f]">OK</td>
                        <td className="py-3 text-[#ef4444]">ERR key not found</td>
                        <td className="py-3 text-[#a7a9ac]">Appends tombstone marker to WAL and updates MemTable.</td>
                      </tr>
                      <tr>
                        <td className="py-3 font-semibold text-white">INFO</td>
                        <td className="py-3 text-[#7084ff]">INFO</td>
                        <td className="py-3 text-[#19a05f]"># Sider Server...</td>
                        <td className="py-3 text-[#ef4444]">ERR auth required</td>
                        <td className="py-3 text-[#a7a9ac]">Returns server telemetry, uptime, keys, and memory stats.</td>
                      </tr>
                      <tr>
                        <td className="py-3 font-semibold text-white">COMPACT</td>
                        <td className="py-3 text-[#7084ff]">COMPACT</td>
                        <td className="py-3 text-[#19a05f]">OK compacted N files</td>
                        <td className="py-3 text-[#ef4444]">ERR no sstables</td>
                        <td className="py-3 text-[#a7a9ac]">Forces MemTable flush and triggers L0 &rarr; L1 SSTable compaction.</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* TAB 2: HTTP REST GATEWAY */}
            {activeTab === "rest" && (
              <div className="space-y-6">
                <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-5 border-b border-[#414042] gap-3">
                  <div>
                    <span className="text-[11px] font-mono text-[#00f0ff] uppercase font-semibold">
                      PORT 5100 // HTTP REST & MONITOR GATEWAY
                    </span>
                    <h3 className="text-[22px] font-semibold text-white tracking-tight mt-0.5">
                      Stateless Execution & SSE Streams
                    </h3>
                  </div>
                  <span className="px-3 py-1 rounded-[30px] bg-[#0e0e0e] border border-white/10 text-[11px] font-mono text-[#a7a9ac]">
                    Base URL: http://100.95.206.7:5100
                  </span>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {/* POST /api/exec */}
                  <div className="p-4 rounded-[16px] bg-[#0e0e0e] border border-[#414042]">
                    <div className="flex items-center justify-between mb-2">
                      <span className="px-2 py-0.5 rounded text-[11px] font-mono bg-[#405bff]/20 text-[#7084ff] font-bold">
                        POST
                      </span>
                      <code className="text-[12px] font-mono text-white">/api/exec</code>
                    </div>
                    <p className="text-[12px] text-[#a7a9ac] mb-3">
                      Execute any raw Sider command with JSON payload.
                    </p>
                    <pre className="p-3 rounded-[10px] bg-[#141414] text-[11px] font-mono text-[#d1d3d4] overflow-x-auto leading-relaxed">
{`curl -X POST http://100.95.206.7:5100/api/exec \\
  -H "Content-Type: application/json" \\
  -d '{"command": "PUT user:1001 active", "token": "sdr_live_..."}'`}
                    </pre>
                  </div>

                  {/* GET /api/stats */}
                  <div className="p-4 rounded-[16px] bg-[#0e0e0e] border border-[#414042]">
                    <div className="flex items-center justify-between mb-2">
                      <span className="px-2 py-0.5 rounded text-[11px] font-mono bg-[#19a05f]/20 text-[#19a05f] font-bold">
                        GET
                      </span>
                      <code className="text-[12px] font-mono text-white">/api/stats</code>
                    </div>
                    <p className="text-[12px] text-[#a7a9ac] mb-3">
                      Fetch instance health, ops count, and memory allocation.
                    </p>
                    <pre className="p-3 rounded-[10px] bg-[#141414] text-[11px] font-mono text-[#d1d3d4] overflow-x-auto leading-relaxed">
{`curl http://100.95.206.7:5100/api/stats
# Returns { "memtable_entries": 42, "ops_per_sec": 316000 }`}
                    </pre>
                  </div>

                  {/* GET /api/keys */}
                  <div className="p-4 rounded-[16px] bg-[#0e0e0e] border border-[#414042]">
                    <div className="flex items-center justify-between mb-2">
                      <span className="px-2 py-0.5 rounded text-[11px] font-mono bg-[#19a05f]/20 text-[#19a05f] font-bold">
                        GET
                      </span>
                      <code className="text-[12px] font-mono text-white">/api/keys</code>
                    </div>
                    <p className="text-[12px] text-[#a7a9ac] mb-3">
                      List all active keys with tier annotation (<code className="text-[#405bff]">memtable</code> or <code className="text-[#00f0ff]">sstable</code>).
                    </p>
                    <pre className="p-3 rounded-[10px] bg-[#141414] text-[11px] font-mono text-[#d1d3d4] overflow-x-auto leading-relaxed">
{`curl http://100.95.206.7:5100/api/keys
# Returns [ {"key": "alpha:token", "tier": "memtable", "ttl": -1} ]`}
                    </pre>
                  </div>

                  {/* GET /api/monitor */}
                  <div className="p-4 rounded-[16px] bg-[#0e0e0e] border border-[#414042]">
                    <div className="flex items-center justify-between mb-2">
                      <span className="px-2 py-0.5 rounded text-[11px] font-mono bg-[#eab308]/20 text-[#eab308] font-bold">
                        SSE
                      </span>
                      <code className="text-[12px] font-mono text-white">/api/monitor</code>
                    </div>
                    <p className="text-[12px] text-[#a7a9ac] mb-3">
                      Real-time Server-Sent Events stream emitting live command events.
                    </p>
                    <pre className="p-3 rounded-[10px] bg-[#141414] text-[11px] font-mono text-[#d1d3d4] overflow-x-auto leading-relaxed">
{`const es = new EventSource("http://100.95.206.7:5100/api/monitor");
es.onmessage = (e) => console.log("Live Mutation:", JSON.parse(e.data));`}
                    </pre>
                  </div>
                </div>
              </div>
            )}

            {/* TAB 3: LSM-TREE INTERNALS */}
            {activeTab === "lsm" && (
              <div className="space-y-6">
                <div className="pb-5 border-b border-[#414042]">
                  <span className="text-[11px] font-mono text-[#7084ff] uppercase font-semibold">
                    STORAGE ENGINE MECHANICS // ZERO EXTERNAL DEPENDENCIES
                  </span>
                  <h3 className="text-[22px] font-semibold text-white tracking-tight mt-0.5">
                    How Sider Achieves Sub-Millisecond Durability
                  </h3>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
                  <div className="p-5 rounded-[18px] bg-[#0e0e0e] border border-[#414042]">
                    <div className="flex items-center gap-2 mb-3">
                      <span className="w-2.5 h-2.5 rounded-full bg-[#19a05f]" />
                      <h4 className="text-[15px] font-semibold text-white">1. SkipList MemTable</h4>
                    </div>
                    <p className="text-[13px] text-[#a7a9ac] leading-relaxed">
                      Written in pure Go with lock-free concurrent reads. Up to 12 probabilistic forward pointers allow <strong className="text-white">O(log N)</strong> point lookups without the rebalancing stalls of red-black trees.
                    </p>
                  </div>

                  <div className="p-5 rounded-[18px] bg-[#0e0e0e] border border-[#414042]">
                    <div className="flex items-center gap-2 mb-3">
                      <span className="w-2.5 h-2.5 rounded-full bg-[#405bff]" />
                      <h4 className="text-[15px] font-semibold text-white">2. Write-Ahead Log</h4>
                    </div>
                    <p className="text-[13px] text-[#a7a9ac] leading-relaxed">
                      Every write is sequentially appended with a 32-bit CRC checksum directly to an append-only file on NVMe ext4 before updating RAM. Guarantees zero data loss on node crashes.
                    </p>
                  </div>

                  <div className="p-5 rounded-[18px] bg-[#0e0e0e] border border-[#414042]">
                    <div className="flex items-center gap-2 mb-3">
                      <span className="w-2.5 h-2.5 rounded-full bg-[#00f0ff]" />
                      <h4 className="text-[15px] font-semibold text-white">3. Tiered SSTables</h4>
                    </div>
                    <p className="text-[13px] text-[#a7a9ac] leading-relaxed">
                      Immutable sorted string tables with 4KB sparse block indexes. Coupled with Murmur3 Bloom filters rejecting 99.8% of misses, eliminating unnecessary disk seeks.
                    </p>
                  </div>
                </div>

                <div className="p-4 rounded-[16px] bg-[#0e0e0e] border border-[#405bff]/30 flex flex-col sm:flex-row sm:items-center justify-between gap-4">
                  <div className="flex items-center gap-3">
                    <span className="text-[20px]">⚡</span>
                    <div className="text-[13px] text-[#d1d3d4]">
                      <strong>Verified Bare-Metal Benchmark:</strong> 316,746 ops/sec peak SkipList throughput with P50 latency of 234 microseconds on region <code className="text-[#7084ff] font-mono">ind-tbn-1</code>.
                    </div>
                  </div>
                  <Link
                    href="/console"
                    className="px-4 py-2 rounded-[30px] bg-[#405bff] hover:bg-[#344bd6] text-white text-[12px] font-medium transition-all whitespace-nowrap text-center"
                  >
                    Run Live Benchmark in Cockpit &rarr;
                  </Link>
                </div>
              </div>
            )}

            {/* TAB 4: CLUSTER SPECIFICATIONS */}
            {activeTab === "cluster" && (
              <div className="space-y-6">
                <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-5 border-b border-[#414042] gap-3">
                  <div>
                    <span className="text-[11px] font-mono text-[#00f0ff] uppercase font-semibold">
                      BARE-METAL CLUSTER TOPOLOGY
                    </span>
                    <h3 className="text-[22px] font-semibold text-white tracking-tight mt-0.5">
                      Edge Node Specifications (ind-tbn-1)
                    </h3>
                  </div>
                  <span className="px-3 py-1 rounded-[30px] bg-[#19a05f]/20 border border-[#19a05f]/40 text-[11px] font-mono text-[#19a05f] font-semibold flex items-center gap-1.5 shrink-0">
                    <span className="w-1.5 h-1.5 rounded-full bg-[#19a05f] animate-pulse" />
                    NODE ONLINE (100.95.206.7)
                  </span>
                </div>

                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4">
                  <div className="p-4 rounded-[14px] bg-[#0e0e0e] border border-[#414042]">
                    <span className="text-[11px] text-[#6d6e71] uppercase font-semibold">CPU Architecture</span>
                    <div className="text-[16px] font-medium text-white font-mono mt-1">Intel i5-9600 (6C)</div>
                    <span className="text-[10px] text-[#a7a9ac]">2.90GHz - 4.60GHz Turbo</span>
                  </div>

                  <div className="p-4 rounded-[14px] bg-[#0e0e0e] border border-[#414042]">
                    <span className="text-[11px] text-[#6d6e71] uppercase font-semibold">Memory Pool</span>
                    <div className="text-[16px] font-medium text-white font-mono mt-1">16GB DDR4</div>
                    <span className="text-[10px] text-[#19a05f]">Dual-Channel High-Speed</span>
                  </div>

                  <div className="p-4 rounded-[14px] bg-[#0e0e0e] border border-[#414042]">
                    <span className="text-[11px] text-[#6d6e71] uppercase font-semibold">GPU Accelerator</span>
                    <div className="text-[16px] font-medium text-[#7084ff] font-mono mt-1">NVIDIA GTX 1660 Ti</div>
                    <span className="text-[10px] text-[#a7a9ac]">6GB VRAM CUDA Core</span>
                  </div>

                  <div className="p-4 rounded-[14px] bg-[#0e0e0e] border border-[#414042]">
                    <span className="text-[11px] text-[#6d6e71] uppercase font-semibold">Persistent Disk</span>
                    <div className="text-[16px] font-medium text-white font-mono mt-1">480GB NVMe SSD</div>
                    <span className="text-[10px] text-[#a7a9ac]">ext4 Direct I/O WAL Storage</span>
                  </div>

                  <div className="p-4 rounded-[14px] bg-[#0e0e0e] border border-[#414042]">
                    <span className="text-[11px] text-[#6d6e71] uppercase font-semibold">Encrypted Wire</span>
                    <div className="text-[16px] font-medium text-[#3dd6f5] font-mono mt-1">WireGuard Mesh</div>
                    <span className="text-[10px] text-[#a7a9ac]">Sub-1ms Edge Transit</span>
                  </div>

                  <div className="p-4 rounded-[14px] bg-[#0e0e0e] border border-[#414042]">
                    <span className="text-[11px] text-[#6d6e71] uppercase font-semibold">Supervisor Daemon</span>
                    <div className="text-[16px] font-medium text-white font-mono mt-1">Port 8080</div>
                    <span className="text-[10px] text-[#a7a9ac]">Multi-tenant Orchestrator</span>
                  </div>

                  <div className="p-4 rounded-[14px] bg-[#0e0e0e] border border-[#414042]">
                    <span className="text-[11px] text-[#6d6e71] uppercase font-semibold">Primary TCP Port</span>
                    <div className="text-[16px] font-medium text-[#405bff] font-mono mt-1">Port 4100</div>
                    <span className="text-[10px] text-[#a7a9ac]">alpha-production Engine</span>
                  </div>

                  <div className="p-4 rounded-[14px] bg-[#0e0e0e] border border-[#414042]">
                    <span className="text-[11px] text-[#6d6e71] uppercase font-semibold">HTTP Gateway</span>
                    <div className="text-[16px] font-medium text-[#00f0ff] font-mono mt-1">Port 5100</div>
                    <span className="text-[10px] text-[#a7a9ac]">REST &amp; SSE Stream</span>
                  </div>
                </div>
              </div>
            )}

          </div>
        </LaserBorderCard>
      </ScrollReveal>
    </section>
  );
}

"use client";

import React, { useState } from "react";
import Link from "next/link";
import { ScrollReveal } from "./ScrollReveal";
import { LaserBorderCard } from "./LaserBorderCard";

interface BuildRecipe {
  id: string;
  level: string;
  xp: string;
  title: string;
  badge: string;
  tagline: string;
  story: string;
  stack: string[];
  steps: { step: string; detail: string }[];
  buildMdContent: string;
}

const RECIPES: BuildRecipe[] = [
  {
    id: "ai-agent-memory",
    level: "MISSION 01",
    xp: "+500 XP",
    title: "Sub-Millisecond AI Agent Neural Memory",
    badge: "AI & LLM AGENTS",
    tagline: "Ultra-fast working memory & session store for autonomous agents.",
    story: "Neural synapses misfire when context lookups exceed 10ms. Wire your AI agent's short-term memory directly into Sider's in-memory SkipList with automated TTL eviction.",
    stack: ["Python", "Anthropic / OpenAI", "Sider TCP", "TTL Eviction"],
    steps: [
      { step: "01. Initialize Neural Session", detail: "Generate a session UUID and authenticate with Sider over raw TCP (:4100)." },
      { step: "02. Atomic Memory Ingestion", detail: "Serialize user utterances & tool call scratchpads into JSON payloads and write via PUT with 3600s TTL." },
      { step: "03. Wire-Speed Context Recall", detail: "Fetch recent memory vectors in 0.14ms before synthesizing LLM prompt completion." },
      { step: "04. Memory Pruning & Persistence", detail: "Expired thoughts naturally drop out of RAM, while critical memories persist to NVMe SSTables." }
    ],
    buildMdContent: `# Mission 01: Build an AI Agent Short-Term Memory Cache with Sider

## Objective
Implement a high-velocity short-term working memory layer for autonomous AI agents using Sider Cloud's raw TCP protocol and SkipList MemTable.

## Architecture
- **Protocol**: Raw TCP on port 4100 with \`\\r\\n\` framed commands
- **Target Node**: 100.95.206.7:4100 (Region: ind-tbn-1)
- **Token Auth**: Send \`AUTH <token>\\r\\n\` immediately on connection
- **Keyspace Pattern**: \`agent:session:<session_id>:turn:<turn_id>\`
- **TTL Strategy**: 3600 seconds for working memory; -1 for persistent facts

## Specification & Implementation Steps
1. Create a socket pool connected to Sider TCP server.
2. Authenticate using \`AUTH sdr_live_793855e4ec0e70901bef05344264ba98\`.
3. Provide \`remember(key, value, ttl=3600)\` using \`PUT <key> <json_value> <ttl>\`.
4. Provide \`recall(key)\` using \`GET <key>\`.
5. Provide \`forget(key)\` using \`DEL <key>\`.
6. Add unit tests validating sub-millisecond retrieval and auto-expiry.

## Ready-to-Run Starter (Python)
\`\`\`python
import socket
import json

class SiderAgentMemory:
    def __init__(self, host="100.95.206.7", port=4100, token="sdr_live_793855e4ec0e70901bef05344264ba98"):
        self.sock = socket.create_connection((host, port))
        self.sock.sendall(f"AUTH {token}\\r\\n".encode())
        assert b"OK" in self.sock.recv(1024)

    def remember(self, session_id: str, turn: int, payload: dict, ttl: int = 3600):
        key = f"agent:{session_id}:turn:{turn}"
        val = json.dumps(payload)
        self.sock.sendall(f"PUT {key} {val} {ttl}\\r\\n".encode())
        return self.sock.recv(1024).decode().strip()

    def recall(self, session_id: str, turn: int) -> dict:
        key = f"agent:{session_id}:turn:{turn}"
        self.sock.sendall(f"GET {key}\\r\\n".encode())
        raw = self.sock.recv(4096).decode().strip()
        return json.loads(raw) if not raw.startswith("ERR") else None
\`\`\`
`
  },
  {
    id: "hft-order-book",
    level: "MISSION 02",
    xp: "+1,200 XP",
    title: "High-Frequency Crypto Order Book & Matching Queue",
    badge: "FINTECH & TRADING",
    tagline: "50,000+ orders/sec order ingestion with zero disk-seek lockups.",
    story: "Relational databases lock and stall during market volatility spikes. Offload tick data and matching queues to Sider's lock-free SkipList with synchronous NVMe WAL durability.",
    stack: ["TypeScript", "Node.js", "WebSockets", "NVMe WAL"],
    steps: [
      { step: "01. Socket Pipeline Bootstrap", detail: "Establish persistent TCP connection pool with keep-alive." },
      { step: "02. Order Tick Processing", detail: "Ingest bid/ask ticks via atomic PUT commands with microsecond timestamps." },
      { step: "03. Real-Time Price Level Retrieval", detail: "Query depth of market via GET with sub-250µs latency." },
      { step: "04. Crash Durability Verification", detail: "Every write is serialized with CRC32 checksums into the Write-Ahead Log." }
    ],
    buildMdContent: `# Mission 02: Build an HFT Order Book Engine with Sider

## Objective
Build a lightweight matching and tick ingestion queue capable of sub-millisecond execution using Sider's SkipList MemTable.

## Architecture
- **Host**: 100.95.206.7:4100
- **Storage Tier**: In-memory SkipList (Level 12) + NVMe WAL
- **Order Model**: \`PUT order:<pair>:<price_ticks>:<order_id> <order_json>\`

## Implementation Guide
\`\`\`typescript
import net from "net";

export class SiderOrderBook {
  private client: net.Socket;

  constructor(host = "100.95.206.7", port = 4100, token = "sdr_live_793855e4ec0e70901bef05344264ba98") {
    this.client = net.createConnection({ host, port });
    this.client.write(\`AUTH \${token}\\r\\n\`);
  }

  async submitOrder(pair: string, side: "BUY" | "SELL", price: number, size: number, id: string) {
    const key = \`order:\${pair}:\${side}:\${price}:\${id}\`;
    const payload = JSON.stringify({ side, price, size, timestamp: Date.now() });
    return new Promise((resolve) => {
      this.client.once("data", (d) => resolve(d.toString().trim()));
      this.client.write(\`PUT \${key} \${payload}\\r\\n\`);
    });
  }
}
\`\`\`
`
  },
  {
    id: "dynamic-feature-flags",
    level: "MISSION 03",
    xp: "+2,500 XP",
    title: "Edge Dynamic Config & Instant Kill-Switch Daemon",
    badge: "INFRASTRUCTURE & DEVOPS",
    tagline: "Propagate circuit breakers and feature flags globally in real-time.",
    story: "A production outage requires an instant kill-switch across 500 edge nodes. Listen to Sider's Server-Sent Events (/api/monitor) to propagate toggles within 2 milliseconds.",
    stack: ["Go", "SSE Monitor", "Circuit Breakers", "HTTP REST"],
    steps: [
      { step: "01. Flag State Initialization", detail: "Seed system configuration flags in Sider with PUT flag:<name> <json>." },
      { step: "02. Daemon Event Stream", detail: "Edge daemons subscribe to GET /api/monitor SSE stream to receive mutation broadcasts." },
      { step: "03. Zero-Delay Circuit Tripping", detail: "When a kill-switch is flipped, all edge nodes immediately halt hazardous codepaths." },
      { step: "04. Redundant Local Cache", detail: "Daemons cache values in memory, remaining operational even during edge network partitions." }
    ],
    buildMdContent: `# Mission 03: Build a Distributed Kill-Switch Daemon with Sider

## Objective
Build a dynamic configuration engine that watches live mutations from Sider Cloud via Server-Sent Events (SSE).

## Architecture
- **Control Plane**: Sider REST Gateway (:5100)
- **Mutation Listener**: \`GET http://100.95.206.7:5100/api/monitor\`
- **Command Dispatch**: \`POST http://100.95.206.7:5100/api/exec\`

## Go Implementation Sample
\`\`\`go
package main

import (
	"bufio"
	"fmt"
	"net/http"
	"strings"
)

func listenKillSwitch() {
	resp, err := http.Get("http://100.95.206.7:5100/api/monitor")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "kill_switch:active") {
			fmt.Println("[CRITICAL] Kill Switch Activated! Halting upstream traffic.")
		}
	}
}
\`\`\`
`
  },
  {
    id: "iot-telemetry-hub",
    level: "MISSION 04",
    xp: "+5,000 XP",
    title: "Edge IoT Sensor Telemetry & SSTable Archive",
    badge: "BIG DATA & HARDWARE",
    tagline: "High-density sensor data pipeline with automated tiered compaction.",
    story: "10,000 smart grid sensors stream voltage fluctuations every 100ms. Sider absorbs writes into RAM, flushes them to immutable SSTable blocks on NVMe, and discards misses via Bloom filters.",
    stack: ["Rust", "Tokio", "Bloom Filters", "Leveled Compaction"],
    steps: [
      { step: "01. High-Density TCP Ingestion", detail: "Send framed TCP packets directly from IoT gateways without HTTP overhead." },
      { step: "02. MemTable Ingestion", detail: "SkipList absorbs bursts of up to 316,000 ops/second with sub-250µs latency." },
      { step: "03. NVMe SSTable Flushing", detail: "When memory threshold is hit, immutable sorted tables are flushed to disk." },
      { step: "04. Background Leveled Compaction", detail: "Trigger COMPACT command to merge overlapping key ranges into optimized levels." }
    ],
    buildMdContent: `# Mission 04: Build an IoT Telemetry Pipeline with Sider

## Objective
Stream high-frequency environmental sensor data into Sider Cloud and leverage tiered LSM-Tree compaction for historical analysis.

## Architecture
- **Wire Endpoint**: 100.95.206.7:4100
- **Ingestion Key Format**: \`sensor:<device_id>:<timestamp_unix>\`
- **Maintenance Command**: \`COMPACT\` (flushes MemTable & merges L0 -> L1 SSTables)

## Rust Implementation Sample
\`\`\`rust
use std::io::{Write, BufReader, BufRead};
use std::net::TcpStream;

fn main() -> std::io::Result<()> {
    let mut stream = TcpStream::connect("100.95.206.7:4100")?;
    writeln!(stream, "AUTH sdr_live_793855e4ec0e70901bef05344264ba98")?;
    
    // Ingest telemetry reading
    let reading = r#"{"voltage": 230.4, "hz": 50.01, "temp": 41.2}"#;
    writeln!(stream, "PUT sensor:grid_401:1727500000 {}", reading)?;

    let mut reader = BufReader::new(stream);
    let mut resp = String::new();
    reader.read_line(&mut resp)?;
    println!("Sider Ingestion Ack: {}", resp);
    Ok(())
}
\`\`\`
`
  }
];

export function BuildSection() {
  const [selectedRecipe, setSelectedRecipe] = useState<BuildRecipe>(RECIPES[0]);
  const [showModal, setShowModal] = useState<boolean>(false);
  const [copiedId, setCopiedId] = useState<string | null>(null);

  const copyBuildMd = (recipe: BuildRecipe) => {
    navigator.clipboard.writeText(recipe.buildMdContent);
    setCopiedId(recipe.id);
    setTimeout(() => setCopiedId(null), 2500);
  };

  return (
    <section id="build" className="w-full max-w-[1200px] mx-auto px-4 sm:px-6 py-20 scroll-mt-24">
      {/* Section Eyebrow & Title */}
      <ScrollReveal variant="drop" delayMs={50}>
        <div className="text-center mb-12">
          <div className="inline-flex items-center gap-2 px-4 py-1 rounded-[30px] bg-[#191919] border border-[#405bff]/40 text-[11px] text-[#7084ff] font-mono font-semibold uppercase tracking-wider mb-3 shadow-[0_0_20px_rgba(64,91,255,0.2)]">
            <span className="w-2 h-2 rounded-full bg-[#00f0ff] animate-ping" />
            <span>CYBER-FOUNDRY QUESTS // BUILD WITH SIDER</span>
          </div>
          <h2 className="text-[34px] sm:text-[48px] font-medium text-white tracking-tight leading-tight">
            Choose your mission.
            <br />
            <span className="bg-gradient-to-r from-[#405bff] via-[#7084ff] to-[#00f0ff] bg-clip-text text-transparent">
              Forge production engines in minutes.
            </span>
          </h2>
          <p className="text-[15px] sm:text-[17px] text-[#d1d3d4] max-w-2xl mx-auto mt-3">
            Select a battle-tested blueprint. Copy the complete <code className="text-[#00f0ff] font-mono px-1.5 py-0.5 rounded bg-white/5 border border-white/10">build.md</code> prompt dossier directly into your AI coding agent (Claude, Cursor, Copilot, Antigravity) and launch live.
          </p>
        </div>
      </ScrollReveal>

      {/* Quest Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-12">
        {RECIPES.map((recipe, index) => {
          const isSelected = selectedRecipe.id === recipe.id;
          return (
            <ScrollReveal
              key={recipe.id}
              variant={index % 2 === 0 ? "swoop-left" : "swoop-right"}
              delayMs={index * 80}
              enableTilt={true}
            >
              <div
                className={`relative rounded-[28px] p-6 transition-all duration-300 cursor-pointer border ${
                  isSelected
                    ? "bg-[#1f1f2e] border-[#405bff] shadow-[0_0_35px_rgba(64,91,255,0.4)]"
                    : "bg-[#191919] border-[#414042] hover:border-[#7084ff]/60 hover:bg-[#1d1d1d]"
                }`}
                onClick={() => setSelectedRecipe(recipe)}
              >
                {/* Top Badge Strip */}
                <div className="flex items-center justify-between gap-2 mb-4">
                  <div className="flex items-center gap-2">
                    <span className="px-2.5 py-0.5 rounded-[30px] bg-[#405bff]/20 border border-[#405bff]/40 text-[10px] font-mono font-bold text-[#7084ff]">
                      {recipe.level}
                    </span>
                    <span className="text-[11px] font-mono text-[#00f0ff] font-semibold">
                      {recipe.xp}
                    </span>
                  </div>
                  <span className="text-[10px] font-mono uppercase text-[#a7a9ac] bg-[#0e0e0e] px-2.5 py-0.5 rounded-[30px] border border-white/10">
                    {recipe.badge}
                  </span>
                </div>

                {/* Title & Tagline */}
                <h3 className="text-[19px] font-semibold text-white tracking-tight mb-2">
                  {recipe.title}
                </h3>
                <p className="text-[13px] text-[#a7a9ac] leading-relaxed mb-4">
                  {recipe.story}
                </p>

                {/* Tech Pills */}
                <div className="flex flex-wrap gap-1.5 mb-5">
                  {recipe.stack.map((t) => (
                    <span
                      key={t}
                      className="px-2 py-0.5 rounded-[4px] text-[10px] font-mono bg-[#0e0e0e] text-[#d1d3d4] border border-[#414042]"
                    >
                      {t}
                    </span>
                  ))}
                </div>

                {/* Actions */}
                <div className="flex items-center gap-3 pt-4 border-t border-[#414042]/60">
                  <button
                    type="button"
                    onClick={(e) => {
                      e.stopPropagation();
                      copyBuildMd(recipe);
                    }}
                    className={`flex-1 py-2 rounded-[30px] text-[12px] font-medium font-mono transition-all flex items-center justify-center gap-1.5 cursor-pointer active:scale-95 ${
                      copiedId === recipe.id
                        ? "bg-[#19a05f] text-white shadow-[0_0_20px_rgba(25,160,95,0.6)]"
                        : "bg-[#405bff] hover:bg-[#344bd6] text-white shadow-[0_0_15px_rgba(64,91,255,0.35)]"
                    }`}
                  >
                    <span>{copiedId === recipe.id ? "✓ Copied build.md" : "⚡ Copy build.md for Agent"}</span>
                  </button>

                  <button
                    type="button"
                    onClick={(e) => {
                      e.stopPropagation();
                      setSelectedRecipe(recipe);
                      setShowModal(true);
                    }}
                    className="px-3.5 py-2 rounded-[30px] bg-[#0e0e0e] hover:bg-[#2c2c2c] border border-[#414042] text-[12px] text-[#d1d3d4] hover:text-white transition-colors cursor-pointer"
                  >
                    Inspect Dossier
                  </button>
                </div>
              </div>
            </ScrollReveal>
          );
        })}
      </div>

      {/* Active Quest Interactive Staging Cockpit */}
      <ScrollReveal variant="swoop-up" delayMs={100} enableTilt={true}>
        <LaserBorderCard glowColor="#00f0ff">
          <div className="p-6 sm:p-8">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-6 border-b border-[#414042] gap-4">
              <div>
                <div className="flex items-center gap-2">
                  <span className="w-2 h-2 rounded-full bg-[#00f0ff] animate-pulse" />
                  <span className="text-[11px] font-mono text-[#00f0ff] font-semibold uppercase">
                    ACTIVE QUEST BLUEPRINT // {selectedRecipe.level}
                  </span>
                </div>
                <h3 className="text-[22px] font-semibold text-white tracking-tight mt-1">
                  {selectedRecipe.title}
                </h3>
                <p className="text-[13px] text-[#a7a9ac] mt-1">
                  {selectedRecipe.tagline}
                </p>
              </div>

              <div className="flex items-center gap-3">
                <button
                  type="button"
                  onClick={() => copyBuildMd(selectedRecipe)}
                  className="px-5 py-2 rounded-[30px] bg-[#405bff] hover:bg-[#344bd6] text-white text-[13px] font-medium shadow-[0_0_20px_rgba(64,91,255,0.4)] transition-all flex items-center gap-2 cursor-pointer active:scale-95"
                >
                  <span>{copiedId === selectedRecipe.id ? "✓ Copied!" : "⚡ Copy build.md File"}</span>
                </button>

                <Link
                  href="/console"
                  className="px-5 py-2 rounded-[30px] bg-[#0e0e0e] hover:bg-[#2c2c2c] border border-[#414042] text-white text-[13px] font-medium transition-colors flex items-center gap-1.5"
                >
                  <span>Open in Cockpit</span>
                  <span>&rarr;</span>
                </Link>
              </div>
            </div>

            {/* 4-Step Quest Roadmap */}
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mt-6">
              {selectedRecipe.steps.map((st, i) => (
                <div
                  key={i}
                  className="p-4 rounded-[16px] bg-[#0e0e0e] border border-[#414042]/70 relative overflow-hidden"
                >
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-[12px] font-mono font-bold text-[#7084ff]">
                      {st.step}
                    </span>
                    <span className="w-5 h-5 rounded-full bg-white/5 border border-white/10 flex items-center justify-center text-[10px] text-[#6d6e71]">
                      ✓
                    </span>
                  </div>
                  <p className="text-[12px] text-[#d1d3d4] leading-relaxed">
                    {st.detail}
                  </p>
                </div>
              ))}
            </div>

            {/* Quick Agent Prompt Preview */}
            <div className="mt-6 p-4 rounded-[16px] bg-[#0e0e0e] border border-[#414042] font-mono text-[12px]">
              <div className="flex items-center justify-between pb-2 mb-2 border-b border-[#414042]/50 text-[#6d6e71]">
                <span className="text-[11px] uppercase">Agent Prompt Snippet (build.md)</span>
                <span className="text-[10px] text-[#00f0ff]">Target: ind-tbn-1:4100</span>
              </div>
              <pre className="text-[#a7a9ac] overflow-x-auto whitespace-pre-wrap leading-relaxed max-h-36">
                {selectedRecipe.buildMdContent.slice(0, 420)}...
              </pre>
            </div>
          </div>
        </LaserBorderCard>
      </ScrollReveal>

      {/* Full Dossier Modal */}
      {showModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/85 backdrop-blur-md p-4">
          <div className="bg-[#191919] border border-[#405bff]/60 rounded-[28px] max-w-2xl w-full max-h-[85vh] flex flex-col shadow-[0_0_50px_rgba(64,91,255,0.4)] overflow-hidden">
            <div className="p-6 border-b border-[#414042] flex items-center justify-between">
              <div>
                <span className="text-[10px] font-mono text-[#00f0ff] uppercase tracking-wider">
                  COMPLETE SPECIFICATION DOSSIER
                </span>
                <h3 className="text-[18px] font-semibold text-white">
                  {selectedRecipe.title} ({selectedRecipe.level})
                </h3>
              </div>
              <button
                type="button"
                onClick={() => setShowModal(false)}
                className="w-8 h-8 rounded-full bg-[#0e0e0e] border border-[#414042] text-[#a7a9ac] hover:text-white flex items-center justify-center cursor-pointer"
              >
                ✕
              </button>
            </div>

            <div className="p-6 overflow-y-auto flex-1 font-mono text-[12px] text-[#d1d3d4] leading-relaxed bg-[#0e0e0e]">
              <pre className="whitespace-pre-wrap">{selectedRecipe.buildMdContent}</pre>
            </div>

            <div className="p-4 border-t border-[#414042] bg-[#191919] flex items-center justify-between">
              <span className="text-[12px] text-[#a7a9ac] font-mono">
                Paste directly into Cursor, Claude, or Copilot
              </span>
              <button
                type="button"
                onClick={() => copyBuildMd(selectedRecipe)}
                className="px-5 py-2 rounded-[30px] bg-[#405bff] hover:bg-[#344bd6] text-white text-[12px] font-medium font-mono transition-all flex items-center gap-1.5 cursor-pointer active:scale-95"
              >
                <span>{copiedId === selectedRecipe.id ? "✓ Copied to Clipboard" : "⚡ Copy build.md"}</span>
              </button>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}

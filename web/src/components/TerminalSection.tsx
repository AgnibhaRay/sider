"use client";

import React, { useState, useRef, useEffect } from "react";

interface CommandLog {
  cmd: string;
  output: string;
  time: string;
}

interface KVItem {
  val: string;
  expiresAt?: number; // timestamp in ms
}

export function TerminalSection() {
  const [activePlate, setActivePlate] = useState<"ttl" | "clear" | "pubsub" | "compaction">("ttl");

  // In-memory key-value store simulator for Sider v2
  const [store, setStore] = useState<Record<string, KVItem>>({
    "vault:btc": { val: "85.42" },
    "vault:eth": { val: "3,250.00" },
    "session:token:9a": { val: "usr_active_904", expiresAt: Date.now() + 45000 },
    "user:1001": { val: "Leonardo da Vinci" },
    "user:1002": { val: "Luca Pacioli" },
  });

  const [history, setHistory] = useState<CommandLog[]>([
    {
      cmd: "INFO",
      output:
        "SIDER v2.0.0 (Go 1.24) — LSM-Tree Engine initialized. WAL: sider.wal active. MemTable: SkipList (size=3).",
      time: "15:00:01",
    },
    {
      cmd: "GET user:1001",
      output: '"Leonardo da Vinci" (0.11ms, MemTable hit)',
      time: "15:00:05",
    },
    {
      cmd: "TTL session:token:9a",
      output: "45 (survives node crash & compaction)",
      time: "15:00:08",
    },
  ]);

  const [inputVal, setInputVal] = useState("");
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [history]);

  const executeCommand = (raw: string) => {
    const trimmed = raw.trim();
    if (!trimmed) return;

    const parts = trimmed.split(/\s+/);
    const op = parts[0].toUpperCase();
    const timeStr = new Date().toTimeString().split(" ")[0];
    const now = Date.now();

    let output = "";

    switch (op) {
      case "HELP":
        output =
          "COMMANDS: PUT <key> <val> | GET <key> | PUTEX <key> <sec> <val> | TTL <key> | CLEAR <prefix> | KEYS | PUBLISH <chan> <msg> | COMPACT | INFO | HELP";
        break;

      case "INFO":
        output = `SIDER v2.0.0 Engine: Go 1.24\nWAL Persistence: synchronous binary\nMemTable: Concurrent SkipList (Items: ${
          Object.keys(store).length
        })\nSSTables: 4 L0 files, FNV-1a Bloom index active (91.4% skip ratio)`;
        break;

      case "PUT": {
        if (parts.length < 3) {
          output = "ERR: syntax error, expected: PUT <key> <value>";
        } else {
          const key = parts[1];
          const val = parts.slice(2).join(" ").replace(/^["']|["']$/g, "");
          setStore((prev) => ({ ...prev, [key]: { val } }));
          output = "OK (appended to WAL & SkipList in 0.14ms)";
        }
        break;
      }

      case "PUTEX": {
        if (parts.length < 4) {
          output = "ERR: syntax error, expected: PUTEX <key> <seconds> <value>";
        } else {
          const key = parts[1];
          const seconds = parseInt(parts[2], 10);
          if (isNaN(seconds) || seconds <= 0) {
            output = "ERR: seconds must be positive integer";
          } else {
            const val = parts.slice(3).join(" ").replace(/^["']|["']$/g, "");
            setStore((prev) => ({
              ...prev,
              [key]: { val, expiresAt: now + seconds * 1000 },
            }));
            output = `OK (expires in ${seconds}s, binary TTL written to WAL)`;
          }
        }
        break;
      }

      case "GET": {
        if (parts.length < 2) {
          output = "ERR: syntax error, expected: GET <key>";
        } else {
          const key = parts[1];
          const item = store[key];
          if (!item) {
            output = "(nil) [Bloom filter bypassed disk lookup]";
          } else if (item.expiresAt && item.expiresAt <= now) {
            output = "(nil) [key expired and evicted]";
          } else {
            output = `"${item.val}" (0.12ms)`;
          }
        }
        break;
      }

      case "TTL": {
        if (parts.length < 2) {
          output = "ERR: syntax error, expected: TTL <key>";
        } else {
          const key = parts[1];
          const item = store[key];
          if (!item || (item.expiresAt && item.expiresAt <= now)) {
            output = "(integer) -2 [key does not exist]";
          } else if (!item.expiresAt) {
            output = "(integer) -1 [persistent key, no TTL assigned]";
          } else {
            const rem = Math.max(0, Math.round((item.expiresAt - now) / 1000));
            output = `(integer) ${rem} seconds remaining`;
          }
        }
        break;
      }

      case "CLEAR": {
        if (parts.length < 2) {
          output = "ERR: syntax error, expected: CLEAR <prefix>";
        } else {
          const prefix = parts[1];
          let count = 0;
          const nextStore: Record<string, KVItem> = {};
          for (const [k, v] of Object.entries(store)) {
            if (k.startsWith(prefix)) {
              count++;
            } else {
              nextStore[k] = v;
            }
          }
          setStore(nextStore);
          output = `OK [evicted ${count} keys matching prefix '${prefix}']`;
        }
        break;
      }

      case "KEYS": {
        const validKeys = Object.entries(store)
          .filter(([, v]) => !v.expiresAt || v.expiresAt > now)
          .map(([k]) => k);
        output = validKeys.length ? validKeys.join("\n") : "(empty set)";
        break;
      }

      case "PUBLISH": {
        if (parts.length < 3) {
          output = "ERR: syntax error, expected: PUBLISH <channel> <message>";
        } else {
          const chan = parts[1];
          const msg = parts.slice(2).join(" ");
          output = `(integer) 3 subscribers received broadcast on [${chan}]: "${msg}"`;
        }
        break;
      }

      case "COMPACT": {
        output =
          "OK [K-Way merge completed: tombstones evicted, duplicate SSTable keys consolidated]";
        break;
      }

      default:
        output = `ERR: unknown command '${op}'. Type HELP for reference.`;
    }

    setHistory((prev) => [...prev, { cmd: raw, output, time: timeStr }]);
    setInputVal("");
  };

  const handleFormSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    executeCommand(inputVal);
  };

  return (
    <section
      id="interactive-terminal"
      className="relative z-10 w-full bg-[#c4c3b6] text-[#000000] px-6 sm:px-12 py-24 sm:py-32 select-none"
    >
      {/* Corner Labels: Opposite corners at 12px uppercase in Helvetica Now */}
      <div className="w-full max-w-7xl mx-auto flex items-center justify-between text-[#595855] text-[12px] uppercase tracking-[0.1em] mb-12 sm:mb-16">
        <span style={{ fontFamily: "var(--font-helvetica-now)" }}>FOLIO NO. 03</span>
        <span style={{ fontFamily: "var(--font-helvetica-now)" }}>INTERACTIVE PROTOCOL</span>
      </div>

      {/* Centered Section Header: Davinci 94px weight 500, #000000, letter-spacing -0.85px, line-height 0.84 */}
      <div className="w-full max-w-5xl mx-auto text-center mb-16 sm:mb-24">
        <h2
          className="text-[#000000] text-[48px] sm:text-[72px] md:text-[94px] font-medium leading-[0.84] tracking-[-0.85px]"
          style={{ fontFamily: "var(--font-davinci)" }}
        >
          COMMAND FOLIO & CLI
        </h2>
      </div>

      {/* Main Grid: Exhibition Wall Plaques (Left) and Interactive Terminal Console (Right) */}
      <div className="w-full max-w-7xl mx-auto grid grid-cols-1 lg:grid-cols-12 gap-8 items-start">
        {/* Left Column: Museum Wall Plaques in Bone (#e7e5e4, 9px radius, hairline #dfdcd5 border, 24px padding) */}
        <div className="lg:col-span-5 space-y-4">
          {/* Plate Tabs */}
          <div className="flex items-center gap-2 mb-2 pb-2 border-b border-[#dfdcd5]">
            {[
              { id: "ttl", label: "I. TTL Durability" },
              { id: "clear", label: "II. Namespace CLEAR" },
              { id: "pubsub", label: "III. Pub/Sub Broker" },
              { id: "compaction", label: "IV. LSM K-Way" },
            ].map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActivePlate(tab.id as any)}
                className={`text-[11px] uppercase tracking-[0.06em] px-3 py-1.5 rounded-[2px] transition-colors ${
                  activePlate === tab.id
                    ? "bg-[#000000] text-[#ffffff]"
                    : "text-[#595855] hover:text-[#000000]"
                }`}
                style={{ fontFamily: "var(--font-helvetica-now)" }}
              >
                {tab.label}
              </button>
            ))}
          </div>

          {/* Exhibition Plaque Body */}
          <div className="bg-[#e7e5e4] p-[24px] rounded-[9px] border border-[#dfdcd5] select-text">
            {activePlate === "ttl" && (
              <div>
                <div className="text-[10px] uppercase tracking-[0.14em] text-[#595855] mb-2 font-medium">
                  SPECIFICATION PLATE NO. 01
                </div>
                <h3
                  className="text-[26px] leading-[1.33] tracking-[-0.13px] font-normal text-[#000000] mb-3"
                  style={{ fontFamily: "var(--font-davinci)" }}
                >
                  Nanosecond-Precision Key Expiry
                </h3>
                <p
                  className="text-[15px] leading-[1.5] text-[#595855] mb-4"
                  style={{ fontFamily: "var(--font-helvetica-now)" }}
                >
                  Unlike naive in-memory timers that vanish when a process dies, Sider packs an 8-byte Unix timestamp directly into every WAL frame and SSTable file record. Key expiration survives unexpected server crashes and compactions.
                </p>
                <div className="space-y-2 text-[12px] font-mono text-[#000000] bg-[#dfdcd5]/40 p-3 rounded-[4px] mb-4">
                  <div>PUTEX session:token 3600 &quot;auth_hash&quot;</div>
                  <div>TTL session:token</div>
                  <div>EXPIRE session:token 7200</div>
                </div>
                <button
                  onClick={() => executeCommand('PUTEX session:token 3600 "auth_hash"')}
                  className="pill-button text-[12px] bg-[#000000] text-[#ffffff] px-[17px] py-[9px] rounded-[28.8px]"
                  style={{ fontFamily: "var(--font-helvetica-now)" }}
                >
                  Test PUTEX in CLI &rarr;
                </button>
              </div>
            )}

            {activePlate === "clear" && (
              <div>
                <div className="text-[10px] uppercase tracking-[0.14em] text-[#595855] mb-2 font-medium">
                  SPECIFICATION PLATE NO. 02
                </div>
                <h3
                  className="text-[26px] leading-[1.33] tracking-[-0.13px] font-normal text-[#000000] mb-3"
                  style={{ fontFamily: "var(--font-davinci)" }}
                >
                  O(1) Namespace Cache Invalidation
                </h3>
                <p
                  className="text-[15px] leading-[1.5] text-[#595855] mb-4"
                  style={{ fontFamily: "var(--font-helvetica-now)" }}
                >
                  Evict entire tenant caches, session partitions, or prefixes without running dangerous blocking SCAN operations. CLEAR writes a single range tombstone directly to the WAL.
                </p>
                <div className="space-y-2 text-[12px] font-mono text-[#000000] bg-[#dfdcd5]/40 p-3 rounded-[4px] mb-4">
                  <div>CLEAR user:</div>
                  <div>KEYS</div>
                </div>
                <button
                  onClick={() => executeCommand("CLEAR user:")}
                  className="pill-button text-[12px] bg-[#000000] text-[#ffffff] px-[17px] py-[9px] rounded-[28.8px]"
                  style={{ fontFamily: "var(--font-helvetica-now)" }}
                >
                  Execute CLEAR in CLI &rarr;
                </button>
              </div>
            )}

            {activePlate === "pubsub" && (
              <div>
                <div className="text-[10px] uppercase tracking-[0.14em] text-[#595855] mb-2 font-medium">
                  SPECIFICATION PLATE NO. 03
                </div>
                <h3
                  className="text-[26px] leading-[1.33] tracking-[-0.13px] font-normal text-[#000000] mb-3"
                  style={{ fontFamily: "var(--font-davinci)" }}
                >
                  Non-Blocking TCP Pub/Sub Broker
                </h3>
                <p
                  className="text-[15px] leading-[1.5] text-[#595855] mb-4"
                  style={{ fontFamily: "var(--font-helvetica-now)" }}
                >
                  Built directly into the Go engine using goroutine channel multiplexing. Thousands of microservices can publish and subscribe over TCP port 4000 with microsecond latency.
                </p>
                <div className="space-y-2 text-[12px] font-mono text-[#000000] bg-[#dfdcd5]/40 p-3 rounded-[4px] mb-4">
                  <div>SUBSCRIBE events:trades</div>
                  <div>PUBLISH events:trades &quot;BTC/USD 98,400&quot;</div>
                </div>
                <button
                  onClick={() => executeCommand('PUBLISH events:trades "BTC/USD 98,400"')}
                  className="pill-button text-[12px] bg-[#000000] text-[#ffffff] px-[17px] py-[9px] rounded-[28.8px]"
                  style={{ fontFamily: "var(--font-helvetica-now)" }}
                >
                  Test PUBLISH in CLI &rarr;
                </button>
              </div>
            )}

            {activePlate === "compaction" && (
              <div>
                <div className="text-[10px] uppercase tracking-[0.14em] text-[#595855] mb-2 font-medium">
                  SPECIFICATION PLATE NO. 04
                </div>
                <h3
                  className="text-[26px] leading-[1.33] tracking-[-0.13px] font-normal text-[#000000] mb-3"
                  style={{ fontFamily: "var(--font-davinci)" }}
                >
                  LSM Compaction & K-Way Merge
                </h3>
                <p
                  className="text-[15px] leading-[1.5] text-[#595855] mb-4"
                  style={{ fontFamily: "var(--font-helvetica-now)" }}
                >
                  Background K-Way merge consolidates immutable SSTables on disk, purging expired keys, applying tombstone deletions, and updating the FNV-1a Bloom filters.
                </p>
                <div className="space-y-2 text-[12px] font-mono text-[#000000] bg-[#dfdcd5]/40 p-3 rounded-[4px] mb-4">
                  <div>COMPACT</div>
                  <div>INFO</div>
                </div>
                <button
                  onClick={() => executeCommand("COMPACT")}
                  className="pill-button text-[12px] bg-[#000000] text-[#ffffff] px-[17px] py-[9px] rounded-[28.8px]"
                  style={{ fontFamily: "var(--font-helvetica-now)" }}
                >
                  Run COMPACT in CLI &rarr;
                </button>
              </div>
            )}
          </div>

          {/* Quick Command Suggestions */}
          <div className="flex flex-wrap gap-2 pt-2">
            {[
              'PUT vault:btc "85.42"',
              'GET vault:btc',
              'PUTEX auth:token 30 "active"',
              'TTL auth:token',
              'KEYS',
              'INFO',
            ].map((cmd) => (
              <button
                key={cmd}
                onClick={() => executeCommand(cmd)}
                className="text-[11px] font-mono bg-[#dfdcd5] hover:bg-[#000000] hover:text-[#ffffff] text-[#000000] px-2.5 py-1 rounded-[2px] transition-colors border-0 cursor-pointer"
              >
                {cmd}
              </button>
            ))}
          </div>
        </div>

        {/* Right Column: Interactive Live Sider CLI Terminal */}
        <div className="lg:col-span-7 bg-[#000000] text-[#ffffff] rounded-[9px] p-6 border border-[#595855] flex flex-col h-[520px] select-text">
          {/* Terminal Title Bar */}
          <div className="flex items-center justify-between border-b border-[#595855]/60 pb-3 mb-4 shrink-0">
            <div className="flex items-center gap-2">
              <span className="w-2.5 h-2.5 rounded-full bg-[#595855]" />
              <span
                className="text-[12px] text-[#dfdcd5] tracking-[0.06em] uppercase font-medium"
                style={{ fontFamily: "var(--font-helvetica-now)" }}
              >
                sider-cli &bull; tcp://127.0.0.1:4000
              </span>
            </div>
            <span
              className="text-[10px] text-[#808080] font-mono uppercase"
              style={{ fontFamily: "var(--font-helvetica-now)" }}
            >
              WAL: SYNCHRONOUS
            </span>
          </div>

          {/* Terminal Output Area */}
          <div className="flex-1 overflow-y-auto space-y-3 font-mono text-[13px] pr-2 scrollbar-thin">
            {history.map((h, i) => (
              <div key={i} className="space-y-1">
                <div className="flex items-center gap-2 text-[#808080]">
                  <span className="text-[#c4c3b6] font-semibold">sider&gt;</span>
                  <span className="text-[#ffffff]">{h.cmd}</span>
                  <span className="text-[10px] text-[#595855] ml-auto">{h.time}</span>
                </div>
                <div className="text-[#dfdcd5] whitespace-pre-wrap pl-6 leading-relaxed">
                  {h.output}
                </div>
              </div>
            ))}
            <div ref={bottomRef} />
          </div>

          {/* Terminal Input Line (NO autoFocus so page does not auto-scroll on mount) */}
          <form
            onSubmit={handleFormSubmit}
            className="mt-4 pt-3 border-t border-[#595855]/60 flex items-center gap-2 shrink-0"
          >
            <span className="font-mono text-[#c4c3b6] font-semibold text-[13px]">sider&gt;</span>
            <input
              type="text"
              value={inputVal}
              onChange={(e) => setInputVal(e.target.value)}
              placeholder="Try: PUTEX session 60 'auth', TTL session, CLEAR user:, INFO..."
              className="flex-1 bg-transparent border-none text-[#ffffff] font-mono text-[13px] outline-none placeholder:text-[#595855]"
            />
            <button
              type="submit"
              className="pill-button text-[11px] bg-[#ffffff] text-[#000000] hover:bg-[#c4c3b6] px-3 py-1 rounded-[28.8px]"
              style={{ fontFamily: "var(--font-helvetica-now)" }}
            >
              RUN
            </button>
          </form>
        </div>
      </div>
    </section>
  );
}

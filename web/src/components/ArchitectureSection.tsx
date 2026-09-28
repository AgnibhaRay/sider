"use client";

import React, { useState } from "react";
import { ThreeArchitectureVisualizer } from "./ThreeArchitectureVisualizer";

export function ArchitectureSection() {
  const [activeDriver, setActiveDriver] = useState<"cli" | "java" | "python" | "docker">("cli");

  return (
    <section className="relative z-10 w-full bg-[#c4c3b6] text-[#000000] px-6 sm:px-12 py-24 sm:py-32 select-none border-t border-[#dfdcd5]">
      {/* Corner Labels: Opposite corners at 12px uppercase in Helvetica Now */}
      <div className="w-full max-w-7xl mx-auto flex items-center justify-between text-[#595855] text-[12px] uppercase tracking-[0.1em] mb-12 sm:mb-16">
        <span style={{ fontFamily: "var(--font-helvetica-now)" }}>FOLIO NO. 05</span>
        <span style={{ fontFamily: "var(--font-helvetica-now)" }}>SYSTEM ARCHITECTURE & 3D VISUALIZER</span>
      </div>

      {/* Centered Section Header: Davinci 94px weight 500, #000000, letter-spacing -0.85px, line-height 0.84 */}
      <div className="w-full max-w-5xl mx-auto text-center mb-8 sm:mb-10">
        <h2
          className="text-[#000000] text-[48px] sm:text-[72px] md:text-[94px] font-medium leading-[0.84] tracking-[-0.85px]"
          style={{ fontFamily: "var(--font-davinci)" }}
        >
          ANATOMY OF SIDER
        </h2>
        <p
          className="mt-6 text-[15px] sm:text-[17px] text-[#595855] max-w-2xl mx-auto leading-relaxed"
          style={{ fontFamily: "var(--font-helvetica-now)" }}
        >
          Interactive 3D exploration of the Log-Structured Merge pipeline. Inspect live write, read, compaction, and pub/sub flows across memory and persistent disk tiers.
        </p>
      </div>

      {/* 3D Interactive Three.js Architecture Visualizer */}
      <div className="w-full max-w-6xl mx-auto mb-20">
        <ThreeArchitectureVisualizer />
      </div>

      {/* Grid of 3 Architectural Folio Cards in Bone (#e7e5e4, 9px radius, hairline #dfdcd5 border, 24px padding) */}
      <div className="w-full max-w-6xl mx-auto grid grid-cols-1 md:grid-cols-3 gap-6 mb-16">
        {/* Card 1 */}
        <div className="bg-[#e7e5e4] p-[24px] rounded-[9px] border border-[#dfdcd5] flex flex-col justify-between">
          <div>
            <div className="text-[10px] uppercase tracking-[0.14em] text-[#595855] mb-2 font-medium">
              TIER 01 &bull; MEMORY
            </div>
            <h3
              className="text-[26px] leading-[1.33] tracking-[-0.13px] font-normal text-[#000000] mb-3"
              style={{ fontFamily: "var(--font-davinci)" }}
            >
              SkipList MemTable
            </h3>
            <p
              className="text-[15px] leading-[1.5] text-[#595855]"
              style={{ fontFamily: "var(--font-helvetica-now)" }}
            >
              Probabilistic multi-level linked list maintaining sorted keys in RAM. Provides O(log N) search and insertion without lock contention or expensive tree rebalancing.
            </p>
          </div>
          <div className="mt-6 pt-3 border-t border-[#dfdcd5] text-[11px] font-mono text-[#595855]">
            CONCURRENCY: MUTEX-PROTECTED
          </div>
        </div>

        {/* Card 2 */}
        <div className="bg-[#e7e5e4] p-[24px] rounded-[9px] border border-[#dfdcd5] flex flex-col justify-between">
          <div>
            <div className="text-[10px] uppercase tracking-[0.14em] text-[#595855] mb-2 font-medium">
              TIER 02 &bull; LOGGING
            </div>
            <h3
              className="text-[26px] leading-[1.33] tracking-[-0.13px] font-normal text-[#000000] mb-3"
              style={{ fontFamily: "var(--font-davinci)" }}
            >
              Binary Write-Ahead Log
            </h3>
            <p
              className="text-[15px] leading-[1.5] text-[#595855]"
              style={{ fontFamily: "var(--font-helvetica-now)" }}
            >
              Append-only binary log with 8-byte TTL timestamps and CRC integrity checks. Instant crash recovery replays committed transactions into RAM in sub-100ms.
            </p>
          </div>
          <div className="mt-6 pt-3 border-t border-[#dfdcd5] text-[11px] font-mono text-[#595855]">
            INTEGRITY: ZERO-LOSS REPLAY
          </div>
        </div>

        {/* Card 3 */}
        <div className="bg-[#e7e5e4] p-[24px] rounded-[9px] border border-[#dfdcd5] flex flex-col justify-between">
          <div>
            <div className="text-[10px] uppercase tracking-[0.14em] text-[#595855] mb-2 font-medium">
              TIER 03 &bull; PERSISTENCE
            </div>
            <h3
              className="text-[26px] leading-[1.33] tracking-[-0.13px] font-normal text-[#000000] mb-3"
              style={{ fontFamily: "var(--font-davinci)" }}
            >
              Bloom SSTables
            </h3>
            <p
              className="text-[15px] leading-[1.5] text-[#595855]"
              style={{ fontFamily: "var(--font-helvetica-now)" }}
            >
              Immutable disk tables with sparse index blocks and FNV-1a BitSets. Reads query the Bloom filter first, bypassing physical disk I/O when a key does not exist.
            </p>
          </div>
          <div className="mt-6 pt-3 border-t border-[#dfdcd5] text-[11px] font-mono text-[#595855]">
            EFFICIENCY: 90%+ DISK SKIP
          </div>
        </div>
      </div>

      {/* Driver Integration Folio Plate */}
      <div className="w-full max-w-4xl mx-auto bg-[#e7e5e4] p-[24px] rounded-[9px] border border-[#dfdcd5]">
        <div className="flex flex-wrap items-center justify-between border-b border-[#dfdcd5] pb-3 mb-4 gap-2">
          <div className="text-[11px] uppercase tracking-[0.12em] font-medium text-[#000000]">
            CLIENT DRIVERS & DEPLOYMENT
          </div>
          <div className="flex items-center gap-2">
            {(["cli", "java", "python", "docker"] as const).map((drv) => (
              <button
                key={drv}
                onClick={() => setActiveDriver(drv)}
                className={`text-[11px] uppercase tracking-[0.06em] px-2.5 py-1 rounded-[2px] transition-colors ${
                  activeDriver === drv
                    ? "bg-[#000000] text-[#ffffff]"
                    : "text-[#595855] hover:text-[#000000]"
                }`}
                style={{ fontFamily: "var(--font-helvetica-now)" }}
              >
                {drv === "cli"
                  ? "Sider CLI"
                  : drv === "java"
                  ? "Java Driver"
                  : drv === "python"
                  ? "Python Driver"
                  : "Docker"}
              </button>
            ))}
          </div>
        </div>

        {/* Code Frame */}
        <div className="bg-[#000000] text-[#ffffff] p-4 rounded-[6px] font-mono text-[12px] leading-relaxed overflow-x-auto select-text">
          {activeDriver === "cli" && (
            <div>
              <span className="text-[#808080]"># 1. Compile Sider single static binary</span>
              <br />
              <span className="text-[#c4c3b6]">$</span> go build -o sider main.go
              <br />
              <br />
              <span className="text-[#808080]"># 2. Start the database engine on :4000</span>
              <br />
              <span className="text-[#c4c3b6]">$</span> ./sider --port 4000
              <br />
              <br />
              <span className="text-[#808080]"># 3. Connect interactive Sider CLI</span>
              <br />
              <span className="text-[#c4c3b6]">$</span> sider-cli --host 127.0.0.1 --port 4000
            </div>
          )}

          {activeDriver === "java" && (
            <div>
              <span className="text-[#808080]">// Production Java / Spring Boot Driver</span>
              <br />
              <span className="text-[#c4c3b6]">try</span> (SiderClient client = new SiderClient(&quot;localhost&quot;, 4000)) &#123;
              <br />
              &nbsp;&nbsp;client.putex(&quot;session:user:42&quot;, 3600, &quot;auth_token_99&quot;);
              <br />
              &nbsp;&nbsp;long ttl = client.ttl(&quot;session:user:42&quot;);
              <br />
              &nbsp;&nbsp;System.out.println(&quot;Session remaining seconds: &quot; + ttl);
              <br />
              &nbsp;&nbsp;client.publish(&quot;audit:channel&quot;, &quot;User 42 logged in&quot;);
              <br />
              &#125;
            </div>
          )}

          {activeDriver === "python" && (
            <div>
              <span className="text-[#808080]"># Production Python Driver (py-driver.py)</span>
              <br />
              <span className="text-[#c4c3b6]">import</span> sider
              <br />
              <br />
              client = sider.Client(host=&quot;127.0.0.1&quot;, port=4000)
              <br />
              client.put(&quot;price:btc&quot;, &quot;98450.20&quot;)
              <br />
              val = client.get(&quot;price:btc&quot;)
              <br />
              print(f&quot;BTC Price: &#123;val&#125;&quot;)
            </div>
          )}

          {activeDriver === "docker" && (
            <div>
              <span className="text-[#808080]"># Pull and run Sider v2 with persistent data mount</span>
              <br />
              <span className="text-[#c4c3b6]">$</span> docker run -d -p 4000:4000 -v $(pwd)/data:/root/data --name sider-db sider:latest
              <br />
              <br />
              <span className="text-[#808080]"># Verify health &amp; interactive netcat connection</span>
              <br />
              <span className="text-[#c4c3b6]">$</span> nc localhost 4000
            </div>
          )}
        </div>
      </div>
    </section>
  );
}

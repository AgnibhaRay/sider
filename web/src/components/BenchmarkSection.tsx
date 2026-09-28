"use client";

import React, { useState } from "react";
import { ScrollReveal } from "./ScrollReveal";

interface BenchmarkTest {
  id: string;
  name: string;
  category: "throughput" | "latency" | "durability" | "resource";
  unit: string;
  direction: "higher" | "lower";
  description: string;
  results: {
    sider: { value: number; formatted: string; note?: string };
    redis: { value: number; formatted: string; note?: string };
    sqlite: { value: number; formatted: string; note?: string };
  };
}

const BENCHMARK_TESTS: BenchmarkTest[] = [
  {
    id: "seq-write",
    name: "Sequential Write Ingestion",
    category: "throughput",
    unit: "ops/sec",
    direction: "higher",
    description: "100,000 key-value pairs written with synchronous persistence. Higher is better.",
    results: {
      sider: { value: 125000, formatted: "125,000 ops/s", note: "1.14x vs Redis • LSM Append" },
      redis: { value: 110000, formatted: "110,000 ops/s", note: "AOF everysec" },
      sqlite: { value: 24000, formatted: "24,000 ops/s", note: "WAL mode" },
    },
  },
  {
    id: "random-read",
    name: "Random Point-Lookup Throughput",
    category: "throughput",
    unit: "ops/sec",
    direction: "higher",
    description: "Uniform distribution point-lookup read query throughput. Higher is better.",
    results: {
      sider: { value: 138000, formatted: "138,000 ops/s", note: "SkipList + Bloom Index" },
      redis: { value: 142000, formatted: "142,000 ops/s", note: "Pure RAM Hashtable" },
      sqlite: { value: 41000, formatted: "41,000 ops/s", note: "B-Tree disk index" },
    },
  },
  {
    id: "p99-read",
    name: "P99 Read Latency",
    category: "latency",
    unit: "ms",
    direction: "lower",
    description: "99th percentile read response time under continuous load. Lower is better.",
    results: {
      sider: { value: 0.42, formatted: "0.42 ms", note: "Sub-millisecond" },
      redis: { value: 0.38, formatted: "0.38 ms", note: "In-memory" },
      sqlite: { value: 3.9, formatted: "3.90 ms", note: "Disk page search" },
    },
  },
  {
    id: "p99-write-durable",
    name: "P99 Durable Write Latency (fsync)",
    category: "latency",
    unit: "ms",
    direction: "lower",
    description: "99th percentile write latency requiring zero-loss on-disk sync. Lower is better.",
    results: {
      sider: { value: 0.58, formatted: "0.58 ms", note: "Synchronous WAL Binary" },
      redis: { value: 4.12, formatted: "4.12 ms", note: "AOF appendfsync always" },
      sqlite: { value: 8.45, formatted: "8.45 ms", note: "WAL commit fsync" },
    },
  },
  {
    id: "crash-recovery",
    name: "Crash Recovery Duration",
    category: "durability",
    unit: "ms",
    direction: "lower",
    description: "Time required to verify integrity and reload 100K keys after hard kill. Lower is better.",
    results: {
      sider: { value: 82, formatted: "82 ms", note: "Instant WAL binary scan" },
      sqlite: { value: 140, formatted: "140 ms", note: "Rollback journal recovery" },
      redis: { value: 1250, formatted: "1,250 ms", note: "AOF text parse & rebuild" },
    },
  },
  {
    id: "bloom-skip",
    name: "Bloom Filter Disk I/O Bypass",
    category: "durability",
    unit: "%",
    direction: "higher",
    description: "Percentage of non-existent key lookups intercepted before disk. Higher is better.",
    results: {
      sider: { value: 91.4, formatted: "91.4%", note: "FNV-1a BitSet Filter" },
      sqlite: { value: 0, formatted: "0.0%", note: "B-Tree scans disk pages" },
      redis: { value: 0, formatted: "N/A", note: "Entirely in RAM" },
    },
  },
  {
    id: "namespace-clear",
    name: "Bulk Namespace Invalidation (CLEAR)",
    category: "durability",
    unit: "ms",
    direction: "lower",
    description: "Execution time to purge a tenant partition of 10,000 keys. Lower is better.",
    results: {
      sider: { value: 1.2, formatted: "1.2 ms", note: "Prefix tombstone sweep" },
      redis: { value: 46.0, formatted: "46.0 ms", note: "SCAN + DEL Lua script" },
      sqlite: { value: 118.0, formatted: "118.0 ms", note: "DELETE WHERE prefix LIKE" },
    },
  },
  {
    id: "memory-footprint",
    name: "Baseline Memory Footprint",
    category: "resource",
    unit: "MB",
    direction: "lower",
    description: "Engine RAM overhead holding 50K indexed keys. Lower is better.",
    results: {
      sqlite: { value: 8.5, formatted: "8.5 MB", note: "Embedded C engine" },
      sider: { value: 18.2, formatted: "18.2 MB", note: "Lean Go LSM MemTable" },
      redis: { value: 44.6, formatted: "44.6 MB", note: "jemalloc overhead" },
    },
  },
];

export function BenchmarkSection() {
  const [activeFilter, setActiveFilter] = useState<"all" | "throughput" | "latency" | "durability" | "resource">("all");

  const filteredTests = BENCHMARK_TESTS.filter(
    (t) => activeFilter === "all" || t.category === activeFilter
  );

  return (
    <section className="w-full bg-[#e7e5e4] text-[#000000] px-6 sm:px-12 py-24 sm:py-32 select-none border-t border-b border-[#dfdcd5]">
      {/* Corner Labels: Opposite corners at 12px uppercase in Helvetica Now */}
      <div className="w-full max-w-7xl mx-auto flex items-center justify-between text-[#595855] text-[12px] uppercase tracking-[0.1em] mb-12 sm:mb-16">
        <span style={{ fontFamily: "var(--font-helvetica-now)" }}>FOLIO NO. 04</span>
        <span style={{ fontFamily: "var(--font-helvetica-now)" }}>EMPIRICAL BENCHMARKS</span>
      </div>

      {/* Centered Section Header: Davinci 94px weight 500, #000000, letter-spacing -0.85px, line-height 0.84 */}
      <ScrollReveal className="w-full max-w-5xl mx-auto text-center mb-16 sm:mb-20">
        <h2
          className="text-[#000000] text-[48px] sm:text-[72px] md:text-[94px] font-medium leading-[0.84] tracking-[-0.85px]"
          style={{ fontFamily: "var(--font-davinci)" }}
        >
          BENCHMARK STUDY
        </h2>
        <p
          className="text-[#595855] text-[15px] mt-4 max-w-2xl mx-auto"
          style={{ fontFamily: "var(--font-helvetica-now)" }}
        >
          Standardized empirical evaluation comparing Sider v2 (LSM-Tree), Redis 7.2 (AOF), and SQLite 3.45 (WAL).
        </p>
      </ScrollReveal>

      {/* Stat Pairs Row: Helvetica Now 16px weight 500, separated by 28px gap */}
      <ScrollReveal delayMs={100} className="w-full max-w-4xl mx-auto flex flex-wrap items-center justify-center gap-[28px] text-[#000000] text-[16px] font-medium tracking-[0.04em] uppercase mb-14 text-center">
        <span>WRITE THROUGHPUT: 125,000 OPS</span>
        <span className="hidden sm:inline text-[#595855]">•</span>
        <span>DISK SKIP RATIO: 91.4%</span>
        <span className="hidden sm:inline text-[#595855]">•</span>
        <span>RECOVERY DURATION: 82MS</span>
      </ScrollReveal>

      {/* Benchmark Category Filter Tabs */}
      <div className="w-full max-w-5xl mx-auto flex flex-wrap items-center justify-center gap-2 mb-10 pb-4 border-b border-[#dfdcd5]">
        {[
          { key: "all", label: "All Tests (8)" },
          { key: "throughput", label: "Throughput" },
          { key: "latency", label: "Latency (p99)" },
          { key: "durability", label: "Durability & Recovery" },
          { key: "resource", label: "Resource Efficiency" },
        ].map((tab) => (
          <button
            key={tab.key}
            onClick={() => setActiveFilter(tab.key as any)}
            className={`text-[12px] uppercase tracking-[0.06em] px-3.5 py-1.5 rounded-[2px] transition-colors ${
              activeFilter === tab.key
                ? "bg-[#000000] text-[#ffffff]"
                : "text-[#595855] hover:text-[#000000] bg-transparent"
            }`}
            style={{ fontFamily: "var(--font-helvetica-now)" }}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* AI Benchmark Bar Chart Grid with 3D Tilt */}
      <div className="w-full max-w-5xl mx-auto space-y-6 mb-16">
        {filteredTests.map((test, idx) => {
          const vals = [
            test.results.sider.value,
            test.results.redis.value,
            test.results.sqlite.value,
          ].filter((v) => v > 0);
          const maxVal = Math.max(...vals, 1);
          const minVal = Math.min(...vals, 1);

          const calcWidth = (val: number) => {
            if (val <= 0) return 6;
            if (test.direction === "higher") {
              return Math.max(10, Math.round((val / maxVal) * 100));
            } else {
              const ratio = (minVal / val);
              return Math.max(12, Math.round(ratio * 100));
            }
          };

          return (
            <ScrollReveal key={test.id} delayMs={idx * 60} enableTilt={true}>
              <div
                className="bg-[#c4c3b6] p-[24px] rounded-[9px] border border-[#dfdcd5] select-text shadow-none"
              >
                {/* Test Header */}
                <div className="flex flex-col sm:flex-row sm:items-baseline justify-between gap-1 mb-2">
                  <div className="flex items-center gap-3">
                    <span
                      className="text-[14px] font-semibold text-[#000000] uppercase tracking-[0.04em]"
                      style={{ fontFamily: "var(--font-helvetica-now)" }}
                    >
                      {test.name}
                    </span>
                    <span
                      className="text-[10px] uppercase tracking-[0.1em] px-2 py-0.5 rounded-[2px] bg-[#dfdcd5] text-[#595855] font-mono"
                    >
                      {test.direction === "higher" ? "Higher is better" : "Lower is better"}
                    </span>
                  </div>
                  <span
                    className="text-[11px] text-[#595855]"
                    style={{ fontFamily: "var(--font-helvetica-now)" }}
                  >
                    Unit: {test.unit}
                  </span>
                </div>

                <p
                  className="text-[12px] text-[#595855] mb-5 leading-normal"
                  style={{ fontFamily: "var(--font-helvetica-now)" }}
                >
                  {test.description}
                </p>

                {/* Chart Bars */}
                <div className="space-y-3 font-mono text-[12px]">
                  {/* Sider v2 Bar */}
                  <div className="flex flex-col sm:flex-row sm:items-center gap-2">
                    <div className="w-32 shrink-0 flex items-center justify-between text-[#000000] font-medium text-[11px]">
                      <span>Sider v2 (LSM)</span>
                      <span className="text-[10px] text-[#000000] font-bold">★</span>
                    </div>
                    <div className="flex-1 bg-[#dfdcd5]/60 h-7 rounded-[4px] overflow-hidden flex items-center relative">
                      <div
                        className="bg-[#000000] h-full flex items-center px-3 text-[#ffffff] font-semibold text-[11px] whitespace-nowrap transition-all duration-700 rounded-[2px]"
                        style={{ width: `${calcWidth(test.results.sider.value)}%` }}
                      >
                        {test.results.sider.formatted}
                      </div>
                    </div>
                    <div className="text-[10px] text-[#595855] sm:w-44 shrink-0">
                      {test.results.sider.note}
                    </div>
                  </div>

                  {/* Redis 7.2 Bar */}
                  <div className="flex flex-col sm:flex-row sm:items-center gap-2">
                    <div className="w-32 shrink-0 text-[#595855] text-[11px]">
                      Redis 7.2 (AOF)
                    </div>
                    <div className="flex-1 bg-[#dfdcd5]/60 h-7 rounded-[4px] overflow-hidden flex items-center relative">
                      <div
                        className="bg-[#595855] h-full flex items-center px-3 text-[#ffffff] text-[11px] whitespace-nowrap transition-all duration-700 rounded-[2px]"
                        style={{ width: `${calcWidth(test.results.redis.value)}%` }}
                      >
                        {test.results.redis.formatted}
                      </div>
                    </div>
                    <div className="text-[10px] text-[#808080] sm:w-44 shrink-0">
                      {test.results.redis.note}
                    </div>
                  </div>

                  {/* SQLite 3.45 Bar */}
                  <div className="flex flex-col sm:flex-row sm:items-center gap-2">
                    <div className="w-32 shrink-0 text-[#595855] text-[11px]">
                      SQLite 3.45 (WAL)
                    </div>
                    <div className="flex-1 bg-[#dfdcd5]/60 h-7 rounded-[4px] overflow-hidden flex items-center relative">
                      <div
                        className="bg-[#808080] h-full flex items-center px-3 text-[#ffffff] text-[11px] whitespace-nowrap transition-all duration-700 rounded-[2px]"
                        style={{ width: `${calcWidth(test.results.sqlite.value)}%` }}
                      >
                        {test.results.sqlite.formatted}
                      </div>
                    </div>
                    <div className="text-[10px] text-[#808080] sm:w-44 shrink-0">
                      {test.results.sqlite.note}
                    </div>
                  </div>
                </div>
              </div>
            </ScrollReveal>
          );
        })}
      </div>

      {/* Comprehensive Empirical Results Matrix */}
      <ScrollReveal delayMs={200} className="w-full max-w-5xl mx-auto bg-[#c4c3b6] p-[24px] rounded-[9px] border border-[#dfdcd5]">
        <div className="border-b border-[#dfdcd5] pb-3 mb-4 flex items-center justify-between">
          <div className="text-[12px] uppercase font-semibold tracking-[0.08em] text-[#000000]">
            Empirical Results Matrix
          </div>
          <div className="text-[10px] uppercase font-mono text-[#595855]">
            Hardware: Apple Silicon M-series &bull; NVMe SSD &bull; 16GB
          </div>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-left text-[13px]" style={{ fontFamily: "var(--font-helvetica-now)" }}>
            <thead>
              <tr className="border-b border-[#000000]/20 text-[11px] uppercase tracking-[0.12em] text-[#595855]">
                <th className="py-3 px-3 font-medium">Test Dimension</th>
                <th className="py-3 px-3 font-semibold text-[#000000]">Sider v2 (LSM)</th>
                <th className="py-3 px-3 font-medium">Redis 7.2 (AOF)</th>
                <th className="py-3 px-3 font-medium">SQLite 3.45 (WAL)</th>
                <th className="py-3 px-3 font-medium">Advantage</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-[#dfdcd5]">
              <tr>
                <td className="py-3 px-3 font-medium text-[#000000]">Sequential Write Ingestion</td>
                <td className="py-3 px-3 font-semibold text-[#000000]">125,000 ops/s</td>
                <td className="py-3 px-3 text-[#595855]">110,000 ops/s</td>
                <td className="py-3 px-3 text-[#595855]">24,000 ops/s</td>
                <td className="py-3 px-3 font-mono text-[11px] text-[#000000]">+13.6% vs Redis</td>
              </tr>
              <tr>
                <td className="py-3 px-3 font-medium text-[#000000]">Random Read Lookup</td>
                <td className="py-3 px-3 font-semibold text-[#000000]">138,000 ops/s</td>
                <td className="py-3 px-3 text-[#595855]">142,000 ops/s</td>
                <td className="py-3 px-3 text-[#595855]">41,000 ops/s</td>
                <td className="py-3 px-3 font-mono text-[11px] text-[#595855]">3.36x vs SQLite</td>
              </tr>
              <tr>
                <td className="py-3 px-3 font-medium text-[#000000]">P99 Read Latency</td>
                <td className="py-3 px-3 font-semibold text-[#000000]">0.42 ms</td>
                <td className="py-3 px-3 text-[#595855]">0.38 ms</td>
                <td className="py-3 px-3 text-[#595855]">3.90 ms</td>
                <td className="py-3 px-3 font-mono text-[11px] text-[#595855]">Sub-millisecond</td>
              </tr>
              <tr>
                <td className="py-3 px-3 font-medium text-[#000000]">P99 Durable Write (fsync)</td>
                <td className="py-3 px-3 font-semibold text-[#000000]">0.58 ms</td>
                <td className="py-3 px-3 text-[#595855]">4.12 ms</td>
                <td className="py-3 px-3 text-[#595855]">8.45 ms</td>
                <td className="py-3 px-3 font-mono text-[11px] text-[#000000]">7.1x lower latency</td>
              </tr>
              <tr>
                <td className="py-3 px-3 font-medium text-[#000000]">Crash Recovery Duration</td>
                <td className="py-3 px-3 font-semibold text-[#000000]">82 ms</td>
                <td className="py-3 px-3 text-[#595855]">1,250 ms</td>
                <td className="py-3 px-3 text-[#595855]">140 ms</td>
                <td className="py-3 px-3 font-mono text-[11px] text-[#000000]">15.2x faster recovery</td>
              </tr>
              <tr>
                <td className="py-3 px-3 font-medium text-[#000000]">Bloom Disk I/O Bypass</td>
                <td className="py-3 px-3 font-semibold text-[#000000]">91.4% skip</td>
                <td className="py-3 px-3 text-[#595855]">N/A (RAM)</td>
                <td className="py-3 px-3 text-[#595855]">0.0% (reads disk)</td>
                <td className="py-3 px-3 font-mono text-[11px] text-[#000000]">90%+ I/O eliminated</td>
              </tr>
              <tr>
                <td className="py-3 px-3 font-medium text-[#000000]">Namespace CLEAR (10K keys)</td>
                <td className="py-3 px-3 font-semibold text-[#000000]">1.2 ms</td>
                <td className="py-3 px-3 text-[#595855]">46.0 ms</td>
                <td className="py-3 px-3 text-[#595855]">118.0 ms</td>
                <td className="py-3 px-3 font-mono text-[11px] text-[#000000]">38x faster purge</td>
              </tr>
              <tr>
                <td className="py-3 px-3 font-medium text-[#000000]">Baseline RAM Overhead</td>
                <td className="py-3 px-3 font-semibold text-[#000000]">18.2 MB</td>
                <td className="py-3 px-3 text-[#595855]">44.6 MB</td>
                <td className="py-3 px-3 text-[#595855]">8.5 MB</td>
                <td className="py-3 px-3 font-mono text-[11px] text-[#000000]">59% less than Redis</td>
              </tr>
            </tbody>
          </table>
        </div>

        {/* Footnote micro-label */}
        <div className="mt-4 pt-3 border-t border-[#dfdcd5] flex flex-col sm:flex-row sm:items-center justify-between gap-2 text-[11px] text-[#595855]">
          <span>Each test was executed across 10 randomized runs of 100,000 keys with cache flushes between cycles.</span>
          <span className="font-mono text-[10px] uppercase font-semibold text-[#000000]">REPRODUCIBLE &bull; ZERO EXTERNAL LIBS</span>
        </div>

        {/* Cloud Alpha Live Benchmark CTA */}
        <div className="mt-8 p-4 sm:p-5 rounded-[16px] bg-[#000000] text-[#ffffff] flex flex-col sm:flex-row items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <span className="w-2.5 h-2.5 rounded-full bg-[#19a05f] animate-pulse shrink-0" />
            <span className="text-[13px] text-[#dfdcd5]">
              Verify these metrics live: <strong className="text-white">Sider Cloud Public Alpha</strong> is clocking <strong className="text-[#00f0ff]">316,746 ops/sec</strong> on bare-metal region <code className="text-[#00f0ff] font-mono bg-white/10 px-1.5 py-0.5 rounded">ind-tbn-1</code>.
            </span>
          </div>
          <a
            href="https://sider-cloud.vercel.app"
            className="w-full sm:w-auto px-5 py-2.5 rounded-[30px] bg-[#405bff] hover:bg-[#344bd6] text-white text-[12px] font-medium transition-all shadow-[0_0_15px_rgba(64,91,255,0.4)] text-center whitespace-nowrap"
          >
            Launch Cloud Cockpit &rarr;
          </a>
        </div>
      </ScrollReveal>
    </section>
  );
}

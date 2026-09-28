"use client";

import React from "react";

const OPS = [
  { type: "PUT", key: "session:token_9482", latency: "0.12ms", tier: "memtable", status: "OK" },
  { type: "GET", key: "user:profile_421", latency: "0.24ms", tier: "memtable", status: "HIT" },
  { type: "WAL", key: "fsync_batch_40", latency: "0.23ms", tier: "nvme-ext4", status: "SYNCED" },
  { type: "BLOOM", key: "miss:random_key", latency: "0.18ms", tier: "ram-filter", status: "REJECTED" },
  { type: "PUT", key: "telemetry:temp_01", latency: "0.14ms", tier: "memtable", status: "OK" },
  { type: "GET", key: "order:items_882", latency: "0.21ms", tier: "memtable", status: "HIT" },
  { type: "COMPACT", key: "L0 -> L1 merge", latency: "1.42ms", tier: "sstable", status: "OPTIMIZED" },
  { type: "BENCH", key: "100_concurrent", latency: "234µs", tier: "alpha-node", status: "316K OPS/S" },
];

export function KineticStreamRibbon() {
  return (
    <div className="w-full overflow-hidden border-y border-[#414042]/50 bg-[#141414]/90 py-2.5 backdrop-blur-md relative select-none">
      {/* Edge gradient masks */}
      <div className="absolute left-0 top-0 bottom-0 w-24 bg-gradient-to-r from-[#0e0e0e] to-transparent z-10 pointer-events-none" />
      <div className="absolute right-0 top-0 bottom-0 w-24 bg-gradient-to-l from-[#0e0e0e] to-transparent z-10 pointer-events-none" />

      {/* Infinite Swooping Marquee Track */}
      <div className="flex items-center gap-6 w-max animate-[marquee_25s_linear_infinite] hover:[animation-play-state:paused]">
        {[...OPS, ...OPS, ...OPS].map((op, i) => (
          <div
            key={i}
            className="flex items-center gap-2.5 px-3 py-1 rounded-[30px] bg-[#0e0e0e] border border-white/10 text-[11px] font-mono whitespace-nowrap shadow-sm hover:border-[#405bff] transition-colors"
          >
            <span
              className={`w-1.5 h-1.5 rounded-full ${
                op.type === "PUT"
                  ? "bg-[#405bff]"
                  : op.type === "GET"
                  ? "bg-[#19a05f]"
                  : op.type === "WAL"
                  ? "bg-[#7084ff]"
                  : op.type === "BENCH"
                  ? "bg-[#00f0ff] animate-pulse"
                  : "bg-[#eab308]"
              }`}
            />
            <span className="font-semibold text-white">{op.type}</span>
            <span className="text-[#a7a9ac]">{op.key}</span>
            <span className="text-[#405bff] font-semibold">{op.latency}</span>
            <span className="text-[10px] text-[#6d6e71] px-1.5 py-0.2 rounded bg-white/5 border border-white/5 uppercase">
              {op.status}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

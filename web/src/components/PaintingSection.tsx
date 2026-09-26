"use client";

import React, { useState, useRef } from "react";

export function PaintingSection() {
  const [writeCount, setWriteCount] = useState(104820);
  const [lastOp, setLastOp] = useState<{ op: string; key: string; ts: string }>({
    op: "PUTEX",
    key: "auth:token:9a7f",
    ts: "0.18ms",
  });
  const [isFlushing, setIsFlushing] = useState(false);
  const [tiltStyle, setTiltStyle] = useState<React.CSSProperties>({});
  const cardRef = useRef<HTMLDivElement>(null);

  const handleSimulateWrite = () => {
    setIsFlushing(true);
    setWriteCount((prev) => prev + 1);
    const keys = ["cache:user:204", "session:v2:active", "pubsub:channel:01", "rate:limit:ip"];
    const chosen = keys[Math.floor(Math.random() * keys.length)];
    setLastOp({
      op: "PUT",
      key: chosen,
      ts: (0.12 + Math.random() * 0.15).toFixed(2) + "ms",
    });
    setTimeout(() => setIsFlushing(false), 300);
  };

  const handleMouseMove = (e: React.MouseEvent<HTMLDivElement>) => {
    if (!cardRef.current) return;
    const rect = cardRef.current.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    const centerX = rect.width / 2;
    const centerY = rect.height / 2;

    const rotX = ((y - centerY) / centerY) * -8;
    const rotY = ((x - centerX) / centerX) * 8;

    setTiltStyle({
      transform: `perspective(1000px) rotateX(${rotX.toFixed(2)}deg) rotateY(${rotY.toFixed(2)}deg)`,
      transition: "transform 0.08s ease-out",
    });
  };

  const handleMouseLeave = () => {
    setTiltStyle({
      transform: "perspective(1000px) rotateX(0deg) rotateY(0deg)",
      transition: "transform 0.6s ease-out",
    });
  };

  return (
    <section className="relative z-10 w-full min-h-screen overflow-hidden flex items-center justify-center bg-[#000000] border-t border-[#dfdcd5]">
      {/* Full-Bleed Classical Painting Panel:
          Renaissance/Baroque oil painting reproduction, edge-to-edge with no border or rounded corners, no overlay.
          The image fills the entire section viewport and scrolls up to cleanly hide the hero section. */}
      <div
        className="absolute inset-0 w-full h-full bg-cover bg-center"
        style={{
          backgroundImage: "url('/paintings/landscape_hd.jpg')",
          backgroundRepeat: "no-repeat",
          backgroundSize: "cover",
          backgroundPosition: "center 42%",
        }}
        aria-label="Heroic Landscape with Rainbow — classical oil painting panel"
      />

      {/* Floating Centered Dark Notched Product Card:
          Dark card (~400px square) with notched/hexagonal corner cuts, #000000 background,
          #ffffff micro-label ('SCROLL' at 9px Helvetica Now) in lower-left, plus 3D interactive tilt. */}
      <div
        ref={cardRef}
        onMouseMove={handleMouseMove}
        onMouseLeave={handleMouseLeave}
        style={tiltStyle}
        className="relative z-10 w-[90vw] max-w-[420px] aspect-square bg-[#000000] p-7 flex flex-col justify-between select-none notched-product-card shadow-none"
      >
        {/* Card Header */}
        <div>
          <div className="flex items-center justify-between border-b border-[#595855]/60 pb-3 mb-4">
            <span
              className="text-[#ffffff] text-[12px] font-medium tracking-[0.06em] uppercase"
              style={{ fontFamily: "var(--font-helvetica-now)" }}
            >
              ENGINE SPECIFICATION
            </span>
            <span
              className="text-[#808080] text-[10px] tracking-[0.04em] uppercase font-mono"
              style={{ fontFamily: "var(--font-helvetica-now)" }}
            >
              TCP :4000
            </span>
          </div>

          <h3
            className="text-[#ffffff] text-[22px] leading-[1.33] tracking-[-0.11px] font-normal mb-3"
            style={{ fontFamily: "var(--font-davinci)" }}
          >
            LSM MemTable & Durability
          </h3>

          <p
            className="text-[#dfdcd5] text-[13px] leading-[1.5] mb-5"
            style={{ fontFamily: "var(--font-helvetica-now)", fontWeight: 400 }}
          >
            Concurrent SkipList in memory, synchronous Write-Ahead Log (WAL), and immutable Bloom-indexed SSTables on disk.
          </p>

          {/* Telemetry rows */}
          <div className="space-y-2 border-t border-[#595855]/40 pt-3">
            <div className="flex items-center justify-between text-[11px]" style={{ fontFamily: "var(--font-helvetica-now)" }}>
              <span className="text-[#808080] uppercase tracking-wider">WAL Engine</span>
              <span className="text-[#ffffff] font-medium">sider.wal (fsync safe)</span>
            </div>
            <div className="flex items-center justify-between text-[11px]" style={{ fontFamily: "var(--font-helvetica-now)" }}>
              <span className="text-[#808080] uppercase tracking-wider">Bloom Filter</span>
              <span className="text-[#ffffff] font-medium">FNV-1a BitSet (90% skip)</span>
            </div>
            <div className="flex items-center justify-between text-[11px]" style={{ fontFamily: "var(--font-helvetica-now)" }}>
              <span className="text-[#808080] uppercase tracking-wider">Writes Ingested</span>
              <span className="text-[#ffffff] font-mono">{writeCount.toLocaleString()} ops</span>
            </div>
            <div className="flex items-center justify-between text-[11px]" style={{ fontFamily: "var(--font-helvetica-now)" }}>
              <span className="text-[#808080] uppercase tracking-wider">Latest Op</span>
              <span className="text-[#dfdcd5] font-mono">
                {lastOp.op} {lastOp.key} ({lastOp.ts})
              </span>
            </div>
          </div>
        </div>

        {/* Card Footer: Micro-label 'SCROLL' at 9px Helvetica Now in lower-left */}
        <div className="flex items-center justify-between pt-3 border-t border-[#595855]/40">
          <div
            className="text-[#ffffff] text-[9px] uppercase tracking-[0.14em]"
            style={{ fontFamily: "var(--font-helvetica-now)", fontWeight: 400 }}
          >
            SCROLL
          </div>

          <button
            onClick={handleSimulateWrite}
            disabled={isFlushing}
            className="text-[10px] text-[#ffffff] hover:text-[#c4c3b6] uppercase tracking-[0.08em] underline transition-colors cursor-pointer bg-transparent border-0 p-0"
            style={{ fontFamily: "var(--font-helvetica-now)" }}
          >
            {isFlushing ? "Flushing to disk..." : "Simulate Append →"}
          </button>
        </div>
      </div>
    </section>
  );
}

"use client";

import React, { useState } from "react";
import { HexagonGroup } from "./Hexagon";
import { ThreeDarkCanvas } from "./ThreeDarkCanvas";
import { ScrollReveal } from "./ScrollReveal";

export function FeatureSection() {
  const [activeDots, setActiveDots] = useState<[number, number, number]>([0, 1, 2]);

  const toggleDot = (colIndex: number) => {
    setActiveDots((prev) => {
      const next = [...prev] as [number, number, number];
      next[colIndex] = (next[colIndex] + 1) % 3;
      return next;
    });
  };

  return (
    <section className="relative z-10 w-full bg-[#000000] text-[#ffffff] px-6 sm:px-12 py-24 sm:py-32 select-none overflow-hidden">
      {/* Three.js Constellation of LSM Nodes */}
      <ThreeDarkCanvas />

      <div className="relative z-10 w-full max-w-7xl mx-auto">
        {/* Corner Labels: Opposite corners at 12px uppercase in Helvetica Now */}
        <div className="w-full flex items-center justify-between text-[#808080] text-[12px] uppercase tracking-[0.1em] mb-12 sm:mb-16">
          <span style={{ fontFamily: "var(--font-helvetica-now)" }}>FOLIO NO. 02</span>
          <span style={{ fontFamily: "var(--font-helvetica-now)" }}>ENGINE ARCHITECTURE</span>
        </div>

        {/* Centered Section Header: Davinci 94px weight 500, #ffffff, letter-spacing -0.85px, line-height 0.84 */}
        <ScrollReveal className="w-full max-w-5xl mx-auto text-center mb-20 sm:mb-28">
          <h2
            className="text-[#ffffff] text-[48px] sm:text-[72px] md:text-[94px] font-medium leading-[0.84] tracking-[-0.85px]"
            style={{ fontFamily: "var(--font-davinci)" }}
          >
            SIDER V2 EXPLAINED
          </h2>
        </ScrollReveal>

        {/* 3-Column Grid:
            Each column has a Davinci 22px caption in #ffffff,
            a ~200px circular image crop,
            and a 12px hexagonal outline indicator in #ffffff stroke below. Column gap 28px. */}
        <div className="w-full max-w-6xl mx-auto grid grid-cols-1 md:grid-cols-3 gap-[28px]">
          {/* Column 1: MemTable & WAL Durability */}
          <ScrollReveal delayMs={100}>
            <div
              className="flex flex-col items-center text-center cursor-pointer group"
              onClick={() => toggleDot(0)}
            >
              {/* Davinci serif caption (22–24px) above */}
              <h3
                className="text-[#ffffff] text-[22px] leading-[1.33] tracking-[-0.11px] font-normal mb-6 min-h-[58px] flex items-center justify-center transition-transform group-hover:-translate-y-1"
                style={{ fontFamily: "var(--font-davinci)" }}
              >
                MemTable & WAL Durability
              </h3>

              {/* Circular crop of classical painting (~200px diameter, no border, no shadow) */}
              <div className="w-[200px] h-[200px] rounded-full overflow-hidden mb-6 shrink-0 bg-[#808080]">
                <img
                  src="/paintings/hare_hd.jpg"
                  alt="Jan Fyt — Hare study representing MemTable durability"
                  className="w-full h-full object-cover grayscale-0 group-hover:scale-110 transition-transform duration-700"
                />
              </div>

              {/* Hexagonal nav indicator below (groups of 3 ~12px) */}
              <div className="mb-6">
                <HexagonGroup
                  stroke="#ffffff"
                  activeIndex={activeDots[0]}
                  count={3}
                  size={12}
                />
              </div>

              {/* Technical copy in Helvetica Now 15px */}
              <p
                className="text-[#dfdcd5] text-[15px] leading-[1.5] max-w-[310px]"
                style={{ fontFamily: "var(--font-helvetica-now)", fontWeight: 400 }}
              >
                Sequential append writes land in memory and disk synchronously. Crashes recover instantaneously by replaying the Write-Ahead Log without data loss.
              </p>
            </div>
          </ScrollReveal>

          {/* Column 2: Bloom Filter & 90% Disk Skip */}
          <ScrollReveal delayMs={200}>
            <div
              className="flex flex-col items-center text-center cursor-pointer group"
              onClick={() => toggleDot(1)}
            >
              {/* Davinci serif caption (22–24px) above */}
              <h3
                className="text-[#ffffff] text-[22px] leading-[1.33] tracking-[-0.11px] font-normal mb-6 min-h-[58px] flex items-center justify-center transition-transform group-hover:-translate-y-1"
                style={{ fontFamily: "var(--font-davinci)" }}
              >
                Bloom Filter Indexing
              </h3>

              {/* Circular crop of classical painting (~200px diameter, no border, no shadow) */}
              <div className="w-[200px] h-[200px] rounded-full overflow-hidden mb-6 shrink-0 bg-[#808080]">
                <img
                  src="/paintings/amphora_hd.jpg"
                  alt="Orsola Caccia — Amphora study representing SSTable Bloom indexing"
                  className="w-full h-full object-cover grayscale-0 group-hover:scale-110 transition-transform duration-700"
                />
              </div>

              {/* Hexagonal nav indicator below (groups of 3 ~12px) */}
              <div className="mb-6">
                <HexagonGroup
                  stroke="#ffffff"
                  activeIndex={activeDots[1]}
                  count={3}
                  size={12}
                />
              </div>

              {/* Technical copy in Helvetica Now 15px */}
              <p
                className="text-[#dfdcd5] text-[15px] leading-[1.5] max-w-[310px]"
                style={{ fontFamily: "var(--font-helvetica-now)", fontWeight: 400 }}
              >
                Every SSTable is indexed by an FNV-1a BitSet Bloom filter. Non-existent keys are caught before touching disk, eliminating 90%+ of random disk penalties.
              </p>
            </div>
          </ScrollReveal>

          {/* Column 3: Streaming Pub/Sub Broker */}
          <ScrollReveal delayMs={300}>
            <div
              className="flex flex-col items-center text-center cursor-pointer group"
              onClick={() => toggleDot(2)}
            >
              {/* Davinci serif caption (22–24px) above */}
              <h3
                className="text-[#ffffff] text-[22px] leading-[1.33] tracking-[-0.11px] font-normal mb-6 min-h-[58px] flex items-center justify-center transition-transform group-hover:-translate-y-1"
                style={{ fontFamily: "var(--font-davinci)" }}
              >
                Streaming Pub/Sub Broker
              </h3>

              {/* Circular crop of classical painting (~200px diameter, no border, no shadow) */}
              <div className="w-[200px] h-[200px] rounded-full overflow-hidden mb-6 shrink-0 bg-[#808080]">
                <img
                  src="/paintings/butterfly_hd.jpg"
                  alt="Joris Hoefnagel — Butterfly study representing real-time message fan-out"
                  className="w-full h-full object-cover grayscale-0 group-hover:scale-110 transition-transform duration-700"
                />
              </div>

              {/* Hexagonal nav indicator below (groups of 3 ~12px) */}
              <div className="mb-6">
                <HexagonGroup
                  stroke="#ffffff"
                  activeIndex={activeDots[2]}
                  count={3}
                  size={12}
                />
              </div>

              {/* Technical copy in Helvetica Now 15px */}
              <p
                className="text-[#dfdcd5] text-[15px] leading-[1.5] max-w-[310px]"
                style={{ fontFamily: "var(--font-helvetica-now)", fontWeight: 400 }}
              >
                Multiplexed non-blocking TCP broker with broadcast channel fan-out. Stream notifications, invalidations, and events without Redis or Kafka overhead.
              </p>
            </div>
          </ScrollReveal>
        </div>
      </div>
    </section>
  );
}

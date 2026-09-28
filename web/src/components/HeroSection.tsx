"use client";

import React from "react";

export function HeroSection({ onExploreClick }: { onExploreClick?: () => void }) {
  return (
    <section
      id="top"
      className="w-full h-full min-h-[calc(100vh-3.5rem)] bg-[#c4c3b6] flex flex-col justify-between overflow-hidden pt-12 sm:pt-16 pb-0 select-none relative"
    >
      {/* Center Cluster */}
      <div className="flex-1 flex flex-col items-center justify-center text-center px-4 max-w-4xl mx-auto z-10">
        {/* Sub-headline: Davinci 52px weight 500, #000000, letter-spacing -0.47px ('on' in italic) */}
        <h1
          className="text-[#000000] text-[36px] sm:text-[46px] md:text-[52px] leading-[1.0] tracking-[-0.47px] font-medium mb-6 select-none"
          style={{ fontFamily: "var(--font-davinci)" }}
        >
          HIGH PERSISTENCE <span className="italic font-normal">on</span> SILICON
        </h1>

        {/* Stat Pair: Two values side by side, Helvetica Now 16px weight 500, separated by 28px gap */}
        <div
          className="flex flex-wrap items-center justify-center gap-[28px] text-[#000000] text-[15px] sm:text-[16px] font-medium tracking-[0.04em] uppercase mb-8"
          style={{ fontFamily: "var(--font-helvetica-now)" }}
        >
          <span>THROUGHPUT: 125,000 OPS</span>
          <span className="hidden sm:inline text-[#595855]">•</span>
          <span>LATENCY: &lt; 1MS</span>
        </div>

        {/* Action Buttons: Primary CTA + Cloud Alpha CTA + GitHub Repo */}
        <div className="flex flex-wrap items-center justify-center gap-3">
          <a
            href="https://sider-cloud.vercel.app"
            className="pill-button inline-flex items-center justify-center gap-2 text-[#ffffff] bg-[#000000] px-[20px] py-[9px] rounded-[28.8px] text-[12px] font-medium tracking-wide transition-all hover:scale-105 hover:bg-[#1a1a1a] no-underline shadow-none"
            style={{ fontFamily: "var(--font-helvetica-now)" }}
          >
            <span className="w-1.5 h-1.5 rounded-full bg-[#405bff] animate-pulse" />
            <span>test sider cloud alpha &rarr;</span>
          </a>

          <a
            href="#interactive-terminal"
            onClick={onExploreClick}
            className="pill-button inline-flex items-center justify-center text-[#ffffff] bg-[#000000] px-[17px] py-[9px] rounded-[28.8px] text-[12px] font-normal tracking-wide transition-all hover:scale-105 hover:opacity-90 no-underline shadow-none"
            style={{ fontFamily: "var(--font-helvetica-now)" }}
          >
            launch sider engine
          </a>

          <a
            href="https://github.com/AgnibhaRay/sider"
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center justify-center gap-2 text-[#000000] bg-transparent border border-[#000000] px-[17px] py-[9px] rounded-[28.8px] text-[12px] font-normal tracking-wide transition-all hover:bg-[#000000] hover:text-[#ffffff] no-underline shadow-none"
            style={{ fontFamily: "var(--font-helvetica-now)" }}
          >
            <svg
              width="14"
              height="14"
              viewBox="0 0 24 24"
              fill="currentColor"
              aria-hidden="true"
            >
              <path
                fillRule="evenodd"
                clipRule="evenodd"
                d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.53 1.032 1.53 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z"
              />
            </svg>
            <span>view source on github ↗</span>
          </a>
        </div>
      </div>

      {/* Monumental Hero Wordmark:
          Davinci serif at 374px weight 500, color #000000, letter-spacing -3.37px, line-height 0.84.
          Extends beyond the visible viewport width — intentionally cropped at the edges.
          The brand IS this wordmark at this scale. */}
      <div className="w-full flex justify-center items-end overflow-hidden select-none pointer-events-none mt-6 sm:mt-10 z-10">
        <div
          className="text-[#000000] font-medium tracking-[-3.37px] leading-[0.84] text-center whitespace-nowrap"
          style={{
            fontFamily: "var(--font-davinci)",
            fontSize: "clamp(120px, 25vw, 374px)",
            transform: "translateY(12%)",
          }}
          aria-hidden="true"
        >
          SIDER
        </div>
      </div>
    </section>
  );
}

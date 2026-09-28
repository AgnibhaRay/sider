"use client";

import React from "react";
import Link from "next/link";
import { SiderLogo } from "./SiderLogo";

export function Footer() {
  return (
    <footer className="relative z-20 w-full bg-[#ebebeb] text-[#000000] px-4 sm:px-12 py-16 border-t border-[#dfdcd5] select-none">
      <div className="w-full max-w-7xl mx-auto">
        
        {/* Sider Cloud Alpha Callout Card */}
        <div className="mb-14 p-6 sm:p-8 rounded-[24px] bg-[#000000] text-[#ffffff] shadow-[0_15px_40px_rgba(0,0,0,0.25)] flex flex-col lg:flex-row lg:items-center justify-between gap-6">
          <div className="space-y-2">
            <div className="flex items-center gap-2">
              <span className="w-2 h-2 rounded-full bg-[#19a05f] animate-pulse" />
              <span className="font-mono text-[11px] uppercase tracking-wider text-[#7084ff] font-semibold">
                PUBLIC ALPHA V0.2.1 ACTIVE // REGION IND-TBN-1
              </span>
            </div>
            <h3 className="text-[22px] sm:text-[28px] font-medium tracking-tight text-white" style={{ fontFamily: "var(--font-helvetica-now)" }}>
              Test Sider Cloud in your browser.
            </h3>
            <p className="text-[13px] sm:text-[14px] text-[#a7a9ac] max-w-xl">
              Zero-install managed LSM engine with native SkipList MemTable, WAL crash recovery, and live web studio telemetry.
            </p>
          </div>

          <div className="flex flex-col sm:flex-row items-stretch sm:items-center gap-3">
            <a
              href="https://sider-cloud.vercel.app"
              className="px-6 py-3 rounded-[30px] bg-[#405bff] hover:bg-[#344bd6] text-white text-[13px] font-medium transition-all shadow-[0_0_20px_rgba(64,91,255,0.4)] flex items-center justify-center gap-2"
              style={{ fontFamily: "var(--font-helvetica-now)" }}
            >
              <span>Launch Sider Cloud Alpha</span>
              <span>&rarr;</span>
            </a>
            <Link
              href="/console"
              className="px-5 py-3 rounded-[30px] bg-[#191919] hover:bg-[#2c2c2c] border border-[#414042] text-white text-[13px] font-medium transition-colors text-center"
              style={{ fontFamily: "var(--font-helvetica-now)" }}
            >
              Console Studio
            </Link>
          </div>
        </div>

        {/* Main Footer Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8 pb-12 border-b border-[#dfdcd5]">
          
          {/* Col 1: Brand & Architect */}
          <div className="space-y-4">
            <div className="flex items-center gap-3">
              <SiderLogo size={32} />
              <div>
                <div
                  className="text-[14px] uppercase font-bold tracking-[0.08em] text-[#000000]"
                  style={{ fontFamily: "var(--font-helvetica-now)" }}
                >
                  SIDER DATABASE
                </div>
                <div
                  className="text-[11px] text-[#595855]"
                  style={{ fontFamily: "var(--font-helvetica-now)" }}
                >
                  Log-Structured Merge Engine &bull; v2.0.0
                </div>
              </div>
            </div>
            
            <p className="text-[13px] text-[#595855] leading-relaxed" style={{ fontFamily: "var(--font-helvetica-now)" }}>
              High-persistence key-value store crafted in pure standard library Go 1.24+ with zero external runtime dependencies.
            </p>

            <div className="p-3.5 rounded-[16px] bg-[#dfdcd5]/60 border border-[#dfdcd5]">
              <span className="text-[10px] font-mono uppercase text-[#595855] block mb-0.5">
                Architect &amp; Author
              </span>
              <span className="text-[14px] font-semibold text-[#000000]">
                Agnibha Ray
              </span>
              <p className="text-[11px] text-[#595855] mt-0.5">
                Systems Engineer &bull; MIT License
              </p>
            </div>
          </div>

          {/* Col 2: Connect with Agnibha Ray */}
          <div className="space-y-3">
            <span
              className="text-[11px] font-mono uppercase tracking-wider text-[#000000] font-semibold block"
              style={{ fontFamily: "var(--font-helvetica-now)" }}
            >
              Connect with Agnibha Ray
            </span>
            
            <div className="flex flex-col gap-2">
              <a
                href="https://github.com/AgnibhaRay"
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center justify-between p-2.5 rounded-[12px] bg-[#ffffff] border border-[#dfdcd5] hover:border-[#000000] transition-all text-[12px] text-[#000000] group"
              >
                <div className="flex items-center gap-2.5 font-medium">
                  <svg className="w-4 h-4 fill-current text-[#000000]" viewBox="0 0 24 24">
                    <path d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 11.385.6.105.825-.255.825-.57 0-.285-.015-1.23-.015-2.235-3.015.555-3.795-.735-4.035-1.41-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.67-.3-5.46-1.335-5.46-5.925 0-1.305.465-2.385 1.23-3.225-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.84 1.23 1.905 1.23 3.225 0 4.605-2.805 5.625-5.475 5.925.435.375.81 1.095.81 2.22 0 1.605-.015 2.895-.015 3.3 0 .315.225.69.825.57A12.02 12.02 0 0024 12c0-6.63-5.37-12-12-12z" />
                  </svg>
                  <span>GitHub Profile</span>
                </div>
                <span className="text-[11px] font-mono group-hover:translate-x-0.5 transition-transform">&rarr;</span>
              </a>

              <a
                href="https://www.linkedin.com/in/agnibharay"
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center justify-between p-2.5 rounded-[12px] bg-[#ffffff] border border-[#dfdcd5] hover:border-[#0077b5] transition-all text-[12px] text-[#000000] group"
              >
                <div className="flex items-center gap-2.5 font-medium">
                  <svg className="w-4 h-4 fill-[#0077b5]" viewBox="0 0 24 24">
                    <path d="M19 0h-14c-2.761 0-5 2.239-5 5v14c0 2.761 2.239 5 5 5h14c2.762 0 5-2.239 5-5v-14c0-2.761-2.238-5-5-5zm-11 19h-3v-11h3v11zm-1.5-12.268c-.966 0-1.75-.79-1.75-1.764s.784-1.764 1.75-1.764 1.75.79 1.75 1.764-.783 1.764-1.75 1.764zm13.5 12.268h-3v-5.604c0-3.368-4-3.113-4 0v5.604h-3v-11h3v1.765c1.396-2.586 7-2.777 7 2.476v6.759z" />
                  </svg>
                  <span>LinkedIn Network</span>
                </div>
                <span className="text-[11px] font-mono text-[#0077b5] group-hover:translate-x-0.5 transition-transform">&rarr;</span>
              </a>

              <a
                href="https://www.instagram.com/agnibharay"
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center justify-between p-2.5 rounded-[12px] bg-[#ffffff] border border-[#dfdcd5] hover:border-[#e1306c] transition-all text-[12px] text-[#000000] group"
              >
                <div className="flex items-center gap-2.5 font-medium">
                  <svg className="w-4 h-4 fill-[#e1306c]" viewBox="0 0 24 24">
                    <path d="M12 2.163c3.204 0 3.584.012 4.85.07 3.252.148 4.771 1.691 4.919 4.919.058 1.265.069 1.645.069 4.849 0 3.205-.012 3.584-.069 4.849-.149 3.225-1.664 4.771-4.919 4.919-1.266.058-1.644.07-4.85.07-3.204 0-3.584-.012-4.849-.07-3.26-.149-4.771-1.699-4.919-4.92-.058-1.265-.07-1.644-.07-4.849 0-3.204.013-3.583.07-4.849.149-3.227 1.664-4.771 4.919-4.919 1.266-.057 1.645-.069 4.849-.069zm0-2.163c-3.259 0-3.667.014-4.947.072-4.358.2-6.78 2.618-6.98 6.98-.059 1.281-.073 1.689-.073 4.948 0 3.259.014 3.668.072 4.948.2 4.358 2.618 6.78 6.98 6.98 1.281.058 1.689.072 4.948.072 3.259 0 3.668-.014 4.948-.072 4.354-.2 6.782-2.618 6.979-6.98.059-1.28.073-1.689.073-4.948 0-3.259-.014-3.667-.072-4.947-.196-4.354-2.617-6.78-6.979-6.98-1.281-.059-1.69-.073-4.949-.073zm0 5.838c-3.403 0-6.162 2.759-6.162 6.162s2.759 6.163 6.162 6.163 6.162-2.759 6.162-6.163c0-3.403-2.759-6.162-6.162-6.162zm0 10.162c-2.209 0-4-1.79-4-4 0-2.209 1.791-4 4-4s4 1.791 4 4c0 2.21-1.791 4-4 4zm6.406-11.845c-.796 0-1.441.645-1.441 1.44s.645 1.44 1.441 1.44c.795 0 1.439-.645 1.439-1.44s-.644-1.44-1.439-1.44z" />
                  </svg>
                  <span>Instagram @agnibharay</span>
                </div>
                <span className="text-[11px] font-mono text-[#e1306c] group-hover:translate-x-0.5 transition-transform">&rarr;</span>
              </a>

              <a
                href="https://github.com/AgnibhaRay/sider"
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center justify-between p-2.5 rounded-[12px] bg-[#ffffff] border border-[#dfdcd5] hover:border-[#19a05f] transition-all text-[12px] text-[#000000] group"
              >
                <div className="flex items-center gap-2.5 font-medium">
                  <span className="text-[#19a05f] font-mono">★</span>
                  <span>Star Sider on GitHub</span>
                </div>
                <span className="text-[11px] font-mono text-[#19a05f] group-hover:translate-x-0.5 transition-transform">&rarr;</span>
              </a>
            </div>
          </div>

          {/* Col 3: Sider Cloud Alpha */}
          <div className="space-y-3">
            <span
              className="text-[11px] font-mono uppercase tracking-wider text-[#000000] font-semibold block"
              style={{ fontFamily: "var(--font-helvetica-now)" }}
            >
              Sider Cloud (Public Alpha)
            </span>
            <ul className="space-y-2 text-[13px] text-[#595855]" style={{ fontFamily: "var(--font-helvetica-now)" }}>
              <li>
                <a href="https://sider-cloud.vercel.app" className="text-[#000000] font-semibold hover:underline flex items-center gap-1.5">
                  <span>Sider Cloud Cockpit</span>
                  <span className="text-[10px] text-[#405bff]">&rarr;</span>
                </a>
              </li>
              <li>
                <a href="https://sider-cloud.vercel.app#build" className="hover:text-[#000000] transition-colors">
                  Cyber-Foundry Build Quests
                </a>
              </li>
              <li>
                <Link href="/console" className="hover:text-[#000000] transition-colors">
                  Web Console Studio
                </Link>
              </li>
              <li>
                <Link href="/docs" className="hover:text-[#000000] transition-colors">
                  Wire Protocol &amp; REST Docs
                </Link>
              </li>
              <li>
                <span className="text-[11px] font-mono text-[#595855]">
                  Cluster: ind-tbn-1 (:4100 TCP)
                </span>
              </li>
            </ul>
          </div>

          {/* Col 4: Engine Documentation & Specs */}
          <div className="space-y-3">
            <span
              className="text-[11px] font-mono uppercase tracking-wider text-[#000000] font-semibold block"
              style={{ fontFamily: "var(--font-helvetica-now)" }}
            >
              Engine Architecture
            </span>
            <ul className="space-y-2 text-[13px] text-[#595855]" style={{ fontFamily: "var(--font-helvetica-now)" }}>
              <li>
                <a href="#interactive-terminal" className="hover:text-[#000000] transition-colors">
                  Live Interactive CLI Folio
                </a>
              </li>
              <li>
                <a href="#empirical-benchmark" className="hover:text-[#000000] transition-colors">
                  Empirical Benchmark Data
                </a>
              </li>
              <li>
                <a href="https://github.com/AgnibhaRay/sider/tree/main/engine" target="_blank" rel="noopener noreferrer" className="hover:text-[#000000] transition-colors">
                  SkipList &amp; WAL Sources
                </a>
              </li>
              <li>
                <a href="https://github.com/AgnibhaRay/sider/blob/main/LICENSE" target="_blank" rel="noopener noreferrer" className="hover:text-[#000000] transition-colors">
                  MIT Open Source License
                </a>
              </li>
            </ul>
          </div>

        </div>

        {/* Bottom Hairline Bar */}
        <div className="mt-8 flex flex-col sm:flex-row items-center justify-between gap-4 text-[11px] text-[#808080] text-center sm:text-left">
          <div style={{ fontFamily: "var(--font-helvetica-now)" }}>
            High-Persistence Storage Engine on Silicon &bull; Architected by <a href="https://github.com/AgnibhaRay" target="_blank" rel="noopener noreferrer" className="text-[#000000] font-medium hover:underline">Agnibha Ray</a>
          </div>
          <div className="flex items-center gap-6" style={{ fontFamily: "var(--font-helvetica-now)" }}>
            <a
              href="https://github.com/AgnibhaRay/sider"
              target="_blank"
              rel="noopener noreferrer"
              className="text-[#000000] hover:underline"
            >
              GitHub Repository
            </a>
            <a href="#top" className="text-[#000000] hover:underline">
              Return to Top &uarr;
            </a>
          </div>
        </div>

      </div>
    </footer>
  );
}

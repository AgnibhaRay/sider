"use client";

import React from "react";
import Link from "next/link";
import { SiderLogo } from "./SiderLogo";

export function CloudFooter() {
  return (
    <footer className="w-full bg-[#141414] border-t border-[#414042] text-[#a7a9ac] select-none mt-16">
      {/* Top Banner Ribbon */}
      <div className="border-b border-[#2c2c2c] py-4 px-4 sm:px-8 bg-[#0e0e0e]/60">
        <div className="max-w-[1240px] mx-auto flex flex-col sm:flex-row items-center justify-between gap-3 text-center sm:text-left">
          <div className="flex items-center gap-2.5">
            <span className="w-2 h-2 rounded-full bg-[#19a05f] animate-pulse shrink-0" />
            <span className="text-[12px] font-mono text-[#d1d3d4]">
              Bare-Metal Node: <strong className="text-white">ind-tbn-1</strong> (100.95.206.7)
            </span>
          </div>
          <div className="text-[11px] font-mono text-[#7084ff] bg-[#405bff]/10 border border-[#405bff]/30 px-3 py-1 rounded-[30px]">
            ⚡ SkipList Throughput: 316,746 ops/sec | P50: 234µs
          </div>
        </div>
      </div>

      {/* Main Grid Section */}
      <div className="max-w-[1240px] mx-auto px-4 sm:px-8 py-12">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8 mb-12">
          
          {/* Col 1: Brand & Architect */}
          <div className="space-y-4">
            <div className="flex items-center gap-3">
              <SiderLogo size={28} />
              <div>
                <span className="text-[16px] font-semibold text-white tracking-tight">
                  Sider Cloud
                </span>
                <span className="block text-[10px] font-mono text-[#7084ff] uppercase">
                  Alpha Cockpit v0.2.1
                </span>
              </div>
            </div>
            <p className="text-[13px] text-[#808285] leading-relaxed">
              Zero-dependency LSM-Tree persistent storage engine on dedicated bare-metal hardware.
            </p>
            <div className="p-3.5 rounded-[16px] bg-[#0e0e0e] border border-[#2c2c2c]">
              <span className="text-[10px] font-mono uppercase text-[#6d6e71] block mb-1">
                Architect &amp; Lead Engineer
              </span>
              <span className="text-[14px] font-semibold text-white">
                Agnibha Ray
              </span>
              <p className="text-[11px] text-[#808285] mt-0.5">
                Systems Engineer &bull; Distributed Systems &amp; Compilers
              </p>
            </div>
          </div>

          {/* Col 2: Social Connect CTAs */}
          <div className="space-y-3">
            <span className="text-[11px] font-mono uppercase tracking-wider text-[#d1d3d4] font-semibold block">
              Connect with Agnibha Ray
            </span>
            <div className="flex flex-col gap-2">
              <a
                href="https://github.com/AgnibhaRay"
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center justify-between p-2.5 rounded-[12px] bg-[#0e0e0e] border border-[#2c2c2c] hover:border-[#405bff] hover:text-white transition-all text-[12px] text-[#d1d3d4] group"
              >
                <div className="flex items-center gap-2.5">
                  <svg className="w-4 h-4 fill-current text-white" viewBox="0 0 24 24">
                    <path d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 11.385.6.105.825-.255.825-.57 0-.285-.015-1.23-.015-2.235-3.015.555-3.795-.735-4.035-1.41-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.67-.3-5.46-1.335-5.46-5.925 0-1.305.465-2.385 1.23-3.225-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.84 1.23 1.905 1.23 3.225 0 4.605-2.805 5.625-5.475 5.925.435.375.81 1.095.81 2.22 0 1.605-.015 2.895-.015 3.3 0 .315.225.69.825.57A12.02 12.02 0 0024 12c0-6.63-5.37-12-12-12z" />
                  </svg>
                  <span>GitHub Profile</span>
                </div>
                <span className="text-[10px] font-mono text-[#7084ff] group-hover:translate-x-0.5 transition-transform">&rarr;</span>
              </a>

              <a
                href="https://www.linkedin.com/in/agnibharay"
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center justify-between p-2.5 rounded-[12px] bg-[#0e0e0e] border border-[#2c2c2c] hover:border-[#0077b5] hover:text-white transition-all text-[12px] text-[#d1d3d4] group"
              >
                <div className="flex items-center gap-2.5">
                  <svg className="w-4 h-4 fill-[#0077b5]" viewBox="0 0 24 24">
                    <path d="M19 0h-14c-2.761 0-5 2.239-5 5v14c0 2.761 2.239 5 5 5h14c2.762 0 5-2.239 5-5v-14c0-2.761-2.238-5-5-5zm-11 19h-3v-11h3v11zm-1.5-12.268c-.966 0-1.75-.79-1.75-1.764s.784-1.764 1.75-1.764 1.75.79 1.75 1.764-.783 1.764-1.75 1.764zm13.5 12.268h-3v-5.604c0-3.368-4-3.113-4 0v5.604h-3v-11h3v1.765c1.396-2.586 7-2.777 7 2.476v6.759z" />
                  </svg>
                  <span>LinkedIn Network</span>
                </div>
                <span className="text-[10px] font-mono text-[#0077b5] group-hover:translate-x-0.5 transition-transform">&rarr;</span>
              </a>

              <a
                href="https://www.instagram.com/agnibharay"
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center justify-between p-2.5 rounded-[12px] bg-[#0e0e0e] border border-[#2c2c2c] hover:border-[#e1306c] hover:text-white transition-all text-[12px] text-[#d1d3d4] group"
              >
                <div className="flex items-center gap-2.5">
                  <svg className="w-4 h-4 fill-[#e1306c]" viewBox="0 0 24 24">
                    <path d="M12 2.163c3.204 0 3.584.012 4.85.07 3.252.148 4.771 1.691 4.919 4.919.058 1.265.069 1.645.069 4.849 0 3.205-.012 3.584-.069 4.849-.149 3.225-1.664 4.771-4.919 4.919-1.266.058-1.644.07-4.85.07-3.204 0-3.584-.012-4.849-.07-3.26-.149-4.771-1.699-4.919-4.92-.058-1.265-.07-1.644-.07-4.849 0-3.204.013-3.583.07-4.849.149-3.227 1.664-4.771 4.919-4.919 1.266-.057 1.645-.069 4.849-.069zm0-2.163c-3.259 0-3.667.014-4.947.072-4.358.2-6.78 2.618-6.98 6.98-.059 1.281-.073 1.689-.073 4.948 0 3.259.014 3.668.072 4.948.2 4.358 2.618 6.78 6.98 6.98 1.281.058 1.689.072 4.948.072 3.259 0 3.668-.014 4.948-.072 4.354-.2 6.782-2.618 6.979-6.98.059-1.28.073-1.689.073-4.948 0-3.259-.014-3.667-.072-4.947-.196-4.354-2.617-6.78-6.979-6.98-1.281-.059-1.69-.073-4.949-.073zm0 5.838c-3.403 0-6.162 2.759-6.162 6.162s2.759 6.163 6.162 6.163 6.162-2.759 6.162-6.163c0-3.403-2.759-6.162-6.162-6.162zm0 10.162c-2.209 0-4-1.79-4-4 0-2.209 1.791-4 4-4s4 1.791 4 4c0 2.21-1.791 4-4 4zm6.406-11.845c-.796 0-1.441.645-1.441 1.44s.645 1.44 1.441 1.44c.795 0 1.439-.645 1.439-1.44s-.644-1.44-1.439-1.44z" />
                  </svg>
                  <span>Instagram @agnibharay</span>
                </div>
                <span className="text-[10px] font-mono text-[#e1306c] group-hover:translate-x-0.5 transition-transform">&rarr;</span>
              </a>

              <a
                href="https://github.com/AgnibhaRay/sider"
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center justify-between p-2.5 rounded-[12px] bg-[#0e0e0e] border border-[#2c2c2c] hover:border-[#19a05f] hover:text-white transition-all text-[12px] text-[#d1d3d4] group"
              >
                <div className="flex items-center gap-2.5">
                  <span className="text-[#19a05f] font-mono">★</span>
                  <span>Star Sider on GitHub</span>
                </div>
                <span className="text-[10px] font-mono text-[#19a05f] group-hover:translate-x-0.5 transition-transform">&rarr;</span>
              </a>
            </div>
          </div>

          {/* Col 3: Cloud Platform */}
          <div className="space-y-3">
            <span className="text-[11px] font-mono uppercase tracking-wider text-[#d1d3d4] font-semibold block">
              Cloud Cockpit
            </span>
            <ul className="space-y-2 text-[13px]">
              <li>
                <Link href="/console" className="hover:text-white text-[#405bff] font-medium flex items-center gap-1.5">
                  <span>Enter Console Studio</span>
                  <span className="text-[10px]">&rarr;</span>
                </Link>
              </li>
              <li>
                <a href="#build" className="hover:text-white text-[#00f0ff] flex items-center gap-1.5">
                  <span>Cyber-Foundry Quests</span>
                  <span className="text-[9px] font-mono px-1 rounded bg-[#00f0ff]/20">NEW</span>
                </a>
              </li>
              <li>
                <Link href="/docs" className="hover:text-white">
                  Technical Codex &amp; Wire Protocol
                </Link>
              </li>
              <li>
                <a href="#quickstart" className="hover:text-white">
                  SDK Drivers (Python, Node, Go, TCP)
                </a>
              </li>
              <li>
                <a href="#specs" className="hover:text-white">
                  Bare-Metal Node Hardware
                </a>
              </li>
            </ul>
          </div>

          {/* Col 4: Engine & Folio */}
          <div className="space-y-3">
            <span className="text-[11px] font-mono uppercase tracking-wider text-[#d1d3d4] font-semibold block">
              Sider Engine Ecosystem
            </span>
            <ul className="space-y-2 text-[13px]">
              <li>
                <a href="https://siderdb.vercel.app" className="hover:text-white flex items-center gap-1.5">
                  <span>Sider Database Engine Folio</span>
                  <span className="text-[10px] text-[#a7a9ac]">↗</span>
                </a>
              </li>
              <li>
                <a href="https://siderdb.vercel.app#interactive-terminal" className="hover:text-white">
                  Interactive Terminal Playground
                </a>
              </li>
              <li>
                <a href="https://siderdb.vercel.app#empirical-benchmark" className="hover:text-white">
                  Empirical Benchmark Study
                </a>
              </li>
              <li>
                <a href="https://github.com/AgnibhaRay/sider/blob/main/LICENSE" target="_blank" rel="noopener noreferrer" className="hover:text-white">
                  MIT Open Source License
                </a>
              </li>
            </ul>
          </div>

        </div>

        {/* Bottom Micro Bar */}
        <div className="pt-8 border-t border-[#2c2c2c] flex flex-col sm:flex-row items-center justify-between gap-4 text-[12px] text-[#6d6e71] text-center sm:text-left">
          <div>
            &copy; {new Date().getFullYear()} Sider Database &bull; Architected by <a href="https://github.com/AgnibhaRay" target="_blank" rel="noopener noreferrer" className="text-white hover:underline font-medium">Agnibha Ray</a>. All rights reserved.
          </div>
          <div className="flex items-center gap-4 text-[11px] font-mono">
            <span className="text-[#19a05f] flex items-center gap-1.5">
              <span className="w-1.5 h-1.5 rounded-full bg-[#19a05f]" />
              Alpha Cluster Active
            </span>
            <span>&bull;</span>
            <a href="#top" className="text-[#a7a9ac] hover:text-white transition-colors">
              Return to Top &uarr;
            </a>
          </div>
        </div>
      </div>
    </footer>
  );
}

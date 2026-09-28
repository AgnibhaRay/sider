"use client";

import React from "react";
import Link from "next/link";
import { SiderLogo } from "@/components/SiderLogo";
import { DocsSection } from "@/components/DocsSection";

export default function DocsPage() {
  return (
    <div
      className="min-h-screen text-[#ffffff] flex flex-col font-sans select-none antialiased relative"
      style={{ backgroundColor: "#0e0e0e", fontFamily: "var(--font-inter), sans-serif" }}
    >
      {/* Top Banner */}
      <div
        className="w-full h-10 px-4 text-white text-[12px] font-medium flex items-center justify-center gap-2 border-b border-[#414042]/50 z-50 sticky top-0"
        style={{
          background: "linear-gradient(179deg, rgba(64,91,255,0.25) 1.06%, rgba(112,132,255,0.06) 123.42%)",
        }}
      >
        <span className="w-1.5 h-1.5 rounded-full bg-[#405bff] animate-pulse" />
        <span className="font-mono text-[#7084ff] uppercase font-bold tracking-wider text-[10px] px-2 py-0.5 rounded-[30px] bg-[#405bff]/20 border border-[#405bff]/40">
          SIDER DOCS
        </span>
        <span className="text-[#d1d3d4]">
          Complete technical codex and wire protocol reference for region <code className="text-[#7084ff] font-mono">ind-tbn-1</code>.
        </span>
      </div>

      {/* Floating Header */}
      <div className="w-full max-w-[1200px] mx-auto pt-6 px-4 sticky top-12 z-40">
        <header className="w-full bg-[#191919] border border-white/10 rounded-[60px] px-6 h-14 flex items-center justify-between shadow-[0_4px_20px_rgba(0,0,0,0.45)] backdrop-blur-md">
          <Link href="/" className="flex items-center gap-3">
            <SiderLogo size={24} />
            <div className="flex items-center gap-2">
              <span className="text-[15px] font-medium text-white tracking-[-0.02em]">
                Sider Cloud
              </span>
              <span className="text-[10px] font-mono uppercase bg-[#405bff]/20 text-[#7084ff] border border-[#405bff]/40 px-2 py-0.5 rounded-[30px]">
                Documentation
              </span>
            </div>
          </Link>

          <div className="flex items-center gap-4">
            <Link href="/#build" className="text-[13px] text-[#d1d3d4] hover:text-white transition-colors">
              Build Quests
            </Link>
            <Link
              href="/console"
              className="px-5 py-1.5 rounded-[30px] bg-[#405bff] hover:bg-[#344bd6] text-white text-[13px] font-medium transition-all shadow-[0_0_20px_rgba(64,91,255,0.4)] flex items-center gap-1.5"
            >
              <span className="w-1.5 h-1.5 rounded-full bg-white animate-pulse" />
              <span>Console Cockpit</span>
            </Link>
          </div>
        </header>
      </div>

      {/* Main Docs Section */}
      <main className="flex-1">
        <DocsSection />
      </main>

      {/* Footer */}
      <footer className="w-full bg-[#191919] border-t border-[#414042] px-6 py-8 text-[13px] text-[#a7a9ac] mt-12">
        <div className="max-w-[1200px] mx-auto flex flex-col sm:flex-row items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <SiderLogo size={20} />
            <span className="font-medium text-white">Sider Cloud Docs</span>
          </div>
          <div className="flex items-center gap-6">
            <Link href="/" className="hover:text-white">Home</Link>
            <Link href="/#build" className="hover:text-white">Build Quests</Link>
            <Link href="/console" className="hover:text-white">Console Cockpit</Link>
          </div>
        </div>
      </footer>
    </div>
  );
}

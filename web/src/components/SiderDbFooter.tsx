"use client";

import React from "react";
import { SiderLogo } from "./SiderLogo";

export function SiderDbFooter() {
  return (
    <footer className="w-full bg-[#141414] border-t border-[#414042] text-[#a7a9ac] select-none mt-16">
      {/* Top Banner Ribbon */}
      <div className="border-b border-[#2c2c2c] py-4 px-4 sm:px-8 bg-[#0e0e0e]/80">
        <div className="max-w-[1240px] mx-auto flex flex-col sm:flex-row items-center justify-between gap-3 text-center sm:text-left">
          <div className="flex items-center gap-2.5">
            <span className="w-2 h-2 rounded-full bg-[#19a05f] animate-pulse shrink-0" />
            <span className="text-[12px] font-mono text-[#d1d3d4]">
              Bare-Metal Region: <strong className="text-white">ind-tbn-1</strong> (100.95.206.7)
            </span>
          </div>
          <div className="text-[11px] font-mono text-[#7084ff] bg-[#405bff]/15 border border-[#405bff]/40 px-3 py-1 rounded-[30px]">
            ⚡ SkipList Engine: 316,746 ops/sec | P50: 234µs
          </div>
        </div>
      </div>

      {/* Main Grid Section */}
      <div className="max-w-[1240px] mx-auto px-4 sm:px-8 py-12">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8 mb-12">
          
          {/* Col 1: SiderDB Brand & Architect */}
          <div className="space-y-4">
            <div className="flex items-center gap-3">
              <SiderLogo size={30} />
              <div>
                <span className="text-[17px] font-semibold text-white tracking-tight">
                  SiderDB
                </span>
                <span className="block text-[10px] font-mono text-[#7084ff] uppercase">
                  Persistent Key-Value Engine v2.0
                </span>
              </div>
            </div>
            <p className="text-[13px] text-[#808285] leading-relaxed">
              Zero-dependency Log-Structured Merge persistent database engine engineered for extreme throughput on bare-metal silicon.
            </p>
            <div className="p-3.5 rounded-[16px] bg-[#0e0e0e] border border-[#2c2c2c]">
              <span className="text-[10px] font-mono uppercase text-[#6d6e71] block mb-1">
                Architect &amp; Lead Systems Engineer
              </span>
              <span className="text-[14px] font-semibold text-white">
                Agnibha Ray
              </span>
              <p className="text-[11px] text-[#808285] mt-0.5">
                Systems Engineer &bull; Distributed Systems &amp; Compilers
              </p>
            </div>
          </div>

          {/* Col 2: Social Connect CTAs for Agnibha Ray */}
          <div className="space-y-3">
            <span className="text-[11px] font-mono uppercase tracking-wider text-[#d1d3d4] font-semibold block">
              Architect Connect
            </span>
            <div className="space-y-2">
              <a
                href="https://github.com/AgnibhaRay"
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center justify-between p-2.5 rounded-[12px] bg-[#1a1a1a] hover:bg-[#222222] border border-[#2c2c2c] hover:border-white/20 transition-all text-[12px] text-white group"
              >
                <div className="flex items-center gap-2">
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                    <path fillRule="evenodd" clipRule="evenodd" d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.53 1.032 1.53 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z"/>
                  </svg>
                  <span>github.com/AgnibhaRay</span>
                </div>
                <span className="text-[10px] text-[#6d6e71] group-hover:text-white transition-colors">&rarr;</span>
              </a>

              <a
                href="https://www.linkedin.com/in/agnibharay"
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center justify-between p-2.5 rounded-[12px] bg-[#1a1a1a] hover:bg-[#222222] border border-[#2c2c2c] hover:border-[#0a66c2]/40 transition-all text-[12px] text-white group"
              >
                <div className="flex items-center gap-2">
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="#0a66c2">
                    <path d="M19 3a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h14m-.5 15.5v-5.3a3.26 3.26 0 0 0-3.26-3.26c-.85 0-1.84.52-2.28 1.3v-1.11h-2.79v8.37h2.79v-4.93c0-.77.62-1.4 1.39-1.4a1.4 1.4 0 0 1 1.4 1.4v4.93h2.75M6.88 8.56a1.68 1.68 0 0 0 1.68-1.68c0-.93-.75-1.69-1.68-1.69a1.69 1.69 0 0 0-1.69 1.69c0 .93.76 1.68 1.69 1.68m1.39 9.94v-8.37H5.5v8.37h2.77z"/>
                  </svg>
                  <span>LinkedIn Profile</span>
                </div>
                <span className="text-[10px] text-[#6d6e71] group-hover:text-[#0a66c2] transition-colors">&rarr;</span>
              </a>

              <a
                href="https://www.instagram.com/agnibharay"
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center justify-between p-2.5 rounded-[12px] bg-[#1a1a1a] hover:bg-[#222222] border border-[#2c2c2c] hover:border-[#e1306c]/40 transition-all text-[12px] text-white group"
              >
                <div className="flex items-center gap-2">
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="#e1306c">
                    <path d="M12 2.163c3.204 0 3.584.012 4.85.07 3.252.148 4.771 1.691 4.919 4.919.058 1.265.069 1.645.069 4.849 0 3.205-.012 3.584-.069 4.849-.149 3.225-1.664 4.771-4.919 4.919-1.266.058-1.644.07-4.85.07-3.204 0-3.584-.012-4.849-.07-3.26-.149-4.771-1.699-4.919-4.92-.058-1.265-.07-1.644-.07-4.849 0-3.204.013-3.583.07-4.849.149-3.227 1.664-4.771 4.919-4.919 1.266-.057 1.645-.069 4.849-.069zm0-2.163c-3.259 0-3.667.014-4.947.072-4.358.2-6.78 2.618-6.98 6.98-.059 1.281-.073 1.689-.073 4.948 0 3.259.014 3.668.072 4.948.2 4.358 2.618 6.78 6.98 6.98 1.281.058 1.689.072 4.948.072 3.259 0 3.668-.014 4.948-.072 4.354-.2 6.782-2.618 6.979-6.98.059-1.28.073-1.689.073-4.948 0-3.259-.014-3.667-.072-4.947-.196-4.354-2.617-6.78-6.979-6.98-1.281-.059-1.69-.073-4.949-.073zm0 5.838c-3.403 0-6.162 2.759-6.162 6.162s2.759 6.163 6.162 6.163 6.162-2.759 6.162-6.163c0-3.403-2.759-6.162-6.162-6.162zm0 10.162c-2.209 0-4-1.79-4-4 0-2.209 1.791-4 4-4s4 1.791 4 4c0 2.21-1.791 4-4 4zm6.406-11.845c-.796 0-1.441.645-1.441 1.44s.645 1.44 1.441 1.44c.795 0 1.439-.645 1.439-1.44s-.644-1.44-1.439-1.44z"/>
                  </svg>
                  <span>@agnibharay</span>
                </div>
                <span className="text-[10px] text-[#6d6e71] group-hover:text-[#e1306c] transition-colors">&rarr;</span>
              </a>

              <a
                href="https://github.com/AgnibhaRay/sider"
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center justify-between p-2.5 rounded-[12px] bg-[#1a1a1a] hover:bg-[#222222] border border-[#2c2c2c] hover:border-yellow-500/40 transition-all text-[12px] text-white group"
              >
                <div className="flex items-center gap-2">
                  <span className="text-yellow-400">★</span>
                  <span>Star Sider on GitHub</span>
                </div>
                <span className="text-[10px] text-[#6d6e71] group-hover:text-yellow-400 transition-colors">&rarr;</span>
              </a>
            </div>
          </div>

          {/* Col 3 & 4: High-Converting Engaging Sider Cloud Alpha Promotion */}
          <div className="lg:col-span-2 space-y-4">
            <div className="relative p-6 rounded-[20px] bg-gradient-to-br from-[#12121e] via-[#0d0d16] to-[#07070b] border border-[#405bff]/30 shadow-[0_8px_30px_rgba(64,91,255,0.15)] overflow-hidden">
              {/* Background ambient neon glow */}
              <div className="absolute top-0 right-0 w-64 h-64 bg-[#405bff]/10 rounded-full blur-3xl pointer-events-none" />
              
              <div className="relative z-10 flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4 mb-4">
                <div className="flex items-center gap-2">
                  <span className="w-2.5 h-2.5 rounded-full bg-[#405bff] animate-ping shrink-0" />
                  <span className="text-[11px] font-mono uppercase tracking-widest text-[#7084ff] font-semibold">
                    Public Alpha Live
                  </span>
                </div>
                <span className="text-[10px] font-mono uppercase px-2.5 py-0.5 rounded-[30px] bg-[#405bff]/20 text-[#a3b1ff] border border-[#405bff]/40">
                  Bare-Metal ind-tbn-1
                </span>
              </div>

              <h4 className="relative z-10 text-[18px] sm:text-[20px] font-medium text-white tracking-tight mb-2">
                Deploy Dedicated Sider Instances in the Cloud
              </h4>

              <p className="relative z-10 text-[13px] text-[#9ca3af] leading-relaxed mb-5">
                Experience instant provisioning with real-time operations telemetry, multi-tenant auth tokens, and our interactive web console studio.
              </p>

              <div className="relative z-10 flex flex-col sm:flex-row items-stretch sm:items-center gap-3">
                <a
                  href="https://sider-cloud.vercel.app"
                  className="px-6 py-2.5 rounded-[30px] bg-[#405bff] hover:bg-[#344bd6] text-white text-[13px] font-medium transition-all shadow-[0_0_20px_rgba(64,91,255,0.5)] hover:shadow-[0_0_25px_rgba(64,91,255,0.7)] hover:scale-105 text-center flex items-center justify-center gap-2"
                >
                  <span>Launch Sider Cloud Studio</span>
                  <span className="text-white/80">&rarr;</span>
                </a>

                <a
                  href="https://sider-cloud.vercel.app/docs"
                  className="px-4 py-2.5 rounded-[30px] bg-white/5 hover:bg-white/10 border border-white/15 text-white text-[13px] font-medium transition-all text-center"
                >
                  Wire Protocol &amp; SDK Docs ↗
                </a>
              </div>
            </div>
          </div>
        </div>

        {/* Bottom Bar */}
        <div className="border-t border-[#2c2c2c] pt-8 flex flex-col sm:flex-row items-center justify-between gap-4 text-[12px] text-[#6d6e71]">
          <div>
            &copy; {new Date().getFullYear()} SiderDB. Engineered by Agnibha Ray. Open source under MIT.
          </div>
          <div className="flex items-center gap-6 font-mono text-[11px]">
            <a href="https://sider-cloud.vercel.app" className="hover:text-[#405bff] transition-colors">
              Sider Cloud Alpha
            </a>
            <a href="https://github.com/AgnibhaRay/sider" target="_blank" rel="noopener noreferrer" className="hover:text-white transition-colors">
              GitHub Source
            </a>
            <a href="https://github.com/AgnibhaRay/sider/issues" target="_blank" rel="noopener noreferrer" className="hover:text-white transition-colors">
              Report Issue
            </a>
          </div>
        </div>
      </div>
    </footer>
  );
}

"use client";

import React, { useState, useEffect } from "react";
import { SiderLogo } from "./SiderLogo";

interface HeaderProps {
  onFolioClick?: () => void;
  activeSection?: string;
}

export function SiderDbHeader({ onFolioClick }: HeaderProps) {
  const [isFloating, setIsFloating] = useState(false);

  useEffect(() => {
    const handleScroll = () => {
      // Transition to floating pill once scrolled past the SIDER hero section
      const threshold = window.innerHeight * 0.75;
      setIsFloating(window.scrollY > threshold);
    };

    window.addEventListener("scroll", handleScroll, { passive: true });
    handleScroll();
    return () => window.removeEventListener("scroll", handleScroll);
  }, []);

  return (
    <header
      className={`fixed z-50 transition-all duration-300 select-none flex items-center justify-between ${
        isFloating
          ? "top-4 left-1/2 -translate-x-1/2 w-[92%] max-w-[1100px] h-14 bg-[#191919]/90 border border-white/15 rounded-[60px] px-6 shadow-[0_8px_32px_rgba(0,0,0,0.5)] backdrop-blur-md text-[#ffffff]"
          : "top-0 left-0 w-full h-14 bg-[#c4c3b6] px-4 sm:px-10 border-b border-[#dfdcd5]/60 text-[#000000]"
      }`}
    >
      {/* Brand Identity — Top-Left Header Mark: Bespoke Sider SVG Logo */}
      <a
        href="#top"
        className="flex items-center gap-2.5 group text-decoration-none select-none"
        aria-label="Sider Home"
      >
        <SiderLogo size={isFloating ? 26 : 32} />
        <span
          className={`text-[13px] uppercase font-medium tracking-[0.1em] transition-colors ${
            isFloating ? "text-white" : "text-[#000000]"
          }`}
          style={{ fontFamily: "var(--font-helvetica-now)" }}
        >
          Sider
        </span>
        {isFloating && (
          <span className="hidden sm:inline-block text-[9px] font-mono uppercase bg-[#405bff]/20 text-[#7084ff] border border-[#405bff]/40 px-2 py-0.5 rounded-[30px] ml-1">
            ind-tbn-1
          </span>
        )}
      </a>

      {/* Navigation — Sider Cloud CTA + Engine Folio + GitHub */}
      <div className="flex items-center gap-3 sm:gap-6">
        {/* Prominent Sider Cloud Alpha CTA */}
        <a
          href="https://sider-cloud.vercel.app"
          className={`inline-flex items-center gap-2 px-3.5 py-1.5 rounded-[28.8px] text-[11px] font-medium tracking-wide hover:scale-105 transition-all ${
            isFloating
              ? "bg-[#405bff] text-white hover:bg-[#344bd6] shadow-[0_0_15px_rgba(64,91,255,0.4)]"
              : "bg-[#000000] text-[#ffffff] shadow-[0_2px_10px_rgba(0,0,0,0.2)]"
          }`}
          style={{ fontFamily: "var(--font-helvetica-now)" }}
        >
          <span
            className={`w-1.5 h-1.5 rounded-full animate-pulse ${
              isFloating ? "bg-white" : "bg-[#405bff]"
            }`}
          />
          <span>Sider Cloud Alpha</span>
          <span className={isFloating ? "text-white/80" : "text-[#a7a9ac]"}>
            &rarr;
          </span>
        </a>

        <a
          href="#interactive-terminal"
          onClick={onFolioClick}
          className={`ghost-text-link text-[12px] hover:underline hidden sm:inline transition-colors ${
            isFloating ? "text-[#a7a9ac] hover:text-white" : "text-[#000000]"
          }`}
          style={{ fontFamily: "var(--font-helvetica-now)", fontWeight: 400 }}
        >
          Engine Folio &amp; CLI
        </a>

        <a
          href="https://github.com/AgnibhaRay/sider"
          target="_blank"
          rel="noopener noreferrer"
          className={`flex items-center gap-1.5 text-[12px] hover:underline transition-colors ${
            isFloating ? "text-[#a7a9ac] hover:text-white" : "text-[#000000]"
          }`}
          style={{ fontFamily: "var(--font-helvetica-now)", fontWeight: 400 }}
          aria-label="GitHub Repository"
        >
          <svg
            width="15"
            height="15"
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
          <span>GitHub</span>
        </a>
      </div>
    </header>
  );
}

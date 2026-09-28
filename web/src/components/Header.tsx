"use client";

import React from "react";
import Link from "next/link";
import { SiderLogo } from "./SiderLogo";

interface HeaderProps {
  onFolioClick?: () => void;
  activeSection?: string;
}

export function Header({ onFolioClick }: HeaderProps) {
  return (
    <header className="w-full bg-[#c4c3b6] px-6 sm:px-10 h-14 flex items-center justify-between border-b border-[#dfdcd5]/60 sticky top-0 z-50">
      {/* Brand Identity — Top-Left Header Mark: Bespoke Sider SVG Logo */}
      <a
        href="#top"
        className="flex items-center gap-2.5 group text-decoration-none select-none"
        aria-label="Sider Home"
      >
        <SiderLogo size={32} />
        <span
          className="text-[13px] uppercase font-medium tracking-[0.1em] text-[#000000] group-hover:opacity-75 transition-opacity"
          style={{ fontFamily: "var(--font-helvetica-now)" }}
        >
          Sider
        </span>
      </a>

      {/* Navigation — Cloud Console, Ghost Text Link & GitHub Link */}
      <div className="flex items-center gap-4 sm:gap-6">
        <Link
          href="/console"
          className="flex items-center gap-1.5 px-2.5 py-1 rounded-[4px] bg-black text-white text-[11px] font-mono hover:bg-neutral-800 transition-colors"
        >
          <span className="w-1.5 h-1.5 rounded-full bg-[#10b981] animate-pulse" />
          <span>Cloud Console</span>
        </Link>

        <a
          href="#interactive-terminal"
          onClick={onFolioClick}
          className="ghost-text-link text-[12px] text-[#000000] hover:underline hidden sm:inline"
          style={{ fontFamily: "var(--font-helvetica-now)", fontWeight: 400 }}
        >
          Engine Folio & CLI
        </a>

        <a
          href="https://github.com/AgnibhaRay/sider"
          target="_blank"
          rel="noopener noreferrer"
          className="flex items-center gap-1.5 text-[12px] text-[#000000] hover:underline"
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
          <span className="hidden sm:inline">GitHub</span>
        </a>
      </div>
    </header>
  );
}

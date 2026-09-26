"use client";

import React from "react";
import { SiderLogo } from "./SiderLogo";

export function Footer() {
  return (
    <footer className="relative z-20 w-full bg-[#ebebeb] text-[#000000] px-6 sm:px-12 py-16 border-t border-[#dfdcd5] select-none">
      <div className="w-full max-w-7xl mx-auto flex flex-col md:flex-row items-start md:items-center justify-between gap-8">
        {/* Monogram Brand Identity */}
        <div className="flex items-center gap-3">
          <SiderLogo size={34} />
          <div>
            <div
              className="text-[13px] uppercase font-medium tracking-[0.08em] text-[#000000]"
              style={{ fontFamily: "var(--font-helvetica-now)" }}
            >
              SIDER DATABASE
            </div>
            <div
              className="text-[11px] text-[#595855]"
              style={{ fontFamily: "var(--font-helvetica-now)" }}
            >
              Log-Structured Merge Engine &bull; Version 2.0.0
            </div>
          </div>
        </div>

        {/* Colophon & Details */}
        <div
          className="text-[12px] text-[#595855] space-y-1 md:text-right"
          style={{ fontFamily: "var(--font-helvetica-now)" }}
        >
          <div>Architected &bull; Pure Standard Library Go 1.24+ &bull; Zero External Dependencies</div>
          <div>MIT License &bull; Open Source Folio &bull; Agnibha Ray</div>
        </div>
      </div>

      {/* Hairline Separator & Micro Bottom Bar */}
      <div className="w-full max-w-7xl mx-auto mt-12 pt-6 border-t border-[#dfdcd5] flex flex-col sm:flex-row items-center justify-between gap-4 text-[11px] text-[#808080]">
        <div style={{ fontFamily: "var(--font-helvetica-now)" }}>
          High-Persistence Storage Engine on Silicon
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
    </footer>
  );
}

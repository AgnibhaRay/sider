"use client";

import React, { useRef, useEffect } from "react";
import { SiderDbHeader } from "./SiderDbHeader";
import { ScrollProgress } from "./ScrollProgress";
import { CustomCursor } from "./CustomCursor";
import { HeroSection } from "./HeroSection";
import { PaintingSection } from "./PaintingSection";
import { FeatureSection } from "./FeatureSection";
import { TerminalSection } from "./TerminalSection";
import { BenchmarkSection } from "./BenchmarkSection";
import { ArchitectureSection } from "./ArchitectureSection";
import { CloudFooter } from "./CloudFooter";

export function SiderDbLanding() {
  const terminalRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    // Ensure the page always starts at top on initial load
    if (typeof window !== "undefined" && !window.location.hash) {
      window.scrollTo(0, 0);
    }
  }, []);

  const scrollToTerminal = () => {
    const el = document.getElementById("interactive-terminal");
    if (el) {
      el.scrollIntoView({ behavior: "smooth" });
    }
  };

  return (
    <main className="min-h-screen bg-[#c4c3b6] text-[#000000] flex flex-col relative overflow-x-hidden">
      {/* Bespoke Renaissance Caliper Custom Cursor */}
      <CustomCursor />

      {/* Delicate Scroll Progress Hairline Indicator */}
      <ScrollProgress />

      {/* Exact Original Sticky Header with Sider Cloud CTA */}
      <SiderDbHeader onFolioClick={scrollToTerminal} />

      {/* Hero & Painting Curtain Stage: Painting scrolls up and covers the hero, then both scroll away together */}
      <div className="relative w-full">
        <div className="sticky top-14 w-full h-[calc(100vh-3.5rem)] overflow-hidden z-0 bg-[#c4c3b6]">
          <HeroSection onExploreClick={scrollToTerminal} />
        </div>
        <div className="relative z-10 w-full bg-[#000000]">
          <PaintingSection />
        </div>
      </div>

      {/* Pitch-Black Feature Section (Ink Room): Three.js circular glowing node constellation */}
      <div className="relative z-20 w-full bg-[#000000]">
        <FeatureSection />
      </div>

      {/* Interactive Sider Command Folio & Live CLI Playground */}
      <div ref={terminalRef} className="relative z-20 w-full bg-[#c4c3b6]">
        <TerminalSection />
      </div>

      {/* Empirical Benchmark Study */}
      <div className="relative z-20 w-full bg-[#e7e5e4]">
        <BenchmarkSection />
      </div>

      {/* Architectural Folio & Driver Integrations */}
      <div className="relative z-20 w-full bg-[#c4c3b6]">
        <ArchitectureSection />
      </div>

      {/* High-Tech Cloud Footer (same as sider-cloud with Agnibha Ray socials and cluster telemetry) */}
      <div className="relative z-20 w-full bg-[#141414]">
        <CloudFooter />
      </div>
    </main>
  );
}

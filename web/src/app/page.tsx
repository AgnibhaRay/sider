"use client";

import React, { useRef, useEffect } from "react";
import { Header } from "../components/Header";
import { ScrollProgress } from "../components/ScrollProgress";
import { CustomCursor } from "../components/CustomCursor";
import { HeroSection } from "../components/HeroSection";
import { PaintingSection } from "../components/PaintingSection";
import { FeatureSection } from "../components/FeatureSection";
import { TerminalSection } from "../components/TerminalSection";
import { BenchmarkSection } from "../components/BenchmarkSection";
import { ArchitectureSection } from "../components/ArchitectureSection";
import { Footer } from "../components/Footer";

export default function Home() {
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
    <main className="min-h-screen bg-[#c4c3b6] text-[#000000] flex flex-col relative">
      {/* Bespoke Renaissance Caliper Custom Cursor */}
      <CustomCursor />

      {/* Delicate Scroll Progress Hairline Indicator */}
      <ScrollProgress />

      {/* Minimal Header: Logo mark top-left, single text link top-right, no menu bar chrome */}
      <Header onFolioClick={scrollToTerminal} />

      {/* Hero & Painting Curtain Stage: Painting scrolls up and covers the hero, then both scroll away together */}
      <div className="relative w-full">
        <div className="sticky top-14 w-full h-[calc(100vh-3.5rem)] overflow-hidden z-0 bg-[#c4c3b6]">
          <HeroSection onExploreClick={scrollToTerminal} />
        </div>
        <div className="relative z-10 w-full bg-[#000000]">
          <PaintingSection />
        </div>
      </div>

      {/* Pitch-Black Feature Section (Ink Room): Three.js circular glowing node constellation, 3 circular vignettes */}
      <div className="relative z-20 w-full bg-[#000000]">
        <FeatureSection />
      </div>

      {/* Interactive Sider Command Folio & Live CLI Playground */}
      <div ref={terminalRef} className="relative z-20 w-full bg-[#c4c3b6]">
        <TerminalSection />
      </div>

      {/* Empirical Benchmark Study: Solid opaque surface with zero hero bleed */}
      <div className="relative z-20 w-full bg-[#e7e5e4]">
        <BenchmarkSection />
      </div>

      {/* Architectural Folio & Driver Integrations */}
      <div className="relative z-20 w-full bg-[#c4c3b6]">
        <ArchitectureSection />
      </div>

      {/* Cool Chalk Footer */}
      <div className="relative z-20 w-full bg-[#ebebeb]">
        <Footer />
      </div>
    </main>
  );
}

"use client";

import React, { useState, useEffect } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { SiderLogo } from "../components/SiderLogo";
import { HorizonLockCanvas } from "../components/HorizonLockCanvas";
import { ScrollReveal } from "../components/ScrollReveal";
import { LaserBorderCard } from "../components/LaserBorderCard";
import { KineticStreamRibbon } from "../components/KineticStreamRibbon";
import { BuildSection } from "../components/BuildSection";
import { DocsSection } from "../components/DocsSection";
import { useUser, UserButton } from "@clerk/nextjs";

import { CloudFooter } from "./CloudFooter";

export function SiderCloudLanding() {
  const router = useRouter();
  const { isSignedIn, user, isLoaded } = useUser();

  // Two-Step Console Launch Wizard State
  const [currentStep, setCurrentStep] = useState<1 | 2>(1);
  const [developerName, setDeveloperName] = useState<string>("");
  const [clusterRegion] = useState<string>("ind-tbn-1 (Edge Bare-Metal Node)");
  const [isLaunching, setIsLaunching] = useState<boolean>(false);
  const [copiedCode, setCopiedCode] = useState<boolean>(false);
  const [activeTab, setActiveTab] = useState<"python" | "node" | "go" | "tcp">("python");
  const [mobileMenuOpen, setMobileMenuOpen] = useState<boolean>(false);

  useEffect(() => {
    if (isSignedIn && user?.firstName) {
      setDeveloperName(user.firstName);
    }
  }, [isSignedIn, user]);

  const handleStep1Complete = (e?: React.FormEvent) => {
    if (e) e.preventDefault();
    setCurrentStep(2);
  };

  const handleLaunchConsole = () => {
    setIsLaunching(true);
    setTimeout(() => {
      router.push("/console");
    }, 400);
  };

  const copyQuickstart = () => {
    const code =
      activeTab === "python"
        ? `import socket\ns = socket.socket()\ns.connect(('100.95.206.7', 4100))\ns.sendall(b'AUTH sdr_live_793855e4ec0e70901bef05344264ba98\\nPUT session:user "active"\\nGET session:user\\n')\nprint(s.recv(1024).decode())`
        : activeTab === "node"
        ? `import net from 'net';\nconst client = net.createConnection({ host: '100.95.206.7', port: 4100 }, () => {\n  client.write('AUTH sdr_live_793855e4ec0e70901bef05344264ba98\\nPUT key "val"\\nGET key\\n');\n});\nclient.on('data', (d) => console.log(d.toString()));`
        : activeTab === "go"
        ? `conn, _ := net.Dial("tcp", "100.95.206.7:4100")\nfmt.Fprintf(conn, "AUTH sdr_live_793855e4ec0e70901bef05344264ba98\\nPUT key value\\nGET key\\n")`
        : `nc 100.95.206.7 4100\nAUTH sdr_live_793855e4ec0e70901bef05344264ba98\nPUT user:101 '{"status":"active"}'\nGET user:101`;

    navigator.clipboard.writeText(code);
    setCopiedCode(true);
    setTimeout(() => setCopiedCode(false), 2000);
  };

  return (
    <div
      className="min-h-screen text-[#ffffff] flex flex-col font-sans select-none antialiased relative overflow-x-hidden"
      style={{ backgroundColor: "#0e0e0e", fontFamily: "var(--font-inter), sans-serif" }}
    >
      {/* 1. TOP SIGNAL STRIP (RESPONSIVE & MOBILE-FRIENDLY) */}
      <div
        className="w-full min-h-10 py-1.5 px-3 text-white text-[11px] sm:text-[12px] font-medium flex items-center justify-center gap-2 border-b border-[#414042]/50 z-50 sticky top-0 backdrop-blur-md"
        style={{
          background: "linear-gradient(179deg, rgba(64,91,255,0.25) 1.06%, rgba(112,132,255,0.06) 123.42%)"
        }}
      >
        <span className="w-1.5 h-1.5 rounded-full bg-[#405bff] animate-pulse shrink-0" />
        <span className="font-mono text-[#7084ff] uppercase font-bold tracking-wider text-[9px] sm:text-[10px] px-2 py-0.5 rounded-[30px] bg-[#405bff]/20 border border-[#405bff]/40 shrink-0">
          ALPHA V0.2.1
        </span>
        <span className="text-[#d1d3d4] text-center truncate sm:overflow-visible sm:whitespace-normal">
          <strong className="text-white">Sider Cloud</strong>: LSM SkipList on <code className="text-[#7084ff] font-mono">ind-tbn-1</code>
        </span>
        <button
          onClick={() => {
            const el = document.getElementById("console-wizard");
            el?.scrollIntoView({ behavior: "smooth" });
          }}
          className="text-[#7084ff] hover:underline font-semibold ml-1 cursor-pointer hidden md:inline shrink-0"
        >
          Claim Endpoint &rarr;
        </button>
      </div>

      {/* 2. FLOATING NAV PILL (LaunchDarkly signature 60px pill with Mobile Drawer) */}
      <div className="w-full max-w-[1240px] mx-auto pt-3 sm:pt-6 px-3 sm:px-4 sticky top-12 z-40">
        <header className="w-full bg-[#191919] border border-white/10 rounded-[60px] px-3.5 sm:px-6 h-13 sm:h-14 flex items-center justify-between gap-2 sm:gap-3 shadow-[0_4px_20px_rgba(0,0,0,0.45)] backdrop-blur-md">
          {/* Brand Left */}
          <Link href="/" className="flex items-center gap-2 sm:gap-2.5 shrink-0 whitespace-nowrap">
            <SiderLogo size={22} />
            <div className="flex items-center gap-1.5 sm:gap-2 whitespace-nowrap">
              <span className="text-[14px] sm:text-[15px] font-medium text-white tracking-[-0.02em] whitespace-nowrap">
                Sider Cloud
              </span>
              <span className="text-[9px] sm:text-[10px] font-mono uppercase bg-[#405bff]/20 text-[#7084ff] border border-[#405bff]/40 px-2 py-0.5 rounded-[30px] whitespace-nowrap hidden sm:inline-block">
                ind-tbn-1
              </span>
            </div>
          </Link>

          {/* Center Links (Desktop only) */}
          <nav className="hidden lg:flex items-center gap-5 text-[13px] text-[#d1d3d4] font-medium shrink-0">
            <a href="#build" className="text-[#00f0ff] hover:text-white transition-colors flex items-center gap-1.5 font-semibold whitespace-nowrap">
              <span>Build Quests</span>
              <span className="text-[9px] font-mono px-1.5 py-0.2 rounded-full bg-[#00f0ff]/20 text-[#00f0ff] border border-[#00f0ff]/30 whitespace-nowrap">
                NEW
              </span>
            </a>
            <a href="#docs" className="hover:text-white transition-colors whitespace-nowrap">
              Documentation
            </a>
            <a href="#console-wizard" className="hover:text-white transition-colors whitespace-nowrap">
              Access Cockpit
            </a>
            <a href="#specs" className="hover:text-white transition-colors whitespace-nowrap">
              Hardware Specs
            </a>
            <a href="#quickstart" className="hover:text-white transition-colors whitespace-nowrap">
              SDKs
            </a>
          </nav>

          {/* Right Actions */}
          <div className="flex items-center gap-2 sm:gap-3 shrink-0 whitespace-nowrap">
            {isLoaded && isSignedIn && (
              <div className="flex items-center gap-2">
                <span className="text-[11px] font-mono text-[#a7a9ac] hidden xl:inline max-w-[140px] truncate whitespace-nowrap">
                  {user?.primaryEmailAddress?.emailAddress || "Developer"}
                </span>
                <UserButton />
              </div>
            )}
            {isLoaded && !isSignedIn && (
              <div className="flex items-center gap-1.5 sm:gap-2">
                <Link
                  href="/sign-in"
                  className="px-2.5 sm:px-3 py-1 text-[12px] sm:text-[13px] font-medium text-[#d1d3d4] hover:text-white transition-colors whitespace-nowrap"
                >
                  Sign in
                </Link>
                <Link
                  href="/sign-up"
                  className="px-3 sm:px-4 py-1.5 rounded-[30px] border border-[#414042] bg-[#191919] hover:bg-[#2c2c2c] text-[12px] sm:text-[13px] font-medium text-white transition-colors hidden md:inline-block whitespace-nowrap"
                >
                  Sign up
                </Link>
              </div>
            )}

            <Link
              href="/console"
              className="px-3.5 sm:px-5 py-1.5 rounded-[30px] bg-[#405bff] hover:bg-[#344bd6] text-white text-[12px] sm:text-[13px] font-medium transition-all shadow-[0_0_20px_rgba(64,91,255,0.4)] flex items-center gap-1.5 shrink-0 whitespace-nowrap"
            >
              <span className="w-1.5 h-1.5 rounded-full bg-white animate-pulse" />
              <span>Console</span>
            </Link>

            {/* Mobile Hamburger Drawer Toggle */}
            <button
              type="button"
              onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
              className="lg:hidden p-2 rounded-full bg-[#0e0e0e] border border-[#414042] text-[#d1d3d4] hover:text-white transition-colors cursor-pointer shrink-0"
              aria-label="Toggle navigation menu"
            >
              {mobileMenuOpen ? (
                <span className="text-[14px] font-bold block w-4 h-4 leading-4 text-center">✕</span>
              ) : (
                <span className="text-[14px] font-bold block w-4 h-4 leading-4 text-center">☰</span>
              )}
            </button>
          </div>
        </header>

        {/* Mobile Navigation Drawer Modal */}
        {mobileMenuOpen && (
          <div className="lg:hidden mt-2 p-3 sm:p-4 rounded-[24px] bg-[#191919]/95 backdrop-blur-xl border border-[#405bff]/40 shadow-[0_10px_35px_rgba(0,0,0,0.8)] animate-in fade-in slide-in-from-top-2 duration-200 z-50">
            <div className="flex flex-col gap-2 font-medium text-[13px] sm:text-[14px]">
              <a
                href="#build"
                onClick={() => setMobileMenuOpen(false)}
                className="p-3 rounded-[16px] bg-[#0e0e0e] border border-[#405bff]/30 text-[#00f0ff] flex items-center justify-between"
              >
                <span>⚡ Build Quests</span>
                <span className="text-[10px] font-mono px-2 py-0.5 rounded-full bg-[#00f0ff]/20">NEW</span>
              </a>
              <a
                href="#docs"
                onClick={() => setMobileMenuOpen(false)}
                className="p-3 rounded-[16px] hover:bg-[#252525] text-white flex items-center gap-2"
              >
                <span>📖 Documentation</span>
              </a>
              <a
                href="#console-wizard"
                onClick={() => setMobileMenuOpen(false)}
                className="p-3 rounded-[16px] hover:bg-[#252525] text-white flex items-center gap-2"
              >
                <span>🕹️ Access Cockpit</span>
              </a>
              <a
                href="#specs"
                onClick={() => setMobileMenuOpen(false)}
                className="p-3 rounded-[16px] hover:bg-[#252525] text-white flex items-center gap-2"
              >
                <span>🖥️ Hardware Specs</span>
              </a>
              <a
                href="#quickstart"
                onClick={() => setMobileMenuOpen(false)}
                className="p-3 rounded-[16px] hover:bg-[#252525] text-white flex items-center gap-2"
              >
                <span>📦 SDK Quickstart</span>
              </a>
            </div>
          </div>
        )}
      </div>

      {/* 3. HERO COCKPIT STAGE */}
      <section className="w-full max-w-[1200px] mx-auto px-4 sm:px-6 pt-16 pb-20 relative">
        {/* Horizon Lock 3D Infinite Perspective Grid with Gyroscopic Bank Stabilization */}
        <div className="absolute inset-0 w-full h-[680px] overflow-hidden pointer-events-none -z-10 opacity-80">
          <HorizonLockCanvas />
        </div>
        {/* Ambient Radial Violet Glow */}
        <div className="absolute top-10 left-1/2 -translate-x-1/2 w-[600px] h-[350px] bg-gradient-to-b from-[#405bff]/25 via-[#7084ff]/10 to-transparent blur-[120px] pointer-events-none -z-10" />

        {/* Eyebrow Tag */}
        <ScrollReveal variant="drop" delayMs={40}>
          <div className="flex justify-center mb-6">
            <div className="inline-flex items-center gap-2 px-4 py-1 rounded-[30px] bg-[#191919] border border-[#405bff]/40 text-[12px] text-[#d1d3d4] shadow-[0_0_20px_rgba(64,91,255,0.2)]">
              <span className="w-2 h-2 rounded-full bg-[#405bff] animate-pulse" />
              <span className="font-mono text-[#7084ff] font-semibold">PUBLIC ALPHA RELEASE</span>
              <span className="text-[#6d6e71]">|</span>
              <span className="font-mono text-[#a7a9ac]">Region: ind-tbn-1 Bare-Metal</span>
            </div>
          </div>
        </ScrollReveal>

        {/* Hero Display Headline (LaunchDarkly signature: Line 1 white, Line 2 Signal Violet, tight 1.0 line height) */}
        <ScrollReveal variant="swoop-up" delayMs={120}>
          <div className="text-center max-w-4xl mx-auto space-y-4">
            <h1 className="text-[44px] sm:text-[70px] lg:text-[88px] font-medium tracking-tight leading-[1.0] text-white">
              Move at wire-speed.
              <br />
              <span className="bg-gradient-to-r from-[#405bff] via-[#7084ff] to-[#3dd6f5] bg-clip-text text-transparent">
                Zero data loss LSM persistence.
              </span>
            </h1>
            <p className="text-[17px] sm:text-[19px] text-[#d1d3d4] max-w-2xl mx-auto leading-relaxed pt-2">
              The managed cloud for Sider. Get dedicated, isolated SkipList MemTable endpoints in seconds with automated WAL crash recovery and NVMe tiered SSTables.
            </p>
          </div>
        </ScrollReveal>

        {/* 4. THE TWO-STEP CONSOLE ACCESS COCKPIT (Laser Border & Spring Drop) */}
        <ScrollReveal variant="drop" delayMs={240} enableTilt={true}>
          <div id="console-wizard" className="mt-14 max-w-2xl mx-auto scroll-mt-28 relative">
            <LaserBorderCard glowColor="#405bff">
              <div className="p-4 sm:p-8">
            
            {/* Step Indicator Header */}
            <div className="flex items-center justify-between border-b border-[#414042] pb-5 mb-6">
              <div className="flex items-center gap-3">
                <span className="flex items-center justify-center w-8 h-8 rounded-full bg-[#405bff] text-white text-[13px] font-semibold shadow-[0_0_10px_rgba(64,91,255,0.5)]">
                  {currentStep}
                </span>
                <div>
                  <h2 className="text-[16px] font-medium text-white">
                    {currentStep === 1
                      ? "Step 1: Developer Identity Verification"
                      : "Step 2: Region & Cluster Verification"}
                  </h2>
                  <p className="text-[12px] text-[#a7a9ac]">
                    {currentStep === 1
                      ? "Verify your authentication status to claim an isolated alpha node."
                      : "Review your allocated bare-metal instance in region ind-tbn-1."}
                  </p>
                </div>
              </div>
              <div className="flex items-center gap-1.5 text-[11px] font-mono font-medium text-[#6d6e71]">
                <span className={currentStep === 1 ? "text-white font-bold" : ""}>01</span>
                <span>/</span>
                <span className={currentStep === 2 ? "text-white font-bold" : ""}>02</span>
              </div>
            </div>

            {/* STEP 1: AUTHENTICATION */}
            {currentStep === 1 && (
              <div className="space-y-5">
                <div className="p-4 rounded-[16px] bg-[#0e0e0e] border border-[#414042] flex items-start gap-3">
                  <span className="text-[18px]">⚡</span>
                  <div className="text-[12px] text-[#a7a9ac] leading-relaxed">
                    <strong className="text-white">Alpha Developer Pass:</strong> Sider Cloud is in active Public Alpha testing. You can authenticate via your Clerk account or proceed with a verified developer session.
                  </div>
                </div>

                {isLoaded && isSignedIn && (
                  <div className="p-4 rounded-[16px] border border-[#405bff]/50 bg-[#405bff]/10 flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      <div className="w-9 h-9 rounded-full bg-[#405bff] text-white flex items-center justify-center font-bold text-[13px] shadow-[0_0_12px_rgba(64,91,255,0.6)]">
                        ✓
                      </div>
                      <div>
                        <div className="text-[13px] font-medium text-white">
                          Authenticated as {user?.fullName || user?.firstName || "Developer"}
                        </div>
                        <div className="text-[11px] font-mono text-[#a7a9ac]">
                          {user?.primaryEmailAddress?.emailAddress}
                        </div>
                      </div>
                    </div>
                    <span className="px-2.5 py-0.5 rounded-[30px] text-[10px] font-mono bg-[#405bff]/20 text-[#7084ff] border border-[#405bff]/40 font-semibold">
                      VERIFIED PASS
                    </span>
                  </div>
                )}

                {isLoaded && !isSignedIn && (
                  <div className="space-y-3">
                    <label className="block text-[11px] font-semibold text-[#a7a9ac] uppercase tracking-wider">
                      Developer Handle or Team Identifier
                    </label>
                    <input
                      type="text"
                      placeholder="e.g. quantum-alpha or dev-team"
                      value={developerName}
                      onChange={(e) => setDeveloperName(e.target.value)}
                      className="w-full bg-[#0e0e0e] border border-[#58595b] px-4 py-2.5 rounded-[10px] text-[13px] font-mono text-white focus:outline-none focus:border-[#405bff]"
                    />

                    <div className="flex items-center gap-3 pt-2">
                      <Link
                        href="/sign-in"
                        className="flex-1 h-11 rounded-[30px] border border-[#414042] bg-[#191919] text-[13px] font-medium text-white hover:bg-[#2c2c2c] transition-colors flex items-center justify-center gap-2"
                      >
                        <span>Sign in with Clerk</span>
                      </Link>
                      <Link
                        href="/sign-up"
                        className="flex-1 h-11 rounded-[30px] border border-[#405bff]/40 bg-[#405bff]/10 text-[13px] font-medium text-[#7084ff] hover:bg-[#405bff]/20 transition-colors flex items-center justify-center gap-2"
                      >
                        <span>Create Account</span>
                      </Link>
                    </div>
                  </div>
                )}

                <div className="pt-4 border-t border-[#414042] flex flex-col sm:flex-row items-stretch sm:items-center justify-between gap-3">
                  <span className="text-[12px] text-[#6d6e71] text-center sm:text-left">
                    Step 1 of 2: Access Protocol
                  </span>
                  <button
                    onClick={() => handleStep1Complete()}
                    className="w-full sm:w-auto px-6 py-2.5 rounded-[30px] bg-[#405bff] hover:bg-[#344bd6] text-white text-[13px] font-medium shadow-[0_0_20px_rgba(64,91,255,0.4)] transition-all flex items-center justify-center gap-2 cursor-pointer"
                  >
                    <span>Proceed to ind-tbn-1 Allocation</span>
                    <span>&rarr;</span>
                  </button>
                </div>
              </div>
            )}

            {/* STEP 2: CLUSTER ALLOCATION */}
            {currentStep === 2 && (
              <div className="space-y-5">
                <div className="space-y-2.5">
                  <div className="flex items-center justify-between text-[12.5px] p-3.5 rounded-[12px] bg-[#0e0e0e] border border-[#414042]">
                    <span className="text-[#a7a9ac]">Developer Profile</span>
                    <span className="font-semibold text-white font-mono">
                      {developerName || (isSignedIn ? user?.firstName : "alpha-developer")}
                    </span>
                  </div>

                  <div className="flex items-center justify-between text-[12.5px] p-3.5 rounded-[12px] bg-[#0e0e0e] border border-[#414042]">
                    <span className="text-[#a7a9ac]">Allocated Cloud Region</span>
                    <span className="font-mono text-[#7084ff] font-semibold">
                      {clusterRegion}
                    </span>
                  </div>

                  <div className="flex items-center justify-between text-[12.5px] p-3.5 rounded-[12px] bg-[#0e0e0e] border border-[#414042]">
                    <span className="text-[#a7a9ac]">Primary Database Target</span>
                    <span className="font-mono text-white">
                      alpha-production (:4100 TCP / :5100 HTTP)
                    </span>
                  </div>

                  <div className="flex items-center justify-between text-[12.5px] p-3.5 rounded-[12px] bg-[#0e0e0e] border border-[#414042]">
                    <span className="text-[#a7a9ac]">Host Hardware Tier</span>
                    <span className="font-mono text-[#3dd6f5]">
                      Intel i5-9600 • 16GB DDR4 • GTX 1660 Ti • 480GB NVMe
                    </span>
                  </div>
                </div>

                {/* Explicit Alpha Release Notice Box */}
                <div className="p-4 rounded-[16px] border border-[#405bff]/30 bg-[#405bff]/5 text-[12px] space-y-1">
                  <div className="font-semibold text-white flex items-center gap-1.5">
                    <span className="text-[#7084ff]">⚡</span>
                    <span>Sider Cloud Public Alpha Agreement</span>
                  </div>
                  <p className="text-[#a7a9ac] leading-relaxed">
                    This cluster instance runs in the <strong>ind-tbn-1</strong> bare-metal zone. Sub-millisecond read/write latency is active. Live WAL crash recovery is automatically engaged.
                  </p>
                </div>

                <div className="pt-4 border-t border-[#414042] flex flex-col-reverse sm:flex-row items-stretch sm:items-center justify-between gap-3">
                  <button
                    onClick={() => setCurrentStep(1)}
                    className="text-[12px] text-[#a7a9ac] hover:text-white underline cursor-pointer text-center sm:text-left py-1"
                  >
                    &larr; Back to Step 1
                  </button>
                  <button
                    onClick={handleLaunchConsole}
                    disabled={isLaunching}
                    className="w-full sm:w-auto px-7 py-2.5 rounded-[30px] bg-[#405bff] hover:bg-[#344bd6] text-white text-[13px] font-medium shadow-[0_0_25px_rgba(64,91,255,0.5)] transition-all flex items-center justify-center gap-2 cursor-pointer"
                  >
                    {isLaunching ? (
                      <>
                        <span className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                        <span>Entering Neon Cockpit...</span>
                      </>
                    ) : (
                      <>
                        <span>Enter Console Cockpit</span>
                        <span>&rarr;</span>
                      </>
                    )}
                  </button>
                </div>
              </div>
            )}
              </div>
            </LaserBorderCard>
          </div>
        </ScrollReveal>
      </section>

      {/* KINETIC LIVE OPERATIONS STREAM RIBBON */}
      <KineticStreamRibbon />

      {/* BUILD SECTION (Quests & build.md Agent Blueprints) */}
      <BuildSection />

      {/* 5. PRODUCT WHITE PANEL (LaunchDarkly high-contrast signature) */}
      <section className="w-full max-w-[1200px] mx-auto px-4 sm:px-6 py-14">
        <ScrollReveal variant="swoop-up" delayMs={80} enableTilt={true}>
          <div className="text-center mb-8">
            <span className="text-[11px] font-semibold text-[#7084ff] uppercase tracking-wider font-mono">
              LIVE CLUSTER DEMONSTRATION
            </span>
            <h2 className="text-[30px] sm:text-[38px] font-medium text-white tracking-tight mt-1">
              Real-Time Engine Visualizer
            </h2>
            <p className="text-[14px] text-[#a7a9ac]">
              Clean white product workspace floating on the dark cockpit canvas.
            </p>
          </div>

        {/* White panel with soft shadow */}
        <div className="bg-[#ffffff] text-[#1b1b1b] rounded-[20px] sm:rounded-[24px] p-4 sm:p-8 shadow-[0_10px_40px_rgba(0,0,0,0.8)] border border-white/20 relative overflow-hidden">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between pb-6 border-b border-[#e0e1e6] gap-4">
            <div className="flex items-center gap-3">
              <SiderLogo size={28} />
              <div>
                <div className="text-[16px] font-semibold text-[#1b1b1b] flex items-center gap-2">
                  <span>alpha-production</span>
                  <span className="px-2 py-0.5 rounded-[30px] text-[10px] font-mono bg-[#405bff]/10 text-[#405bff] font-semibold">
                    ind-tbn-1
                  </span>
                </div>
                <div className="text-[12px] text-[#60646c]">
                  Endpoint: sider://default:sdr_live_••••••@100.95.206.7:4100
                </div>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <span className="w-2.5 h-2.5 rounded-full bg-[#19a05f] animate-pulse" />
              <span className="text-[12px] font-mono font-medium text-[#1b1b1b]">
                SkipList Active: 316,746 ops/sec (P50: 234µs)
              </span>
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mt-6">
            <div className="p-4 rounded-[12px] bg-[#f7f7f8] border border-[#e0e1e6]">
              <span className="text-[11px] text-[#60646c] uppercase font-semibold">In-Memory Keys</span>
              <div className="text-[24px] font-semibold font-mono text-[#1b1b1b] mt-1">42,891</div>
              <span className="text-[10px] text-[#19a05f] font-medium">SkipList Level 12</span>
            </div>
            <div className="p-4 rounded-[12px] bg-[#f7f7f8] border border-[#e0e1e6]">
              <span className="text-[11px] text-[#60646c] uppercase font-semibold">WAL Append Rate</span>
              <div className="text-[24px] font-semibold font-mono text-[#405bff] mt-1">0.23 ms</div>
              <span className="text-[10px] text-[#60646c] font-medium">CRC32 Verified</span>
            </div>
            <div className="p-4 rounded-[12px] bg-[#f7f7f8] border border-[#e0e1e6]">
              <span className="text-[11px] text-[#60646c] uppercase font-semibold">Bloom Filter Accuracy</span>
              <div className="text-[24px] font-semibold font-mono text-[#1b1b1b] mt-1">99.8%</div>
              <span className="text-[10px] text-[#60646c] font-medium">0 Disk Seeks on Miss</span>
            </div>
            <div className="p-4 rounded-[12px] bg-[#f7f7f8] border border-[#e0e1e6]">
              <span className="text-[11px] text-[#60646c] uppercase font-semibold">SSTable Tier</span>
              <div className="text-[24px] font-semibold font-mono text-[#1b1b1b] mt-1">L0 &rarr; L1</div>
              <span className="text-[10px] text-[#60646c] font-medium">Leveled Compaction</span>
            </div>
          </div>
        </div>
        </ScrollReveal>
      </section>



      {/* 6. CODE SNIPPET INTEGRATION (Carbon Panel with Dracula Syntax) */}
      <section id="quickstart" className="w-full max-w-[1200px] mx-auto px-4 sm:px-6 py-14 border-t border-[#414042]/50">
        <ScrollReveal variant="swoop-up" delayMs={80} enableTilt={true}>
          <div className="bg-[#191919] rounded-[30px] border border-[#414042] p-6 sm:p-8 shadow-[0_0_30px_rgba(0,0,0,0.6)]">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-6">
            <div>
              <span className="text-[11px] font-semibold text-[#7084ff] uppercase tracking-wider font-mono">
                MULTI-LANGUAGE INTEGRATION
              </span>
              <h2 className="text-[24px] font-medium text-white tracking-tight mt-1">
                Copy, paste, go.
              </h2>
              <p className="text-[13px] text-[#a7a9ac]">
                Connect natively over raw TCP or use our high-throughput SDK drivers.
              </p>
            </div>

            {/* Language tabs */}
            <div className="flex items-center gap-1.5 bg-[#0e0e0e] p-1 rounded-[30px] border border-[#414042]">
              {(["python", "node", "go", "tcp"] as const).map((lang) => (
                <button
                  key={lang}
                  onClick={() => setActiveTab(lang)}
                  className={`px-3 py-1 rounded-[30px] text-[12px] font-mono transition-colors cursor-pointer ${
                    activeTab === lang
                      ? "bg-[#405bff] text-white font-medium"
                      : "text-[#d1d3d4] hover:text-white"
                  }`}
                >
                  {lang.toUpperCase()}
                </button>
              ))}
            </div>
          </div>

          {/* Dracula syntax code block with Kinetic Swoop */}
          <div className="relative rounded-[16px] bg-[#0e0e0e] p-5 font-mono text-[13px] border border-[#414042] overflow-x-auto">
            <button
              onClick={copyQuickstart}
              className="absolute top-4 right-4 px-3.5 py-1 rounded-[30px] bg-[#191919] border border-[#414042] text-[11px] text-[#d1d3d4] hover:text-white hover:border-[#405bff] transition-all cursor-pointer z-20 active:scale-95"
            >
              {copiedCode ? "✓ Copied" : "Copy Code"}
            </button>

            <div key={activeTab} className="animate-[tabSlideIn_0.32s_cubic-bezier(0.16,1,0.3,1)]">

            {activeTab === "python" && (
              <pre className="text-[#f8f8f2] leading-relaxed">
                <code>
                  <span className="text-[#66d9ef]">import</span> socket{"\n"}
                  {"\n"}
                  <span className="text-[#75715e]"># Connect to Sider Cloud Alpha node in region ind-tbn-1</span>{"\n"}
                  client = socket.socket(socket.AF_INET, socket.SOCK_STREAM){"\n"}
                  client.connect((<span className="text-[#a6e22e]">&quot;100.95.206.7&quot;</span>, <span className="text-[#ae81ff]">4100</span>)){"\n"}
                  {"\n"}
                  <span className="text-[#75715e]"># Authenticate and execute commands</span>{"\n"}
                  client.sendall(<span className="text-[#a6e22e]">b&quot;AUTH sdr_live_793855e4ec0e70901bef05344264ba98\n&quot;</span>){"\n"}
                  client.sendall(<span className="text-[#a6e22e]">{"b\"PUT user:1001 {\\\"role\\\":\\\"admin\\\"}\\n\""}</span>){"\n"}
                  client.sendall(<span className="text-[#a6e22e]">b&quot;GET user:1001\n&quot;</span>){"\n"}
                  print(client.recv(<span className="text-[#ae81ff]">1024</span>).decode())
                </code>
              </pre>
            )}

            {activeTab === "node" && (
              <pre className="text-[#f8f8f2] leading-relaxed">
                <code>
                  <span className="text-[#66d9ef]">import</span> net <span className="text-[#66d9ef]">from</span> <span className="text-[#a6e22e]">&quot;net&quot;</span>;{"\n"}
                  {"\n"}
                  <span className="text-[#66d9ef]">const</span> client = net.createConnection({`{`} host: <span className="text-[#a6e22e]">&quot;100.95.206.7&quot;</span>, port: <span className="text-[#ae81ff]">4100</span> {`}`}, () =&gt; {`{`}{"\n"}
                  {"  "}client.write(<span className="text-[#a6e22e]">&quot;AUTH sdr_live_793855e4ec0e70901bef05344264ba98\n&quot;</span>);{"\n"}
                  {"  "}client.write(<span className="text-[#a6e22e]">&quot;PUT cache:session &#39;active&#39;\n&quot;</span>);{"\n"}
                  {"  "}client.write(<span className="text-[#a6e22e]">&quot;GET cache:session\n&quot;</span>);{"\n"}
                  {`}`});{"\n"}
                  client.on(<span className="text-[#a6e22e]">&quot;data&quot;</span>, (d) =&gt; console.log(d.toString()));
                </code>
              </pre>
            )}

            {activeTab === "go" && (
              <pre className="text-[#f8f8f2] leading-relaxed">
                <code>
                  <span className="text-[#66d9ef]">package</span> main{"\n"}
                  <span className="text-[#66d9ef]">import</span> (<span className="text-[#a6e22e]">&quot;fmt&quot;</span>; <span className="text-[#a6e22e]">&quot;net&quot;</span>){"\n"}
                  {"\n"}
                  <span className="text-[#66d9ef]">func</span> main() {`{`}{"\n"}
                  {"  "}conn, _ := net.Dial(<span className="text-[#a6e22e]">&quot;tcp&quot;</span>, <span className="text-[#a6e22e]">&quot;100.95.206.7:4100&quot;</span>){"\n"}
                  {"  "}fmt.Fprintf(conn, <span className="text-[#a6e22e]">&quot;AUTH sdr_live_793855e4ec0e70901bef05344264ba98\n&quot;</span>){"\n"}
                  {"  "}fmt.Fprintf(conn, <span className="text-[#a6e22e]">&quot;PUT metric:latency 140\n&quot;</span>){"\n"}
                  {"  "}fmt.Fprintf(conn, <span className="text-[#a6e22e]">&quot;GET metric:latency\n&quot;</span>){"\n"}
                  {`}`}
                </code>
              </pre>
            )}

            {activeTab === "tcp" && (
              <pre className="text-[#f8f8f2] leading-relaxed">
                <code>
                  <span className="text-[#75715e]"># Netcat pipeline over raw TCP socket</span>{"\n"}
                  nc <span className="text-[#a6e22e]">100.95.206.7</span> <span className="text-[#ae81ff]">4100</span>{"\n"}
                  AUTH sdr_live_793855e4ec0e70901bef05344264ba98{"\n"}
                  PUT user:status active{"\n"}
                  GET user:status{"\n"}
                  INFO
                </code>
              </pre>
            )}
            </div>
          </div>
        </div>
        </ScrollReveal>
      </section>

      {/* DOCUMENTATION CODEX (Wire Protocol & APIs) */}
      <DocsSection />

      {/* 7. BARE-METAL CLUSTER SPECIFICATIONS CARD */}
      <section id="specs" className="w-full max-w-[1200px] mx-auto px-4 sm:px-6 py-14 border-t border-[#414042]/50">
        <ScrollReveal variant="whoop-in" delayMs={80} enableTilt={true}>
          <div className="bg-[#191919] rounded-[24px] sm:rounded-[30px] border border-[#414042] p-4 sm:p-8 shadow-[0_0_30px_rgba(0,0,0,0.5)]">
          <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 pb-6 border-b border-[#414042]">
            <div>
              <span className="text-[11px] font-semibold text-[#7084ff] uppercase tracking-wider font-mono">
                HOST ENVIRONMENT
              </span>
              <h2 className="text-[26px] font-medium text-white tracking-tight mt-1">
                Alpha Cloud Node Hardware Specifications
              </h2>
              <p className="text-[13px] text-[#a7a9ac] mt-1">
                All Sider Cloud instances in the Alpha tier run on dedicated bare-metal hardware.
              </p>
            </div>
            <span className="px-3.5 py-1 rounded-[30px] text-[12px] font-medium bg-[#405bff]/20 text-[#7084ff] border border-[#405bff]/40 font-mono shrink-0">
              ● REGION: ind-tbn-1 ACTIVE
            </span>
          </div>

          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-6 mt-6">
            <div>
              <span className="text-[11px] text-[#6d6e71] uppercase font-semibold">Architecture</span>
              <div className="font-mono text-white text-[14px] font-medium mt-1">x86_64 Bare-Metal</div>
            </div>
            <div>
              <span className="text-[11px] text-[#6d6e71] uppercase font-semibold">Processor (CPU)</span>
              <div className="font-mono text-white text-[14px] font-medium mt-1">Intel i5-9600 (6C)</div>
            </div>
            <div>
              <span className="text-[11px] text-[#6d6e71] uppercase font-semibold">Memory (RAM)</span>
              <div className="font-mono text-white text-[14px] font-medium mt-1">16GB DDR4</div>
            </div>
            <div>
              <span className="text-[11px] text-[#6d6e71] uppercase font-semibold">GPU Accelerator</span>
              <div className="font-mono text-[#7084ff] text-[14px] font-medium mt-1">NVIDIA GTX 1660 Ti</div>
            </div>
            <div>
              <span className="text-[11px] text-[#6d6e71] uppercase font-semibold">Primary Storage</span>
              <div className="font-mono text-white text-[14px] font-medium mt-1">480GB NVMe SSD</div>
            </div>
            <div>
              <span className="text-[11px] text-[#6d6e71] uppercase font-semibold">Encrypted Mesh</span>
              <div className="font-mono text-[#3dd6f5] text-[14px] font-medium mt-1">WireGuard Tailscale</div>
            </div>
          </div>
        </div>
        </ScrollReveal>
      </section>

      <CloudFooter />
    </div>
  );
}

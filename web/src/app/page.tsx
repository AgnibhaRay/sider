"use client";

import React, { useState, useEffect } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { SiderLogo } from "../components/SiderLogo";
import { useUser, SignInButton, SignUpButton, UserButton } from "@clerk/nextjs";

export default function SiderCloudLanding() {
  const router = useRouter();
  const { isSignedIn, user, isLoaded } = useUser();

  // Two-Step Console Launch Wizard State
  const [currentStep, setCurrentStep] = useState<1 | 2>(1);
  const [developerName, setDeveloperName] = useState<string>("");
  const [clusterRegion] = useState<string>("ap-south-1 (Edge Bare-Metal Node)");
  const [isLaunching, setIsLaunching] = useState<boolean>(false);
  const [copiedCode, setCopiedCode] = useState<boolean>(false);

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
    }, 600);
  };

  const copyQuickstart = () => {
    navigator.clipboard.writeText(`pip install sider-py\n# or connect directly via raw TCP socket on port 4100:\nnc 100.95.206.7 4100\nAUTH sdr_live_793855e4ec0e70901bef05344264ba98\nPUT user:101 '{"status":"active"}'\nGET user:101`);
    setCopiedCode(true);
    setTimeout(() => setCopiedCode(false), 2000);
  };

  return (
    <div
      className="min-h-screen text-[#1b1b1b] flex flex-col font-sans select-none antialiased"
      style={{ backgroundColor: "#eaeaea", fontFamily: "var(--font-inter), sans-serif" }}
    >
      {/* 1. TOP ANNOUNCEMENT BAR — PROMINENT ALPHA RELEASE NOTICE */}
      <div
        className="w-full h-11 px-4 text-white text-[13px] font-medium flex items-center justify-center gap-2 shadow-sm z-50 sticky top-0"
        style={{
          background: "linear-gradient(89.97deg, rgb(25, 160, 95) 0.02%, rgb(13, 127, 140) 123.85%)"
        }}
      >
        <span className="bg-white/20 text-white font-mono text-[10px] uppercase font-bold px-2 py-0.5 rounded tracking-wider">
          PUBLIC ALPHA v0.2.1
        </span>
        <span>
          <strong>Sider Cloud is now live in Public Alpha.</strong> Sub-millisecond distributed LSM key-value store with instant TCP endpoints.
        </span>
        <button
          onClick={() => {
            const el = document.getElementById("console-wizard");
            el?.scrollIntoView({ behavior: "smooth" });
          }}
          className="underline hover:opacity-90 font-semibold ml-1 cursor-pointer"
        >
          Claim Alpha Endpoint &rarr;
        </button>
      </div>

      {/* 2. CLINICAL MARBLE NAVIGATION BAR */}
      <header className="w-full bg-[#ffffff] border-b border-[#e0e1e6] px-6 h-16 flex items-center justify-between sticky top-11 z-40">
        <div className="flex items-center gap-4">
          <Link href="/" className="flex items-center gap-3">
            <SiderLogo size={30} />
            <div className="flex flex-col">
              <span className="text-[16px] font-semibold text-[#1b1b1b] tracking-[-0.5px]">
                Sider Cloud
              </span>
              <span className="text-[10px] text-[#7c7c7c] uppercase tracking-wider font-semibold">
                Distributed LSM Platform
              </span>
            </div>
          </Link>
          <span className="hidden sm:inline-block px-2.5 py-0.5 rounded-[40px] text-[11px] font-medium bg-[#0d7f8c]/15 text-[#0d7f8c] border border-[#0d7f8c]/30 font-mono">
            ALPHA RELEASE
          </span>
        </div>

        {/* Center Links */}
        <nav className="hidden md:flex items-center gap-6 text-[13px] text-[#60646c] font-medium">
          <a href="#console-wizard" className="hover:text-[#1b1b1b] transition-colors">
            Get Started
          </a>
          <a href="#architecture" className="hover:text-[#1b1b1b] transition-colors">
            Architecture
          </a>
          <a href="#specs" className="hover:text-[#1b1b1b] transition-colors">
            Bare-Metal Specs
          </a>
          <a href="#quickstart" className="hover:text-[#1b1b1b] transition-colors">
            SDK Quickstart
          </a>
        </nav>

        {/* Right Actions */}
        <div className="flex items-center gap-3">
          {isLoaded && isSignedIn ? (
            <div className="flex items-center gap-2">
              <span className="text-[12px] text-[#60646c] hidden sm:inline">
                {user?.primaryEmailAddress?.emailAddress || "Developer"}
              </span>
              <UserButton />
            </div>
          ) : (
            <div className="flex items-center gap-2.5">
              <Link
                href="/sign-in"
                className="px-4 py-1.5 rounded-[40px] border border-[#e0e1e6] bg-white text-[#1b1b1b] text-[13px] font-medium hover:bg-[#eaeaea]/60 transition-colors"
              >
                Sign in
              </Link>
              <Link
                href="/sign-up"
                className="px-4 py-1.5 rounded-[6px] bg-[#1b1b1b] text-white text-[13px] font-medium hover:opacity-90 transition-opacity shadow-[0px_4px_20px_0px_rgba(0,0,0,0.15)] hidden sm:inline-block"
              >
                Sign up
              </Link>
            </div>
          )}

          <Link
            href="/console"
            className="px-4 py-1.5 rounded-[6px] bg-[#1b1b1b] text-white text-[13px] font-medium hover:opacity-90 transition-opacity shadow-[0px_4px_20px_0px_rgba(0,0,0,0.15)] flex items-center gap-1.5"
          >
            <span className="w-1.5 h-1.5 rounded-full bg-[#19a05f] animate-pulse" />
            <span>Console</span>
          </Link>
        </div>
      </header>

      {/* 3. HERO STAGE */}
      <section className="w-full max-w-[1280px] mx-auto px-4 sm:px-6 pt-12 pb-16 lg:pt-16 lg:pb-20">
        
        {/* Status Callout Pill */}
        <div className="flex justify-center mb-6">
          <div className="inline-flex items-center gap-2 px-3.5 py-1 rounded-[40px] bg-[#ffffff] border border-[#e0e1e6] text-[12px] text-[#60646c] shadow-sm">
            <span className="w-2 h-2 rounded-full bg-[#19a05f] animate-pulse" />
            <strong className="text-[#1b1b1b] font-semibold">Public Alpha</strong>
            <span>— Edge Bare-Metal Node active in ap-south-1</span>
          </div>
        </div>

        {/* Hero Title */}
        <div className="text-center max-w-4xl mx-auto space-y-4">
          <h1
            className="text-[36px] sm:text-[56px] lg:text-[68px] font-semibold text-[#1b1b1b] tracking-[-1.5px] leading-[1.08]"
          >
            The Managed Cloud for Sider.
            <br />
            <span className="text-[#60646c]">Built for Sub-Millisecond Speed.</span>
          </h1>
          <p className="text-[16px] sm:text-[19px] text-[#60646c] max-w-2xl mx-auto leading-relaxed">
            Provision dedicated, isolated LSM-tree database instances in seconds. Zero dependencies, lock-free SkipList MemTable, WAL persistence, and 8 native language drivers.
          </p>
        </div>

        {/* 4. THE TWO-STEP CONSOLE ACCESS WIZARD */}
        <div id="console-wizard" className="mt-12 max-w-2xl mx-auto scroll-mt-28">
          <div className="bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] p-6 sm:p-8 shadow-sm relative overflow-hidden">
            
            {/* Step Progress Header */}
            <div className="flex items-center justify-between border-b border-[#e0e1e6] pb-5 mb-6">
              <div className="flex items-center gap-3">
                <span className="flex items-center justify-center w-7 h-7 rounded-full bg-[#1b1b1b] text-white text-[12px] font-semibold">
                  {currentStep}
                </span>
                <div>
                  <h2 className="text-[16px] font-semibold text-[#1b1b1b]">
                    {currentStep === 1
                      ? "Step 1: Developer Access & Authentication"
                      : "Step 2: Alpha Cluster Verification & Launch"}
                  </h2>
                  <p className="text-[12px] text-[#60646c]">
                    {currentStep === 1
                      ? "Verify your developer identity to claim your alpha tenant token."
                      : "Review your isolated bare-metal cloud node specifications."}
                  </p>
                </div>
              </div>
              <div className="flex items-center gap-1.5 text-[11px] font-mono font-medium text-[#7c7c7c]">
                <span className={currentStep === 1 ? "text-[#1b1b1b] font-bold" : ""}>1</span>
                <span>/</span>
                <span className={currentStep === 2 ? "text-[#1b1b1b] font-bold" : ""}>2</span>
              </div>
            </div>

            {/* STEP 1: AUTHENTICATION / DEVELOPER PROFILE */}
            {currentStep === 1 && (
              <div className="space-y-5">
                <div className="p-4 rounded-[12px] bg-[#eaeaea]/40 border border-[#e0e1e6] flex items-start gap-3">
                  <span className="text-[18px]">⚡</span>
                  <div className="text-[12px] text-[#60646c] leading-relaxed">
                    <strong className="text-[#1b1b1b]">Alpha Developer Pass:</strong> Sider Cloud is in active Public Alpha testing. You can authenticate via your Clerk account, continue with GitHub, or test instantly with a guest developer token.
                  </div>
                </div>

                {isLoaded && isSignedIn ? (
                  <div className="p-4 rounded-[12px] border border-[#19a05f]/40 bg-[#19a05f]/5 flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      <div className="w-8 h-8 rounded-full bg-[#19a05f] text-white flex items-center justify-center font-bold text-[12px]">
                        ✓
                      </div>
                      <div>
                        <div className="text-[13px] font-semibold text-[#1b1b1b]">
                          Authenticated as {user?.fullName || user?.firstName || "Developer"}
                        </div>
                        <div className="text-[11px] font-mono text-[#60646c]">
                          {user?.primaryEmailAddress?.emailAddress}
                        </div>
                      </div>
                    </div>
                    <span className="px-2 py-0.5 rounded text-[10px] font-mono bg-[#19a05f]/20 text-[#19a05f] font-semibold">
                      VERIFIED
                    </span>
                  </div>
                ) : (
                  <div className="space-y-3">
                    <label className="block text-[12px] font-semibold text-[#7c7c7c] uppercase">
                      Developer Name / Team Handle
                    </label>
                    <input
                      type="text"
                      placeholder="e.g. alex-team or alpha-tester"
                      value={developerName}
                      onChange={(e) => setDeveloperName(e.target.value)}
                      className="w-full bg-[#eaeaea]/60 border border-[#e0e1e6] px-3.5 py-2.5 rounded-[6px] text-[13px] font-mono text-[#1b1b1b] focus:outline-none focus:border-[#1b1b1b]"
                    />

                    <div className="flex items-center gap-3 pt-2">
                      <Link
                        href="/sign-in"
                        className="flex-1 h-10 rounded-[6px] border border-[#e0e1e6] bg-white text-[13px] font-medium text-[#1b1b1b] hover:bg-[#eaeaea]/60 transition-colors flex items-center justify-center gap-2"
                      >
                        <span>Sign in with Clerk</span>
                      </Link>
                      <Link
                        href="/sign-up"
                        className="flex-1 h-10 rounded-[6px] border border-[#e0e1e6] bg-white text-[13px] font-medium text-[#1b1b1b] hover:bg-[#eaeaea]/60 transition-colors flex items-center justify-center gap-2"
                      >
                        <span>Create Account</span>
                      </Link>
                    </div>
                  </div>
                )}

                <div className="pt-4 border-t border-[#e0e1e6] flex items-center justify-between">
                  <span className="text-[12px] text-[#7c7c7c]">
                    Step 1 of 2: Identity Setup
                  </span>
                  <button
                    onClick={() => handleStep1Complete()}
                    className="px-5 py-2.5 rounded-[6px] bg-[#1b1b1b] text-white text-[13px] font-medium hover:opacity-90 transition-opacity shadow-[0px_4px_20px_0px_rgba(0,0,0,0.15)] flex items-center gap-1.5 cursor-pointer"
                  >
                    <span>Proceed to Cluster Allocation</span>
                    <span>&rarr;</span>
                  </button>
                </div>
              </div>
            )}

            {/* STEP 2: CLUSTER VERIFICATION & CONSOLE LAUNCH */}
            {currentStep === 2 && (
              <div className="space-y-5">
                <div className="space-y-3">
                  <div className="flex items-center justify-between text-[12px] p-3 rounded-[8px] bg-[#eaeaea]/50 border border-[#e0e1e6]">
                    <span className="text-[#60646c]">Developer Identity</span>
                    <span className="font-semibold text-[#1b1b1b]">
                      {developerName || (isSignedIn ? user?.firstName : "alpha-developer")}
                    </span>
                  </div>

                  <div className="flex items-center justify-between text-[12px] p-3 rounded-[8px] bg-[#eaeaea]/50 border border-[#e0e1e6]">
                    <span className="text-[#60646c]">Allocated Cloud Region</span>
                    <span className="font-mono text-[#0d7f8c] font-medium">
                      {clusterRegion}
                    </span>
                  </div>

                  <div className="flex items-center justify-between text-[12px] p-3 rounded-[8px] bg-[#eaeaea]/50 border border-[#e0e1e6]">
                    <span className="text-[#60646c]">Target Primary Database</span>
                    <span className="font-mono text-[#1b1b1b] font-medium">
                      alpha-production (:4100)
                    </span>
                  </div>

                  <div className="flex items-center justify-between text-[12px] p-3 rounded-[8px] bg-[#eaeaea]/50 border border-[#e0e1e6]">
                    <span className="text-[#60646c]">Node Hardware Specs</span>
                    <span className="font-mono text-[#1b1b1b]">
                      i5-9600 • 16GB DDR4 • GTX 1660 Ti • NVMe
                    </span>
                  </div>
                </div>

                {/* Explicit Alpha Release Notice Box */}
                <div className="p-4 rounded-[12px] border border-[#e0e1e6] bg-[#eaeaea]/40 text-[12px] space-y-1">
                  <div className="font-semibold text-[#1b1b1b] flex items-center gap-1.5">
                    <span className="text-[#eab308]">⚠️</span>
                    <span>Sider Cloud Public Alpha Agreement</span>
                  </div>
                  <p className="text-[#60646c] leading-relaxed">
                    This instance is part of the Sider Cloud Public Alpha. Workloads run on bare-metal edge hardware with crash-recovery WAL and memory SkipList active. No credit card required.
                  </p>
                </div>

                <div className="pt-4 border-t border-[#e0e1e6] flex items-center justify-between">
                  <button
                    onClick={() => setCurrentStep(1)}
                    className="text-[12px] text-[#60646c] hover:text-[#1b1b1b] underline cursor-pointer"
                  >
                    &larr; Back to Step 1
                  </button>
                  <button
                    onClick={handleLaunchConsole}
                    disabled={isLaunching}
                    className="px-6 py-2.5 rounded-[6px] bg-[#1b1b1b] text-white text-[13px] font-medium hover:opacity-90 transition-opacity shadow-[0px_4px_20px_0px_rgba(0,0,0,0.15)] flex items-center gap-2 cursor-pointer"
                  >
                    {isLaunching ? (
                      <>
                        <span className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                        <span>Launching Console Studio...</span>
                      </>
                    ) : (
                      <>
                        <span>Enter Cloud Console Studio</span>
                        <span>&rarr;</span>
                      </>
                    )}
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      </section>

      {/* 5. ARCHITECTURAL PILLARS (CLINICAL GRAPH PAPER GRID) */}
      <section id="architecture" className="w-full max-w-[1280px] mx-auto px-4 sm:px-6 py-16 border-t border-[#e0e1e6]">
        <div className="mb-10 text-center sm:text-left">
          <span className="text-[11px] font-semibold text-[#7c7c7c] uppercase tracking-wider font-mono">
            ENGINEERING HIGHLIGHTS
          </span>
          <h2 className="text-[28px] sm:text-[36px] font-semibold text-[#1b1b1b] tracking-[-1px] mt-1">
            Engineered like Graph Paper. Built from Scratch.
          </h2>
          <p className="text-[14px] text-[#60646c] max-w-xl mt-1">
            Zero external Cgo dependencies, zero heavy runtime baggage. Pure Go LSM storage designed for developers who demand deterministic latency.
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div className="bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] p-6 shadow-sm">
            <div className="w-10 h-10 rounded-[8px] bg-[#eaeaea] flex items-center justify-center font-mono font-bold text-[14px] text-[#1b1b1b] mb-4">
              01
            </div>
            <h3 className="text-[17px] font-semibold text-[#1b1b1b]">
              Native SkipList MemTable
            </h3>
            <p className="text-[13px] text-[#60646c] mt-2 leading-relaxed">
              In-memory writes execute concurrently in $O(\log N)$ time with lock-free pointer swizzling. Flushes seamlessly to immutable SSTables once threshold is reached.
            </p>
          </div>

          <div className="bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] p-6 shadow-sm">
            <div className="w-10 h-10 rounded-[8px] bg-[#eaeaea] flex items-center justify-center font-mono font-bold text-[14px] text-[#0d7f8c] mb-4">
              02
            </div>
            <h3 className="text-[17px] font-semibold text-[#1b1b1b]">
              Zero Data-Loss WAL
            </h3>
            <p className="text-[13px] text-[#60646c] mt-2 leading-relaxed">
              Every single write is appended sequentially to disk with CRC32 integrity validation. Even during unexpected power loss, all keys recover in milliseconds on startup.
            </p>
          </div>

          <div className="bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] p-6 shadow-sm">
            <div className="w-10 h-10 rounded-[8px] bg-[#eaeaea] flex items-center justify-center font-mono font-bold text-[14px] text-[#19a05f] mb-4">
              03
            </div>
            <h3 className="text-[17px] font-semibold text-[#1b1b1b]">
              Bloom Filter & Sparse Index
            </h3>
            <p className="text-[13px] text-[#60646c] mt-2 leading-relaxed">
              Disk seeks are virtually eliminated on misses using high-efficiency Murmur3 Bloom Filters and sorted string index blocks on NVMe SSD storage.
            </p>
          </div>
        </div>
      </section>

      {/* 6. BARE-METAL CLOUD SPECIFICATIONS CARD */}
      <section id="specs" className="w-full max-w-[1280px] mx-auto px-4 sm:px-6 py-16 border-t border-[#e0e1e6]">
        <div className="bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] p-8 shadow-sm">
          <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 pb-6 border-b border-[#e0e1e6]">
            <div>
              <span className="text-[11px] font-semibold text-[#7c7c7c] uppercase tracking-wider font-mono">
                HOST ENVIRONMENT
              </span>
              <h2 className="text-[26px] font-semibold text-[#1b1b1b] tracking-[-0.5px] mt-1">
                Alpha Cloud Node Hardware Specifications
              </h2>
              <p className="text-[13px] text-[#60646c] mt-1">
                All Sider Cloud instances in the Alpha tier run on dedicated bare-metal hardware.
              </p>
            </div>
            <span className="px-3 py-1 rounded-[40px] text-[12px] font-medium bg-[#19a05f]/15 text-[#19a05f] border border-[#19a05f]/30 font-mono shrink-0">
              ● AP-SOUTH-1 ACTIVE
            </span>
          </div>

          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-6 mt-6">
            <div>
              <span className="text-[11px] text-[#7c7c7c] uppercase font-semibold">Architecture</span>
              <div className="font-mono text-[#1b1b1b] text-[14px] font-medium mt-1">x86_64 Bare-Metal</div>
            </div>
            <div>
              <span className="text-[11px] text-[#7c7c7c] uppercase font-semibold">Processor (CPU)</span>
              <div className="font-mono text-[#1b1b1b] text-[14px] font-medium mt-1">Intel i5-9600 (6C)</div>
            </div>
            <div>
              <span className="text-[11px] text-[#7c7c7c] uppercase font-semibold">Memory (RAM)</span>
              <div className="font-mono text-[#1b1b1b] text-[14px] font-medium mt-1">16GB DDR4</div>
            </div>
            <div>
              <span className="text-[11px] text-[#7c7c7c] uppercase font-semibold">GPU Accelerator</span>
              <div className="font-mono text-[#0d7f8c] text-[14px] font-medium mt-1">NVIDIA GTX 1660 Ti</div>
            </div>
            <div>
              <span className="text-[11px] text-[#7c7c7c] uppercase font-semibold">Primary Storage</span>
              <div className="font-mono text-[#1b1b1b] text-[14px] font-medium mt-1">480GB NVMe SSD</div>
            </div>
            <div>
              <span className="text-[11px] text-[#7c7c7c] uppercase font-semibold">Network Mesh</span>
              <div className="font-mono text-[#19a05f] text-[14px] font-medium mt-1">Encrypted WireGuard</div>
            </div>
          </div>
        </div>
      </section>

      {/* 7. QUICKSTART CODE STRIP */}
      <section id="quickstart" className="w-full max-w-[1280px] mx-auto px-4 sm:px-6 py-16 border-t border-[#e0e1e6]">
        <div className="bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] p-8 shadow-sm">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-5">
            <div>
              <h2 className="text-[22px] font-semibold text-[#1b1b1b]" style={{ letterSpacing: "-0.5px" }}>
                Connect via Any TCP Client or SDK
              </h2>
              <p className="text-[13px] text-[#60646c]">
                Works with Python, Node.js, Go, Rust, Java, C#, or raw netcat pipelines.
              </p>
            </div>
            <button
              onClick={copyQuickstart}
              className="px-4 py-1.5 rounded-[6px] bg-[#1b1b1b] text-white text-[12px] font-medium hover:opacity-90 transition-opacity shrink-0 shadow-[0px_4px_20px_0px_rgba(0,0,0,0.15)] cursor-pointer"
            >
              {copiedCode ? "✓ Copied" : "Copy Pipeline"}
            </button>
          </div>

          <div className="rounded-[12px] bg-[#1b1b1b] p-4 text-[#eaeaea] font-mono text-[12.5px] overflow-x-auto leading-relaxed border border-[#e0e1e6]">
            <pre>
              <code>{`# Raw TCP connection to Alpha cluster
nc 100.95.206.7 4100
AUTH sdr_live_793855e4ec0e70901bef05344264ba98
PUT order:2049 '{"items":4,"status":"paid"}'
GET order:2049
INFO`}</code>
            </pre>
          </div>
        </div>
      </section>

      {/* 8. CLINICAL FOOTER */}
      <footer className="w-full bg-[#ffffff] border-t border-[#e0e1e6] px-6 py-10 text-[13px] text-[#60646c]">
        <div className="max-w-[1280px] mx-auto flex flex-col sm:flex-row items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <SiderLogo size={24} />
            <span className="font-semibold text-[#1b1b1b]">Sider Cloud</span>
            <span className="text-[11px] font-mono bg-[#eaeaea] px-2 py-0.5 rounded border border-[#e0e1e6]">
              Alpha v0.2.1
            </span>
          </div>

          <div className="flex items-center gap-6">
            <Link href="/console" className="hover:text-[#1b1b1b] font-medium">
              Console Studio
            </Link>
            <Link href="/sign-in" className="hover:text-[#1b1b1b]">
              Sign in
            </Link>
            <Link href="/sign-up" className="hover:text-[#1b1b1b]">
              Sign up
            </Link>
            <a
              href="https://github.com/AgnibhaRay/sider"
              target="_blank"
              rel="noopener noreferrer"
              className="hover:text-[#1b1b1b]"
            >
              GitHub Repository
            </a>
          </div>

          <div className="text-[12px] text-[#7c7c7c]">
            Built with precision in Go & Next.js
          </div>
        </div>
      </footer>
    </div>
  );
}

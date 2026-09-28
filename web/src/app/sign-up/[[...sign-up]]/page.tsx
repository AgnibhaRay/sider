"use client";

import React, { useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { SignUp } from "@clerk/nextjs";
import { SiderLogo } from "@/components/SiderLogo";

export default function SignUpPage() {
  const router = useRouter();
  const publishableKey = process.env.NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY;
  const isClerkConfigured = !!publishableKey && publishableKey.startsWith("pk_");

  const [email, setEmail] = useState("");
  const [teamName, setTeamName] = useState("");
  const [isLoading, setIsLoading] = useState(false);

  const handleDemoSignUp = (e: React.FormEvent) => {
    e.preventDefault();
    setIsLoading(true);
    setTimeout(() => {
      setIsLoading(false);
      router.push("/console");
    }, 400);
  };

  return (
    <div className="min-h-screen bg-[#0e0e0e] text-[#ffffff] flex flex-col font-sans select-none antialiased">
      {/* Top Signal Strip */}
      <div
        className="w-full h-10 px-4 text-white text-[12px] font-medium flex items-center justify-center gap-2 border-b border-[#414042]/40"
        style={{
          background: "linear-gradient(179deg, rgba(64,91,255,0.2) 1.06%, rgba(112,132,255,0.05) 123.42%)"
        }}
      >
        <span className="w-1.5 h-1.5 rounded-full bg-[#405bff] animate-pulse" />
        <span className="font-mono text-[#7084ff] uppercase font-semibold tracking-wider text-[11px]">
          ind-tbn-1 ALPHA NODE
        </span>
        <span className="text-[#d1d3d4]">
          — Zero-dependency LSM storage with native SkipList MemTable.
        </span>
      </div>

      {/* Header Pill */}
      <div className="w-full max-w-[1100px] mx-auto pt-6 px-4">
        <header className="w-full bg-[#191919] border border-white/10 rounded-[60px] px-6 h-14 flex items-center justify-between shadow-[0_4px_20px_rgba(0,0,0,0.45)]">
          <Link href="/" className="flex items-center gap-3">
            <SiderLogo size={24} />
            <div className="flex items-center gap-2">
              <span className="text-[15px] font-semibold text-white tracking-[-0.02em]">
                Sider Cloud
              </span>
              <span className="text-[10px] font-mono uppercase bg-[#405bff]/20 text-[#7084ff] border border-[#405bff]/40 px-2 py-0.5 rounded-[30px]">
                ALPHA
              </span>
            </div>
          </Link>
          <Link
            href="/console"
            className="text-[13px] text-[#a7a9ac] hover:text-white font-medium transition-colors"
          >
            Launch Console &rarr;
          </Link>
        </header>
      </div>

      {/* Main Form Center */}
      <main className="flex-1 flex items-center justify-center p-4 sm:p-6 my-8">
        <div className="w-full max-w-md relative">
          <div className="absolute -inset-2 bg-gradient-to-b from-[#405bff]/20 to-[#7084ff]/5 rounded-[36px] blur-xl opacity-70 -z-10" />

          {isClerkConfigured ? (
            <div className="flex justify-center">
              <SignUp
                routing="path"
                path="/sign-up"
                signInUrl="/sign-in"
                forceRedirectUrl="/console"
                signInForceRedirectUrl="/console"
              />
            </div>
          ) : (
            <div className="bg-[#191919] rounded-[30px] border border-[#414042] p-7 sm:p-8 shadow-[0_0_40px_rgba(64,91,255,0.25)]">
              <div className="flex items-center gap-3 mb-6">
                <SiderLogo size={32} />
                <div>
                  <h1 className="text-[22px] font-medium text-white tracking-tight">
                    Join Sider Cloud Alpha
                  </h1>
                  <p className="text-[13px] text-[#a7a9ac]">
                    Provision your isolated bare-metal cluster in ind-tbn-1
                  </p>
                </div>
              </div>

              {/* GitHub Button */}
              <div className="mb-5">
                <button
                  type="button"
                  onClick={() => router.push("/console")}
                  className="w-full h-11 px-4 rounded-[30px] border border-[#414042] bg-[#191919] hover:bg-[#2c2c2c] text-[13px] font-medium text-white flex items-center justify-center gap-2.5 transition-colors"
                >
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor">
                    <path
                      fillRule="evenodd"
                      clipRule="evenodd"
                      d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.53 1.032 1.53 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z"
                    />
                  </svg>
                  <span>Register with GitHub</span>
                </button>
              </div>

              <div className="flex items-center gap-3 my-5">
                <div className="flex-1 h-px bg-[#414042]" />
                <span className="text-[11px] text-[#6d6e71] uppercase font-semibold">Or with work email</span>
                <div className="flex-1 h-px bg-[#414042]" />
              </div>

              <form onSubmit={handleDemoSignUp} className="space-y-4">
                <div>
                  <label className="block text-[12px] font-semibold text-[#a7a9ac] uppercase mb-1">
                    Team or Project Name
                  </label>
                  <input
                    type="text"
                    value={teamName}
                    onChange={(e) => setTeamName(e.target.value)}
                    placeholder="e.g. acme-cache"
                    className="w-full bg-[#0e0e0e] border border-[#58595b] px-3.5 py-2.5 rounded-[10px] text-[13px] font-mono text-white focus:outline-none focus:border-[#405bff]"
                  />
                </div>

                <div>
                  <label className="block text-[12px] font-semibold text-[#a7a9ac] uppercase mb-1">
                    Email Address
                  </label>
                  <input
                    type="email"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    placeholder="alex@example.com"
                    className="w-full bg-[#0e0e0e] border border-[#58595b] px-3.5 py-2.5 rounded-[10px] text-[13px] font-mono text-white focus:outline-none focus:border-[#405bff]"
                  />
                </div>

                <button
                  type="submit"
                  disabled={isLoading}
                  className="w-full h-11 rounded-[30px] bg-[#405bff] hover:bg-[#344bd6] text-white text-[13px] font-medium shadow-[0_0_20px_rgba(64,91,255,0.4)] transition-all flex items-center justify-center gap-2 cursor-pointer mt-2"
                >
                  {isLoading ? (
                    <span className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                  ) : (
                    <span>Provision Alpha Cluster &rarr;</span>
                  )}
                </button>
              </form>

              <div className="mt-6 text-center text-[12px] text-[#6d6e71]">
                Already registered?{" "}
                <Link href="/sign-in" className="text-[#7084ff] font-medium hover:underline">
                  Sign in here &rarr;
                </Link>
              </div>
            </div>
          )}
        </div>
      </main>
    </div>
  );
}

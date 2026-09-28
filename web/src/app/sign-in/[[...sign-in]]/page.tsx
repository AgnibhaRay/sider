"use client";

import React, { useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { SignIn } from "@clerk/nextjs";
import { SiderLogo } from "@/components/SiderLogo";

export default function SignInPage() {
  const router = useRouter();
  const publishableKey = process.env.NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY;
  const isClerkConfigured = !!publishableKey && publishableKey.startsWith("pk_");

  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [isLoading, setIsLoading] = useState(false);

  const handleDemoSignIn = (e: React.FormEvent) => {
    e.preventDefault();
    setIsLoading(true);
    setTimeout(() => {
      setIsLoading(false);
      router.push("/console");
    }, 600);
  };

  return (
    <div
      className="min-h-screen text-[#1b1b1b] flex flex-col font-sans select-none antialiased"
      style={{ backgroundColor: "#eaeaea", fontFamily: "var(--font-inter), sans-serif" }}
    >
      {/* Announcement Bar */}
      <div
        className="w-full h-10 px-4 text-white text-[13px] font-medium flex items-center justify-center gap-2 shadow-sm"
        style={{
          background: "linear-gradient(89.97deg, rgb(25, 160, 95) 0.02%, rgb(13, 127, 140) 123.85%)"
        }}
      >
        <span>
          <strong>Sider Cloud v2.1.0</strong>: Zero-dependency LSM storage with native SkipList MemTable.
        </span>
        <Link href="/" className="underline hover:opacity-85 font-medium ml-1">
          Explore Architecture &rarr;
        </Link>
      </div>

      {/* Header */}
      <header className="w-full bg-[#ffffff] border-b border-[#e0e1e6] px-6 h-16 flex items-center justify-between">
        <Link href="/" className="flex items-center gap-3">
          <SiderLogo size={28} />
          <span className="text-[15px] font-semibold text-[#1b1b1b] tracking-[-0.03em]">
            Sider Cloud
          </span>
        </Link>
        <Link
          href="/console"
          className="text-[13px] text-[#60646c] hover:text-[#1b1b1b] font-medium"
        >
          Open Console &rarr;
        </Link>
      </header>

      {/* Main Form Center */}
      <main className="flex-1 flex items-center justify-center p-4 sm:p-6">
        <div className="w-full max-w-md">
          {isClerkConfigured ? (
            <div className="flex justify-center">
              <SignIn
                routing="path"
                path="/sign-in"
                signUpUrl="/sign-up"
                fallbackRedirectUrl="/console"
              />
            </div>
          ) : (
            <div className="bg-[#ffffff] rounded-[20px] border border-[#e0e1e6] p-7 sm:p-8 shadow-sm">
              <div className="flex items-center gap-3 mb-6">
                <SiderLogo size={32} />
                <div>
                  <h1 className="text-[22px] font-semibold text-[#1b1b1b]" style={{ letterSpacing: "-0.5px" }}>
                    Sign in to Sider Cloud
                  </h1>
                  <p className="text-[13px] text-[#60646c]">
                    Manage your distributed LSM-tree database clusters
                  </p>
                </div>
              </div>

              {/* OAuth Buttons */}
              <div className="space-y-2.5 mb-5">
                <button
                  type="button"
                  onClick={() => router.push("/console")}
                  className="w-full h-10 px-4 rounded-[6px] border border-[#e0e1e6] bg-[#ffffff] hover:bg-[#eaeaea]/60 text-[13px] font-medium text-[#1b1b1b] flex items-center justify-center gap-2.5 transition-colors"
                >
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor">
                    <path
                      fillRule="evenodd"
                      clipRule="evenodd"
                      d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.53 1.032 1.53 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z"
                    />
                  </svg>
                  <span>Continue with GitHub</span>
                </button>
              </div>

              <div className="flex items-center gap-3 my-5">
                <div className="flex-1 h-px bg-[#e0e1e6]" />
                <span className="text-[11px] text-[#7c7c7c] uppercase font-semibold">Or with email</span>
                <div className="flex-1 h-px bg-[#e0e1e6]" />
              </div>

              <form onSubmit={handleDemoSignIn} className="space-y-4">
                <div>
                  <label className="block text-[12px] font-semibold text-[#7c7c7c] uppercase mb-1">
                    Email Address
                  </label>
                  <input
                    type="email"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    placeholder="developer@sider.dev"
                    required
                    className="w-full bg-[#eaeaea]/60 border border-[#e0e1e6] px-3.5 py-2.5 rounded-[6px] text-[13px] text-[#1b1b1b] focus:outline-none focus:border-[#1b1b1b]"
                  />
                </div>

                <div>
                  <div className="flex items-center justify-between mb-1">
                    <label className="text-[12px] font-semibold text-[#7c7c7c] uppercase">
                      Password
                    </label>
                  </div>
                  <input
                    type="password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    placeholder="••••••••••••"
                    required
                    className="w-full bg-[#eaeaea]/60 border border-[#e0e1e6] px-3.5 py-2.5 rounded-[6px] text-[13px] text-[#1b1b1b] focus:outline-none focus:border-[#1b1b1b]"
                  />
                </div>

                <button
                  type="submit"
                  disabled={isLoading}
                  className="w-full h-10 mt-2 rounded-[6px] bg-[#1b1b1b] text-white text-[13px] font-medium hover:opacity-90 transition-opacity shadow-[0px_4px_20px_0px_rgba(0,0,0,0.15)] flex items-center justify-center gap-2"
                >
                  {isLoading ? (
                    <span className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                  ) : (
                    "Sign in to Console"
                  )}
                </button>
              </form>

              <div className="mt-6 pt-5 border-t border-[#e0e1e6] text-center text-[13px] text-[#60646c]">
                Don&apos;t have an account?{" "}
                <Link href="/sign-up" className="text-[#1b1b1b] font-medium underline">
                  Sign up
                </Link>
              </div>

              {/* Clerk Key Setup Note */}
              <div className="mt-4 p-3 bg-[#eaeaea]/40 rounded-[8px] border border-[#e0e1e6] text-[11px] text-[#7c7c7c]">
                💡 <strong>Clerk Auth Ready:</strong> Add <code>NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY</code> and <code>CLERK_SECRET_KEY</code> to your environment variables to automatically enable full Clerk identity services.
              </div>
            </div>
          )}
        </div>
      </main>
    </div>
  );
}

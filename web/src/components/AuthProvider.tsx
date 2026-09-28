"use client";

import React from "react";
import { ClerkProvider } from "@clerk/nextjs";

interface AuthProviderProps {
  children: React.ReactNode;
}

export function AuthProvider({ children }: AuthProviderProps) {
  const publishableKey = process.env.NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY;

  if (publishableKey && publishableKey.startsWith("pk_")) {
    return (
      <ClerkProvider
        publishableKey={publishableKey}
        appearance={{
          elements: {
            formButtonPrimary:
              "bg-[#1b1b1b] hover:bg-[#2d2d2d] text-white text-sm font-medium rounded-[6px] shadow-[0px_4px_20px_0px_rgba(0,0,0,0.15)]",
            card: "bg-white rounded-[20px] border border-[#e0e1e6] shadow-sm",
            headerTitle: "text-[#1b1b1b] font-semibold tracking-tight text-xl",
            headerSubtitle: "text-[#60646c] text-sm",
            socialButtonsBlockButton:
              "rounded-[6px] border border-[#e0e1e6] text-[#1b1b1b] hover:bg-[#eaeaea]",
            formFieldInput:
              "rounded-[6px] border border-[#e0e1e6] bg-[#eaeaea]/40 text-[#1b1b1b] focus:border-[#1b1b1b]",
            footerActionLink: "text-[#1b1b1b] font-semibold underline hover:opacity-80"
          }
        }}
      >
        {children}
      </ClerkProvider>
    );
  }

  return <>{children}</>;
}

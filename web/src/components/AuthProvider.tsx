"use client";

import React from "react";
import { ClerkProvider } from "@clerk/nextjs";
import { dark } from "@clerk/themes";

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
          theme: dark,
          variables: {
            colorBackground: "#191919",
            colorNeutral: "#ffffff",
            colorPrimary: "#405bff",
            colorPrimaryForeground: "#ffffff",
            colorForeground: "#ffffff",
            colorInputForeground: "#ffffff",
            colorInput: "#0e0e0e",
            borderRadius: "30px"
          },
          elements: {
            card: "bg-[#191919] rounded-[30px] border border-[#414042] shadow-[0_0_40px_rgba(64,91,255,0.25)]",
            formButtonPrimary:
              "bg-[#405bff] hover:bg-[#344bd6] text-white text-sm font-medium rounded-[30px] shadow-[0_0_20px_rgba(64,91,255,0.4)] transition-all",
            headerTitle: "text-[#ffffff] font-medium tracking-tight text-xl",
            headerSubtitle: "text-[#a7a9ac] text-sm",
            socialButtonsBlockButton:
              "rounded-[30px] border border-[#414042] bg-[#191919] text-[#ffffff] hover:bg-[#2c2c2c] transition-colors",
            formFieldInput:
              "rounded-[10px] border border-[#58595b] bg-[#0e0e0e] text-[#ffffff] focus:border-[#405bff]",
            footerActionLink: "text-[#7084ff] font-medium hover:underline",
            userButtonAvatarBox: "w-8 h-8 rounded-full border border-[#405bff]/50 shadow-[0_0_10px_rgba(64,91,255,0.3)]",
            userButtonPopoverCard: "bg-[#191919] border border-[#414042] rounded-[20px] shadow-[0_0_30px_rgba(0,0,0,0.8)]"
          }
        }}
      >
        {children}
      </ClerkProvider>
    );
  }

  return <>{children}</>;
}

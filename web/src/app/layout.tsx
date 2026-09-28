import type { Metadata } from "next";
import { Playfair_Display, Inter } from "next/font/google";
import { AuthProvider } from "@/components/AuthProvider";
import "./globals.css";

const playfair = Playfair_Display({
  subsets: ["latin"],
  variable: "--font-playfair",
  weight: ["400", "500", "600"],
  style: ["normal", "italic"],
  display: "swap",
});

const inter = Inter({
  subsets: ["latin"],
  variable: "--font-inter",
  weight: ["400", "500", "600", "700"],
  display: "swap",
});

export const metadata: Metadata = {
  metadataBase: new URL("https://sider-cloud.vercel.app"),
  title: "Sider Cloud — Managed Bare-Metal LSM Storage Engine",
  description:
    "Zero-dependency LSM-Tree persistent storage engine on dedicated bare-metal hardware. Sub-millisecond latency, instant WAL crash recovery, and real-time interactive cockpit.",
  keywords: [
    "Sider",
    "SiderDB",
    "Sider Cloud",
    "LSM-Tree",
    "Key-Value Store",
    "Database",
    "Go",
    "SkipList",
    "Distributed Systems",
    "Agnibha Ray",
  ],
  authors: [{ name: "Agnibha Ray", url: "https://github.com/AgnibhaRay" }],
  creator: "Agnibha Ray",
  openGraph: {
    type: "website",
    locale: "en_US",
    url: "https://sider-cloud.vercel.app",
    siteName: "Sider Cloud",
    title: "Sider Cloud — Managed Bare-Metal LSM Storage Engine",
    description:
      "Zero-dependency LSM-Tree persistent storage engine on dedicated bare-metal hardware. Sub-millisecond latency, instant WAL crash recovery, and real-time interactive cockpit.",
  },
  twitter: {
    card: "summary_large_image",
    title: "Sider Cloud — Managed Bare-Metal LSM Storage Engine",
    description:
      "Zero-dependency LSM-Tree persistent storage engine on dedicated bare-metal hardware. Sub-millisecond latency, instant WAL crash recovery, and real-time interactive cockpit.",
    creator: "@AgnibhaRay",
  },
  icons: {
    icon: "/favicon.ico",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className={`${playfair.variable} ${inter.variable}`}>
      <body className="antialiased selection:bg-black selection:text-white">
        <AuthProvider>{children}</AuthProvider>
      </body>
    </html>
  );
}

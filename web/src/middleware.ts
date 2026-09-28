import { clerkMiddleware } from "@clerk/nextjs/server";
import { NextResponse } from "next/server";

export default clerkMiddleware((auth, req) => {
  const host = req.headers.get("host") || "";
  const { pathname } = req.nextUrl;

  // Seamless domain routing:
  // sider-cloud.vercel.app -> serves /cloud (Sider Cloud Cockpit & Quests)
  // siderdb.vercel.app -> serves / (Sider Database Engine Folio & Info)
  if (host.includes("sider-cloud") && pathname === "/") {
    return NextResponse.rewrite(new URL("/cloud", req.url));
  }

  return NextResponse.next();
});

export const config = {
  matcher: [
    // Skip Next.js internals and all static files, unless found in search params
    "/((?!_next|[^?]*\\.(?:html?|css|js(?!on)|jpe?g|webp|png|gif|svg|ttf|woff2?|ico|csv|docx?|xlsx?|zip|webmanifest)).*)",
    // Always run for API routes
    "/(api|trpc)(.*)",
  ],
};

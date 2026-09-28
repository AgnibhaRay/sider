import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

export function middleware(req: NextRequest) {
  const host = req.headers.get("host") || "";
  const url = req.nextUrl.clone();

  const isSiderCloudHost = host.includes("sider-cloud");
  const isSiderDbHost = host.includes("siderdb");

  // 1. If visiting sider-cloud.vercel.app directly on root, rewrite to /cloud
  if (isSiderCloudHost && url.pathname === "/") {
    url.pathname = "/cloud";
    return NextResponse.rewrite(url);
  }

  // 2. Remove /console and all Sider Cloud pages from the siderdb domain:
  // Redirect any cloud pages directly to the dedicated sider-cloud domain
  if (isSiderDbHost) {
    const cloudPaths = ["/console", "/cloud", "/sign-in", "/sign-up"];
    const matchesCloudPath = cloudPaths.some(
      (p) => url.pathname === p || url.pathname.startsWith(`${p}/`)
    );

    if (matchesCloudPath) {
      const redirectUrl = new URL(
        url.pathname + url.search,
        "https://sider-cloud.vercel.app"
      );
      return NextResponse.redirect(redirectUrl, 307);
    }
  }

  return NextResponse.next();
}

export const config = {
  matcher: [
    "/((?!api|_next/static|_next/image|favicon.ico|paintings|benchmarks).*)",
  ],
};

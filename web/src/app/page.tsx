"use client";

import React, { useState, useEffect } from "react";
import { SiderDbLanding } from "../components/SiderDbLanding";
import { SiderCloudLanding } from "../components/SiderCloudLanding";

export default function DynamicRootPage() {
  const [isCloud, setIsCloud] = useState<boolean>(false);

  useEffect(() => {
    if (typeof window !== "undefined") {
      const host = window.location.hostname;
      const searchParams = new URLSearchParams(window.location.search);
      if (
        host.includes("sider-cloud") ||
        searchParams.get("view") === "cloud" ||
        window.location.hash.includes("cloud")
      ) {
        setIsCloud(true);
      }
    }
  }, []);

  if (isCloud) {
    return <SiderCloudLanding />;
  }

  return <SiderDbLanding />;
}

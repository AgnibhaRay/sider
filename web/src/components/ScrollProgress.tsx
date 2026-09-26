"use client";

import React, { useEffect, useState } from "react";

export function ScrollProgress() {
  const [progress, setProgress] = useState(0);

  useEffect(() => {
    const handleScroll = () => {
      const totalHeight = document.documentElement.scrollHeight - window.innerHeight;
      if (totalHeight > 0) {
        const current = window.scrollY / totalHeight;
        setProgress(Math.min(1, Math.max(0, current)));
      }
    };

    window.addEventListener("scroll", handleScroll, { passive: true });
    return () => window.removeEventListener("scroll", handleScroll);
  }, []);

  return (
    <div
      className="fixed top-0 left-0 w-full h-[2px] z-[100] pointer-events-none"
      aria-hidden="true"
    >
      <div
        className="h-full bg-[#000000] transition-transform duration-75 origin-left"
        style={{ transform: `scaleX(${progress})` }}
      />
    </div>
  );
}

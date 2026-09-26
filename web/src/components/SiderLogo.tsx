"use client";

import React from "react";

interface SiderLogoProps {
  size?: number;
  className?: string;
  inverted?: boolean;
}

export function SiderLogo({ size = 36, className = "", inverted = false }: SiderLogoProps) {
  const color = inverted ? "#ffffff" : "#000000";
  const subColor = inverted ? "#dfdcd5" : "#595855";

  return (
    <div
      className={`relative inline-flex items-center justify-center group ${className}`}
      style={{ width: size, height: size }}
    >
      <svg
        width={size}
        height={size}
        viewBox="0 0 48 48"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
        className="transition-transform duration-300 ease-out group-hover:rotate-6 group-hover:scale-105"
        aria-label="Sider Emblem"
      >
        {/* Outer Circular Compass & Precision Ticks */}
        <circle
          cx="24"
          cy="24"
          r="22"
          stroke={color}
          strokeWidth="1"
          strokeDasharray="1.5 2.5"
          className="opacity-40 transition-opacity duration-300 group-hover:opacity-80"
        />
        <circle
          cx="24"
          cy="24"
          r="20"
          stroke={color}
          strokeWidth="1.2"
        />

        {/* 4 Precision Cardinal Ticks */}
        <line x1="24" y1="0.5" x2="24" y2="3.5" stroke={color} strokeWidth="1.5" />
        <line x1="24" y1="44.5" x2="24" y2="47.5" stroke={color} strokeWidth="1.5" />
        <line x1="0.5" y1="24" x2="3.5" y2="24" stroke={color} strokeWidth="1.5" />
        <line x1="44.5" y1="24" x2="47.5" y2="24" stroke={color} strokeWidth="1.5" />

        {/* Architectural Faceted 'S' (LSM-Tree Silicon Prism) */}
        {/* Tier 1: Top Bar & Right Drop */}
        <path
          d="M 14 14.5 L 24 9 L 34 14.5 L 34 20.5 L 29.5 23 L 29.5 17 L 24 14 L 18.5 17 L 14 14.5 Z"
          fill={color}
        />

        {/* Tier 2: Central Connecting Node / WAL Nexus */}
        <path
          d="M 18.5 19.5 L 24 16.5 L 29.5 19.5 L 29.5 25 L 24 28 L 18.5 25 Z"
          stroke={color}
          strokeWidth="1.2"
          fill="none"
          strokeDasharray="2 2"
          className="opacity-60"
        />

        {/* Tier 3: Central Diagonal Spine */}
        <path
          d="M 29.5 22.5 L 24 25.5 L 18.5 22.5 L 14 25 L 18.5 27.5 L 24 24.5 L 29.5 27.5 L 34 25 L 29.5 22.5 Z"
          fill={color}
        />

        {/* Tier 4: Left Drop & Bottom Foundation Bar */}
        <path
          d="M 14 27.5 L 18.5 25 L 18.5 31 L 24 34 L 29.5 31 L 34 33.5 L 24 39 L 14 33.5 Z"
          fill={color}
        />

        {/* Core Silicon Crystal Anchor */}
        <circle cx="24" cy="24" r="1.75" fill={color} />
      </svg>
    </div>
  );
}

"use client";

import React from "react";

interface LaserBorderCardProps {
  children: React.ReactNode;
  className?: string;
  glowColor?: string;
}

export function LaserBorderCard({
  children,
  className = "",
  glowColor = "#405bff",
}: LaserBorderCardProps) {
  return (
    <div className={`relative p-[1.5px] rounded-[32px] overflow-hidden group ${className}`}>
      {/* Rotating High-Speed Laser Border Beam */}
      <div
        className="absolute -inset-[200%] animate-[spin_4s_linear_infinite] opacity-75 group-hover:opacity-100 transition-opacity"
        style={{
          background: `conic-gradient(from 0deg, transparent 65%, ${glowColor} 85%, #00f0ff 95%, transparent 100%)`,
        }}
      />

      {/* Internal Content Shield */}
      <div className="relative w-full h-full bg-[#191919] rounded-[30.5px] overflow-hidden">
        {children}
      </div>
    </div>
  );
}

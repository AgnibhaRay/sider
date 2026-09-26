"use client";

import React from "react";

interface HexagonProps {
  stroke?: string;
  fill?: string;
  size?: number;
  className?: string;
}

export function Hexagon({
  stroke = "#ffffff",
  fill = "transparent",
  size = 12,
  className = "",
}: HexagonProps) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 12 12"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      className={className}
    >
      <polygon
        points="6,1 11,3.8 11,9.2 6,12 1,9.2 1,3.8"
        stroke={stroke}
        strokeWidth="1.2"
        fill={fill}
      />
    </svg>
  );
}

export function HexagonGroup({
  stroke = "#ffffff",
  activeIndex = 0,
  count = 3,
  size = 12,
}: {
  stroke?: string;
  activeIndex?: number;
  count?: number;
  size?: number;
}) {
  return (
    <div className="flex items-center gap-[6px] justify-center">
      {Array.from({ length: count }).map((_, i) => (
        <Hexagon
          key={i}
          size={size}
          stroke={stroke}
          fill={i === activeIndex ? stroke : "transparent"}
        />
      ))}
    </div>
  );
}

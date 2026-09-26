"use client";

import React, { useEffect, useState, useRef } from "react";

export function CustomCursor() {
  const [mounted, setMounted] = useState(false);
  const [visible, setVisible] = useState(false);
  const [isHovered, setIsHovered] = useState(false);
  const [isPressed, setIsPressed] = useState(false);
  const [hoverLabel, setHoverLabel] = useState<string | null>(null);

  const dotRef = useRef<HTMLDivElement>(null);
  const ringRef = useRef<HTMLDivElement>(null);

  // Target and current interpolated coordinates for smooth mechanical lag
  const mouse = useRef({ x: -100, y: -100 });
  const ring = useRef({ x: -100, y: -100 });
  const animFrame = useRef<number | null>(null);

  useEffect(() => {
    // Only enable custom cursor for fine pointer devices (mice, trackpads)
    const isFinePointer = window.matchMedia("(pointer: fine)").matches;
    if (!isFinePointer) return;

    setMounted(true);
    document.documentElement.classList.add("custom-cursor-active");

    const onMouseMove = (e: MouseEvent) => {
      mouse.current.x = e.clientX;
      mouse.current.y = e.clientY;

      if (!visible) setVisible(true);

      // Instant update for the central precision dot
      if (dotRef.current) {
        dotRef.current.style.transform = `translate3d(${e.clientX}px, ${e.clientY}px, 0) translate(-50%, -50%)`;
      }

      // Check hovered element
      const target = e.target as HTMLElement | null;
      if (target) {
        const interactive = target.closest(
          'a, button, input, textarea, select, [role="button"], .cursor-pointer, .pill-button, [data-cursor="hover"]'
        );
        if (interactive) {
          setIsHovered(true);
          const label = interactive.getAttribute("data-cursor-label");
          setHoverLabel(label || null);
        } else {
          setIsHovered(false);
          setHoverLabel(null);
        }
      }
    };

    const onMouseDown = () => setIsPressed(true);
    const onMouseUp = () => setIsPressed(false);

    const onMouseEnter = () => setVisible(true);
    const onMouseLeave = () => {
      setVisible(false);
      setIsHovered(false);
    };

    // Smooth RAF animation loop for the Renaissance caliper ring
    const render = () => {
      // Lerp ring towards mouse
      const ease = isPressed ? 0.35 : 0.18;
      ring.current.x += (mouse.current.x - ring.current.x) * ease;
      ring.current.y += (mouse.current.y - ring.current.y) * ease;

      if (ringRef.current) {
        ringRef.current.style.transform = `translate3d(${ring.current.x}px, ${ring.current.y}px, 0) translate(-50%, -50%)`;
      }

      animFrame.current = requestAnimationFrame(render);
    };

    animFrame.current = requestAnimationFrame(render);

    window.addEventListener("mousemove", onMouseMove, { passive: true });
    window.addEventListener("mousedown", onMouseDown);
    window.addEventListener("mouseup", onMouseUp);
    document.addEventListener("mouseenter", onMouseEnter);
    document.addEventListener("mouseleave", onMouseLeave);

    return () => {
      if (animFrame.current) cancelAnimationFrame(animFrame.current);
      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mousedown", onMouseDown);
      window.removeEventListener("mouseup", onMouseUp);
      document.removeEventListener("mouseenter", onMouseEnter);
      document.removeEventListener("mouseleave", onMouseLeave);
      document.documentElement.classList.remove("custom-cursor-active");
    };
  }, [visible, isPressed]);

  if (!mounted) return null;

  return (
    <div
      className={`pointer-events-none fixed inset-0 z-[99999] overflow-hidden transition-opacity duration-300 ${
        visible ? "opacity-100" : "opacity-0"
      }`}
      aria-hidden="true"
    >
      {/* 1. Precision Center Dot: Instant lock with difference blend */}
      <div
        ref={dotRef}
        className={`fixed top-0 left-0 rounded-full transition-[width,height,opacity] duration-150 ease-out will-change-transform mix-blend-difference bg-white ${
          isHovered ? "w-2 h-2" : "w-1 h-1"
        }`}
      />

      {/* 2. Renaissance Caliper Drafting Ring: Fluid lag & adaptive expansion */}
      <div
        ref={ringRef}
        className={`fixed top-0 left-0 rounded-full will-change-transform mix-blend-difference flex items-center justify-center transition-[width,height,border-color,background-color] duration-200 ease-out ${
          isHovered
            ? "w-12 h-12 border border-white/90 bg-white/10"
            : isPressed
            ? "w-5 h-5 border border-white bg-white/20"
            : "w-7 h-7 border border-white/70 bg-transparent"
        }`}
      >
        {/* Subtle Cardinal Caliper Notches when hovered */}
        {isHovered && (
          <>
            <span className="absolute -top-1 w-[1px] h-1.5 bg-white" />
            <span className="absolute -bottom-1 w-[1px] h-1.5 bg-white" />
            <span className="absolute -left-1 w-1.5 h-[1px] bg-white" />
            <span className="absolute -right-1 w-1.5 h-[1px] bg-white" />
          </>
        )}

        {/* Micro-label on custom tagged interactions */}
        {hoverLabel && (
          <span className="absolute -bottom-5 text-[9px] uppercase tracking-widest text-white whitespace-nowrap font-mono">
            {hoverLabel}
          </span>
        )}
      </div>
    </div>
  );
}

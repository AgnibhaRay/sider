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

  const mouse = useRef({ x: -100, y: -100 });
  const ring = useRef({ x: -100, y: -100 });
  const animFrame = useRef<number | null>(null);
  const isRunning = useRef(false);

  const isPressedRef = useRef(false);
  const isHoveredRef = useRef(false);

  useEffect(() => {
    // Only enable custom cursor for fine pointer devices (mice, trackpads)
    const isFinePointer = window.matchMedia("(pointer: fine)").matches;
    if (!isFinePointer) return;

    setMounted(true);
    document.documentElement.classList.add("custom-cursor-active");

    const render = () => {
      const dx = mouse.current.x - ring.current.x;
      const dy = mouse.current.y - ring.current.y;
      const dist = Math.sqrt(dx * dx + dy * dy);

      const ease = isPressedRef.current ? 0.35 : 0.2;
      ring.current.x += dx * ease;
      ring.current.y += dy * ease;

      if (ringRef.current) {
        ringRef.current.style.transform = `translate3d(${ring.current.x}px, ${ring.current.y}px, 0) translate(-50%, -50%)`;
      }

      // If very close and not moving, idle the loop to save CPU & RAM
      if (dist < 0.1) {
        isRunning.current = false;
        animFrame.current = null;
        return;
      }

      animFrame.current = requestAnimationFrame(render);
    };

    const wakeLoop = () => {
      if (!isRunning.current) {
        isRunning.current = true;
        animFrame.current = requestAnimationFrame(render);
      }
    };

    let lastCheck = 0;
    const onMouseMove = (e: MouseEvent) => {
      mouse.current.x = e.clientX;
      mouse.current.y = e.clientY;

      setVisible(true);

      // Instant update for the central precision dot
      if (dotRef.current) {
        dotRef.current.style.transform = `translate3d(${e.clientX}px, ${e.clientY}px, 0) translate(-50%, -50%)`;
      }

      wakeLoop();

      // Throttle hover checks to avoid querySelector thrashing
      const now = performance.now();
      if (now - lastCheck > 80) {
        lastCheck = now;
        const target = e.target as HTMLElement | null;
        if (target) {
          const interactive = target.closest(
            'a, button, input, textarea, select, [role="button"], .cursor-pointer, .pill-button, [data-cursor="hover"]'
          );
          const hasInteractive = !!interactive;
          if (hasInteractive !== isHoveredRef.current) {
            isHoveredRef.current = hasInteractive;
            setIsHovered(hasInteractive);
            const label = interactive ? interactive.getAttribute("data-cursor-label") : null;
            setHoverLabel(label || null);
          }
        }
      }
    };

    const onMouseDown = () => {
      isPressedRef.current = true;
      setIsPressed(true);
      wakeLoop();
    };

    const onMouseUp = () => {
      isPressedRef.current = false;
      setIsPressed(false);
      wakeLoop();
    };

    const onMouseEnter = () => setVisible(true);
    const onMouseLeave = () => {
      setVisible(false);
      isHoveredRef.current = false;
      setIsHovered(false);
    };

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
  }, []);

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
          isPressed
            ? "w-2.5 h-2.5 opacity-90"
            : isHovered
            ? "w-1 h-1 opacity-70"
            : "w-1.5 h-1.5 opacity-100"
        }`}
        style={{
          boxShadow: isPressed
            ? "0 0 10px rgba(255,255,255,0.8)"
            : "0 0 6px rgba(255,255,255,0.5)",
        }}
      />

      {/* 2. Renaissance Caliper Outer Ring: Smooth physics lag & subtle breathe */}
      <div
        ref={ringRef}
        className={`fixed top-0 left-0 rounded-full will-change-transform transition-[width,height,border-color,background-color] duration-200 ease-out flex items-center justify-center ${
          isPressed
            ? "w-8 h-8 border-[1.5px] border-[#000000] bg-black/10 scale-90"
            : isHovered
            ? "w-12 h-12 border border-[#405bff] bg-[#405bff]/10 shadow-[0_0_15px_rgba(64,91,255,0.35)]"
            : "w-9 h-9 border border-[#000000]/40 bg-transparent"
        }`}
      >
        {/* Subtle Cardinal Measurement Ticks (Renaissance Caliper Crosshairs) */}
        {!isHovered && !isPressed && (
          <div className="absolute inset-0 flex items-center justify-center opacity-30">
            <span className="absolute top-0 w-[1px] h-[3px] bg-[#000000]" />
            <span className="absolute bottom-0 w-[1px] h-[3px] bg-[#000000]" />
            <span className="absolute left-0 w-[3px] h-[1px] bg-[#000000]" />
            <span className="absolute right-0 w-[3px] h-[1px] bg-[#000000]" />
          </div>
        )}

        {/* Dynamic Contextual Cursor Label */}
        {hoverLabel && (
          <span
            className="absolute top-full mt-2 px-2 py-0.5 rounded-[4px] bg-[#000000] text-[#ffffff] font-mono text-[9px] uppercase tracking-wider whitespace-nowrap shadow-md"
            style={{ fontFamily: "var(--font-helvetica-now)" }}
          >
            {hoverLabel}
          </span>
        )}
      </div>
    </div>
  );
}

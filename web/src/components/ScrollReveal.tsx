"use client";

import React, { useEffect, useRef, useState } from "react";

export type AnimationVariant = "swoop-up" | "drop" | "whoop-in" | "swoop-left" | "swoop-right";

interface ScrollRevealProps {
  children: React.ReactNode;
  className?: string;
  delayMs?: number;
  variant?: AnimationVariant;
  enableTilt?: boolean;
}

export function ScrollReveal({
  children,
  className = "",
  delayMs = 0,
  variant = "swoop-up",
  enableTilt = false,
}: ScrollRevealProps) {
  const ref = useRef<HTMLDivElement>(null);
  const [isVisible, setIsVisible] = useState(false);
  const [tiltStyle, setTiltStyle] = useState<React.CSSProperties>({});
  const [glarePos, setGlarePos] = useState<{ x: number; y: number; opacity: number }>({ x: 50, y: 50, opacity: 0 });

  useEffect(() => {
    const el = ref.current;
    if (!el) return;

    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setIsVisible(true);
          observer.unobserve(el);
        }
      },
      { threshold: 0.08, rootMargin: "0px 0px -40px 0px" }
    );

    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  const handleMouseMove = (e: React.MouseEvent<HTMLDivElement>) => {
    if (!enableTilt || !ref.current) return;
    const rect = ref.current.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;

    const centerX = rect.width / 2;
    const centerY = rect.height / 2;

    const rotateX = ((y - centerY) / centerY) * -7; // max 7 deg
    const rotateY = ((x - centerX) / centerX) * 7;

    setTiltStyle({
      transform: `perspective(1200px) rotateX(${rotateX.toFixed(2)}deg) rotateY(${rotateY.toFixed(2)}deg) scale3d(1.015, 1.015, 1.015)`,
      transition: "transform 0.12s cubic-bezier(0.16, 1, 0.3, 1)",
    });

    setGlarePos({
      x: (x / rect.width) * 100,
      y: (y / rect.height) * 100,
      opacity: 0.15,
    });
  };

  const handleMouseLeave = () => {
    if (!enableTilt) return;
    setTiltStyle({
      transform: "perspective(1200px) rotateX(0deg) rotateY(0deg) scale3d(1, 1, 1)",
      transition: "transform 0.6s cubic-bezier(0.16, 1, 0.3, 1)",
    });
    setGlarePos((prev) => ({ ...prev, opacity: 0 }));
  };

  // Base state and animated state mapping
  const getTransformClasses = () => {
    switch (variant) {
      case "drop":
        return isVisible
          ? "opacity-100 translate-y-0 scale-100 rotate-0 blur-none"
          : "opacity-0 -translate-y-16 scale-95 -rotate-1 blur-sm";
      case "whoop-in":
        return isVisible
          ? "opacity-100 translate-y-0 scale-100 blur-none"
          : "opacity-0 translate-y-10 scale-85 blur-md";
      case "swoop-left":
        return isVisible
          ? "opacity-100 translate-x-0 scale-100 blur-none"
          : "opacity-0 -translate-x-16 scale-95 blur-sm";
      case "swoop-right":
        return isVisible
          ? "opacity-100 translate-x-0 scale-100 blur-none"
          : "opacity-0 translate-x-16 scale-95 blur-sm";
      case "swoop-up":
      default:
        return isVisible
          ? "opacity-100 translate-y-0 scale-100 rotate-x-0 blur-none"
          : "opacity-0 translate-y-20 scale-95 -rotate-x-12 blur-md";
    }
  };

  return (
    <div
      ref={ref}
      onMouseMove={handleMouseMove}
      onMouseLeave={handleMouseLeave}
      style={{
        ...tiltStyle,
        transitionDelay: `${delayMs}ms`,
        transitionTimingFunction: "cubic-bezier(0.16, 1, 0.3, 1)",
        transformStyle: "preserve-3d",
      }}
      className={`relative transition-all duration-1000 ${getTransformClasses()} ${className}`}
    >
      {/* Specular Glare Flare */}
      {enableTilt && (
        <div
          className="absolute inset-0 pointer-events-none rounded-[inherit] transition-opacity duration-300 z-30"
          style={{
            background: `radial-gradient(circle at ${glarePos.x}% ${glarePos.y}%, rgba(255, 255, 255, 0.22), transparent 60%)`,
            opacity: glarePos.opacity,
          }}
        />
      )}
      {children}
    </div>
  );
}

"use client";

import React, { useEffect, useRef } from "react";
import * as THREE from "three";

function createCircleNodeTexture(): THREE.CanvasTexture {
  const canvas = document.createElement("canvas");
  canvas.width = 128;
  canvas.height = 128;
  const ctx = canvas.getContext("2d");

  if (ctx) {
    const centerX = 64;
    const centerY = 64;
    const radius = 58;

    // Outer soft glow
    const grad = ctx.createRadialGradient(centerX, centerY, 0, centerX, centerY, radius);
    grad.addColorStop(0, "rgba(255, 255, 255, 1.0)");
    grad.addColorStop(0.25, "rgba(235, 235, 235, 0.95)");
    grad.addColorStop(0.55, "rgba(200, 195, 185, 0.45)");
    grad.addColorStop(0.85, "rgba(150, 145, 135, 0.15)");
    grad.addColorStop(1, "rgba(0, 0, 0, 0)");

    ctx.fillStyle = grad;
    ctx.beginPath();
    ctx.arc(centerX, centerY, radius, 0, Math.PI * 2);
    ctx.fill();

    // Solid inner core for crisp high-density center
    ctx.beginPath();
    ctx.arc(centerX, centerY, 14, 0, Math.PI * 2);
    ctx.fillStyle = "rgba(255, 255, 255, 1.0)";
    ctx.fill();
  }

  const texture = new THREE.CanvasTexture(canvas);
  texture.generateMipmaps = true;
  texture.minFilter = THREE.LinearMipmapLinearFilter;
  texture.magFilter = THREE.LinearFilter;
  texture.needsUpdate = true;
  return texture;
}

export function ThreeDarkCanvas() {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    let width = container.clientWidth;
    let height = container.clientHeight;

    const scene = new THREE.Scene();
    const camera = new THREE.PerspectiveCamera(50, width / height, 0.1, 100);
    camera.position.z = 8;

    const renderer = new THREE.WebGLRenderer({ alpha: true, antialias: true, powerPreference: "high-performance" });
    renderer.setSize(width, height);
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    container.appendChild(renderer.domElement);

    // Constellation of Nodes (LSM SkipList representation)
    const nodeCount = 70;
    const positions = new Float32Array(nodeCount * 3);
    const velocities: { x: number; y: number; z: number }[] = [];

    for (let i = 0; i < nodeCount; i++) {
      const idx = i * 3;
      positions[idx] = (Math.random() - 0.5) * 14;
      positions[idx + 1] = (Math.random() - 0.5) * 8;
      positions[idx + 2] = (Math.random() - 0.5) * 6;

      velocities.push({
        x: (Math.random() - 0.5) * 0.005,
        y: (Math.random() - 0.5) * 0.005,
        z: (Math.random() - 0.5) * 0.003,
      });
    }

    const nodeGeo = new THREE.BufferGeometry();
    nodeGeo.setAttribute("position", new THREE.BufferAttribute(positions, 3));

    const circleMap = createCircleNodeTexture();

    const nodeMat = new THREE.PointsMaterial({
      color: 0xffffff,
      size: 0.18,
      map: circleMap,
      transparent: true,
      opacity: 0.85,
      blending: THREE.AdditiveBlending,
      depthWrite: false,
    });

    const nodePoints = new THREE.Points(nodeGeo, nodeMat);
    scene.add(nodePoints);

    // Connecting Lines between adjacent nodes
    const maxConnections = (nodeCount * (nodeCount - 1)) / 2;
    const linePositions = new Float32Array(maxConnections * 6);
    const lineGeo = new THREE.BufferGeometry();
    lineGeo.setAttribute("position", new THREE.BufferAttribute(linePositions, 3));

    const lineMat = new THREE.LineBasicMaterial({
      color: 0xdfdcd5,
      transparent: true,
      opacity: 0.14,
      blending: THREE.AdditiveBlending,
    });

    const lines = new THREE.LineSegments(lineGeo, lineMat);
    scene.add(lines);

    // Mouse Tracking
    let mouse = { x: 0, y: 0 };
    const handleMouseMove = (e: MouseEvent) => {
      const rect = container.getBoundingClientRect();
      mouse.x = ((e.clientX - rect.left) / width) * 2 - 1;
      mouse.y = -(((e.clientY - rect.top) / height) * 2 - 1);
    };

    window.addEventListener("mousemove", handleMouseMove, { passive: true });

    // Scroll Tracking for Tier Separation
    let scrollOffset = 0;
    const handleScroll = () => {
      const rect = container.getBoundingClientRect();
      const visibleRatio = 1 - Math.max(0, Math.min(1, rect.top / window.innerHeight));
      scrollOffset = visibleRatio;
    };

    window.addEventListener("scroll", handleScroll, { passive: true });

    const handleResize = () => {
      if (!container) return;
      width = container.clientWidth;
      height = container.clientHeight;
      camera.aspect = width / height;
      camera.updateProjectionMatrix();
      renderer.setSize(width, height);
    };

    window.addEventListener("resize", handleResize);

    let animId: number;
    let clock = 0;
    const animate = () => {
      animId = requestAnimationFrame(animate);
      clock += 0.015;

      const pos = nodeGeo.attributes.position.array as Float32Array;
      let lineIndex = 0;
      const linePos = lineGeo.attributes.position.array as Float32Array;

      // Update positions
      for (let i = 0; i < nodeCount; i++) {
        const idx = i * 3;

        pos[idx] += velocities[i].x;
        pos[idx + 1] += velocities[i].y;
        pos[idx + 2] += velocities[i].z;

        // Subtle harmonic floating
        pos[idx + 1] += Math.sin(clock + i) * 0.0008;

        // Bounce at boundaries
        if (Math.abs(pos[idx]) > 7.2) velocities[i].x *= -1;
        if (Math.abs(pos[idx + 1]) > 4.2) velocities[i].y *= -1;
        if (Math.abs(pos[idx + 2]) > 3.2) velocities[i].z *= -1;

        // Subtle mouse influence
        const dx = pos[idx] - mouse.x * 4;
        const dy = pos[idx + 1] - mouse.y * 3;
        const dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < 2.5) {
          pos[idx] += (dx / dist) * 0.015;
          pos[idx + 1] += (dy / dist) * 0.015;
        }

        // Connect nearby nodes
        for (let j = i + 1; j < nodeCount; j++) {
          const jdx = j * 3;
          const distNodes = Math.hypot(
            pos[idx] - pos[jdx],
            pos[idx + 1] - pos[jdx + 1],
            pos[idx + 2] - pos[jdx + 2]
          );

          if (distNodes < 2.4 && lineIndex < linePositions.length - 6) {
            linePos[lineIndex++] = pos[idx];
            linePos[lineIndex++] = pos[idx + 1];
            linePos[lineIndex++] = pos[idx + 2];

            linePos[lineIndex++] = pos[jdx];
            linePos[lineIndex++] = pos[jdx + 1];
            linePos[lineIndex++] = pos[jdx + 2];
          }
        }
      }

      nodeGeo.attributes.position.needsUpdate = true;
      lineGeo.attributes.position.needsUpdate = true;
      lineGeo.setDrawRange(0, lineIndex / 3);

      // Rotate slightly with scroll
      scene.rotation.y = scrollOffset * 0.5;
      scene.rotation.x = scrollOffset * 0.15;

      renderer.render(scene, camera);
    };

    animate();

    return () => {
      cancelAnimationFrame(animId);
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("scroll", handleScroll);
      window.removeEventListener("resize", handleResize);
      circleMap.dispose();
      nodeGeo.dispose();
      nodeMat.dispose();
      lineGeo.dispose();
      lineMat.dispose();
      renderer.dispose();
      if (container.contains(renderer.domElement)) {
        container.removeChild(renderer.domElement);
      }
    };
  }, []);

  return (
    <div
      ref={containerRef}
      className="absolute inset-0 w-full h-full pointer-events-none z-0 opacity-80"
      aria-hidden="true"
    />
  );
}

"use client";

import React, { useEffect, useRef } from "react";
import * as THREE from "three";

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

    // Constellation of Nodes (LSM SkipList representation in Voltage Blue & Signal Violet)
    const nodeCount = 85;
    const positions = new Float32Array(nodeCount * 3);
    const colors = new Float32Array(nodeCount * 3);
    const velocities: { x: number; y: number; z: number }[] = [];

    const cVoltage = new THREE.Color(0x405bff);
    const cViolet = new THREE.Color(0x7084ff);
    const cCyan = new THREE.Color(0x00f0ff);

    for (let i = 0; i < nodeCount; i++) {
      const idx = i * 3;
      positions[idx] = (Math.random() - 0.5) * 16;
      positions[idx + 1] = (Math.random() - 0.5) * 10;
      positions[idx + 2] = (Math.random() - 0.5) * 6;

      const r = Math.random();
      const chosen = r < 0.5 ? cVoltage : r < 0.85 ? cViolet : cCyan;
      colors[idx] = chosen.r;
      colors[idx + 1] = chosen.g;
      colors[idx + 2] = chosen.b;

      velocities.push({
        x: (Math.random() - 0.5) * 0.006,
        y: (Math.random() - 0.5) * 0.006,
        z: (Math.random() - 0.5) * 0.004,
      });
    }

    const nodeGeo = new THREE.BufferGeometry();
    nodeGeo.setAttribute("position", new THREE.BufferAttribute(positions, 3));
    nodeGeo.setAttribute("color", new THREE.BufferAttribute(colors, 3));

    const nodeMat = new THREE.PointsMaterial({
      size: 0.12,
      vertexColors: true,
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
      color: 0x405bff,
      transparent: true,
      opacity: 0.22,
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

    let animId: number;
    const clock = new THREE.Clock();

    const animate = () => {
      animId = requestAnimationFrame(animate);
      const elapsed = clock.getElapsedTime();

      const posAttr = nodeGeo.attributes.position as THREE.BufferAttribute;
      const posArray = posAttr.array as Float32Array;

      // Update node positions
      for (let i = 0; i < nodeCount; i++) {
        const idx = i * 3;
        posArray[idx] += velocities[i].x;
        posArray[idx + 1] += velocities[i].y;
        posArray[idx + 2] += velocities[i].z;

        // Bounce bounds
        if (Math.abs(posArray[idx]) > 8) velocities[i].x *= -1;
        if (Math.abs(posArray[idx + 1]) > 5) velocities[i].y *= -1;
        if (Math.abs(posArray[idx + 2]) > 3) velocities[i].z *= -1;
      }
      posAttr.needsUpdate = true;

      // Update lines between nearby nodes
      let lineIndex = 0;
      const lineArray = lineGeo.attributes.position.array as Float32Array;
      const maxDist = 2.2;

      for (let i = 0; i < nodeCount; i++) {
        for (let j = i + 1; j < nodeCount; j++) {
          const dx = posArray[i * 3] - posArray[j * 3];
          const dy = posArray[i * 3 + 1] - posArray[j * 3 + 1];
          const dz = posArray[i * 3 + 2] - posArray[j * 3 + 2];
          const dist = Math.sqrt(dx * dx + dy * dy + dz * dz);

          if (dist < maxDist) {
            lineArray[lineIndex++] = posArray[i * 3];
            lineArray[lineIndex++] = posArray[i * 3 + 1];
            lineArray[lineIndex++] = posArray[i * 3 + 2];

            lineArray[lineIndex++] = posArray[j * 3];
            lineArray[lineIndex++] = posArray[j * 3 + 1];
            lineArray[lineIndex++] = posArray[j * 3 + 2];
          }
        }
      }

      lineGeo.setDrawRange(0, lineIndex / 3);
      lineGeo.attributes.position.needsUpdate = true;

      // Subtle scene parallax based on mouse
      scene.rotation.y = mouse.x * 0.15 + elapsed * 0.02;
      scene.rotation.x = -mouse.y * 0.15;

      renderer.render(scene, camera);
    };

    animate();

    const handleResize = () => {
      if (!container) return;
      width = container.clientWidth;
      height = container.clientHeight;
      camera.aspect = width / height;
      camera.updateProjectionMatrix();
      renderer.setSize(width, height);
    };

    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("resize", handleResize);
      cancelAnimationFrame(animId);
      renderer.dispose();
      if (container.contains(renderer.domElement)) {
        container.removeChild(renderer.domElement);
      }
    };
  }, []);

  return <div ref={containerRef} className="absolute inset-0 w-full h-full pointer-events-none" />;
}

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
    let isVisible = true;
    let animId: number | null = null;

    const scene = new THREE.Scene();
    const camera = new THREE.PerspectiveCamera(50, width / height, 0.1, 100);
    camera.position.z = 8;

    const renderer = new THREE.WebGLRenderer({
      alpha: true,
      antialias: false,
      powerPreference: "low-power",
    });
    renderer.setSize(width, height);
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 1.25));
    container.appendChild(renderer.domElement);

    // Optimized constellation (40 nodes for high performance & minimal RAM)
    const nodeCount = 40;
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
        x: (Math.random() - 0.5) * 0.005,
        y: (Math.random() - 0.5) * 0.005,
        z: (Math.random() - 0.5) * 0.003,
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
    });

    const nodePoints = new THREE.Points(nodeGeo, nodeMat);
    scene.add(nodePoints);

    // Dynamic Connections
    const maxLines = (nodeCount * (nodeCount - 1)) / 2;
    const linePositions = new Float32Array(maxLines * 6);
    const lineGeo = new THREE.BufferGeometry();
    lineGeo.setAttribute("position", new THREE.BufferAttribute(linePositions, 3));

    const lineMat = new THREE.LineBasicMaterial({
      color: 0x405bff,
      transparent: true,
      opacity: 0.25,
      blending: THREE.AdditiveBlending,
    });

    const lines = new THREE.LineSegments(lineGeo, lineMat);
    scene.add(lines);

    const mouse = { x: 0, y: 0 };
    const handleMouseMove = (e: MouseEvent) => {
      mouse.x = (e.clientX / window.innerWidth) * 2 - 1;
      mouse.y = -(e.clientY / window.innerHeight) * 2 + 1;
    };
    window.addEventListener("mousemove", handleMouseMove, { passive: true });

    const clock = new THREE.Clock();

    const animate = () => {
      if (!isVisible) {
        animId = null;
        return;
      }

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

        if (Math.abs(posArray[idx]) > 8) velocities[i].x *= -1;
        if (Math.abs(posArray[idx + 1]) > 5) velocities[i].y *= -1;
        if (Math.abs(posArray[idx + 2]) > 3) velocities[i].z *= -1;
      }
      posAttr.needsUpdate = true;

      // Update lines between nearby nodes
      let lineIndex = 0;
      const lineArray = lineGeo.attributes.position.array as Float32Array;
      const maxDist = 2.4;

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

      scene.rotation.y = mouse.x * 0.12 + elapsed * 0.015;
      scene.rotation.x = -mouse.y * 0.12;

      renderer.render(scene, camera);
    };

    // Pause rendering when scrolled out of view to save RAM & CPU
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          isVisible = entry.isIntersecting;
          if (isVisible && !animId) {
            animate();
          }
        });
      },
      { threshold: 0.05 }
    );
    observer.observe(container);

    const handleResize = () => {
      if (!container) return;
      width = container.clientWidth;
      height = container.clientHeight;
      camera.aspect = width / height;
      camera.updateProjectionMatrix();
      renderer.setSize(width, height);
    };
    window.addEventListener("resize", handleResize, { passive: true });

    return () => {
      observer.disconnect();
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("resize", handleResize);
      if (animId) cancelAnimationFrame(animId);
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

  return <div ref={containerRef} className="absolute inset-0 w-full h-full pointer-events-none" />;
}

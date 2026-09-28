"use client";

import React, { useEffect, useRef } from "react";
import * as THREE from "three";

export function ThreeHeroCanvas() {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    let width = container.clientWidth;
    let height = container.clientHeight;

    const scene = new THREE.Scene();
    const camera = new THREE.PerspectiveCamera(45, width / height, 0.1, 100);
    camera.position.z = 7.5;

    const renderer = new THREE.WebGLRenderer({ alpha: true, antialias: true, powerPreference: "high-performance" });
    renderer.setSize(width, height);
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    container.appendChild(renderer.domElement);

    // Group for all rotating elements
    const mainGroup = new THREE.Group();
    scene.add(mainGroup);

    // 1. Outer Gyroscope Cage (Voltage Blue 0x405bff)
    const outerGeo = new THREE.IcosahedronGeometry(2.6, 2);
    const outerMat = new THREE.MeshBasicMaterial({
      color: 0x405bff,
      wireframe: true,
      transparent: true,
      opacity: 0.35,
      blending: THREE.AdditiveBlending,
    });
    const outerMesh = new THREE.Mesh(outerGeo, outerMat);
    mainGroup.add(outerMesh);

    // 2. Middle Polyhedron Core (Signal Violet 0x7084ff)
    const middleGeo = new THREE.DodecahedronGeometry(1.7, 0);
    const middleMat = new THREE.MeshBasicMaterial({
      color: 0x7084ff,
      wireframe: true,
      transparent: true,
      opacity: 0.55,
      blending: THREE.AdditiveBlending,
    });
    const middleMesh = new THREE.Mesh(middleGeo, middleMat);
    mainGroup.add(middleMesh);

    // 3. Central Energy Seed (Bright Electric Cyan 0x00f0ff)
    const innerGeo = new THREE.OctahedronGeometry(0.85, 0);
    const innerMat = new THREE.MeshBasicMaterial({
      color: 0x00f0ff,
      wireframe: true,
      transparent: true,
      opacity: 0.85,
      blending: THREE.AdditiveBlending,
    });
    const innerMesh = new THREE.Mesh(innerGeo, innerMat);
    mainGroup.add(innerMesh);

    // 4. Orbital Accelerator Rings (LaunchDarkly Neon Rings)
    const ringGeo1 = new THREE.TorusGeometry(3.3, 0.015, 16, 100);
    const ringMat1 = new THREE.MeshBasicMaterial({
      color: 0x405bff,
      transparent: true,
      opacity: 0.45,
      blending: THREE.AdditiveBlending,
    });
    const ring1 = new THREE.Mesh(ringGeo1, ringMat1);
    ring1.rotation.x = Math.PI / 2.3;
    mainGroup.add(ring1);

    const ringGeo2 = new THREE.TorusGeometry(3.0, 0.012, 16, 100);
    const ringMat2 = new THREE.MeshBasicMaterial({
      color: 0x7084ff,
      transparent: true,
      opacity: 0.4,
      blending: THREE.AdditiveBlending,
    });
    const ring2 = new THREE.Mesh(ringGeo2, ringMat2);
    ring2.rotation.y = Math.PI / 3;
    ring2.rotation.z = Math.PI / 6;
    mainGroup.add(ring2);

    const ringGeo3 = new THREE.TorusGeometry(2.2, 0.02, 16, 80);
    const ringMat3 = new THREE.MeshBasicMaterial({
      color: 0x00f0ff,
      transparent: true,
      opacity: 0.6,
      blending: THREE.AdditiveBlending,
    });
    const ring3 = new THREE.Mesh(ringGeo3, ringMat3);
    ring3.rotation.x = Math.PI / 4;
    ring3.rotation.y = Math.PI / 4;
    mainGroup.add(ring3);

    // 5. Constellation Particles (250 Glowing Data Sparks)
    const particleCount = 260;
    const particlePositions = new Float32Array(particleCount * 3);
    const particleColors = new Float32Array(particleCount * 3);

    const colorA = new THREE.Color(0x405bff);
    const colorB = new THREE.Color(0x7084ff);
    const colorC = new THREE.Color(0x00f0ff);

    for (let i = 0; i < particleCount * 3; i += 3) {
      const radius = 2.0 + Math.random() * 5.0;
      const theta = Math.random() * Math.PI * 2;
      const phi = (Math.random() - 0.5) * Math.PI;

      particlePositions[i] = radius * Math.cos(theta) * Math.cos(phi);
      particlePositions[i + 1] = radius * Math.sin(phi);
      particlePositions[i + 2] = radius * Math.sin(theta) * Math.cos(phi);

      const rChoice = Math.random();
      const chosenColor = rChoice < 0.45 ? colorA : rChoice < 0.8 ? colorB : colorC;
      particleColors[i] = chosenColor.r;
      particleColors[i + 1] = chosenColor.g;
      particleColors[i + 2] = chosenColor.b;
    }

    const particleGeo = new THREE.BufferGeometry();
    particleGeo.setAttribute("position", new THREE.BufferAttribute(particlePositions, 3));
    particleGeo.setAttribute("color", new THREE.BufferAttribute(particleColors, 3));

    const particleMat = new THREE.PointsMaterial({
      size: 0.065,
      vertexColors: true,
      transparent: true,
      opacity: 0.85,
      blending: THREE.AdditiveBlending,
      depthWrite: false,
    });

    const particles = new THREE.Points(particleGeo, particleMat);
    scene.add(particles);

    // Mouse Interaction with Inertial Spring Damping
    let mouseX = 0;
    let mouseY = 0;
    let targetX = 0;
    let targetY = 0;

    const handleMouseMove = (e: MouseEvent) => {
      const rect = container.getBoundingClientRect();
      const x = ((e.clientX - rect.left) / width) * 2 - 1;
      const y = -(((e.clientY - rect.top) / height) * 2 - 1);
      targetX = x * 0.75;
      targetY = y * 0.75;
    };

    window.addEventListener("mousemove", handleMouseMove, { passive: true });

    // Click wave shock effect
    let shockwave = 0;
    const handleClick = () => {
      shockwave = 1.0;
    };
    container.addEventListener("click", handleClick);

    // Animation Loop
    let animId: number;
    let clock = new THREE.Clock();

    const animate = () => {
      animId = requestAnimationFrame(animate);
      const elapsed = clock.getElapsedTime();

      // Smooth camera / group rotation with mouse spring
      mouseX += (targetX - mouseX) * 0.05;
      mouseY += (targetY - mouseY) * 0.05;

      mainGroup.rotation.y = elapsed * 0.15 + mouseX;
      mainGroup.rotation.x = Math.sin(elapsed * 0.1) * 0.15 + mouseY;
      mainGroup.rotation.z = Math.cos(elapsed * 0.08) * 0.08;

      // Polyhedrons counter-rotation
      outerMesh.rotation.y += 0.003;
      outerMesh.rotation.x -= 0.002;

      middleMesh.rotation.y -= 0.006;
      middleMesh.rotation.z += 0.004;

      innerMesh.rotation.x += 0.01;
      innerMesh.rotation.y += 0.012;

      // Pulsing scale for central energy core
      const pulse = 1.0 + Math.sin(elapsed * 3.5) * 0.08 + shockwave * 0.4;
      innerMesh.scale.set(pulse, pulse, pulse);

      // Rings spin along unique orbital planes
      ring1.rotation.z += 0.004;
      ring2.rotation.x += 0.005;
      ring3.rotation.y += 0.008;

      // Shockwave decay
      if (shockwave > 0) {
        shockwave *= 0.92;
        if (shockwave < 0.01) shockwave = 0;
      }

      // Constellation slow galaxy drift
      particles.rotation.y = -elapsed * 0.04;
      particles.rotation.x = Math.sin(elapsed * 0.03) * 0.05;

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
      container.removeEventListener("click", handleClick);
      cancelAnimationFrame(animId);
      renderer.dispose();
      if (container.contains(renderer.domElement)) {
        container.removeChild(renderer.domElement);
      }
    };
  }, []);

  return (
    <div
      ref={containerRef}
      className="absolute inset-0 w-full h-full pointer-events-auto cursor-grab active:cursor-grabbing overflow-hidden"
    />
  );
}

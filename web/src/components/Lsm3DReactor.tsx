"use client";

import React, { useEffect, useRef, useState } from "react";
import * as THREE from "three";

export function Lsm3DReactor() {
  const containerRef = useRef<HTMLDivElement>(null);
  const [activeMode, setActiveMode] = useState<"read" | "flush" | "compact">("read");
  const [opsCounter, setOpsCounter] = useState(316746);
  const triggerActionRef = useRef<((action: string) => void) | null>(null);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    let width = container.clientWidth;
    let height = container.clientHeight;

    const scene = new THREE.Scene();
    scene.fog = new THREE.FogExp2(0x0e0e0e, 0.05);

    const camera = new THREE.PerspectiveCamera(45, width / height, 0.1, 100);
    camera.position.set(0, 2.5, 7.5);
    camera.lookAt(0, 0, 0);

    const renderer = new THREE.WebGLRenderer({ alpha: true, antialias: true, powerPreference: "high-performance" });
    renderer.setSize(width, height);
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    container.appendChild(renderer.domElement);

    // Root Group
    const root = new THREE.Group();
    scene.add(root);

    // --- 1. MEMTABLE SKIPLIST 3D GRID (Top Floating Tier) ---
    const memtableGroup = new THREE.Group();
    memtableGroup.position.y = 1.2;
    root.add(memtableGroup);

    // SkipList Levels (3 floating wireframe planes)
    const planeGeo = new THREE.PlaneGeometry(3.6, 2.0, 6, 4);
    const planeMat = new THREE.MeshBasicMaterial({
      color: 0x405bff,
      wireframe: true,
      transparent: true,
      opacity: 0.25,
      blending: THREE.AdditiveBlending,
    });

    const level0 = new THREE.Mesh(planeGeo, planeMat);
    level0.rotation.x = -Math.PI / 2.5;
    level0.position.y = 0.5;
    memtableGroup.add(level0);

    const level1 = new THREE.Mesh(planeGeo, planeMat);
    level1.rotation.x = -Math.PI / 2.5;
    level1.position.y = 0.0;
    memtableGroup.add(level1);

    const level2 = new THREE.Mesh(planeGeo, planeMat);
    level2.rotation.x = -Math.PI / 2.5;
    level2.position.y = -0.5;
    memtableGroup.add(level2);

    // Glowing MemTable Nodes (Data items)
    const nodeCount = 36;
    const nodeGeo = new THREE.SphereGeometry(0.065, 8, 8);
    const nodeMat = new THREE.MeshBasicMaterial({
      color: 0x00f0ff,
      blending: THREE.AdditiveBlending,
    });

    const nodesMesh = new THREE.InstancedMesh(nodeGeo, nodeMat, nodeCount);
    const dummy = new THREE.Object3D();
    const nodeInitialPositions: THREE.Vector3[] = [];

    let nodeIdx = 0;
    for (let lvl = 0; lvl < 3; lvl++) {
      const count = lvl === 0 ? 18 : lvl === 1 ? 12 : 6;
      for (let i = 0; i < count; i++) {
        const x = (i / (count - 1 || 1) - 0.5) * 3.0;
        const y = 0.5 - lvl * 0.5;
        const z = (Math.random() - 0.5) * 0.8;
        dummy.position.set(x, y, z);
        dummy.updateMatrix();
        nodesMesh.setMatrixAt(nodeIdx, dummy.matrix);
        nodeInitialPositions.push(new THREE.Vector3(x, y, z));
        nodeIdx++;
      }
    }
    nodesMesh.instanceMatrix.needsUpdate = true;
    memtableGroup.add(nodesMesh);

    // --- 2. WAL STREAM HELIX (Write-Ahead Log) ---
    const walCurvePoints: THREE.Vector3[] = [];
    const helixTurns = 3;
    const helixPoints = 120;
    for (let i = 0; i < helixPoints; i++) {
      const t = i / helixPoints;
      const angle = t * Math.PI * 2 * helixTurns;
      const r = 1.4 * (1 - t * 0.4);
      const x = Math.cos(angle) * r;
      const z = Math.sin(angle) * r;
      const y = 1.2 - t * 2.4;
      walCurvePoints.push(new THREE.Vector3(x, y, z));
    }
    const walGeo = new THREE.BufferGeometry().setFromPoints(walCurvePoints);
    const walMat = new THREE.LineBasicMaterial({
      color: 0x7084ff,
      transparent: true,
      opacity: 0.45,
      blending: THREE.AdditiveBlending,
    });
    const walLine = new THREE.Line(walGeo, walMat);
    root.add(walLine);

    // --- 3. PERSISTENT NVMe SSTABLE DISK CRYSTALS (Bottom Tier) ---
    const diskGroup = new THREE.Group();
    diskGroup.position.y = -1.6;
    root.add(diskGroup);

    // Crystalline SSTable storage prisms
    const sstableGeo = new THREE.BoxGeometry(0.7, 0.4, 0.7);
    const sstableMat = new THREE.MeshBasicMaterial({
      color: 0x405bff,
      wireframe: true,
      transparent: true,
      opacity: 0.6,
      blending: THREE.AdditiveBlending,
    });

    const sstables: THREE.Mesh[] = [];
    for (let i = 0; i < 5; i++) {
      const m = new THREE.Mesh(sstableGeo, sstableMat);
      const angle = (i / 5) * Math.PI * 2;
      m.position.set(Math.cos(angle) * 1.5, 0, Math.sin(angle) * 1.5);
      m.rotation.y = angle;
      diskGroup.add(m);
      sstables.push(m);
    }

    // Central NVMe Foundation Ring
    const baseRingGeo = new THREE.TorusGeometry(1.9, 0.02, 16, 64);
    const baseRingMat = new THREE.MeshBasicMaterial({
      color: 0x7084ff,
      transparent: true,
      opacity: 0.35,
      blending: THREE.AdditiveBlending,
    });
    const baseRing = new THREE.Mesh(baseRingGeo, baseRingMat);
    baseRing.rotation.x = Math.PI / 2;
    diskGroup.add(baseRing);

    // --- 4. PHOTON SPARK PARTICLES (Read/Write pulses) ---
    const sparkCount = 80;
    const sparkPositions = new Float32Array(sparkCount * 3);
    const sparkVelocities: THREE.Vector3[] = [];

    for (let i = 0; i < sparkCount; i++) {
      sparkPositions[i * 3] = (Math.random() - 0.5) * 4;
      sparkPositions[i * 3 + 1] = (Math.random() - 0.5) * 3;
      sparkPositions[i * 3 + 2] = (Math.random() - 0.5) * 2;
      sparkVelocities.push(
        new THREE.Vector3(
          (Math.random() - 0.5) * 0.03,
          (Math.random() - 0.5) * 0.03,
          (Math.random() - 0.5) * 0.03
        )
      );
    }

    const sparkGeo = new THREE.BufferGeometry();
    sparkGeo.setAttribute("position", new THREE.BufferAttribute(sparkPositions, 3));
    const sparkMat = new THREE.PointsMaterial({
      color: 0x00f0ff,
      size: 0.08,
      transparent: true,
      opacity: 0.9,
      blending: THREE.AdditiveBlending,
      depthWrite: false,
    });
    const sparks = new THREE.Points(sparkGeo, sparkMat);
    root.add(sparks);

    // Action Trigger Handler
    triggerActionRef.current = (action: string) => {
      if (action === "read") {
        setActiveMode("read");
        sparkMat.color.setHex(0x00f0ff);
        setOpsCounter((prev) => prev + Math.floor(Math.random() * 5000 + 2000));
        // Energize read particles
        for (let i = 0; i < sparkCount; i++) {
          sparkVelocities[i].x = (Math.random() - 0.5) * 0.08;
          sparkVelocities[i].y = (Math.random() - 0.5) * 0.08;
        }
      } else if (action === "flush") {
        setActiveMode("flush");
        sparkMat.color.setHex(0x7084ff);
        // Cascade particles down to disk tier
        for (let i = 0; i < sparkCount; i++) {
          sparkVelocities[i].y = -0.06 - Math.random() * 0.04;
        }
      } else if (action === "compact") {
        setActiveMode("compact");
        sparkMat.color.setHex(0xff007f);
        // Spin disk group and pulse sstables
        for (const m of sstables) {
          m.scale.set(1.4, 1.4, 1.4);
        }
      }
    };

    // Mouse Tracking for Interactive 3D Perspective Tilt
    let mouse = { x: 0, y: 0 };
    let target = { x: 0, y: 0 };

    const handleMouseMove = (e: MouseEvent) => {
      const rect = container.getBoundingClientRect();
      target.x = ((e.clientX - rect.left) / width) * 2 - 1;
      target.y = -(((e.clientY - rect.top) / height) * 2 - 1);
    };

    container.addEventListener("mousemove", handleMouseMove);

    // Animation Loop
    let animId: number;
    const clock = new THREE.Clock();

    const animate = () => {
      animId = requestAnimationFrame(animate);
      const elapsed = clock.getElapsedTime();

      // Smooth mouse damping
      mouse.x += (target.x * 0.5 - mouse.x) * 0.06;
      mouse.y += (target.y * 0.3 - mouse.y) * 0.06;

      root.rotation.y = elapsed * 0.12 + mouse.x;
      root.rotation.x = mouse.y * 0.5;

      // WAL Line Rotation
      walLine.rotation.y = -elapsed * 0.4;

      // Disk SSTable Rotation
      diskGroup.rotation.y = elapsed * 0.25;
      for (const m of sstables) {
        m.scale.lerp(new THREE.Vector3(1, 1, 1), 0.05);
      }

      // Spark Particles Motion
      const posAttr = sparkGeo.attributes.position as THREE.BufferAttribute;
      const array = posAttr.array as Float32Array;
      for (let i = 0; i < sparkCount; i++) {
        array[i * 3] += sparkVelocities[i].x;
        array[i * 3 + 1] += sparkVelocities[i].y;
        array[i * 3 + 2] += sparkVelocities[i].z;

        // Boundary wrap
        if (Math.abs(array[i * 3]) > 2.5) array[i * 3] *= -0.8;
        if (array[i * 3 + 1] < -2.2) array[i * 3 + 1] = 2.0;
        if (array[i * 3 + 1] > 2.2) array[i * 3 + 1] = -2.0;
      }
      posAttr.needsUpdate = true;

      // Memtable Pulsing
      const pulse = 1.0 + Math.sin(elapsed * 4) * 0.03;
      memtableGroup.scale.set(pulse, pulse, pulse);

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
      container.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("resize", handleResize);
      cancelAnimationFrame(animId);
      renderer.dispose();
      if (container.contains(renderer.domElement)) {
        container.removeChild(renderer.domElement);
      }
    };
  }, []);

  return (
    <div className="relative w-full h-[480px] bg-[#141414] border border-[#414042] rounded-[30px] overflow-hidden shadow-[0_0_40px_rgba(64,91,255,0.2)]">
      {/* 3D WebGL Canvas */}
      <div ref={containerRef} className="absolute inset-0 w-full h-full cursor-grab active:cursor-grabbing" />

      {/* Top Overlay Badge & Telemetry */}
      <div className="absolute top-4 left-4 right-4 flex items-center justify-between pointer-events-none">
        <div className="flex items-center gap-2 px-3 py-1 rounded-[30px] bg-[#0e0e0e]/80 border border-white/10 backdrop-blur-md">
          <span className="w-2 h-2 rounded-full bg-[#00f0ff] animate-pulse" />
          <span className="text-[11px] font-mono text-white tracking-wide uppercase">
            Interactive 3D Engine Reactor
          </span>
        </div>
        <div className="px-3 py-1 rounded-[30px] bg-[#405bff]/20 border border-[#405bff]/40 text-[#7084ff] text-[11px] font-mono backdrop-blur-md">
          Throughput: {opsCounter.toLocaleString()} ops/s
        </div>
      </div>

      {/* Tier Labels Overlay */}
      <div className="absolute left-6 top-1/2 -translate-y-1/2 space-y-4 pointer-events-none hidden sm:block">
        <div className="flex items-center gap-2">
          <span className="w-1.5 h-1.5 rounded-full bg-[#00f0ff]" />
          <span className="text-[11px] font-mono text-[#a7a9ac]">Tier 0: MemTable (SkipList)</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="w-1.5 h-1.5 rounded-full bg-[#7084ff]" />
          <span className="text-[11px] font-mono text-[#a7a9ac]">Tier 1: WAL Double Helix</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="w-1.5 h-1.5 rounded-full bg-[#405bff]" />
          <span className="text-[11px] font-mono text-[#a7a9ac]">Tier 2: NVMe Ext4 SSTables</span>
        </div>
      </div>

      {/* Interactive Control Pill Bar (Bottom) */}
      <div className="absolute bottom-4 left-1/2 -translate-x-1/2 flex items-center gap-2 bg-[#0e0e0e]/90 border border-white/10 p-1.5 rounded-[30px] shadow-2xl backdrop-blur-lg z-10">
        <button
          onClick={() => triggerActionRef.current?.("read")}
          className={`px-4 py-1.5 rounded-[30px] text-[12px] font-medium transition-all ${
            activeMode === "read"
              ? "bg-[#00f0ff] text-black shadow-[0_0_15px_rgba(0,240,255,0.6)]"
              : "text-[#d1d3d4] hover:text-white hover:bg-white/5"
          }`}
        >
          ⚡ Concurrent Read
        </button>

        <button
          onClick={() => triggerActionRef.current?.("flush")}
          className={`px-4 py-1.5 rounded-[30px] text-[12px] font-medium transition-all ${
            activeMode === "flush"
              ? "bg-[#405bff] text-white shadow-[0_0_15px_rgba(64,91,255,0.6)]"
              : "text-[#d1d3d4] hover:text-white hover:bg-white/5"
          }`}
        >
          💾 MemTable Flush
        </button>

        <button
          onClick={() => triggerActionRef.current?.("compact")}
          className={`px-4 py-1.5 rounded-[30px] text-[12px] font-medium transition-all ${
            activeMode === "compact"
              ? "bg-[#ff007f] text-white shadow-[0_0_15px_rgba(255,0,127,0.6)]"
              : "text-[#d1d3d4] hover:text-white hover:bg-white/5"
          }`}
        >
          🔄 SSTable Compaction
        </button>
      </div>
    </div>
  );
}

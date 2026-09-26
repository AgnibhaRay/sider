"use client";

import React, { useEffect, useRef, useState } from "react";
import * as THREE from "three";

export type FlowType = "write" | "read" | "compact" | "pubsub";

interface NodeInfo {
  id: string;
  name: string;
  tier: string;
  complexity: string;
  summary: string;
  details: string[];
  codeSnippet: string;
}

const ARCHITECTURE_NODES: Record<string, NodeInfo> = {
  client: {
    id: "client",
    name: "Client Ingestion Gateway",
    tier: "Network Layer",
    complexity: "O(1) TCP Socket Framing",
    summary: "Handles high-concurrency client connections over TCP (port 4000) using custom line-delimited ASCII protocol with zero-copy stream parsing.",
    details: [
      "Non-blocking TCP socket pool with connection keep-alive",
      "Native support for Netcat, Python, Java Spring Boot & Go drivers",
      "Multiplexed command parser routing KV operations vs Pub/Sub channels"
    ],
    codeSnippet: "conn, err := listener.Accept()\ngo handleConnection(conn, engine, broker)"
  },
  wal: {
    id: "wal",
    name: "Write-Ahead Log (WAL)",
    tier: "Tier 01 • Durability",
    complexity: "O(1) Sequential Disk Append",
    summary: "Synchronous append-only binary log. Every write is guaranteed to disk before in-memory acknowledgment, surviving unexpected crashes and power failures.",
    details: [
      "Sequential binary records with 8-byte nanosecond TTL timestamps",
      "Instantaneous replay upon engine restart in sub-100ms",
      "Auto-rotated and truncated upon successful SSTable flush"
    ],
    codeSnippet: "wal.WriteRecord(OpPutEx, key, val, expireNano)\nwal.Sync()"
  },
  memtable: {
    id: "memtable",
    name: "SkipList MemTable",
    tier: "Tier 02 • Fast RAM",
    complexity: "O(log N) Search & Insert",
    summary: "Probabilistic multi-tier linked list holding active keys in RAM. Provides predictable logarithmic reads and writes without heavy mutex contention or B-Tree rebalancing.",
    details: [
      "Dynamic probabilistic level generation (p=0.5, MaxLevel=16)",
      "Supports range scans and prefix iteration directly from memory",
      "Triggers immutable flush when reaching configured threshold (e.g. 64KB - 4MB)"
    ],
    codeSnippet: "node := memtable.Insert(key, value, expireAt)\nif memtable.ByteSize() > threshold { s.Flush() }"
  },
  bloom: {
    id: "bloom",
    name: "FNV-1a Bloom Filter",
    tier: "Tier 03 • Disk I/O Guard",
    complexity: "O(k) Hash Evaluations (~100ns)",
    summary: "In-memory BitSet guard that intercepts read operations before touching physical disk. Guarantees 90%+ disk read skip for non-existent or stale keys.",
    details: [
      "1024-byte compact BitSet embedded in each SSTable header",
      "Dual FNV-1a hash salt algorithms with near-zero false-positive rates",
      "Completely eliminates costly random disk seeks on cache misses"
    ],
    codeSnippet: "if !sstable.BloomFilter.MayContain(key) {\n    return nil, ErrNotFound // Skipped physical disk seek!\n}"
  },
  sstable: {
    id: "sstable",
    name: "Immutable SSTables",
    tier: "Tier 04 • Disk Storage",
    complexity: "O(log M) Binary Search over Sparse Blocks",
    summary: "Sorted String Tables persisted as immutable .db files on disk. Sorted keys allow efficient binary search and contiguous block sequential reading.",
    details: [
      "Immutable disk layout prevents file corruption and lock contention",
      "Sparse index blocks load into RAM for rapid offset location",
      "Reverse chronological search (L0 newest to oldest)"
    ],
    codeSnippet: "offset := sstable.SearchIndex(key)\nrecord := sstable.ReadBlockAt(offset)"
  },
  compactor: {
    id: "compactor",
    name: "K-Way Merge Compactor",
    tier: "Tier 05 • Space Reclamation",
    complexity: "O(N log K) Multi-Way Merge",
    summary: "Background asynchronous compactor that merges multiple SSTables, purges expired TTL entries and tombstoned keys, and creates a consolidated storage table.",
    details: [
      "Zero-downtime asynchronous background compaction routine",
      "Purges tombstoned records deleted with DEL or expired via TTL",
      "Recovers 100% of deleted disk space and reconstructs unified Bloom filters"
    ],
    codeSnippet: "go s.compactTables([]string{table0, table1})\n// Merged table ready, old tables unlinked"
  },
  pubsub: {
    id: "pubsub",
    name: "Streaming Pub/Sub Broker",
    tier: "Real-Time Bus",
    complexity: "O(S) Fan-out to S Subscribers",
    summary: "Thread-safe event streaming hub multiplexed within the Sider engine. Allows distributed clients to publish and subscribe to real-time event topics.",
    details: [
      "Zero-copy broadcast loop over active client TCP socket streams",
      "Dynamic channel creation with instant subscriber fan-out",
      "Powers real-time alerts, hospital emergency dispatch, and cache sync"
    ],
    codeSnippet: "broker.Publish(channel, payload)\n// Broadcast to all active TCP listener streams"
  }
};

export function ThreeArchitectureVisualizer() {
  const containerRef = useRef<HTMLDivElement>(null);
  const [activeFlow, setActiveFlow] = useState<FlowType>("write");
  const [selectedNode, setSelectedNode] = useState<NodeInfo>(ARCHITECTURE_NODES.memtable);
  const [hoveredNodeId, setHoveredNodeId] = useState<string | null>(null);
  const [isRotating, setIsRotating] = useState(true);

  // Flow explanation text
  const flowDescriptions: Record<FlowType, { title: string; subtitle: string; path: string }> = {
    write: {
      title: "WRITE PIPELINE (PUT / PUTEX)",
      subtitle: "Sequential WAL durability + Concurrent SkipList MemTable ingestion",
      path: "Client TCP  ──►  Router  ──►  WAL (Disk Append)  &  MemTable (RAM O(log N))"
    },
    read: {
      title: "READ PIPELINE (GET)",
      subtitle: "Hierarchical memory check with Bloom Filter disk skip acceleration",
      path: "Client TCP  ──►  MemTable (Hit)  │ (Miss) ──►  Bloom Filter  ──►  SSTables"
    },
    compact: {
      title: "FLUSH & COMPACTION LIFECYCLE",
      subtitle: "MemTable freeze to SSTable + K-Way merge tombstone purge",
      path: "MemTable (Full)  ──►  Flush Immutable SSTable  ──►  K-Way Merge Compactor"
    },
    pubsub: {
      title: "STREAMING PUB/SUB BROADCAST",
      subtitle: "Non-blocking multiplexed TCP event distribution",
      path: "Publisher TCP  ──►  Engine Broker  ──►  Subscribers 1, 2, 3... (Broadcast)"
    }
  };

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    // Scene, Camera, Renderer
    const scene = new THREE.Scene();
    scene.background = null; // transparent to inherit putty paper

    const width = container.clientWidth || 900;
    const height = container.clientHeight || 550;

    const camera = new THREE.PerspectiveCamera(40, width / height, 0.1, 1000);
    camera.position.set(13, 10, 16);
    camera.lookAt(0, 0, 0);

    const renderer = new THREE.WebGLRenderer({
      antialias: true,
      alpha: true,
      powerPreference: "high-performance"
    });
    renderer.setSize(width, height);
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
    renderer.shadowMap.enabled = false;
    container.appendChild(renderer.domElement);

    // Subtle Lighting (architectural aesthetic: crisp ink & bone contrast)
    const ambientLight = new THREE.AmbientLight(0xffffff, 0.95);
    scene.add(ambientLight);

    const dirLight1 = new THREE.DirectionalLight(0xffffff, 1.2);
    dirLight1.position.set(15, 25, 20);
    scene.add(dirLight1);

    const dirLight2 = new THREE.DirectionalLight(0xc4c3b6, 0.8);
    dirLight2.position.set(-15, -10, -15);
    scene.add(dirLight2);

    // Architectural Ground Grid on Putty Paper
    const gridHelper = new THREE.GridHelper(26, 26, 0x808080, 0xdfdcd5);
    gridHelper.position.y = -3.5;
    scene.add(gridHelper);

    // Materials Palette (Renaissance / Putty Paper / Ink / Bone)
    const inkColor = 0x000000;
    const boneColor = 0xe7e5e4;
    const puttyDark = 0x595855;
    const goldAccent = 0xb48a4d;
    const activeColor = 0x1f1e1c;

    const nodeMeshes: { id: string; mesh: THREE.Object3D }[] = [];
    const interactiveObjects: THREE.Object3D[] = [];

    // Helper: Create an architectural framed box
    const createFramedBox = (
      w: number,
      h: number,
      d: number,
      fillColor: number,
      opacity = 0.9,
      wireColor = inkColor
    ) => {
      const group = new THREE.Group();
      const geom = new THREE.BoxGeometry(w, h, d);
      const mat = new THREE.MeshStandardMaterial({
        color: fillColor,
        roughness: 0.35,
        metalness: 0.1,
        transparent: opacity < 1,
        opacity: opacity
      });
      const mesh = new THREE.Mesh(geom, mat);
      group.add(mesh);

      const edges = new THREE.EdgesGeometry(geom);
      const line = new THREE.LineSegments(
        edges,
        new THREE.LineBasicMaterial({ color: wireColor, linewidth: 1.5 })
      );
      group.add(line);

      return { group, mesh, mat };
    };

    // 1. Client Gateway (Left Emitter)
    const clientGroup = new THREE.Group();
    clientGroup.position.set(-8, 1.5, 0);
    {
      const { group: base, mesh } = createFramedBox(2, 2.2, 2, boneColor, 0.95, inkColor);
      clientGroup.add(base);
      mesh.userData = { nodeId: "client" };
      interactiveObjects.push(mesh);

      // Compass Ring around client
      const ringGeom = new THREE.RingGeometry(1.6, 1.7, 32);
      const ringMat = new THREE.MeshBasicMaterial({ color: inkColor, side: THREE.DoubleSide });
      const ringMesh = new THREE.Mesh(ringGeom, ringMat);
      ringMesh.rotation.x = Math.PI / 2;
      ringMesh.position.y = -1;
      clientGroup.add(ringMesh);
    }
    scene.add(clientGroup);
    nodeMeshes.push({ id: "client", mesh: clientGroup });

    // 2. WAL (Write-Ahead Log Cylinder / Tape)
    const walGroup = new THREE.Group();
    walGroup.position.set(-2.5, 3.2, -4);
    {
      const cylGeom = new THREE.CylinderGeometry(1.4, 1.4, 3, 24, 4);
      const cylMat = new THREE.MeshStandardMaterial({
        color: 0xdfdcd5,
        roughness: 0.25,
        metalness: 0.3,
        transparent: true,
        opacity: 0.88
      });
      const cylMesh = new THREE.Mesh(cylGeom, cylMat);
      cylMesh.userData = { nodeId: "wal" };
      interactiveObjects.push(cylMesh);
      walGroup.add(cylMesh);

      const cylEdges = new THREE.EdgesGeometry(cylGeom);
      const cylLine = new THREE.LineSegments(
        cylEdges,
        new THREE.LineBasicMaterial({ color: inkColor })
      );
      walGroup.add(cylLine);

      // Rings on WAL cylinder (representing committed binary records)
      for (let i = -1; i <= 1; i += 0.8) {
        const discGeom = new THREE.TorusGeometry(1.42, 0.03, 8, 24);
        const discMat = new THREE.MeshBasicMaterial({ color: goldAccent });
        const discMesh = new THREE.Mesh(discGeom, discMat);
        discMesh.rotation.x = Math.PI / 2;
        discMesh.position.y = i;
        walGroup.add(discMesh);
      }
    }
    scene.add(walGroup);
    nodeMeshes.push({ id: "wal", mesh: walGroup });

    // 3. SkipList MemTable (Tiered Isometric Stepped Glass Slabs)
    const memtableGroup = new THREE.Group();
    memtableGroup.position.set(0, 1.8, 1);
    {
      // 3 Layers representing SkipList Levels
      const slabHeights = [0.25, 0.25, 0.25];
      const slabSizes = [4.2, 3.2, 2.2];
      const yOffsets = [-0.6, 0.1, 0.8];

      slabSizes.forEach((size, idx) => {
        const { group: slab, mesh } = createFramedBox(
          size,
          slabHeights[idx],
          size,
          boneColor,
          0.85,
          inkColor
        );
        slab.position.y = yOffsets[idx];
        memtableGroup.add(slab);
        mesh.userData = { nodeId: "memtable" };
        interactiveObjects.push(mesh);
      });

      // Pointer connecting pillars between tiers
      const p1 = new THREE.Mesh(
        new THREE.CylinderGeometry(0.04, 0.04, 1.4, 8),
        new THREE.MeshBasicMaterial({ color: inkColor })
      );
      p1.position.set(0.8, 0.2, 0.8);
      memtableGroup.add(p1);

      const p2 = p1.clone();
      p2.position.set(-0.8, 0.2, -0.8);
      memtableGroup.add(p2);
    }
    scene.add(memtableGroup);
    nodeMeshes.push({ id: "memtable", mesh: memtableGroup });

    // 4. Bloom Filter (Scanning Precision Matrix)
    const bloomGroup = new THREE.Group();
    bloomGroup.position.set(4, 0.5, -3.5);
    {
      const { group: panel, mesh } = createFramedBox(3.4, 0.2, 3.4, 0x1f1e1c, 0.95, goldAccent);
      bloomGroup.add(panel);
      mesh.userData = { nodeId: "bloom" };
      interactiveObjects.push(mesh);

      // Embedded 4x4 laser bitset grid
      for (let x = -1.2; x <= 1.2; x += 0.8) {
        for (let z = -1.2; z <= 1.2; z += 0.8) {
          const bitGeom = new THREE.CylinderGeometry(0.12, 0.12, 0.35, 12);
          const bitMat = new THREE.MeshBasicMaterial({
            color: Math.random() > 0.4 ? goldAccent : 0x595855
          });
          const bitMesh = new THREE.Mesh(bitGeom, bitMat);
          bitMesh.position.set(x, 0.1, z);
          bloomGroup.add(bitMesh);
        }
      }
    }
    scene.add(bloomGroup);
    nodeMeshes.push({ id: "bloom", mesh: bloomGroup });

    // 5. SSTables on Disk (Heavy Monolith Slabs Stacked)
    const sstableGroup = new THREE.Group();
    sstableGroup.position.set(6, -1.2, 2.5);
    {
      // 3 Stacked Disk SSTable Blocks (L0, L1, Compacted)
      for (let i = 0; i < 3; i++) {
        const { group: block, mesh } = createFramedBox(3.2, 0.7, 3.2, 0xdfdcd5, 0.98, inkColor);
        block.position.y = i * 0.85;
        sstableGroup.add(block);
        mesh.userData = { nodeId: "sstable" };
        interactiveObjects.push(mesh);
      }
    }
    scene.add(sstableGroup);
    nodeMeshes.push({ id: "sstable", mesh: sstableGroup });

    // 6. K-Way Compactor (Asynchronous Turbine / Core)
    const compactorGroup = new THREE.Group();
    compactorGroup.position.set(1.5, -1.8, 4.5);
    {
      const { group: box, mesh } = createFramedBox(2.2, 1.4, 2.2, boneColor, 0.9, inkColor);
      compactorGroup.add(box);
      mesh.userData = { nodeId: "compactor" };
      interactiveObjects.push(mesh);

      // Dual Intersecting Caliper Rings
      const ring1 = new THREE.Mesh(
        new THREE.TorusGeometry(1.3, 0.04, 8, 32),
        new THREE.MeshBasicMaterial({ color: inkColor })
      );
      ring1.rotation.y = Math.PI / 4;
      compactorGroup.add(ring1);
    }
    scene.add(compactorGroup);
    nodeMeshes.push({ id: "compactor", mesh: compactorGroup });

    // 7. Pub/Sub Broker Hub (Central Fan-out Antenna)
    const pubsubGroup = new THREE.Group();
    pubsubGroup.position.set(-4, -1.2, 3);
    {
      const { group: hub, mesh } = createFramedBox(1.8, 1.2, 1.8, boneColor, 0.95, inkColor);
      pubsubGroup.add(hub);
      mesh.userData = { nodeId: "pubsub" };
      interactiveObjects.push(mesh);

      // Radial Subscriber Satellites
      for (let i = 0; i < 4; i++) {
        const angle = (i * Math.PI) / 2;
        const sub = new THREE.Mesh(
          new THREE.SphereGeometry(0.25, 16, 16),
          new THREE.MeshStandardMaterial({ color: inkColor, roughness: 0.3 })
        );
        sub.position.set(Math.cos(angle) * 1.8, 0, Math.sin(angle) * 1.8);
        pubsubGroup.add(sub);

        // Connector line
        const lineGeom = new THREE.BufferGeometry().setFromPoints([
          new THREE.Vector3(0, 0, 0),
          new THREE.Vector3(Math.cos(angle) * 1.8, 0, Math.sin(angle) * 1.8)
        ]);
        const line = new THREE.Line(
          lineGeom,
          new THREE.LineBasicMaterial({ color: puttyDark, transparent: true, opacity: 0.5 })
        );
        pubsubGroup.add(line);
      }
    }
    scene.add(pubsubGroup);
    nodeMeshes.push({ id: "pubsub", mesh: pubsubGroup });

    // -------------------------------------------------------------
    // Architectural Pathway Splines & Dynamic Data Pulses
    // -------------------------------------------------------------
    interface FlowPathway {
      flow: FlowType;
      curve: THREE.CatmullRomCurve3;
      tubeMesh: THREE.Line;
    }

    const pathways: FlowPathway[] = [];

    // Helper: create smooth arched curve between points
    const makeCurve = (p1: [number, number, number], p2: [number, number, number], midYOffset = 1.2) => {
      const v1 = new THREE.Vector3(...p1);
      const v2 = new THREE.Vector3(...p2);
      const mid = new THREE.Vector3().addVectors(v1, v2).multiplyScalar(0.5);
      mid.y += midYOffset;
      return new THREE.CatmullRomCurve3([v1, mid, v2]);
    };

    const writeCurve1 = makeCurve([-8, 1.5, 0], [-2.5, 3.2, -4], 1.5); // Client -> WAL
    const writeCurve2 = makeCurve([-8, 1.5, 0], [0, 1.8, 1], 1.0);    // Client -> MemTable
    const readCurve1 = makeCurve([-8, 1.5, 0], [0, 1.8, 1], 0.8);     // Client -> MemTable
    const readCurve2 = makeCurve([0, 1.8, 1], [4, 0.5, -3.5], 1.2);    // MemTable -> Bloom
    const readCurve3 = makeCurve([4, 0.5, -3.5], [6, -0.5, 2.5], 1.0); // Bloom -> SSTable
    const compactCurve1 = makeCurve([0, 1.8, 1], [6, 0.5, 2.5], 1.5); // MemTable -> Flush SSTable
    const compactCurve2 = makeCurve([6, -0.5, 2.5], [1.5, -1.8, 4.5], 1.0); // SSTable -> Compactor
    const pubsubCurve = makeCurve([-8, 1.5, 0], [-4, -1.2, 3], 0.8); // Client -> PubSub

    const addPathway = (curve: THREE.CatmullRomCurve3, flow: FlowType) => {
      const points = curve.getPoints(50);
      const geom = new THREE.BufferGeometry().setFromPoints(points);
      const line = new THREE.Line(
        geom,
        new THREE.LineBasicMaterial({
          color: inkColor,
          transparent: true,
          opacity: 0.25
        })
      );
      scene.add(line);
      pathways.push({ flow, curve, tubeMesh: line });
    };

    addPathway(writeCurve1, "write");
    addPathway(writeCurve2, "write");
    addPathway(readCurve1, "read");
    addPathway(readCurve2, "read");
    addPathway(readCurve3, "read");
    addPathway(compactCurve1, "compact");
    addPathway(compactCurve2, "compact");
    addPathway(pubsubCurve, "pubsub");

    // -------------------------------------------------------------
    // Data Packet Spheres (Pulses traveling along curves)
    // -------------------------------------------------------------
    const packetCount = 18;
    const packetGeom = new THREE.SphereGeometry(0.18, 12, 12);
    const packetMat = new THREE.MeshBasicMaterial({ color: inkColor });
    const packetMeshes: { mesh: THREE.Mesh; pathwayIdx: number; t: number; speed: number }[] = [];

    for (let i = 0; i < packetCount; i++) {
      const mesh = new THREE.Mesh(packetGeom, packetMat.clone());
      scene.add(mesh);
      packetMeshes.push({
        mesh,
        pathwayIdx: i % pathways.length,
        t: Math.random(),
        speed: 0.006 + Math.random() * 0.005
      });
    }

    // -------------------------------------------------------------
    // Interactive Raycaster & Mouse Orbit Control
    // -------------------------------------------------------------
    const raycaster = new THREE.Raycaster();
    const mouse = new THREE.Vector2();

    let isMouseDown = false;
    let prevMouseX = 0;
    let prevMouseY = 0;
    let spherical = { radius: 24, theta: 0.8, phi: 1.1 }; // isometric default

    const updateCameraFromSpherical = () => {
      camera.position.x = spherical.radius * Math.sin(spherical.phi) * Math.sin(spherical.theta);
      camera.position.y = spherical.radius * Math.cos(spherical.phi);
      camera.position.z = spherical.radius * Math.sin(spherical.phi) * Math.cos(spherical.theta);
      camera.lookAt(0, 0, 0);
    };
    updateCameraFromSpherical();

    const onPointerDown = (e: MouseEvent) => {
      isMouseDown = true;
      prevMouseX = e.clientX;
      prevMouseY = e.clientY;
    };

    const onPointerMove = (e: MouseEvent) => {
      const rect = renderer.domElement.getBoundingClientRect();
      mouse.x = ((e.clientX - rect.left) / rect.width) * 2 - 1;
      mouse.y = -((e.clientY - rect.top) / rect.height) * 2 + 1;

      if (isMouseDown) {
        setIsRotating(false);
        const deltaX = e.clientX - prevMouseX;
        const deltaY = e.clientY - prevMouseY;
        prevMouseX = e.clientX;
        prevMouseY = e.clientY;

        spherical.theta -= deltaX * 0.008;
        spherical.phi = Math.max(0.2, Math.min(Math.PI / 2 - 0.05, spherical.phi - deltaY * 0.008));
        updateCameraFromSpherical();
      } else {
        // Hover Raycast
        raycaster.setFromCamera(mouse, camera);
        const intersects = raycaster.intersectObjects(interactiveObjects, true);
        if (intersects.length > 0) {
          const hitObj = intersects[0].object;
          const nid = hitObj.userData.nodeId;
          if (nid) {
            setHoveredNodeId(nid);
            renderer.domElement.style.cursor = "pointer";
            return;
          }
        }
        setHoveredNodeId(null);
        renderer.domElement.style.cursor = "default";
      }
    };

    const onPointerUp = () => {
      isMouseDown = false;
    };

    const onClick = (e: MouseEvent) => {
      const rect = renderer.domElement.getBoundingClientRect();
      mouse.x = ((e.clientX - rect.left) / rect.width) * 2 - 1;
      mouse.y = -((e.clientY - rect.top) / rect.height) * 2 + 1;

      raycaster.setFromCamera(mouse, camera);
      const intersects = raycaster.intersectObjects(interactiveObjects, true);
      if (intersects.length > 0) {
        const hitObj = intersects[0].object;
        const nid = hitObj.userData.nodeId;
        if (nid && ARCHITECTURE_NODES[nid]) {
          setSelectedNode(ARCHITECTURE_NODES[nid]);
        }
      }
    };

    const dom = renderer.domElement;
    dom.addEventListener("pointerdown", onPointerDown);
    window.addEventListener("pointermove", onPointerMove);
    window.addEventListener("pointerup", onPointerUp);
    dom.addEventListener("click", onClick);

    // -------------------------------------------------------------
    // Animation Loop
    // -------------------------------------------------------------
    let animationFrameId: number;
    let clock = new THREE.Clock();

    const animate = () => {
      animationFrameId = requestAnimationFrame(animate);
      const delta = clock.getDelta();
      const elapsed = clock.getElapsedTime();

      // Subtle slow auto-orbit if not dragging
      if (isRotating) {
        spherical.theta += delta * 0.05;
        updateCameraFromSpherical();
      }

      // Gentle floating levitation for SkipList tiers
      memtableGroup.position.y = 1.8 + Math.sin(elapsed * 1.5) * 0.08;
      walGroup.rotation.y = elapsed * 0.4;
      compactorGroup.rotation.y = -elapsed * 0.5;

      // Update pathways and packets according to active flow
      pathways.forEach((p) => {
        const isCurrent = p.flow === activeFlow;
        const mat = p.tubeMesh.material as THREE.LineBasicMaterial;
        mat.opacity = isCurrent ? 0.75 : 0.12;
        mat.color.setHex(isCurrent ? inkColor : 0x808080);
      });

      // Filter active pathways for particles
      const activePathways = pathways.filter((p) => p.flow === activeFlow);

      packetMeshes.forEach((pkt, idx) => {
        if (activePathways.length === 0) {
          pkt.mesh.visible = false;
          return;
        }
        pkt.mesh.visible = true;
        const path = activePathways[idx % activePathways.length];

        pkt.t = (pkt.t + pkt.speed) % 1;
        const pos = path.curve.getPointAt(pkt.t);
        pkt.mesh.position.copy(pos);

        // Highlight packet
        const pMat = pkt.mesh.material as THREE.MeshBasicMaterial;
        pMat.color.setHex(goldAccent);
      });

      renderer.render(scene, camera);
    };

    animate();

    // Resize Handler
    const handleResize = () => {
      if (!container) return;
      const newW = container.clientWidth;
      const newH = container.clientHeight || 550;
      camera.aspect = newW / newH;
      camera.updateProjectionMatrix();
      renderer.setSize(newW, newH);
    };
    window.addEventListener("resize", handleResize);

    return () => {
      cancelAnimationFrame(animationFrameId);
      window.removeEventListener("resize", handleResize);
      dom.removeEventListener("pointerdown", onPointerDown);
      window.removeEventListener("pointermove", onPointerMove);
      window.removeEventListener("pointerup", onPointerUp);
      dom.removeEventListener("click", onClick);
      if (container && renderer.domElement) {
        container.removeChild(renderer.domElement);
      }
      renderer.dispose();
    };
  }, [activeFlow, isRotating]);

  return (
    <div className="w-full flex flex-col items-center select-none">
      {/* Visualizer Top Control Bar */}
      <div className="w-full max-w-6xl flex flex-wrap items-center justify-between gap-4 mb-4 pb-4 border-b border-[#dfdcd5]">
        {/* Pipeline Flow Switchers */}
        <div className="flex flex-wrap items-center gap-2">
          {(["write", "read", "compact", "pubsub"] as const).map((flow) => (
            <button
              key={flow}
              onClick={() => setActiveFlow(flow)}
              className={`px-3.5 py-1.5 text-[11px] uppercase tracking-[0.08em] font-medium transition-all rounded-[3px] border ${
                activeFlow === flow
                  ? "bg-[#000000] text-[#ffffff] border-[#000000] shadow-none"
                  : "bg-[#e7e5e4] text-[#595855] border-[#dfdcd5] hover:text-[#000000] hover:border-[#000000]"
              }`}
              style={{ fontFamily: "var(--font-helvetica-now)" }}
            >
              {flow === "write" && "⚡ Write Pipeline (PUT)"}
              {flow === "read" && "🔍 Read Pipeline (GET)"}
              {flow === "compact" && "🗜️ Compaction Pipeline"}
              {flow === "pubsub" && "📡 Pub/Sub Broadcast"}
            </button>
          ))}
        </div>

        {/* View Controls */}
        <div className="flex items-center gap-3 text-[11px] text-[#595855]">
          <button
            onClick={() => setIsRotating((prev) => !prev)}
            className="flex items-center gap-1.5 px-2.5 py-1 bg-[#e7e5e4] border border-[#dfdcd5] rounded-[3px] hover:text-[#000000] hover:border-[#000000] transition-colors"
          >
            <span>{isRotating ? "⏸ Pause Rotation" : "▶ Resume Auto-Orbit"}</span>
          </button>
          <span className="hidden sm:inline text-[#808080]">&bull; Drag to rotate in 3D</span>
        </div>
      </div>

      {/* Active Flow Subtitle Banner */}
      <div className="w-full max-w-6xl mb-6 flex flex-col sm:flex-row sm:items-center justify-between gap-2 bg-[#dfdcd5]/40 px-4 py-2.5 border border-[#dfdcd5] rounded-[4px]">
        <div>
          <span
            className="text-[12px] font-semibold text-[#000000] tracking-wide"
            style={{ fontFamily: "var(--font-helvetica-now)" }}
          >
            {flowDescriptions[activeFlow].title}:
          </span>{" "}
          <span
            className="text-[12px] text-[#595855]"
            style={{ fontFamily: "var(--font-helvetica-now)" }}
          >
            {flowDescriptions[activeFlow].subtitle}
          </span>
        </div>
        <div className="font-mono text-[11px] text-[#000000] tracking-tight">
          {flowDescriptions[activeFlow].path}
        </div>
      </div>

      {/* Main 3D Canvas Stage with Live Inspector Panel */}
      <div className="w-full max-w-6xl grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
        {/* 3D WebGL Canvas Viewport (8 Columns on desktop) */}
        <div className="lg:col-span-8 relative w-full h-[450px] sm:h-[540px] bg-[#dfdcd5]/20 rounded-[8px] border border-[#dfdcd5] overflow-hidden flex items-center justify-center">
          {/* Subtle canvas watermark & coordinate reticle */}
          <div className="absolute top-3 left-4 text-[10px] uppercase font-mono tracking-widest text-[#808080] pointer-events-none">
            [ 3D LSM ISOMETRIC PROJECTION &bull; DRAG TO ORBIT ]
          </div>

          {hoveredNodeId && (
            <div className="absolute top-3 right-4 px-2 py-0.5 bg-[#000000] text-[#ffffff] text-[10px] font-mono tracking-wide rounded-[2px] pointer-events-none">
              TARGET: {hoveredNodeId.toUpperCase()} &bull; CLICK TO INSPECT
            </div>
          )}

          {/* Canvas Mount Container */}
          <div ref={containerRef} className="w-full h-full cursor-grab active:cursor-grabbing" />
        </div>

        {/* Component Deep-Dive Inspector Panel (4 Columns on desktop) */}
        <div className="lg:col-span-4 bg-[#e7e5e4] border border-[#dfdcd5] rounded-[8px] p-5 flex flex-col justify-between min-h-[450px] sm:min-h-[540px]">
          <div>
            <div className="flex items-center justify-between text-[10px] uppercase tracking-[0.14em] text-[#595855] mb-2 font-mono">
              <span>{selectedNode.tier}</span>
              <span className="text-[#b48a4d] font-semibold">{selectedNode.complexity}</span>
            </div>

            <h3
              className="text-[26px] leading-[1.2] font-normal text-[#000000] mb-3"
              style={{ fontFamily: "var(--font-davinci)" }}
            >
              {selectedNode.name}
            </h3>

            <p
              className="text-[13px] leading-[1.6] text-[#595855] mb-4"
              style={{ fontFamily: "var(--font-helvetica-now)" }}
            >
              {selectedNode.summary}
            </p>

            {/* Architectural Highlights */}
            <div className="space-y-2 mb-4">
              {selectedNode.details.map((item, idx) => (
                <div key={idx} className="flex items-start gap-2 text-[12px] text-[#000000]">
                  <span className="text-[#b48a4d] text-[14px] leading-none">&bull;</span>
                  <span style={{ fontFamily: "var(--font-helvetica-now)" }}>{item}</span>
                </div>
              ))}
            </div>
          </div>

          {/* Go Engine Code Snippet */}
          <div className="mt-4 pt-4 border-t border-[#dfdcd5]">
            <div className="text-[10px] uppercase font-mono tracking-wider text-[#808080] mb-1.5">
              Engine Go Implementation:
            </div>
            <pre className="bg-[#1f1e1c] text-[#dfdcd5] p-3 rounded-[4px] text-[11px] font-mono overflow-x-auto leading-relaxed">
              <code>{selectedNode.codeSnippet}</code>
            </pre>
          </div>
        </div>
      </div>

      {/* Interactive Node Selector Pills below canvas */}
      <div className="w-full max-w-6xl mt-4 flex flex-wrap items-center justify-center gap-2">
        <span className="text-[11px] text-[#808080] uppercase tracking-wider mr-1">
          Direct Node Inspection:
        </span>
        {Object.values(ARCHITECTURE_NODES).map((node) => (
          <button
            key={node.id}
            onClick={() => setSelectedNode(node)}
            className={`px-2.5 py-1 text-[11px] font-mono rounded-[3px] border transition-colors ${
              selectedNode.id === node.id
                ? "bg-[#000000] text-[#ffffff] border-[#000000]"
                : "bg-[#e7e5e4] text-[#595855] border-[#dfdcd5] hover:text-[#000000] hover:border-[#595855]"
            }`}
          >
            {node.name}
          </button>
        ))}
      </div>
    </div>
  );
}

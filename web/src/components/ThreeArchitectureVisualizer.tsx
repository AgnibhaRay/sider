"use client";

import React, { useEffect, useRef, useState, useMemo } from "react";
import * as THREE from "three";

export type FlowType = "write" | "read" | "compact" | "pubsub";

interface NodeInfo {
  id: string;
  tag: string;
  name: string;
  tier: string;
  badge: string;
  accentColor: string;
  complexity: string;
  pos: [number, number, number];
  summary: string;
  details: string[];
  codeSnippet: string;
  telemetry: { label: string; val: string }[];
}

const ARCHITECTURE_NODES: Record<string, NodeInfo> = {
  client: {
    id: "client",
    tag: "01",
    name: "TCP Client Gateway",
    tier: "Network Layer",
    badge: "PORT :4000",
    accentColor: "#38bdf8", // Electric Sky Blue
    complexity: "O(1) Socket Stream Framing",
    pos: [-7, 1.8, 0],
    summary: "High-concurrency connection pool handling TCP client streams with custom line-delimited ASCII protocol and zero-copy command parsing.",
    details: [
      "Multiplexed goroutine-per-client event dispatching",
      "Native compatibility with Netcat, Python, Java Spring Boot & Go",
      "Sub-microsecond command tokenization & buffer pooling"
    ],
    codeSnippet: `func handleConnection(conn net.Conn, engine *Engine) {
    reader := bufio.NewReader(conn)
    for {
        line, err := reader.ReadString('\\n')
        cmd, args := parseCommand(line)
        resp := engine.Execute(cmd, args)
        conn.Write([]byte(resp + "\\n"))
    }
}`,
    telemetry: [
      { label: "CONNECTED CLIENTS", val: "1,248 active" },
      { label: "PROTOCOL", val: "ASCII Line / TCP" },
      { label: "AVG PARSE TIME", val: "85 ns" }
    ]
  },
  wal: {
    id: "wal",
    tag: "02",
    name: "Write-Ahead Log (WAL)",
    tier: "Durability Tier",
    badge: "sider.wal",
    accentColor: "#f59e0b", // Radiant Amber
    complexity: "O(1) Sequential Disk Append",
    pos: [-2.5, 3.8, -3.5],
    summary: "Synchronous append-only binary log with 8-byte TTL timestamps. Guarantees zero data loss across unannounced crashes and power cuts.",
    details: [
      "Binary encoded [OpCode | KeyLen | Key | ValLen | Val | ExpireNano | CRC32]",
      "Fsync frequency optimized for sub-millisecond append latency",
      "Instant crash replay into SkipList MemTable upon node startup"
    ],
    codeSnippet: `func (w *WAL) Append(op byte, key, val string, expireAt int64) error {
    w.mu.Lock()
    defer w.mu.Unlock()
    binary.Write(w.buf, binary.BigEndian, expireAt)
    w.buf.WriteString(key)
    w.buf.WriteString(val)
    return w.file.Sync()
}`,
    telemetry: [
      { label: "SYNC MODE", val: "Synchronous WAL" },
      { label: "APPEND LATENCY", val: "0.12 ms" },
      { label: "RECOVERY TIME", val: "< 45 ms" }
    ]
  },
  memtable: {
    id: "memtable",
    tag: "03",
    name: "SkipList MemTable",
    tier: "In-Memory Tier",
    badge: "LOCK-FREE RAM",
    accentColor: "#10b981", // Emerald Green
    complexity: "O(log N) Search & Insert",
    pos: [0, 2.2, 1.2],
    summary: "Probabilistic multi-level linked list holding active keys in RAM. Delivers logarithmic reads and writes without heavy mutex contention or B-Tree rebalancing.",
    details: [
      "Probabilistic geometric level distribution (p=0.5, MaxLevel=16)",
      "Supports range queries and instantaneous prefix iteration in RAM",
      "Freezes and flushes immutable SSTable when reaching capacity limit"
    ],
    codeSnippet: `func (s *SkipList) Insert(key, val string, expireAt int64) {
    update := make([]*Node, MaxLevel)
    curr := s.header
    for i := s.level - 1; i >= 0; i-- {
        for curr.forward[i] != nil && curr.forward[i].key < key {
            curr = curr.forward[i]
        }
        update[i] = curr
    }
    // O(log N) node link insertion
}`,
    telemetry: [
      { label: "MEMTABLE SIZE", val: "1.2 MB / 4.0 MB" },
      { label: "SKIPLIST LEVELS", val: "8 Active" },
      { label: "RAM SEARCH", val: "420 ns" }
    ]
  },
  bloom: {
    id: "bloom",
    tag: "04",
    name: "FNV-1a Bloom Filter",
    tier: "Disk I/O Shield",
    badge: "98.4% SKIP RATE",
    accentColor: "#06b6d4", // Electric Cyan
    complexity: "O(k) Hashes (~90ns)",
    pos: [4.2, 0.8, -3.2],
    summary: "Compact 1024-byte in-memory BitSet embedded in each SSTable header. Catches non-existent keys in sub-microsecond time, bypassing physical disk I/O.",
    details: [
      "1024-byte BitSet with 3 independent FNV-1a hash functions",
      "Evaluates key presence before initiating any disk block seeks",
      "Guarantees zero false negatives and < 1.2% false positives"
    ],
    codeSnippet: `func (bf *BloomFilter) MayContain(key string) bool {
    h1 := fnv1a(key)
    h2 := fnv1aWithSeed(key, 0x5bd1e995)
    for i := 0; i < 3; i++ {
        idx := (h1 + uint32(i)*h2) % uint32(len(bf.bits)*8)
        if (bf.bits[idx/8] & (1 << (idx%8))) == 0 {
            return false // Definitely NOT on disk!
        }
    }
    return true
}`,
    telemetry: [
      { label: "BITSET SIZE", val: "1,024 Bytes" },
      { label: "FALSE POSITIVE", val: "< 1.2%" },
      { label: "DISK IO SAVED", val: "98.4%" }
    ]
  },
  sstable: {
    id: "sstable",
    tag: "05",
    name: "Immutable SSTables",
    tier: "Storage Tier",
    badge: "PERSISTENT DISK",
    accentColor: "#a855f7", // Holographic Violet
    complexity: "O(log M) Binary Search",
    pos: [6.5, -1.2, 2],
    summary: "Sorted String Tables written as immutable .db files on disk. Sorted order allows binary searching sparse index blocks with high sequential disk read throughput.",
    details: [
      "Zero lock contention: SSTables are strictly immutable once written",
      "Embedded sparse index blocks cached in RAM for rapid block locating",
      "Reverse chronological search (L0 newest to oldest tables)"
    ],
    codeSnippet: `func (s *SSTable) Get(key string) (string, bool) {
    if !s.bloom.MayContain(key) { return "", false }
    blockOffset := s.searchSparseIndex(key)
    return s.readFromBlock(blockOffset, key)
}`,
    telemetry: [
      { label: "SSTABLES ON DISK", val: "3 Active (.db)" },
      { label: "INDEX TYPE", val: "Sparse Key Index" },
      { label: "READ LATENCY", val: "0.45 ms" }
    ]
  },
  compactor: {
    id: "compactor",
    tag: "06",
    name: "K-Way Compactor",
    tier: "Engine Core",
    badge: "BACKGROUND WORKER",
    accentColor: "#ec4899", // Neon Rose
    complexity: "O(N log K) Multi-Way Merge",
    pos: [1.8, -2.4, 4.2],
    summary: "Asynchronous background engine that merges overlapping SSTables, purges tombstones from DEL operations, cleans expired TTL keys, and frees disk space.",
    details: [
      "Zero-downtime background routine runs without blocking live queries",
      "Reclaims 100% of fragmented and deleted storage space",
      "Reconstructs unified Bloom filters for newly consolidated SSTables"
    ],
    codeSnippet: `func (e *Engine) TriggerCompaction() {
    go func() {
        mergedTable := e.kWayMerge(e.tables)
        e.atomicReplaceTables(mergedTable)
        e.unlinkOldTables()
    }()
}`,
    telemetry: [
      { label: "PURGE RATE", val: "100% Tombstones" },
      { label: "IO STRATEGY", val: "Rate-limited Sequential" },
      { label: "STATUS", val: "Idle / Ready" }
    ]
  },
  pubsub: {
    id: "pubsub",
    tag: "07",
    name: "Pub/Sub Streaming Hub",
    tier: "Real-Time Layer",
    badge: "FAN-OUT BUS",
    accentColor: "#eab308", // Sun Gold
    complexity: "O(S) Active Subscribers",
    pos: [-4.2, -1.6, 3.2],
    summary: "Multiplexed thread-safe message broker integrated directly into the database engine. Delivers real-time pub/sub notifications over open TCP sockets.",
    details: [
      "Lock-free channel multiplexing over active subscriber TCP connections",
      "Zero serialization overhead for low-latency broadcast dispatching",
      "Powers real-time alerts, hospital emergency dispatch, and live cache sync"
    ],
    codeSnippet: `func (b *Broker) Publish(channel, message string) int {
    b.mu.RLock()
    defer b.mu.RUnlock()
    subs := b.channels[channel]
    for _, client := range subs {
        client.Send("MESSAGE " + channel + " " + message)
    }
    return len(subs)
}`,
    telemetry: [
      { label: "CHANNELS", val: "38 Active" },
      { label: "FAN-OUT SPEED", val: "< 0.05 ms" },
      { label: "OVERHEAD", val: "Zero Disk I/O" }
    ]
  }
};

export function ThreeArchitectureVisualizer() {
  const containerRef = useRef<HTMLDivElement>(null);
  const [activeFlow, setActiveFlow] = useState<FlowType>("write");
  const [selectedNodeId, setSelectedNodeId] = useState<string>("memtable");
  const [hoveredNodeId, setHoveredNodeId] = useState<string | null>(null);
  const [isAutoOrbit, setIsAutoOrbit] = useState<boolean>(true);
  const [screenCoords, setScreenCoords] = useState<Record<string, { x: number; y: number; visible: boolean }>>({});
  const [pulseTrigger, setPulseTrigger] = useState<number>(0);

  const activeFlowRef = useRef(activeFlow);
  useEffect(() => { activeFlowRef.current = activeFlow; }, [activeFlow]);

  const isAutoOrbitRef = useRef(isAutoOrbit);
  useEffect(() => { isAutoOrbitRef.current = isAutoOrbit; }, [isAutoOrbit]);

  const selectedNode = ARCHITECTURE_NODES[selectedNodeId] || ARCHITECTURE_NODES.memtable;

  // Flow explanations
  const flowDescriptions: Record<FlowType, { title: string; subtitle: string; path: string; color: string }> = {
    write: {
      title: "WRITE INGESTION PIPELINE",
      subtitle: "Synchronous binary WAL persistence + Concurrent SkipList MemTable RAM insertion",
      path: "TCP Client  ──►  WAL Append (Disk)  &  SkipList MemTable (RAM O(log N))",
      color: "#f59e0b"
    },
    read: {
      title: "READ HIERARCHY PIPELINE",
      subtitle: "Instant RAM cache hit or Bloom Filter bitset check before touching disk",
      path: "TCP Client  ──►  MemTable (Hit)  │ (Miss) ──►  Bloom Filter (98% Skip)  ──►  SSTable",
      color: "#06b6d4"
    },
    compact: {
      title: "FLUSH & COMPACTION LIFECYCLE",
      subtitle: "MemTable freezes into immutable SSTable; K-Way compactor cleans tombstones",
      path: "MemTable Full  ──►  Freeze & Flush SSTable  ──►  K-Way Merge & Purge",
      color: "#a855f7"
    },
    pubsub: {
      title: "REAL-TIME STREAMING PUB/SUB",
      subtitle: "Multiplexed non-blocking TCP socket broadcast to active subscriber streams",
      path: "Publisher TCP  ──►  Engine Broker  ──►  Subscribers 1, 2, 3... (Instant Broadcast)",
      color: "#eab308"
    }
  };

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    // Scene setup
    const scene = new THREE.Scene();
    scene.fog = new THREE.FogExp2(0x09090b, 0.025);

    const width = container.clientWidth || 960;
    const height = container.clientHeight || 600;

    const camera = new THREE.PerspectiveCamera(42, width / height, 0.1, 1000);
    // Camera coordinates for crisp isometric projection
    const targetCameraPos = new THREE.Vector3(14, 11, 17);
    camera.position.copy(targetCameraPos);
    camera.lookAt(0, 0.5, 0);

    const renderer = new THREE.WebGLRenderer({
      antialias: false,
      alpha: false,
      powerPreference: "low-power"
    });
    renderer.setClearColor(0x09090b, 1);
    renderer.setSize(width, height);
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 1.25));
    renderer.toneMapping = THREE.ACESFilmicToneMapping;
    renderer.toneMappingExposure = 1.1;
    container.appendChild(renderer.domElement);

    // -----------------------------------------------------------------
    // High-Tech Architectural Floor Grid & Coordinate Rings
    // -----------------------------------------------------------------
    const gridHelper = new THREE.GridHelper(32, 32, 0x27273a, 0x14141e);
    gridHelper.position.y = -3.5;
    scene.add(gridHelper);

    // Concentric Radar Coordinate Circles on floor
    [6, 12, 18].forEach((radius) => {
      const ringGeom = new THREE.RingGeometry(radius - 0.03, radius + 0.03, 64);
      const ringMat = new THREE.MeshBasicMaterial({
        color: 0x1e1e2d,
        side: THREE.DoubleSide,
        transparent: true,
        opacity: 0.6
      });
      const ringMesh = new THREE.Mesh(ringGeom, ringMat);
      ringMesh.rotation.x = Math.PI / 2;
      ringMesh.position.y = -3.49;
      scene.add(ringMesh);
    });

    // -----------------------------------------------------------------
    // Atmospheric & Architectural Lighting
    // -----------------------------------------------------------------
    const ambientLight = new THREE.AmbientLight(0xffffff, 0.7);
    scene.add(ambientLight);

    const mainLight = new THREE.DirectionalLight(0xffffff, 1.4);
    mainLight.position.set(12, 22, 16);
    scene.add(mainLight);

    const blueBacklight = new THREE.DirectionalLight(0x38bdf8, 0.8);
    blueBacklight.position.set(-16, 12, -14);
    scene.add(blueBacklight);

    const goldFill = new THREE.PointLight(0xf59e0b, 1.8, 25);
    goldFill.position.set(0, 4, 0);
    scene.add(goldFill);

    // -----------------------------------------------------------------
    // 3D Architectural Component Assemblies
    // -----------------------------------------------------------------
    const interactiveMeshes: THREE.Object3D[] = [];
    const nodeGroups: Record<string, THREE.Group> = {};

    // Helper: Create glassmorphic obsidian box with glowing neon edges
    const createObsidianNode = (
      w: number,
      h: number,
      d: number,
      neonHex: number,
      nodeId: string,
      opacity = 0.85
    ) => {
      const group = new THREE.Group();
      const geom = new THREE.BoxGeometry(w, h, d);

      // Glass core
      const mat = new THREE.MeshStandardMaterial({
        color: 0x121218,
        roughness: 0.15,
        metalness: 0.3,
        transparent: true,
        opacity: opacity,
        
        
      });
      const mesh = new THREE.Mesh(geom, mat);
      mesh.userData = { nodeId };
      group.add(mesh);
      interactiveMeshes.push(mesh);

      // Glowing Wireframe Edges
      const edges = new THREE.EdgesGeometry(geom);
      const edgeMat = new THREE.LineBasicMaterial({
        color: neonHex,
        linewidth: 2,
        transparent: true,
        opacity: 0.95
      });
      const edgeLine = new THREE.LineSegments(edges, edgeMat);
      group.add(edgeLine);

      // Bottom glowing platform glow ring
      const platGeom = new THREE.PlaneGeometry(w * 1.3, d * 1.3);
      const platMat = new THREE.MeshBasicMaterial({
        color: neonHex,
        transparent: true,
        opacity: 0.12,
        side: THREE.DoubleSide
      });
      const platMesh = new THREE.Mesh(platGeom, platMat);
      platMesh.rotation.x = Math.PI / 2;
      platMesh.position.y = -h / 2 - 0.05;
      group.add(platMesh);

      return { group, mesh, edgeLine };
    };

    // 1. Client Gateway (01)
    const clientNode = createObsidianNode(2.2, 2.2, 2.2, 0x38bdf8, "client");
    clientNode.group.position.set(-7, 1.8, 0);
    scene.add(clientNode.group);
    nodeGroups["client"] = clientNode.group;

    // Glowing core crystal inside client
    const clientCore = new THREE.Mesh(
      new THREE.OctahedronGeometry(0.7, 0),
      new THREE.MeshStandardMaterial({
        color: 0x38bdf8,
        emissive: 0x38bdf8,
        emissiveIntensity: 0.8,
        roughness: 0.2
      })
    );
    clientNode.group.add(clientCore);

    // 2. WAL Log Cylinder (02)
    const walGroup = new THREE.Group();
    walGroup.position.set(-2.5, 3.8, -3.5);
    {
      const cylGeom = new THREE.CylinderGeometry(1.3, 1.3, 3.2, 32);
      const cylMat = new THREE.MeshStandardMaterial({
        color: 0x16120e,
        roughness: 0.2,
        metalness: 0.3,
        transparent: true,
        opacity: 0.85,

      });
      const cylMesh = new THREE.Mesh(cylGeom, cylMat);
      cylMesh.userData = { nodeId: "wal" };
      walGroup.add(cylMesh);
      interactiveMeshes.push(cylMesh);

      const cylEdges = new THREE.EdgesGeometry(cylGeom);
      const cylLines = new THREE.LineSegments(
        cylEdges,
        new THREE.LineBasicMaterial({ color: 0xf59e0b, transparent: true, opacity: 0.95 })
      );
      walGroup.add(cylLines);

      // Rotating Amber Record Rings
      for (let i = -1.1; i <= 1.1; i += 0.55) {
        const ring = new THREE.Mesh(
          new THREE.TorusGeometry(1.32, 0.03, 8, 32),
          new THREE.MeshBasicMaterial({ color: 0xf59e0b })
        );
        ring.rotation.x = Math.PI / 2;
        ring.position.y = i;
        walGroup.add(ring);
      }
    }
    scene.add(walGroup);
    nodeGroups["wal"] = walGroup;

    // 3. SkipList MemTable (03) - 3 Stepped Holographic Emerald Slabs
    const memtableGroup = new THREE.Group();
    memtableGroup.position.set(0, 2.2, 1.2);
    {
      const sizes = [4.2, 3.1, 2.0];
      const yPos = [-0.65, 0.15, 0.95];

      sizes.forEach((s, idx) => {
        const { group: slab, mesh } = createObsidianNode(s, 0.3, s, 0x10b981, "memtable", 0.75);
        slab.position.y = yPos[idx];
        memtableGroup.add(slab);
      });

      // SkipList Vertical Pointer Pillars
      [-0.9, 0.9].forEach((px) => {
        [-0.9, 0.9].forEach((pz) => {
          const pillar = new THREE.Mesh(
            new THREE.CylinderGeometry(0.04, 0.04, 1.8, 12),
            new THREE.MeshBasicMaterial({ color: 0x10b981, transparent: true, opacity: 0.8 })
          );
          pillar.position.set(px, 0.15, pz);
          memtableGroup.add(pillar);
        });
      });
    }
    scene.add(memtableGroup);
    nodeGroups["memtable"] = memtableGroup;

    // 4. Bloom Filter Matrix (04) - Luminous BitSet Grid
    const bloomGroup = new THREE.Group();
    bloomGroup.position.set(4.2, 0.8, -3.2);
    {
      const { group: base } = createObsidianNode(3.6, 0.25, 3.6, 0x06b6d4, "bloom", 0.95);
      bloomGroup.add(base);

      // Embedded 5x5 Glowing Cyan Bit Matrix
      for (let x = -1.4; x <= 1.4; x += 0.7) {
        for (let z = -1.4; z <= 1.4; z += 0.7) {
          const isLit = (Math.abs(x * 10) + Math.abs(z * 10)) % 3 !== 0;
          const bitMesh = new THREE.Mesh(
            new THREE.BoxGeometry(0.2, 0.35, 0.2),
            new THREE.MeshStandardMaterial({
              color: isLit ? 0x06b6d4 : 0x1f2937,
              emissive: isLit ? 0x06b6d4 : 0x000000,
              emissiveIntensity: isLit ? 0.9 : 0
            })
          );
          bitMesh.position.set(x, 0.18, z);
          bitMesh.userData = { nodeId: "bloom" };
          bloomGroup.add(bitMesh);
          interactiveMeshes.push(bitMesh);
        }
      }
    }
    scene.add(bloomGroup);
    nodeGroups["bloom"] = bloomGroup;

    // 5. SSTables Monoliths (05) - Stacked Obsidian/Titanium Storage Tablets
    const sstableGroup = new THREE.Group();
    sstableGroup.position.set(6.5, -1.2, 2);
    {
      for (let i = 0; i < 3; i++) {
        const { group: tab } = createObsidianNode(3.5, 0.75, 3.5, 0xa855f7, "sstable", 0.9);
        tab.position.y = i * 0.95;
        sstableGroup.add(tab);
      }
    }
    scene.add(sstableGroup);
    nodeGroups["sstable"] = sstableGroup;

    // 6. K-Way Compactor (06) - Neon Turbine
    const compactorGroup = new THREE.Group();
    compactorGroup.position.set(1.8, -2.4, 4.2);
    {
      const { group: core } = createObsidianNode(2.4, 1.4, 2.4, 0xec4899, "compactor", 0.9);
      compactorGroup.add(core);

      // Dual Intersecting Glowing Gyro Rings
      const r1 = new THREE.Mesh(
        new THREE.TorusGeometry(1.6, 0.04, 12, 32),
        new THREE.MeshBasicMaterial({ color: 0xec4899 })
      );
      r1.rotation.x = Math.PI / 3;
      compactorGroup.add(r1);

      const r2 = new THREE.Mesh(
        new THREE.TorusGeometry(1.6, 0.04, 12, 32),
        new THREE.MeshBasicMaterial({ color: 0xa855f7 })
      );
      r2.rotation.y = Math.PI / 3;
      compactorGroup.add(r2);
    }
    scene.add(compactorGroup);
    nodeGroups["compactor"] = compactorGroup;

    // 7. Pub/Sub Broker Hub (07) - Gold Beacon with Satellites
    const pubsubGroup = new THREE.Group();
    pubsubGroup.position.set(-4.2, -1.6, 3.2);
    {
      const { group: hub } = createObsidianNode(2.0, 1.4, 2.0, 0xeab308, "pubsub", 0.95);
      pubsubGroup.add(hub);

      // 4 Floating Satellites
      for (let i = 0; i < 4; i++) {
        const angle = (i * Math.PI) / 2;
        const sat = new THREE.Mesh(
          new THREE.SphereGeometry(0.28, 16, 16),
          new THREE.MeshStandardMaterial({
            color: 0xeab308,
            emissive: 0xeab308,
            emissiveIntensity: 0.6
          })
        );
        sat.position.set(Math.cos(angle) * 2.2, 0, Math.sin(angle) * 2.2);
        sat.userData = { nodeId: "pubsub" };
        pubsubGroup.add(sat);
        interactiveMeshes.push(sat);

        const beamGeom = new THREE.BufferGeometry().setFromPoints([
          new THREE.Vector3(0, 0, 0),
          new THREE.Vector3(Math.cos(angle) * 2.2, 0, Math.sin(angle) * 2.2)
        ]);
        const beam = new THREE.Line(
          beamGeom,
          new THREE.LineBasicMaterial({ color: 0xeab308, transparent: true, opacity: 0.45 })
        );
        pubsubGroup.add(beam);
      }
    }
    scene.add(pubsubGroup);
    nodeGroups["pubsub"] = pubsubGroup;

    // -----------------------------------------------------------------
    // High-Tech Luminous Data Conduits (Catmull-Rom Arches)
    // -----------------------------------------------------------------
    interface FlowConduit {
      flow: FlowType;
      curve: THREE.CatmullRomCurve3;
      tubeMesh: THREE.Mesh;
      colorHex: number;
    }

    const conduits: FlowConduit[] = [];

    const makeConduit = (
      p1: [number, number, number],
      p2: [number, number, number],
      colorHex: number,
      flow: FlowType,
      midYBonus = 1.4
    ) => {
      const v1 = new THREE.Vector3(...p1);
      const v2 = new THREE.Vector3(...p2);
      const mid = new THREE.Vector3().addVectors(v1, v2).multiplyScalar(0.5);
      mid.y += midYBonus;

      const curve = new THREE.CatmullRomCurve3([v1, mid, v2]);
      const tubeGeom = new THREE.TubeGeometry(curve, 40, 0.07, 8, false);
      const tubeMat = new THREE.MeshStandardMaterial({
        color: colorHex,
        emissive: colorHex,
        emissiveIntensity: 0.25,
        roughness: 0.3,
        transparent: true,
        opacity: 0.35
      });
      const tubeMesh = new THREE.Mesh(tubeGeom, tubeMat);
      scene.add(tubeMesh);

      conduits.push({ flow, curve, tubeMesh, colorHex });
    };

    // Build Conduits
    makeConduit([-7, 1.8, 0], [-2.5, 3.8, -3.5], 0xf59e0b, "write", 1.8); // Client -> WAL
    makeConduit([-7, 1.8, 0], [0, 2.2, 1.2], 0x10b981, "write", 1.0);     // Client -> MemTable
    makeConduit([-7, 1.8, 0], [0, 2.2, 1.2], 0x10b981, "read", 1.0);      // Client -> MemTable
    makeConduit([0, 2.2, 1.2], [4.2, 0.8, -3.2], 0x06b6d4, "read", 1.2);   // MemTable -> Bloom
    makeConduit([4.2, 0.8, -3.2], [6.5, 0.5, 2], 0xa855f7, "read", 1.0);  // Bloom -> SSTable
    makeConduit([0, 2.2, 1.2], [6.5, 1.0, 2], 0xa855f7, "compact", 1.6);  // MemTable -> Flush SSTable
    makeConduit([6.5, -0.5, 2], [1.8, -2.4, 4.2], 0xec4899, "compact", 0.9); // SSTable -> Compactor
    makeConduit([-7, 1.8, 0], [-4.2, -1.6, 3.2], 0xeab308, "pubsub", 1.1);  // Client -> PubSub

    // -----------------------------------------------------------------
    // Animated Glowing Laser Energy Pulses (Data Packets)
    // -----------------------------------------------------------------
    const packetsPerConduit = 3;
    const packetMeshes: {
      mesh: THREE.Mesh;
      conduit: FlowConduit;
      t: number;
      speed: number;
    }[] = [];

    const packetGeom = new THREE.SphereGeometry(0.22, 16, 16);

    conduits.forEach((c) => {
      for (let i = 0; i < packetsPerConduit; i++) {
        const pMat = new THREE.MeshStandardMaterial({
          color: c.colorHex,
          emissive: c.colorHex,
          emissiveIntensity: 1.5,
          roughness: 0.1
        });
        const mesh = new THREE.Mesh(packetGeom, pMat);
        scene.add(mesh);
        packetMeshes.push({
          mesh,
          conduit: c,
          t: (i / packetsPerConduit) + Math.random() * 0.1,
          speed: 0.007 + Math.random() * 0.004
        });
      }
    });

    // -----------------------------------------------------------------
    // Interactive Raycaster, Mouse Drag Orbit & Node Click
    // -----------------------------------------------------------------
    const raycaster = new THREE.Raycaster();
    const mouse = new THREE.Vector2();

    let isMouseDown = false;
    let prevMouseX = 0;
    let prevMouseY = 0;
    let spherical = { radius: 25, theta: 0.82, phi: 1.05 };

    const updateCamera = () => {
      camera.position.x = spherical.radius * Math.sin(spherical.phi) * Math.sin(spherical.theta);
      camera.position.y = spherical.radius * Math.cos(spherical.phi);
      camera.position.z = spherical.radius * Math.sin(spherical.phi) * Math.cos(spherical.theta);
      camera.lookAt(0, 0.4, 0);
    };
    updateCamera();

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
        setIsAutoOrbit(false);
        const deltaX = e.clientX - prevMouseX;
        const deltaY = e.clientY - prevMouseY;
        prevMouseX = e.clientX;
        prevMouseY = e.clientY;

        spherical.theta -= deltaX * 0.007;
        spherical.phi = Math.max(0.15, Math.min(Math.PI / 2 - 0.05, spherical.phi - deltaY * 0.007));
        updateCamera();
      } else {
        // Raycast Hover
        raycaster.setFromCamera(mouse, camera);
        const hits = raycaster.intersectObjects(interactiveMeshes, false);
        if (hits.length > 0) {
          const nid = hits[0].object.userData.nodeId;
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
      const hits = raycaster.intersectObjects(interactiveMeshes, false);
      if (hits.length > 0) {
        const nid = hits[0].object.userData.nodeId;
        if (nid && ARCHITECTURE_NODES[nid]) {
          setSelectedNodeId(nid);
        }
      }
    };

    const dom = renderer.domElement;
    dom.addEventListener("pointerdown", onPointerDown);
    window.addEventListener("pointermove", onPointerMove);
    window.addEventListener("pointerup", onPointerUp);
    dom.addEventListener("click", onClick);

    // -----------------------------------------------------------------
    // Animation Loop
    // -----------------------------------------------------------------
    let animationFrameId: number | null = null;
    let clock = new THREE.Clock();
    let frameCount = 0;
    let isVisible = true;

    const animate = () => {
      if (!isVisible) {
        animationFrameId = null;
        return;
      }
      animationFrameId = requestAnimationFrame(animate);
      const delta = clock.getDelta();
      const elapsed = clock.getElapsedTime();

      // Slow cinematic auto-orbit
      if (isAutoOrbitRef.current) {
        spherical.theta += delta * 0.06;
        updateCamera();
      }

      // Levitation and rotation kinetics
      clientCore.rotation.y = elapsed * 1.5;
      clientCore.rotation.x = elapsed * 0.8;
      walGroup.rotation.y = elapsed * 0.5;
      memtableGroup.position.y = 2.2 + Math.sin(elapsed * 1.8) * 0.12;
      compactorGroup.rotation.y = -elapsed * 0.8;
      pubsubGroup.rotation.y = elapsed * 0.35;

      // Update conduits and packet visibility according to active flow
      conduits.forEach((c) => {
        const isCurrent = c.flow === activeFlowRef.current;
        const mat = c.tubeMesh.material as THREE.MeshStandardMaterial;
        mat.opacity = isCurrent ? 0.85 : 0.08;
        mat.emissiveIntensity = isCurrent ? 0.8 : 0.02;
      });

      // Update packet positions along active conduits
      packetMeshes.forEach((pkt) => {
        const isCurrent = pkt.conduit.flow === activeFlowRef.current;
        pkt.mesh.visible = isCurrent;
        if (!isCurrent) return;

        pkt.t = (pkt.t + pkt.speed) % 1;
        const pos = pkt.conduit.curve.getPointAt(pkt.t);
        pkt.mesh.position.copy(pos);
      });

      // Calculate 2D Screen Positions for floating HUD Labels
      const newCoords: Record<string, { x: number; y: number; visible: boolean }> = {};
      const tempVec = new THREE.Vector3();

      Object.entries(ARCHITECTURE_NODES).forEach(([id, n]) => {
        const group = nodeGroups[id];
        if (group) {
          group.getWorldPosition(tempVec);
          tempVec.y += 1.8; // Offset above component
          tempVec.project(camera);

          const isBehind = tempVec.z > 1;
          const screenX = ((tempVec.x + 1) * width) / 2;
          const screenY = ((-tempVec.y + 1) * height) / 2;

          newCoords[id] = {
            x: screenX,
            y: screenY,
            visible: !isBehind && screenX > 20 && screenX < width - 20 && screenY > 20 && screenY < height - 20
          };
        }
      });
      frameCount++;
      if (frameCount % 4 === 0) {
        setScreenCoords(newCoords);
      }

      renderer.render(scene, camera);
    };

    animate();

    const observer = new IntersectionObserver((entries) => {
      entries.forEach((e) => {
        isVisible = e.isIntersecting;
        if (isVisible && !animationFrameId) {
          animate();
        }
      });
    }, { threshold: 0.05 });
    observer.observe(container);

    const handleResize = () => {
      if (!container) return;
      const nw = container.clientWidth;
      const nh = container.clientHeight || 600;
      camera.aspect = nw / nh;
      camera.updateProjectionMatrix();
      renderer.setSize(nw, nh);
    };
    window.addEventListener("resize", handleResize);

    return () => {
      observer.disconnect();
      if (animationFrameId) cancelAnimationFrame(animationFrameId);
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
  }, []);

  return (
    <div className="w-full flex flex-col items-center select-none">
      {/* Visualizer Obsidian Frame */}
      <div className="w-full max-w-7xl bg-[#09090b] rounded-[16px] border border-[#27273a] shadow-2xl p-4 sm:p-7 overflow-hidden text-white">
        {/* Top Control Bar: Active Telemetry HUD & Pipeline Switcher */}
        <div className="flex flex-col lg:flex-row items-start lg:items-center justify-between gap-4 pb-5 border-b border-[#1f1f2e]">
          {/* Sider Live Engine Status */}
          <div className="flex items-center gap-3">
            <div className="w-2.5 h-2.5 rounded-full bg-[#10b981] animate-pulse" />
            <div className="font-mono text-[12px] tracking-wider text-[#9ca3af]">
              ENGINE STATUS: <span className="text-[#10b981] font-semibold">LSM ONLINE</span>
            </div>
            <div className="hidden sm:flex items-center gap-2 border-l border-[#27273a] pl-3 font-mono text-[11px] text-[#6b7280]">
              <span>THROUGHPUT: <strong className="text-white">184,200 OPS/S</strong></span>
              <span>&bull;</span>
              <span>AVG LATENCY: <strong className="text-[#38bdf8]">0.18 MS</strong></span>
            </div>
          </div>

          {/* Pipeline Flow Tabs */}
          <div className="flex flex-wrap items-center gap-2">
            {(["write", "read", "compact", "pubsub"] as const).map((flow) => {
              const active = activeFlow === flow;
              return (
                <button
                  key={flow}
                  onClick={() => {
                    setActiveFlow(flow);
                    setPulseTrigger((p) => p + 1);
                  }}
                  className={`px-3 py-1.5 rounded-[6px] text-[11px] font-mono tracking-wider uppercase transition-all border ${
                    active
                      ? "bg-white text-black font-semibold border-white shadow-[0_0_15px_rgba(255,255,255,0.2)]"
                      : "bg-[#14141e] text-[#9ca3af] border-[#27273a] hover:text-white hover:border-[#4b5563]"
                  }`}
                >
                  {flow === "write" && "⚡ Write Pipeline (PUT)"}
                  {flow === "read" && "🔍 Read Hierarchy (GET)"}
                  {flow === "compact" && "🗜️ Compaction Engine"}
                  {flow === "pubsub" && "📡 Pub/Sub Broadcast"}
                </button>
              );
            })}
          </div>

          {/* Camera Orbit Toggle */}
          <button
            onClick={() => setIsAutoOrbit((prev) => !prev)}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-[6px] bg-[#14141e] border border-[#27273a] text-[11px] font-mono text-[#9ca3af] hover:text-white hover:border-[#4b5563] transition-colors"
          >
            <span>{isAutoOrbit ? "⏸ Pause Orbit" : "▶ Resume Auto-Orbit"}</span>
          </button>
        </div>

        {/* Active Flow Pipeline Banner */}
        <div className="my-4 px-4 py-3 bg-[#111118] border border-[#1f1f2e] rounded-[8px] flex flex-col md:flex-row md:items-center justify-between gap-2 font-mono">
          <div className="flex items-center gap-2">
            <span
              className="text-[12px] font-bold tracking-wider"
              style={{ color: flowDescriptions[activeFlow].color }}
            >
              {flowDescriptions[activeFlow].title}
            </span>
            <span className="text-[12px] text-[#6b7280] hidden sm:inline">&bull;</span>
            <span className="text-[12px] text-[#9ca3af]">
              {flowDescriptions[activeFlow].subtitle}
            </span>
          </div>
          <div className="text-[11px] text-white/90 bg-[#181824] px-2.5 py-1 rounded-[4px] border border-[#27273a] self-start md:self-auto">
            {flowDescriptions[activeFlow].path}
          </div>
        </div>

        {/* 3D Canvas Stage & Inspector Grid */}
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-6 items-stretch">
          {/* Main 3D Viewport (8 Columns) */}
          <div className="lg:col-span-8 relative w-full h-[480px] sm:h-[580px] bg-[#0c0c12] rounded-[12px] border border-[#1f1f2e] overflow-hidden">
            {/* Viewport Header Controls */}
            <div className="absolute top-4 left-4 z-10 flex items-center gap-2 pointer-events-none">
              <span className="px-2 py-0.5 rounded-[4px] bg-[#14141e]/90 border border-[#27273a] font-mono text-[10px] text-[#9ca3af]">
                3D LSM ISOMETRIC SCENE &bull; DRAG TO ORBIT &bull; CLICK NODES
              </span>
            </div>

            {/* Floating 3D HUD Node Tags */}
            {Object.entries(screenCoords).map(([id, coord]) => {
              if (!coord.visible) return null;
              const node = ARCHITECTURE_NODES[id];
              if (!node) return null;
              const isSelected = selectedNodeId === id;
              const isHovered = hoveredNodeId === id;

              return (
                <div
                  key={id}
                  onClick={() => setSelectedNodeId(id)}
                  style={{
                    transform: `translate(${coord.x}px, ${coord.y}px) translate(-50%, -50%)`,
                    borderColor: isSelected ? node.accentColor : isHovered ? "#ffffff" : "#27273a"
                  }}
                  className={`absolute z-20 cursor-pointer pointer-events-auto px-2.5 py-1 rounded-[6px] backdrop-blur-md transition-all font-mono text-[11px] flex items-center gap-1.5 shadow-lg ${
                    isSelected
                      ? "bg-black/90 scale-110 shadow-[0_0_15px_rgba(255,255,255,0.15)] ring-1"
                      : "bg-[#09090b]/80 hover:scale-105"
                  }`}
                >
                  <span
                    className="w-1.5 h-1.5 rounded-full"
                    style={{ backgroundColor: node.accentColor }}
                  />
                  <span className="text-[#6b7280] font-semibold">{node.tag}</span>
                  <span className="text-white font-medium">{node.name.split(" ")[0]}</span>
                </div>
              );
            })}

            {/* Three.js Canvas Mount */}
            <div ref={containerRef} className="w-full h-full cursor-grab active:cursor-grabbing" />
          </div>

          {/* Deep-Dive Component Inspector Panel (4 Columns) */}
          <div className="lg:col-span-4 bg-[#0e0e16] border border-[#1f1f2e] rounded-[12px] p-5 sm:p-6 flex flex-col justify-between min-h-[480px] sm:min-h-[580px]">
            <div>
              {/* Header Badges */}
              <div className="flex items-center justify-between mb-3 font-mono text-[10px] uppercase tracking-wider">
                <span className="text-[#9ca3af]">{selectedNode.tier}</span>
                <span
                  className="px-2 py-0.5 rounded-[4px] font-semibold"
                  style={{
                    backgroundColor: `${selectedNode.accentColor}20`,
                    color: selectedNode.accentColor,
                    border: `1px solid ${selectedNode.accentColor}50`
                  }}
                >
                  {selectedNode.badge}
                </span>
              </div>

              {/* Title & Complexity */}
              <h3
                className="text-[28px] sm:text-[32px] font-normal text-white leading-tight mb-1"
                style={{ fontFamily: "var(--font-davinci)" }}
              >
                {selectedNode.name}
              </h3>
              <div className="font-mono text-[11px] mb-4" style={{ color: selectedNode.accentColor }}>
                Complexity: {selectedNode.complexity}
              </div>

              {/* Summary Description */}
              <p
                className="text-[13px] sm:text-[14px] leading-relaxed text-[#9ca3af] mb-5"
                style={{ fontFamily: "var(--font-helvetica-now)" }}
              >
                {selectedNode.summary}
              </p>

              {/* Architectural Technical Points */}
              <div className="space-y-2.5 mb-6">
                {selectedNode.details.map((point, idx) => (
                  <div key={idx} className="flex items-start gap-2.5 text-[12px] text-white/90">
                    <span
                      className="mt-1 w-1.5 h-1.5 rounded-full shrink-0"
                      style={{ backgroundColor: selectedNode.accentColor }}
                    />
                    <span style={{ fontFamily: "var(--font-helvetica-now)" }}>{point}</span>
                  </div>
                ))}
              </div>

              {/* Live Telemetry Metrics Matrix */}
              <div className="grid grid-cols-3 gap-2 p-3 bg-[#14141e] border border-[#27273a] rounded-[8px] mb-5">
                {selectedNode.telemetry.map((t, idx) => (
                  <div key={idx} className="flex flex-col">
                    <span className="text-[9px] font-mono uppercase tracking-tight text-[#6b7280]">
                      {t.label}
                    </span>
                    <span className="text-[11px] font-mono font-semibold text-white mt-0.5">
                      {t.val}
                    </span>
                  </div>
                ))}
              </div>
            </div>

            {/* Go Implementation Snippet */}
            <div className="pt-3 border-t border-[#1f1f2e]">
              <div className="flex items-center justify-between mb-2">
                <span className="text-[10px] font-mono uppercase tracking-wider text-[#6b7280]">
                  Core Engine Go Implementation:
                </span>
                <span className="text-[10px] font-mono text-[#38bdf8]">Zero Dependencies</span>
              </div>
              <pre className="bg-[#050508] text-[#e2e8f0] p-3 rounded-[6px] text-[11px] font-mono overflow-x-auto leading-relaxed border border-[#1f1f2e]">
                <code>{selectedNode.codeSnippet}</code>
              </pre>
            </div>
          </div>
        </div>

        {/* Quick Node Navigation Ribbon */}
        <div className="mt-5 pt-4 border-t border-[#1f1f2e] flex flex-wrap items-center justify-between gap-2">
          <div className="flex items-center gap-2 font-mono text-[11px] text-[#6b7280]">
            <span>SELECT ARCHITECTURAL LAYER:</span>
          </div>
          <div className="flex flex-wrap items-center gap-1.5">
            {Object.values(ARCHITECTURE_NODES).map((node) => {
              const active = selectedNodeId === node.id;
              return (
                <button
                  key={node.id}
                  onClick={() => setSelectedNodeId(node.id)}
                  className={`px-3 py-1 rounded-[5px] text-[11px] font-mono transition-all border ${
                    active
                      ? "bg-white text-black font-semibold border-white"
                      : "bg-[#14141e] text-[#9ca3af] border-[#27273a] hover:text-white hover:border-[#4b5563]"
                  }`}
                >
                  <span className="text-[#6b7280] mr-1">{node.tag}</span>
                  {node.name.split(" ")[0]}
                </button>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}

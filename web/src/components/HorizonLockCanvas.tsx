"use client";

import React, { useEffect, useRef } from "react";
import * as THREE from "three";

export function HorizonLockCanvas() {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    let width = container.clientWidth;
    let height = container.clientHeight;

    const scene = new THREE.Scene();
    scene.fog = new THREE.FogExp2(0x0e0e0e, 0.045);

    const camera = new THREE.PerspectiveCamera(55, width / height, 0.1, 100);
    // Camera positioned slightly elevated, looking directly forward towards the horizon
    camera.position.set(0, 0.85, 3.5);
    camera.lookAt(0, 0.85, -20);

    const renderer = new THREE.WebGLRenderer({
      alpha: true,
      antialias: true,
      powerPreference: "high-performance",
    });
    renderer.setSize(width, height);
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    container.appendChild(renderer.domElement);

    // --- INFINITE HORIZON GRID (Bottom Plane) ---
    const gridWidth = 40;
    const gridDepth = 60;
    const stepX = 0.8;
    const stepZ = 0.8;

    const linePoints: THREE.Vector3[] = [];

    // Longitudinal lines (running toward the horizon)
    for (let x = -gridWidth / 2; x <= gridWidth / 2; x += stepX) {
      linePoints.push(new THREE.Vector3(x, 0, 5));
      linePoints.push(new THREE.Vector3(x, 0, -gridDepth));
    }

    // Transverse lines (perpendicular to direction of motion)
    for (let z = 5; z >= -gridDepth; z -= stepZ) {
      linePoints.push(new THREE.Vector3(-gridWidth / 2, 0, z));
      linePoints.push(new THREE.Vector3(gridWidth / 2, 0, z));
    }

    const gridGeo = new THREE.BufferGeometry().setFromPoints(linePoints);
    const gridMat = new THREE.LineBasicMaterial({
      color: 0x405bff,
      transparent: true,
      opacity: 0.18,
      blending: THREE.AdditiveBlending,
    });
    const gridMesh = new THREE.LineSegments(gridGeo, gridMat);
    scene.add(gridMesh);

    // --- SECOND SUBTLE TOP CEILING GRID (Encapsulating cockpit feeling) ---
    const topGridPoints: THREE.Vector3[] = [];
    for (let x = -gridWidth / 2; x <= gridWidth / 2; x += stepX * 2) {
      topGridPoints.push(new THREE.Vector3(x, 4.0, 5));
      topGridPoints.push(new THREE.Vector3(x, 4.0, -gridDepth));
    }
    for (let z = 5; z >= -gridDepth; z -= stepZ * 2) {
      topGridPoints.push(new THREE.Vector3(-gridWidth / 2, 4.0, z));
      topGridPoints.push(new THREE.Vector3(gridWidth / 2, 4.0, z));
    }
    const topGridGeo = new THREE.BufferGeometry().setFromPoints(topGridPoints);
    const topGridMat = new THREE.LineBasicMaterial({
      color: 0x7084ff,
      transparent: true,
      opacity: 0.07,
      blending: THREE.AdditiveBlending,
    });
    const topGridMesh = new THREE.LineSegments(topGridGeo, topGridMat);
    scene.add(topGridMesh);

    // --- HORIZON LASER GUIDE (Center Vanishing Line) ---
    const centerGuideGeo = new THREE.BufferGeometry().setFromPoints([
      new THREE.Vector3(0, 0.01, 5),
      new THREE.Vector3(0, 0.01, -gridDepth),
    ]);
    const centerGuideMat = new THREE.LineBasicMaterial({
      color: 0x7084ff,
      transparent: true,
      opacity: 0.7,
      blending: THREE.AdditiveBlending,
    });
    const centerGuide = new THREE.Line(centerGuideGeo, centerGuideMat);
    scene.add(centerGuide);

    // --- HORIZON BEAM (Glowing Bar at the vanishing plane) ---
    const horizonBeamGeo = new THREE.BufferGeometry().setFromPoints([
      new THREE.Vector3(-gridWidth / 2, 0.02, -22),
      new THREE.Vector3(gridWidth / 2, 0.02, -22),
    ]);
    const horizonBeamMat = new THREE.LineBasicMaterial({
      color: 0x405bff,
      transparent: true,
      opacity: 0.85,
      blending: THREE.AdditiveBlending,
    });
    const horizonBeam = new THREE.Line(horizonBeamGeo, horizonBeamMat);
    scene.add(horizonBeam);

    // --- SLEEK FLOATING DATA BEACONS (Representing live LSM SkipList packets) ---
    const beaconCount = 40;
    const beaconGeo = new THREE.SphereGeometry(0.035, 6, 6);
    const beaconMat = new THREE.MeshBasicMaterial({
      color: 0x7084ff,
      transparent: true,
      opacity: 0.75,
      blending: THREE.AdditiveBlending,
    });
    const beacons = new THREE.InstancedMesh(beaconGeo, beaconMat, beaconCount);
    const dummy = new THREE.Object3D();
    const beaconCoords: { x: number; y: number; z: number; speed: number }[] = [];

    for (let i = 0; i < beaconCount; i++) {
      const x = (Math.random() - 0.5) * 14;
      const y = 0.05 + Math.random() * 1.5;
      const z = -Math.random() * 25;
      const speed = 0.04 + Math.random() * 0.08;
      beaconCoords.push({ x, y, z, speed });
      dummy.position.set(x, y, z);
      dummy.updateMatrix();
      beacons.setMatrixAt(i, dummy.matrix);
    }
    beacons.instanceMatrix.needsUpdate = true;
    scene.add(beacons);

    // --- GYROSCOPIC HORIZON LOCK & SCROLL COUPLING ---
    let scrollY = 0;
    let targetScrollY = 0;
    let mouseX = 0;
    let targetMouseX = 0;
    let mouseY = 0;
    let targetMouseY = 0;

    const handleScroll = () => {
      targetScrollY = window.scrollY;
    };

    const handleMouseMove = (e: MouseEvent) => {
      targetMouseX = (e.clientX / width) * 2 - 1;
      targetMouseY = (e.clientY / height) * 2 - 1;
    };

    window.addEventListener("scroll", handleScroll, { passive: true });
    window.addEventListener("mousemove", handleMouseMove, { passive: true });

    // Animation Loop with true Horizon Stabilization
    let animId: number;
    let clock = new THREE.Clock();

    const animate = () => {
      animId = requestAnimationFrame(animate);
      const delta = clock.getDelta();

      // Smooth inertia damping
      scrollY += (targetScrollY - scrollY) * 0.08;
      mouseX += (targetMouseX - mouseX) * 0.05;
      mouseY += (targetMouseY - mouseY) * 0.05;

      // Horizon Lock Physics:
      // The grid continuously scrolls forward into the distance, accelerated by page scroll
      const forwardVelocity = 1.8 * delta + (targetScrollY - scrollY) * 0.005;
      gridMesh.position.z = (gridMesh.position.z + forwardVelocity) % stepZ;
      topGridMesh.position.z = (topGridMesh.position.z + forwardVelocity) % (stepZ * 2);

      // Gyroscopic Bank Angle (spacecraft / HUD attitude tilt)
      // Camera rolls slightly with mouseX, but the horizon lock maintains stability:
      const rollAngle = -mouseX * 0.04;
      camera.rotation.z = rollAngle;

      // Pitch follows scroll subtly, elevating view as user scrolls down
      const elevation = Math.min(scrollY * 0.0008, 0.4);
      camera.position.y = 0.85 + elevation + mouseY * 0.15;
      camera.position.x = mouseX * 0.4;

      // Vanishing target locks the horizon line strictly at Y = 0.85
      camera.lookAt(mouseX * 0.1, 0.85, -20);

      // Update data beacons
      for (let i = 0; i < beaconCount; i++) {
        const b = beaconCoords[i];
        b.z += b.speed + forwardVelocity * 0.5;
        if (b.z > 4) {
          b.z = -25;
          b.x = (Math.random() - 0.5) * 14;
        }
        dummy.position.set(b.x, b.y, b.z);
        dummy.updateMatrix();
        beacons.setMatrixAt(i, dummy.matrix);
      }
      beacons.instanceMatrix.needsUpdate = true;

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
      window.removeEventListener("scroll", handleScroll);
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("resize", handleResize);
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
      className="absolute inset-0 w-full h-full pointer-events-none overflow-hidden select-none"
    />
  );
}

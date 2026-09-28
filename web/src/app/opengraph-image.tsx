import { ImageResponse } from "next/og";


export const alt = "Sider Cloud — Managed Bare-Metal LSM Storage Engine";
export const size = {
  width: 1200,
  height: 630,
};
export const contentType = "image/png";

export default async function Image() {
  return new ImageResponse(
    (
      <div
        style={{
          width: "100%",
          height: "100%",
          display: "flex",
          flexDirection: "column",
          justifyContent: "space-between",
          padding: "60px 80px",
          backgroundColor: "#09090c",
          backgroundImage:
            "radial-gradient(circle at 80% 20%, rgba(64, 91, 255, 0.25) 0%, transparent 50%), radial-gradient(circle at 20% 80%, rgba(0, 240, 255, 0.15) 0%, transparent 50%)",
          color: "#ffffff",
          fontFamily: "system-ui, sans-serif",
          position: "relative",
        }}
      >
        {/* Subtle grid border overlay */}
        <div
          style={{
            position: "absolute",
            top: 24,
            left: 24,
            right: 24,
            bottom: 24,
            border: "1px solid rgba(255, 255, 255, 0.08)",
            borderRadius: 24,
            pointerEvents: "none",
          }}
        />

        {/* Top Header */}
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
            <div
              style={{
                width: 44,
                height: 44,
                borderRadius: 12,
                backgroundColor: "#405bff",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                boxShadow: "0 0 30px rgba(64, 91, 255, 0.6)",
                fontSize: 24,
                fontWeight: "bold",
              }}
            >
              ⚡
            </div>
            <span style={{ fontSize: 26, fontWeight: 700, letterSpacing: "-0.02em" }}>
              Sider Cloud
            </span>
          </div>

          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 8,
              padding: "8px 18px",
              borderRadius: 30,
              backgroundColor: "rgba(64, 91, 255, 0.15)",
              border: "1px solid rgba(64, 91, 255, 0.4)",
              fontSize: 14,
              color: "#a3b1ff",
              fontFamily: "monospace",
            }}
          >
            <span
              style={{
                width: 8,
                height: 8,
                borderRadius: "50%",
                backgroundColor: "#19a05f",
              }}
            />
            PUBLIC ALPHA LIVE &bull; ind-tbn-1
          </div>
        </div>

        {/* Hero Body */}
        <div style={{ display: "flex", flexDirection: "column", gap: 18, maxWidth: 960 }}>
          <h1
            style={{
              fontSize: 64,
              fontWeight: 800,
              letterSpacing: "-0.03em",
              lineHeight: 1.08,
              margin: 0,
            }}
          >
            Bare-metal speed.
            <br />
            <span
              style={{
                backgroundImage: "linear-gradient(90deg, #405bff, #7084ff, #00f0ff)",
                backgroundClip: "text",
                color: "transparent",
              }}
            >
              Zero data loss LSM persistence.
            </span>
          </h1>

          <p style={{ fontSize: 22, color: "#a7a9ac", margin: 0, lineHeight: 1.4 }}>
            Dedicated SkipList MemTable clusters in seconds. Synchronous NVMe Write-Ahead Log
            durability and real-time operations cockpit.
          </p>
        </div>

        {/* Telemetry Footer */}
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            paddingTop: 24,
            borderTop: "1px solid rgba(255, 255, 255, 0.1)",
          }}
        >
          <div style={{ display: "flex", gap: 32 }}>
            <div style={{ display: "flex", flexDirection: "column" }}>
              <span style={{ fontSize: 12, color: "#6d6e71", fontFamily: "monospace" }}>
                THROUGHPUT
              </span>
              <span style={{ fontSize: 20, fontWeight: 700, color: "#ffffff" }}>
                316,746 ops/s
              </span>
            </div>
            <div style={{ display: "flex", flexDirection: "column" }}>
              <span style={{ fontSize: 12, color: "#6d6e71", fontFamily: "monospace" }}>
                P50 LATENCY
              </span>
              <span style={{ fontSize: 20, fontWeight: 700, color: "#405bff" }}>
                234 µs
              </span>
            </div>
            <div style={{ display: "flex", flexDirection: "column" }}>
              <span style={{ fontSize: 12, color: "#6d6e71", fontFamily: "monospace" }}>
                ENGINE ARCHITECTURE
              </span>
              <span style={{ fontSize: 20, fontWeight: 700, color: "#00f0ff" }}>
                LSM-Tree in Go
              </span>
            </div>
          </div>

          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <span style={{ fontSize: 15, color: "#a7a9ac" }}>Architect:</span>
            <span style={{ fontSize: 16, fontWeight: 700, color: "#ffffff" }}>
              Agnibha Ray
            </span>
          </div>
        </div>
      </div>
    ),
    {
      ...size,
    }
  );
}

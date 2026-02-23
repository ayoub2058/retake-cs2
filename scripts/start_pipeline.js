"use strict";

/**
 * ClipsToCS – Unified Background Pipeline Launcher
 *
 * Starts all three daemon services in parallel:
 *   1. poll_matches.py   – polls Steam API for new match codes
 *   2. replay_downloader.js – downloads demo files via Steam GC
 *   3. parse_match.py    – parses demos + generates AI coaching tips
 *
 * Usage:
 *   node scripts/start_pipeline.js
 *
 * All output is prefixed with the service name and timestamps.
 * Press Ctrl+C to gracefully stop all services.
 */

const { spawn } = require("child_process");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");

const ts = () => new Date().toLocaleTimeString("en-GB", { hour12: false });

const log = (prefix, msg) => {
  console.log(`[${ts()}] [${prefix}] ${msg}`);
};

// Detect Python executable
const pythonCmd =
  process.platform === "win32"
    ? path.join(ROOT, ".venv", "Scripts", "python.exe")
    : "python3";

const services = [
  {
    name: "Poller",
    cmd: pythonCmd,
    args: [path.join(__dirname, "poll_matches.py")],
    color: "\x1b[36m", // cyan
  },
  {
    name: "Downloader",
    cmd: "node",
    args: [path.join(__dirname, "replay_downloader.js")],
    color: "\x1b[33m", // yellow
  },
  {
    name: "Parser",
    cmd: pythonCmd,
    args: [path.join(__dirname, "parse_match.py")],
    color: "\x1b[35m", // magenta
  },
];

const RESET = "\x1b[0m";
const children = [];

const startService = ({ name, cmd, args, color }) => {
  log("Pipeline", `Starting ${name}...`);

  const child = spawn(cmd, args, {
    cwd: ROOT,
    env: { ...process.env, PYTHONUNBUFFERED: "1", PYTHONIOENCODING: "utf-8" },
    stdio: ["ignore", "pipe", "pipe"],
  });

  const prefix = `${color}${name.padEnd(12)}${RESET}`;

  child.stdout.on("data", (data) => {
    String(data)
      .trim()
      .split("\n")
      .forEach((line) => console.log(`${prefix} ${line}`));
  });

  child.stderr.on("data", (data) => {
    String(data)
      .trim()
      .split("\n")
      .forEach((line) => console.error(`${prefix} ${line}`));
  });

  child.on("close", (code) => {
    log("Pipeline", `${name} exited with code ${code}`);
    // Auto-restart after 5s unless we're shutting down
    if (!shuttingDown) {
      log("Pipeline", `Restarting ${name} in 5s...`);
      setTimeout(() => {
        const svc = services.find((s) => s.name === name);
        if (svc && !shuttingDown) {
          const newChild = startService(svc);
          children.push(newChild);
        }
      }, 5000);
    }
  });

  children.push(child);
  return child;
};

let shuttingDown = false;

const shutdown = () => {
  if (shuttingDown) return;
  shuttingDown = true;
  log("Pipeline", "Shutting down all services...");
  children.forEach((child) => {
    try {
      child.kill("SIGTERM");
    } catch {
      // already dead
    }
  });
  setTimeout(() => {
    log("Pipeline", "Force killing remaining processes...");
    children.forEach((child) => {
      try {
        child.kill("SIGKILL");
      } catch {
        // already dead
      }
    });
    process.exit(0);
  }, 5000);
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

// ── Banner ──────────────────────────────────────────
console.log("");
console.log("  ╔══════════════════════════════════════════╗");
console.log("  ║     ClipsToCS  Background Pipeline       ║");
console.log("  ║                                          ║");
console.log("  ║  [1] Match Poller     (poll_matches.py)  ║");
console.log("  ║  [2] Replay Downloader (replay_downloader)║");
console.log("  ║  [3] Demo Parser      (parse_match.py)   ║");
console.log("  ║                                          ║");
console.log("  ║  Press Ctrl+C to stop all services       ║");
console.log("  ╚══════════════════════════════════════════╝");
console.log("");

// Start all services
services.forEach(startService);

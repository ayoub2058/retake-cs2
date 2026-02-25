"use strict";

const fs = require("fs");
const fsp = require("fs/promises");
const path = require("path");
const { pipeline } = require("stream/promises");
const { spawn } = require("child_process");
const SteamUser = require("steam-user");
const SteamTotp = require("steam-totp");
const GlobalOffensive = require("globaloffensive");
const unbzip2 = require("unbzip2-stream");
const { Pool } = require("pg");
const dotenv = require("dotenv");

dotenv.config({ path: ".env.local" });
dotenv.config();

const POLL_INTERVAL_MS = 30_000;
const GC_RECONNECT_DELAY_MS = 15_000;
const MAX_CONCURRENT_DOWNLOADS = Number.parseInt(
  process.env.MAX_CONCURRENT_DOWNLOADS || "3",
  10
);
const MAX_CONCURRENT_PARSES = Number.parseInt(
  process.env.MAX_CONCURRENT_PARSES || "2",
  10
);
const RETRY_COOLDOWN_MS = Number.parseInt(
  process.env.RETRY_COOLDOWN_MS || "300000",
  10
);
const DOWNLOAD_TIMEOUT_MS = Number.parseInt(
  process.env.REPLAY_DOWNLOAD_TIMEOUT_MS || "120000",
  10
);
const APP_ID_CS2 = 730;

const log = (prefix, msg) => {
  const ts = new Date().toLocaleTimeString("en-GB", { hour12: false });
  console.log(`[${ts}] [${prefix}] ${msg}`);
};

const dbUrl =
  process.env.SUPABASE_DB_URL ||
  process.env.DATABASE_URL ||
  process.env.SUPABASE_DATABASE_URL ||
  "";

const steamUser = process.env.STEAM_BOT_USER;
const steamPass = process.env.STEAM_BOT_PASS;
const steamSharedSecret = process.env.STEAM_BOT_SHARED_SECRET;

if (!dbUrl) {
  throw new Error("SUPABASE_DB_URL (or DATABASE_URL) must be set");
}

if (!steamUser || !steamPass || !steamSharedSecret) {
  throw new Error(
    "STEAM_BOT_USER, STEAM_BOT_PASS, and STEAM_BOT_SHARED_SECRET must be set"
  );
}

const pool = new Pool({ connectionString: dbUrl, max: 20 });
const client = new SteamUser();
const csgo = new GlobalOffensive(client);

const downloadsDir = path.join(process.cwd(), "downloads");

let activeDownloads = 0;
let gcReady = false;
let isSendingTips = false;
let steamReady = false;
let friendsReady = false;
let downloadPollerStarted = false;
let messagePollerStarted = false;
let loginAttempts = 0;
let activeParses = 0;
const MAX_LOGIN_ATTEMPTS = 10;
const HEARTBEAT_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes
const MAX_DISCONNECTED_MS = 15 * 60 * 1000;  // 15 min offline → exit for pm2 restart
let lastConnectedAt = Date.now();
let disconnectedSince = null;
const retryAfterByMatchId = new Map();

const ensureDownloadsDir = async () => {
  await fsp.mkdir(downloadsDir, { recursive: true });
};

const getPendingMatches = async (limit) => {
  const db = await pool.connect();
  try {
    await db.query("BEGIN");
    const result = await db.query(
      `
      with pending as (
        select id, share_code, user_id
        from public.matches_to_download
        where status = 'pending'
        order by id desc
        limit $1
        for update skip locked
      )
      select
        pending.id,
        pending.share_code,
        pending.user_id,
        coalesce(u.steam_id::text, pending.user_id::text) as user_steam_id
      from pending
      left join public.users u on u.steam_id::text = pending.user_id::text
      `,
      [limit]
    );

    if (result.rowCount === 0) {
      await db.query("COMMIT");
      return [];
    }

    const ids = result.rows.map((r) => r.id);
    await db.query(
      `
      update public.matches_to_download
      set status = 'processing'
      where id = ANY($1)
      `,
      [ids]
    );
    await db.query("COMMIT");
    return result.rows;
  } catch (error) {
    await db.query("ROLLBACK");
    throw error;
  } finally {
    db.release();
  }
};

const markDownloaded = async (id, filePath) => {
  await pool.query(
    `
    update public.matches_to_download
    set status = 'downloaded', file_path = $2
    where id = $1
    `,
    [id, filePath]
  );
};

const markError = async (id) => {
  await pool.query(
    `
    update public.matches_to_download
    set status = 'error'
    where id = $1
    `,
    [id]
  );
};

const markPending = async (id) => {
  await pool.query(
    `
    update public.matches_to_download
    set status = 'pending'
    where id = $1
    `,
    [id]
  );
};

const requestMatch = (shareCode) =>
  new Promise((resolve, reject) => {
    if (!gcReady) {
      return reject(new Error("Game Coordinator not ready"));
    }

    const timeout = setTimeout(() => {
      csgo.removeListener("matchList", handler);
      reject(new Error("Timed out waiting for matchList (20s)"));
    }, 20_000);

    const handler = (matchList) => {
      clearTimeout(timeout);
      resolve(matchList);
    };

    csgo.once("matchList", handler);

    try {
      csgo.requestGame(shareCode);
    } catch (error) {
      clearTimeout(timeout);
      csgo.removeListener("matchList", handler);
      reject(error);
    }
  });

const findReplayUrlInObject = (value, maxDepth = 6) => {
  const queue = [{ node: value, depth: 0 }];

  while (queue.length) {
    const { node, depth } = queue.shift();
    if (!node || depth > maxDepth) {
      continue;
    }

    if (typeof node === "string") {
      if (node.includes(".dem.bz2")) {
        return node;
      }
      continue;
    }

    if (Array.isArray(node)) {
      node.forEach((item) => queue.push({ node: item, depth: depth + 1 }));
      continue;
    }

    if (typeof node === "object") {
      Object.values(node).forEach((item) =>
        queue.push({ node: item, depth: depth + 1 })
      );
    }
  }

  return null;
};

const buildReplayUrl = (matchList) => {
  const watchable = matchList.watchablematchinfo || matchList?.[0]?.watchablematchinfo;
  const serverIp = watchable?.server_ip;
  const tvPort = watchable?.tv_port;
  const roundStats = matchList.round_stats_all || matchList?.[0]?.round_stats_all;
  const lastRound = Array.isArray(roundStats)
    ? roundStats[roundStats.length - 1]
    : null;
  const reservationId = lastRound?.reservationid;

  if (!serverIp || !tvPort || !reservationId) {
    return null;
  }

  const paddedReservation = String(reservationId).padStart(21, "0");
  return `http://replay${serverIp}.valve.net/730/${paddedReservation}_${tvPort}.dem.bz2`;
};

const findReplayUrl = (matchList) => {
  if (!matchList || typeof matchList !== "object") {
    return null;
  }

  const roundStats = matchList.round_stats_all || matchList?.[0]?.round_stats_all;
  const lastRound = Array.isArray(roundStats)
    ? roundStats[roundStats.length - 1]
    : null;
  const alternativeUrl =
    lastRound?.reservation?.replay_url ||
    lastRound?.map ||
    lastRound?.replay_url;

  const candidates = [
    matchList?.match?.replay_url,
    matchList?.matches?.[0]?.match?.replay_url,
    matchList?.matches?.[0]?.replay_url,
    matchList?.replay_url,
    matchList?.[0]?.replay_url,
    alternativeUrl,
    buildReplayUrl(matchList),
    findReplayUrlInObject(matchList),
  ];

  return candidates.find((value) => typeof value === "string" && value.length);
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const isRetryableFetchError = (error) => {
  if (!error) {
    return false;
  }
  const message = typeof error.message === "string" ? error.message : "";
  const causeCode = error.cause?.code;
  return (
    error.name === "AbortError" ||
    causeCode === "UND_ERR_SOCKET" ||
    message.includes("terminated") ||
    message.includes("other side closed")
  );
};

const downloadReplay = async (url, outputFile) => {
  const retryable = new Set([502, 503, 504]);
  const maxAttempts = 6;
  const headers = {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    Accept:
      "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    Connection: "keep-alive",
  };

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    console.log("🔗 TRYING TO DOWNLOAD:", url);
    let response;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), DOWNLOAD_TIMEOUT_MS);

    try {
      response = await fetch(url, { headers, signal: controller.signal });
      clearTimeout(timeout);

      if (response.ok && response.body) {
        try {
          await pipeline(
            response.body,
            unbzip2(),
            fs.createWriteStream(outputFile)
          );
          return;
        } catch (error) {
          await fsp.rm(outputFile, { force: true });
          if (attempt < maxAttempts && isRetryableFetchError(error)) {
            await sleep(3000 * attempt);
            continue;
          }
          throw error;
        }
      }
    } catch (error) {
      clearTimeout(timeout);
      if (attempt < maxAttempts && isRetryableFetchError(error)) {
        await sleep(3000 * attempt);
        continue;
      }
      throw error;
    }

    if (retryable.has(response.status) && attempt < maxAttempts) {
      await sleep(3000 * attempt);
      continue;
    }

    throw new Error(`Replay download failed: ${response.status}`);
  }
};

const canMessageUser = (steamId) => {
  if (!steamId) {
    return false;
  }
  const relationship =
    client.myFriends?.[steamId] ?? client.friends?.[steamId];
  return (
    relationship &&
    relationship !== SteamUser.EFriendRelationship.None &&
    relationship !== SteamUser.EFriendRelationship.Blocked &&
    relationship !== SteamUser.EFriendRelationship.Ignored
  );
};

const sendDownloadMessage = async (steamId, matchId) => {
  if (!canMessageUser(steamId)) {
    return;
  }

  try {
    client.chat.sendFriendMessage(
      steamId,
      `✅ Your match ${matchId} has been downloaded and is ready for stats!`
    );
  } catch (error) {
    console.warn(`Failed to message ${steamId}:`, error);
  }
};

const fetchPendingTips = async () => {
  const result = await pool.query(
    `
    select
      m.id,
      m.user_id,
      m.coach_tip,
      coalesce(u.steam_id::text, m.user_id::text) as user_steam_id
    from public.matches_to_download m
    left join public.users u on u.steam_id::text = m.user_id::text
    where m.status in ('processed', 'parsed')
      and m.coach_tip is not null
      and (m.tip_sent is null or m.tip_sent = false)
    order by m.id asc
    `
  );
  return result.rows;
};

const markTipSent = async (matchId) => {
  await pool.query(
    `
    update public.matches_to_download
    set status = 'notified',
        tip_sent = true
    where id = $1
    `,
    [matchId]
  );
};

const sendPendingMessages = async () => {
  if (isSendingTips || !steamReady) {
    return;
  }

  isSendingTips = true;
  try {
    const rows = await fetchPendingTips();
    if (!rows.length) {
      return;
    }

    for (const row of rows) {
      const steamId = row.user_steam_id;
      console.log(
        `Friend Status for ${steamId}: ${client.myFriends?.[steamId]}`
      );
      if (!canMessageUser(steamId)) {
        console.warn(`Cannot message ${steamId}; not a friend or blocked.`);
        continue;
      }

      try {
        client.chat.sendFriendMessage(steamId, row.coach_tip);
        await markTipSent(row.id);
        console.log(`Sent coach tip for match ${row.id}.`);
      } catch (error) {
        console.warn(`Failed to send tip to ${steamId}:`, error);
      }
    }
  } finally {
    isSendingTips = false;
  }
};

const processOneMatch = async (row) => {
  const retryAfter = retryAfterByMatchId.get(row.id);
  if (retryAfter && Date.now() < retryAfter) {
    await markPending(row.id);
    return;
  }

  const shareCode = row.share_code;

  // GC request is serialized (Valve limitation) — await in sequence
  const matchList = await requestMatch(shareCode);
  const replayUrl = findReplayUrl(matchList);

  if (!replayUrl) {
    log("DL", `Replay URL missing for share code ${shareCode}`);
    await markPending(row.id);
    return;
  }

  await ensureDownloadsDir();
  const outputFile = path.join(downloadsDir, `match_${row.id}.dem`);
  await downloadReplay(replayUrl, outputFile);

  await markDownloaded(row.id, outputFile);
  log("DL", `Downloaded ${shareCode} → ${outputFile}`);
  await sendDownloadMessage(row.user_steam_id, row.id);

  // Auto-trigger demo parsing in background (throttled)
  triggerParse(row.id);
};

const processPending = async () => {
  if (!gcReady) {
    return;
  }

  const slotsAvailable = MAX_CONCURRENT_DOWNLOADS - activeDownloads;
  if (slotsAvailable <= 0) {
    return;
  }

  let rows;
  try {
    rows = await getPendingMatches(slotsAvailable);
  } catch (error) {
    log("DL", `Failed to fetch pending matches: ${error.message}`);
    return;
  }

  if (!rows.length) {
    return;
  }

  log("DL", `Processing ${rows.length} match(es) (${activeDownloads} active)...`);

  for (const row of rows) {
    activeDownloads += 1;
    processOneMatch(row)
      .catch((error) => {
        console.error("Replay downloader error:", error);
        const message = typeof error?.message === "string" ? error.message : "";
        if (message.includes("Replay download failed: 502")) {
          retryAfterByMatchId.set(row.id, Date.now() + RETRY_COOLDOWN_MS);
          markPending(row.id).catch(() => {});
          log("DL", `Match ${row.id} re-queued (CDN 502).`);
        } else {
          markError(row.id).catch(() => {});
        }
      })
      .finally(() => {
        activeDownloads -= 1;
      });

    // Small delay between GC requests to avoid rate limits
    await sleep(1500);
  }
};

const parseQueue = [];
let parseRunning = 0;

const drainParseQueue = () => {
  while (parseRunning < MAX_CONCURRENT_PARSES && parseQueue.length > 0) {
    const matchId = parseQueue.shift();
    parseRunning += 1;
    const pythonCmd =
      process.platform === "win32"
        ? ".venv\\Scripts\\python.exe"
        : "python3";
    const scriptPath = path.join(__dirname, "parse_match.py");
    log("Parse", `Parsing match ${matchId} (${parseRunning}/${MAX_CONCURRENT_PARSES} slots)...`);
    const child = spawn(pythonCmd, [scriptPath, String(matchId)], {
      cwd: path.resolve(__dirname, ".."),
      stdio: "pipe",
      env: { ...process.env },
    });
    child.stdout.on("data", (data) => {
      String(data)
        .trim()
        .split("\n")
        .forEach((line) => log("Parse", line));
    });
    child.stderr.on("data", (data) => {
      String(data)
        .trim()
        .split("\n")
        .forEach((line) => log("Parse", `ERR: ${line}`));
    });
    child.on("close", (code) => {
      parseRunning -= 1;
      if (code === 0) {
        log("Parse", `Match ${matchId} parsed successfully.`);
      } else {
        log("Parse", `Match ${matchId} parse exited with code ${code}.`);
      }
      drainParseQueue();
    });
  }
};

const triggerParse = (matchId) => {
  parseQueue.push(matchId);
  drainParseQueue();
};

const startDownloadPolling = () => {
  if (downloadPollerStarted) {
    return;
  }
  downloadPollerStarted = true;
  log("DL", `Polling every ${POLL_INTERVAL_MS / 1000}s for pending matches...`);
  setInterval(() => {
    processPending().catch((error) => console.error(error));
  }, POLL_INTERVAL_MS);
  processPending().catch((error) => console.error(error));
};

const startMessagePolling = () => {
  if (messagePollerStarted || !friendsReady) {
    return;
  }
  messagePollerStarted = true;
  setInterval(() => {
    sendPendingMessages().catch((error) => console.error(error));
  }, POLL_INTERVAL_MS);
  sendPendingMessages().catch((error) => console.error(error));
};

client.on("loggedOn", () => {
  log("Steam", "Bot logged in successfully.");
  steamReady = true;
  loginAttempts = 0;
  disconnectedSince = null;
  lastConnectedAt = Date.now();
  client.setPersona(SteamUser.EPersonaState.Online);
  client.gamesPlayed(APP_ID_CS2);
});

client.on("friendRelationship", (steamId, relationship) => {
  if (relationship === SteamUser.EFriendRelationship.RequestRecipient) {
    client.addFriend(steamId);
  }
});

client.on("friendsList", () => {
  friendsReady = true;
  startMessagePolling();
});

client.on("error", (error) => {
  console.error("[Steam] Client error:", error.message || error);
  steamReady = false;
  gcReady = false;
  if (!disconnectedSince) {
    disconnectedSince = Date.now();
  }

  loginAttempts += 1;
  if (loginAttempts >= MAX_LOGIN_ATTEMPTS) {
    console.error(`[Steam] Max login attempts (${MAX_LOGIN_ATTEMPTS}) reached. Exiting for pm2 restart.`);
    process.exit(1);
  }

  const delay = Math.min(GC_RECONNECT_DELAY_MS * loginAttempts, 120_000);
  console.log(`[Steam] Reconnecting in ${delay / 1000}s (attempt ${loginAttempts})...`);
  setTimeout(() => {
    try {
      client.logOn({
        accountName: steamUser,
        password: steamPass,
        twoFactorCode: SteamTotp.generateAuthCode(steamSharedSecret),
      });
    } catch (reconnectError) {
      console.error("[Steam] Reconnect failed:", reconnectError);
    }
  }, delay);
});

csgo.on("connectedToGC", () => {
  log("GC", "Connected to CS2 Game Coordinator.");
  gcReady = true;
  disconnectedSince = null;
  startDownloadPolling();
});

csgo.on("disconnectedFromGC", () => {
  log("GC", "Disconnected from CS2 Game Coordinator. Waiting for reconnection...");
  gcReady = false;
  if (!disconnectedSince) {
    disconnectedSince = Date.now();
  }
});

// ── Heartbeat: detect silently dead connections ──
setInterval(() => {
  if (!steamReady || !gcReady) {
    const downFor = disconnectedSince
      ? Math.round((Date.now() - disconnectedSince) / 1000)
      : 0;
    log("Heartbeat", `Steam=${steamReady ? "OK" : "DOWN"} GC=${gcReady ? "OK" : "DOWN"} (down ${downFor}s)`);

    // If disconnected too long, exit so pm2 does a clean restart
    if (disconnectedSince && Date.now() - disconnectedSince > MAX_DISCONNECTED_MS) {
      log("Heartbeat", `Disconnected for >${MAX_DISCONNECTED_MS / 60000}min. Exiting for pm2 restart.`);
      process.exit(1);
    }
  } else {
    const uptime = Math.round((Date.now() - lastConnectedAt) / 1000);
    log("Heartbeat", `Steam=OK GC=OK (uptime ${uptime}s, downloads=${activeDownloads})`);
  }
}, HEARTBEAT_INTERVAL_MS);

// ── Startup banner ──────────────────────────────────────────
log("Boot", "=".repeat(50));
log("Boot", "Replay Downloader daemon starting");
log("Boot", `  Poll interval     : ${POLL_INTERVAL_MS / 1000}s`);
log("Boot", `  Download timeout  : ${DOWNLOAD_TIMEOUT_MS / 1000}s`);
log("Boot", `  Max downloads     : ${MAX_CONCURRENT_DOWNLOADS}`);
log("Boot", `  Max parses        : ${MAX_CONCURRENT_PARSES}`);
log("Boot", `  Steam user        : ${steamUser}`);
log("Boot", "=".repeat(50));

client.logOn({
  accountName: steamUser,
  password: steamPass,
  twoFactorCode: SteamTotp.generateAuthCode(steamSharedSecret),
});

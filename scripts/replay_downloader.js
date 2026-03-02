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

/**
 * Check if we can message a user by their SteamID64.
 * Handles potential SteamID format mismatches between DB (SteamID64) and
 * client.myFriends keys (which may be SteamID64 OR Steam3 format).
 */
const canMessageUser = (steamId) => {
  if (!steamId) {
    return false;
  }

  const friends = client.myFriends || client.friends || {};
  const sid64 = String(steamId);

  // 1) Direct lookup (works when keys are SteamID64)
  let relationship = friends[sid64];

  // 2) Try Steam3 format [U:1:accountId] (some steam-user versions use this)
  if (relationship === undefined || relationship === null) {
    if (/^\d{17}$/.test(sid64)) {
      try {
        const accountId = BigInt(sid64) - 76561197960265728n;
        const steam3 = `[U:1:${accountId}]`;
        relationship = friends[steam3];
      } catch (_) { /* invalid id */ }
    }
  }

  // 3) Brute-force scan: iterate keys and compare SteamID64 values
  if (relationship === undefined || relationship === null) {
    for (const key of Object.keys(friends)) {
      try {
        // If key is a SteamID object with getSteamID64, compare that way
        if (typeof key === "string" && key.startsWith("[")) {
          // Parse Steam3 format [U:1:accountId] → SteamID64
          const m = key.match(/^\[U:1:(\d+)\]$/);
          if (m) {
            const reconstructed = String(BigInt(m[1]) + 76561197960265728n);
            if (reconstructed === sid64) {
              relationship = friends[key];
              break;
            }
          }
        } else if (key === sid64) {
          relationship = friends[key];
          break;
        }
      } catch (_) { /* skip malformed keys */ }
    }
  }

  if (relationship === undefined || relationship === null) {
    return false;
  }

  return (
    relationship !== SteamUser.EFriendRelationship.None &&
    relationship !== SteamUser.EFriendRelationship.Blocked &&
    relationship !== SteamUser.EFriendRelationship.Ignored
  );
};

/**
 * Send a Steam chat message with automatic fallback:
 *   1) client.chat.sendFriendMessage   (new chat proto)
 *   2) client.chatMessage              (legacy EMsg)
 * Throws only when BOTH methods fail.
 */
const sendSteamMessage = async (steamId, message) => {
  const preview = message.length > 80 ? message.slice(0, 80) + "…" : message;
  log("Tip", `sendSteamMessage → ${steamId} (${message.length} chars): "${preview}"`);

  // Normalise to SteamID object if the library provides it (better compatibility)
  let targetId = steamId;
  try {
    if (SteamUser.SteamID) {
      targetId = new SteamUser.SteamID(String(steamId));
    }
  } catch (_) { /* keep original string */ }

  // Try new chat API first
  if (typeof client.chat?.sendFriendMessage === "function") {
    try {
      const result = await client.chat.sendFriendMessage(targetId, message);
      log("Tip", `chat.sendFriendMessage OK for ${steamId} (result: ${JSON.stringify(result)?.slice(0, 120)})`);
      return;
    } catch (err) {
      log("Tip", `chat.sendFriendMessage FAILED for ${steamId}: ${err.message} — trying legacy API`);
    }
  } else {
    log("Tip", `client.chat.sendFriendMessage not available (chat=${typeof client.chat})`);
  }

  // Fallback: legacy chatMessage (fire-and-forget)
  if (typeof client.chatMessage === "function") {
    log("Tip", `Using legacy client.chatMessage for ${steamId}`);
    client.chatMessage(targetId, message);
    return;
  }

  throw new Error("No usable Steam chat API available");
};

/**
 * Atomically claim ONE pending tip using SELECT ... FOR UPDATE SKIP LOCKED
 * so that concurrent instances / restarts never send the same tip twice.
 * Returns null when there is nothing to send.
 */
const claimNextTip = async () => {
  const db = await pool.connect();
  try {
    await db.query("BEGIN");
    // CTE locks ONLY the matches_to_download row (no JOIN inside the lock)
    // then we join users OUTSIDE the CTE — avoids the PostgreSQL
    // "FOR UPDATE cannot be applied to the nullable side of an outer join" error.
    const result = await db.query(
      `
      with tip as (
        select id, user_id, coach_tip, tip_image_url
        from public.matches_to_download
        where status in ('processed', 'parsed')
          and coach_tip is not null
          and (tip_sent is null or tip_sent = false)
        order by id asc
        limit 1
        for update skip locked
      )
      select
        tip.id,
        tip.user_id,
        tip.coach_tip,
        tip.tip_image_url,
        coalesce(u.steam_id::text, tip.user_id::text) as user_steam_id
      from tip
      left join public.users u on u.steam_id::text = tip.user_id::text
      `
    );
    if (!result.rows.length) {
      await db.query("COMMIT");
      return null;
    }
    const row = result.rows[0];
    // Mark claimed immediately so no other process picks it up
    await db.query(
      `update public.matches_to_download
       set tip_sent = true
       where id = $1`,
      [row.id]
    );
    await db.query("COMMIT");
    return row;
  } catch (err) {
    await db.query("ROLLBACK").catch(() => {});
    throw err;
  } finally {
    db.release();
  }
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

/** Revert tip_sent so the next cycle retries delivery. */
const revertTipClaim = async (matchId) => {
  await pool.query(
    `update public.matches_to_download
     set tip_sent = false
     where id = $1 and status not in ('notified')`,
    [matchId]
  );
};

/**
 * Recovery: reset tip_sent for matches that got "stuck" — claimed (tip_sent=true)
 * but never moved to 'notified' within STUCK_TIP_THRESHOLD_MS.
 * This handles bot crashes, restarts, or unexpected errors that bypass revertTipClaim.
 */
const STUCK_TIP_THRESHOLD_MS = 5 * 60 * 1000; // 5 minutes
const recoverStuckTips = async () => {
  try {
    const result = await pool.query(
      `UPDATE public.matches_to_download
       SET tip_sent = false
       WHERE status IN ('processed', 'parsed')
         AND tip_sent = true
         AND coach_tip IS NOT NULL
         AND created_at < NOW() - INTERVAL '5 minutes'
       RETURNING id`
    );
    if (result.rowCount > 0) {
      const ids = result.rows.map(r => r.id);
      log("Tip", `Recovered ${result.rowCount} stuck tip(s): [${ids.join(", ")}] — reset tip_sent to false.`);
    }
  } catch (err) {
    log("Tip", `recoverStuckTips error: ${err.message}`);
  }
};

const sendPendingMessages = async () => {
  if (isSendingTips) {
    return;
  }
  if (!steamReady) {
    log("Tip", "Steam not ready — skipping tip cycle.");
    return;
  }

  // Recover any stuck tips before starting (handles bot crash / restart)
  await recoverStuckTips();

  // Don't attempt to send tips until the friends list has loaded
  const friends = client.myFriends || client.friends || {};
  const friendCount = Object.keys(friends).length;
  if (friendCount === 0) {
    log("Tip", "Friends list not loaded yet (0 entries) — skipping tip cycle.");
    return;
  }

  isSendingTips = true;
  log("Tip", `Starting tip cycle (${friendCount} friends loaded).`);

  // Quick diagnostic: how many tips are pending in DB?
  try {
    const pendingCheck = await pool.query(
      `SELECT COUNT(*) as cnt FROM public.matches_to_download
       WHERE status IN ('processed', 'parsed')
         AND coach_tip IS NOT NULL
         AND (tip_sent IS NULL OR tip_sent = false)`
    );
    log("Tip", `Pending tips in DB: ${pendingCheck.rows[0]?.cnt || 0}`);
  } catch (_) { /* non-critical */ }

  const skippedIds = new Set();  // track IDs we can't deliver this cycle
  let currentClaimedId = null;   // track currently claimed tip for safety revert
  try {
    // Process tips one-at-a-time with atomic claim
    let sent = 0;
    let failures = 0;
    const MAX_FAILURES_PER_CYCLE = 3;  // stop cycling after N consecutive failures
    while (true) {
      currentClaimedId = null;
      const row = await claimNextTip();
      if (!row) {
        log("Tip", "No more pending tips to claim.");
        break;
      }
      currentClaimedId = row.id;

      // ── Guard: already tried this ID this cycle (prevents infinite loop) ──
      if (skippedIds.has(row.id)) {
        await revertTipClaim(row.id);
        currentClaimedId = null;
        break;  // we've looped back → stop
      }

      const steamId = row.user_steam_id;
      log("Tip", `Claimed match ${row.id} for steam ${steamId} (user_id=${row.user_id})`);

      // Diagnostic: show what canMessageUser sees
      const friendKeys = Object.keys(client.myFriends || client.friends || {});
      const directLookup = (client.myFriends || client.friends || {})[steamId];
      log("Tip", `  canMessageUser check: directLookup=${directLookup}, friendKeys.length=${friendKeys.length}`);

      if (!canMessageUser(steamId)) {
        // Log all friend keys for debugging (first match skip only)
        if (skippedIds.size === 0) {
          const fkeys = friendKeys.slice(0, 10);
          log("Tip", `  Cannot message ${steamId} — not on friends list.`);
          log("Tip", `  Sample friend keys: [${fkeys.map(k => `"${k}"`).join(", ") || "<empty>"}]`);
          // Try to explain the format
          if (fkeys.length > 0 && fkeys[0].startsWith("[")) {
            log("Tip", `  Keys are Steam3 format. Trying conversion...`);
            try {
              const accountId = BigInt(steamId) - 76561197960265728n;
              log("Tip", `  Expected Steam3 key: [U:1:${accountId}]`);
            } catch (_) {}
          }
        } else {
          log("Tip", `  Cannot message ${steamId} — will retry next cycle.`);
        }
        skippedIds.add(row.id);
        await revertTipClaim(row.id);
        currentClaimedId = null;
        continue;
      }

      log("Tip", `  canMessageUser(${steamId}) = true — proceeding to send.`);

      try {
        // Double-check the row hasn't already been sent (guards against pm2 restart overlap)
        const guard = await pool.query(
          `SELECT tip_sent, status FROM public.matches_to_download WHERE id = $1`,
          [row.id]
        );
        if (guard.rows[0]?.status === "notified") {
          log("Tip", `Match ${row.id} already notified — skipping duplicate send.`);
          currentClaimedId = null;
          continue;
        }

        // 1) Send stats card image URL alone so Steam auto-embeds it as an image
        if (row.tip_image_url) {
          log("Tip", `  Sending stats card image for match ${row.id}...`);
          await sendSteamMessage(steamId, row.tip_image_url);
          await sleep(2000);
        }

        // 2) Send the AI coaching tip — split into chunks if too long for Steam (~5000 char limit)
        const STEAM_MAX = 4500;
        const tipText = row.coach_tip || "";
        if (tipText.length > 0) {
          log("Tip", `  Sending coach tip for match ${row.id}: ${tipText.length} chars`);
          if (tipText.length <= STEAM_MAX) {
            await sendSteamMessage(steamId, tipText);
            await sleep(2000);
          } else {
            // Split on double-newlines (section breaks) to keep sections intact
            const sections = tipText.split(/\n\n/);
            let chunk = "";
            let chunkNum = 0;
            for (const section of sections) {
              if (chunk.length + section.length + 2 > STEAM_MAX && chunk.length > 0) {
                chunkNum++;
                log("Tip", `  Sending chunk ${chunkNum} (${chunk.length} chars)...`);
                await sendSteamMessage(steamId, chunk.trim());
                await sleep(2000);
                chunk = "";
              }
              chunk += (chunk ? "\n\n" : "") + section;
            }
            if (chunk.trim()) {
              chunkNum++;
              log("Tip", `  Sending chunk ${chunkNum} (${chunk.length} chars)...`);
              await sendSteamMessage(steamId, chunk.trim());
              await sleep(2000);
            }
          }
        } else {
          log("Tip", `  WARNING: coach_tip is empty for match ${row.id}`);
        }

        // 3) Send match stats link as the last message
        const baseUrl = process.env.NEXT_PUBLIC_BASE_URL || "https://retake-cs2.vercel.app";
        const matchUrl = `${baseUrl}/dashboard/matches/${row.id}`;
        log("Tip", `  Sending match link for match ${row.id}...`);
        await sendSteamMessage(steamId, `📊 View your full match stats here:\n${matchUrl}`);

        await markTipSent(row.id);
        currentClaimedId = null;
        sent++;
        failures = 0;  // reset consecutive-failure counter
        log("Tip", `✓ Sent coach tip for match ${row.id} — ${tipText.length} chars (${sent} this cycle).`);
      } catch (error) {
        console.warn(`[Tip] Failed to send tip for match ${row.id} to ${steamId}:`, error?.message || error);
        skippedIds.add(row.id);
        await revertTipClaim(row.id).catch(e => log("Tip", `revertTipClaim error: ${e.message}`));
        currentClaimedId = null;
        failures++;
        if (failures >= MAX_FAILURES_PER_CYCLE) {
          log("Tip", `${failures} consecutive send failures — stopping this cycle.`);
          break;
        }
      }

      // Small delay between messages to avoid Steam rate-limiting
      await sleep(1500);
    }
    log("Tip", `Cycle done: ${sent} tip(s) delivered, ${skippedIds.size} skipped.`);
  } catch (outerErr) {
    // Safety net: revert any claimed tip that wasn't handled
    log("Tip", `Unexpected error in tip cycle: ${outerErr?.message || outerErr}`);
    if (currentClaimedId) {
      log("Tip", `Safety-reverting claimed match ${currentClaimedId}`);
      await revertTipClaim(currentClaimedId).catch(e => log("Tip", `Safety revert failed: ${e.message}`));
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
  if (messagePollerStarted) {
    return;
  }
  messagePollerStarted = true;
  log("Tip", `Message polling started (every ${POLL_INTERVAL_MS / 1000}s). friendsReady=${friendsReady}`);
  setInterval(() => {
    sendPendingMessages().catch((error) => console.error("[Tip] polling error:", error));
  }, POLL_INTERVAL_MS);
  // Run recovery immediately on startup for any tips stuck from previous crash
  recoverStuckTips().then(() => {
    sendPendingMessages().catch((error) => console.error("[Tip] polling error:", error));
  });
};

client.on("loggedOn", () => {
  log("Steam", "Bot logged in successfully.");
  steamReady = true;
  loginAttempts = 0;
  disconnectedSince = null;
  lastConnectedAt = Date.now();
  client.setPersona(SteamUser.EPersonaState.Online);
  client.gamesPlayed(APP_ID_CS2);
  // Start message polling — actual sending waits for friends list in sendPendingMessages()
  startMessagePolling();
  log("Steam", "Waiting for friends list before sending tips...");
});

client.on("friendRelationship", (steamId, relationship) => {
  if (relationship === SteamUser.EFriendRelationship.RequestRecipient) {
    client.addFriend(steamId);
  }
});

client.on("friendsList", () => {
  friendsReady = true;
  const friends = client.myFriends || client.friends || {};
  const keys = Object.keys(friends);
  log("Steam", `Friends list loaded: ${keys.length} entries.`);
  if (keys.length > 0) {
    log("Steam", `  Sample key format: "${keys[0]}" (type: ${typeof keys[0]})`);
  }
  startMessagePolling();
  // Immediately try sending any pending tips now that friends are loaded
  sendPendingMessages().catch((err) => console.error("[Tip] post-friendsList error:", err));
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

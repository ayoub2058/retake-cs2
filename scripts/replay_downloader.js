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
  process.env.MAX_CONCURRENT_DOWNLOADS || "10",
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

// Supabase Storage config for cleanup
const SUPABASE_URL = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL || "";
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "";
const STORAGE_BUCKET = "stats-cards";

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

// Download deduplication: track in-flight downloads by replay URL
const inflightDownloads = new Map(); // replayUrl → Promise<string>

// Cross-cycle tracking: pending friend requests (don't spam every 30s)
const pendingFriendRequests = new Map(); // steamId → timestamp
const FRIEND_REQUEST_COOLDOWN_MS = 10 * 60 * 1000; // 10 min before retrying

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

/** Check if another row with the same share_code already has a downloaded file. */
const findExistingDownload = async (shareCode, excludeId) => {
  const result = await pool.query(
    `SELECT file_path FROM public.matches_to_download
     WHERE share_code = $1 AND id != $2
       AND file_path IS NOT NULL
       AND status NOT IN ('pending', 'error')
     LIMIT 1`,
    [shareCode, excludeId]
  );
  return result.rows[0]?.file_path || null;
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
    causeCode === "ETIMEDOUT" ||
    causeCode === "ECONNRESET" ||
    causeCode === "ECONNREFUSED" ||
    message.includes("terminated") ||
    message.includes("other side closed") ||
    message.includes("fetch failed") ||
    message.includes("ETIMEDOUT")
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

  log("DL", `⬇ Downloading: ${url}`);
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    if (attempt > 1) log("DL", `  Retry ${attempt}/${maxAttempts}...`);
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

/** Wrapper with automatic retry for transient Steam chat failures. */
const sendSteamMessageWithRetry = async (steamId, message, maxRetries = 2) => {
  for (let i = 0; i <= maxRetries; i++) {
    try {
      await sendSteamMessage(steamId, message);
      return;
    } catch (err) {
      if (i === maxRetries) throw err;
      log("Tip", `  Message send failed (attempt ${i + 1}/${maxRetries + 1}): ${err.message}. Retrying in 3s...`);
      await sleep(3000);
    }
  }
};

/**
 * Delete an image from Supabase Storage to free space.
 * Extracts filename from the public URL and deletes it.
 * Fails silently on error (non-critical cleanup).
 */
const deleteSupabaseImage = async (imageUrl) => {
  if (!imageUrl || !SUPABASE_URL || !SUPABASE_SERVICE_KEY) return;

  try {
    // URL format: {SUPABASE_URL}/storage/v1/object/public/{bucket}/{filename}
    const publicPath = `/storage/v1/object/public/${STORAGE_BUCKET}/`;
    const idx = imageUrl.indexOf(publicPath);
    if (idx === -1) return;

    const filename = imageUrl.slice(idx + publicPath.length);
    if (!filename) return;

    const deleteUrl = `${SUPABASE_URL}/storage/v1/object/${STORAGE_BUCKET}/${filename}`;
    const resp = await fetch(deleteUrl, {
      method: "DELETE",
      headers: {
        "Authorization": `Bearer ${SUPABASE_SERVICE_KEY}`,
      },
    });

    if (resp.ok) {
      log("Tip", `  Deleted image from storage: ${filename}`);
    } else {
      // Non-critical, just log
      log("Tip", `  Image delete failed (${resp.status}): ${filename}`);
    }
  } catch (err) {
    log("Tip", `  Image delete error: ${err.message}`);
  }
};

/**
 * Atomically claim ONE pending tip using SELECT ... FOR UPDATE SKIP LOCKED
 * so that concurrent instances / restarts never send the same tip twice.
 * Returns null when there is nothing to send.
 */
const claimNextTip = async (excludeIds = [], excludeSteamIds = []) => {
  const db = await pool.connect();
  try {
    await db.query("BEGIN");
    // CTE locks ONLY the matches_to_download row (no JOIN inside the lock)
    // then we join users OUTSIDE the CTE — avoids the PostgreSQL
    // "FOR UPDATE cannot be applied to the nullable side of an outer join" error.
    const result = await db.query(
      `
      with tip as (
        select id, user_id, coach_tip, tip_image_url, tip_text_image_url
        from public.matches_to_download
        where status in ('processed', 'parsed')
          and coach_tip is not null
          and length(trim(coach_tip)) > 0
          and (tip_sent is null or tip_sent = false)
          and id != ALL($1::int[])
          and user_id::text != ALL($2::text[])
        order by id asc
        limit 1
        for update skip locked
      )
      select
        tip.id,
        tip.user_id,
        tip.coach_tip,
        tip.tip_image_url,
        tip.tip_text_image_url,
        coalesce(u.steam_id::text, tip.user_id::text) as user_steam_id,
        coalesce(u.bot_send_card, true)  as bot_send_card,
        coalesce(u.bot_send_tip, true)   as bot_send_tip,
        coalesce(u.bot_send_link, true)  as bot_send_link
      from tip
      left join public.users u on u.steam_id::text = tip.user_id::text
      `,
      [excludeIds, excludeSteamIds]
    );
    if (!result.rows.length) {
      await db.query("COMMIT");
      return null;
    }
    const row = result.rows[0];
    // Mark claimed with timestamp so recovery knows when it was claimed
    await db.query(
      `update public.matches_to_download
       set tip_sent = true, tip_claimed_at = now()
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
    // Only recover tips that were claimed (tip_claimed_at) more than 5 minutes ago.
    // Falls back to created_at for rows without tip_claimed_at (legacy rows).
    const result = await pool.query(
      `UPDATE public.matches_to_download
       SET tip_sent = false, tip_claimed_at = null
       WHERE status IN ('processed', 'parsed')
         AND tip_sent = true
         AND coach_tip IS NOT NULL
         AND coalesce(tip_claimed_at, created_at) < NOW() - INTERVAL '5 minutes'
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
    return; // silent — avoids log spam when Steam is reconnecting
  }

  // Recover any stuck tips before starting (handles bot crash / restart)
  await recoverStuckTips();

  // Don't attempt to send tips until the friends list has loaded
  const friends = client.myFriends || client.friends || {};
  const friendCount = Object.keys(friends).length;
  if (friendCount === 0) {
    return;
  }

  isSendingTips = true;

  // Quick diagnostic: how many tips are pending in DB?
  let pendingCount = 0;
  try {
    const pendingCheck = await pool.query(
      `SELECT COUNT(*) as cnt FROM public.matches_to_download
       WHERE status IN ('processed', 'parsed')
         AND coach_tip IS NOT NULL
         AND length(trim(coach_tip)) > 0
         AND (tip_sent IS NULL OR tip_sent = false)`
    );
    pendingCount = parseInt(pendingCheck.rows[0]?.cnt || "0", 10);
  } catch (_) { /* non-critical */ }

  // Only log if there's work to do (reduces noise)
  if (pendingCount === 0) {
    isSendingTips = false;
    return;
  }
  log("Tip", `Starting tip cycle (${friendCount} friends, ${pendingCount} pending).`);

  // Build cross-cycle exclusion list from pending friend requests
  const excludeSteamIds = [];
  const now = Date.now();
  for (const [steamId, ts] of pendingFriendRequests) {
    if (now - ts < FRIEND_REQUEST_COOLDOWN_MS) {
      excludeSteamIds.push(steamId);
    } else {
      pendingFriendRequests.delete(steamId); // cooldown expired — retry
    }
  }
  if (excludeSteamIds.length > 0) {
    log("Tip", `  Skipping ${excludeSteamIds.length} user(s) with pending friend requests.`);
  }

  const skippedIds = new Set();  // track IDs we can't deliver this cycle
  let currentClaimedId = null;   // track currently claimed tip for safety revert
  try {
    let sent = 0;
    let failures = 0;
    const MAX_FAILURES_PER_CYCLE = 3;
    while (true) {
      currentClaimedId = null;
      const row = await claimNextTip([...skippedIds], excludeSteamIds);
      if (!row) {
        break;
      }
      currentClaimedId = row.id;

      const steamId = row.user_steam_id;
      log("Tip", `Claimed match ${row.id} for ${steamId}`);

      if (!canMessageUser(steamId)) {
        // User is not on friends list — skip quietly (bot only accepts, never sends requests)
        if (!pendingFriendRequests.has(steamId)) {
          pendingFriendRequests.set(steamId, Date.now());
          excludeSteamIds.push(steamId);
          log("Tip", `  ${steamId} not on friends list — skipping. They need to add the bot first.`);
        }

        skippedIds.add(row.id);
        await revertTipClaim(row.id);
        currentClaimedId = null;
        continue;
      }

      try {
        // Double-check not already sent (guards against pm2 restart overlap)
        const guard = await pool.query(
          `SELECT status FROM public.matches_to_download WHERE id = $1`,
          [row.id]
        );
        if (guard.rows[0]?.status === "notified") {
          currentClaimedId = null;
          continue;
        }

        // ── Send messages based on user preferences ──
        const STEAM_MAX = 4500;
        const tipText = (row.coach_tip || "").trim();
        const tipTextImageUrl = row.tip_text_image_url; // Arabic tips rendered as image
        const baseUrl = process.env.NEXT_PUBLIC_BASE_URL || "https://retake-cs2.vercel.app";
        const matchUrl = `${baseUrl}/dashboard/matches/${row.id}`;
        const wantCard = row.bot_send_card !== false;
        const wantTip  = row.bot_send_tip !== false;
        const wantLink = row.bot_send_link !== false;

        let anythingSent = false;

        // 1) AI coaching tip (with retry) — the most important message
        if (wantTip && tipText.length > 0) {
          // If Arabic tip image exists, send that instead of raw text
          if (tipTextImageUrl) {
            await sendSteamMessageWithRetry(steamId, "🎮 تحليل المباراة من RetakeAI:");
            await sleep(500);
            await sendSteamMessageWithRetry(steamId, tipTextImageUrl);
            anythingSent = true;
            await sleep(1000);
          } else if (tipText.length <= STEAM_MAX) {
            await sendSteamMessageWithRetry(steamId, tipText);
            anythingSent = true;
            await sleep(1000);
          } else {
            const sections = tipText.split(/\n\n/);
            let chunk = "";
            for (const section of sections) {
              if (chunk.length + section.length + 2 > STEAM_MAX && chunk.length > 0) {
                await sendSteamMessageWithRetry(steamId, chunk.trim());
                anythingSent = true;
                await sleep(1000);
                chunk = "";
              }
              chunk += (chunk ? "\n\n" : "") + section;
            }
            if (chunk.trim()) {
              await sendSteamMessageWithRetry(steamId, chunk.trim());
              anythingSent = true;
              await sleep(1000);
            }
          }
        }

        // Mark as notified after coach tip succeeds (prevents duplicate re-sends)
        if (anythingSent) {
          await markTipSent(row.id);
          currentClaimedId = null;
        }

        // 2) Stats card image (best-effort — don't re-send tip if this fails)
        if (wantCard && row.tip_image_url) {
          try {
            await sendSteamMessageWithRetry(steamId, row.tip_image_url);
            anythingSent = true;
            await sleep(1000);
          } catch (imgErr) {
            log("Tip", `  Image send failed for match ${row.id}: ${imgErr.message}`);
          }
        }

        // 3) Dashboard link (best-effort)
        if (wantLink) {
          try {
            await sendSteamMessageWithRetry(steamId, `📊 View your full match stats here:\n${matchUrl}`);
            anythingSent = true;
          } catch (linkErr) {
            log("Tip", `  Link send failed for match ${row.id}: ${linkErr.message}`);
          }
        }

        // If nothing was sent (all prefs off, or tip was empty), still mark notified
        if (!anythingSent) {
          log("Tip", `  No messages to send for match ${row.id} (user prefs: card=${wantCard}, tip=${wantTip}, link=${wantLink}).`);
        }
        // Ensure marked as notified even if only card/link were sent
        await markTipSent(row.id);
        currentClaimedId = null;

        // Clean up images from Supabase Storage after a delay.
        // Steam needs time to fetch the image for link preview / user to open it.
        const IMAGE_DELETE_DELAY_MS = 5 * 60 * 1000; // 5 minutes
        const matchIdForCleanup = row.id;
        const imageUrlForCleanup = row.tip_image_url;
        const tipTextImageUrlForCleanup = tipTextImageUrl;
        setTimeout(() => {
          if (imageUrlForCleanup) {
            deleteSupabaseImage(imageUrlForCleanup).catch(() => {});
          }
          if (tipTextImageUrlForCleanup) {
            deleteSupabaseImage(tipTextImageUrlForCleanup).catch(() => {});
          }
          pool.query(
            `UPDATE public.matches_to_download SET tip_image_url = NULL, tip_text_image_url = NULL WHERE id = $1`,
            [matchIdForCleanup]
          ).catch(() => {});
        }, IMAGE_DELETE_DELAY_MS);

        sent++;
        failures = 0;
        log("Tip", `✓ Match ${row.id} delivered (${tipText.length} chars, ${sent} this cycle).`);
      } catch (error) {
        console.warn(`[Tip] Failed match ${row.id} → ${steamId}:`, error?.message || error);
        skippedIds.add(row.id);
        await revertTipClaim(row.id).catch(e => log("Tip", `revertTipClaim error: ${e.message}`));
        currentClaimedId = null;
        failures++;
        if (failures >= MAX_FAILURES_PER_CYCLE) {
          log("Tip", `${failures} consecutive failures — stopping cycle.`);
          break;
        }
      }

      await sleep(1000); // small delay between users to avoid rate-limit
    }
    if (sent > 0 || skippedIds.size > 0) {
      log("Tip", `Cycle done: ${sent} delivered, ${skippedIds.size} skipped.`);
    }
  } catch (outerErr) {
    log("Tip", `Unexpected error in tip cycle: ${outerErr?.message || outerErr}`);
    if (currentClaimedId) {
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

  // ── Dedup: check if another row with the same share_code already has the file ──
  const existingFile = await findExistingDownload(shareCode, row.id);
  if (existingFile && fs.existsSync(existingFile)) {
    await ensureDownloadsDir();
    const outputFile = path.join(downloadsDir, `match_${row.id}.dem`);
    await fsp.copyFile(existingFile, outputFile);
    await markDownloaded(row.id, outputFile);
    log("DL", `Reused existing file for ${shareCode} → match_${row.id}.dem`);
    triggerParse(row.id);
    return;
  }

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

  // ── Dedup: if same URL is already being downloaded, wait for it ──
  if (inflightDownloads.has(replayUrl)) {
    log("DL", `Waiting for in-flight download of same replay...`);
    try {
      const srcFile = await inflightDownloads.get(replayUrl);
      if (srcFile && fs.existsSync(srcFile)) {
        await fsp.copyFile(srcFile, outputFile);
        await markDownloaded(row.id, outputFile);
        log("DL", `Shared download for ${shareCode} → match_${row.id}.dem`);
        triggerParse(row.id);
        return;
      }
    } catch (_) {
      // Original download failed — fall through to try our own
    }
  }

  // Start download and register in the in-flight map
  const downloadPromise = (async () => {
    await downloadReplay(replayUrl, outputFile);
    return outputFile;
  })();
  inflightDownloads.set(replayUrl, downloadPromise);

  try {
    await downloadPromise;
  } finally {
    // Clean up after a short delay (other matches may still be resolving)
    setTimeout(() => inflightDownloads.delete(replayUrl), 60_000);
  }

  await markDownloaded(row.id, outputFile);
  log("DL", `Downloaded ${shareCode} → match_${row.id}.dem`);

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
        const message = typeof error?.message === "string" ? error.message : "";
        const causeCode = error?.cause?.code;
        const isTransient =
          message.includes("502") ||
          message.includes("503") ||
          message.includes("504") ||
          causeCode === "ETIMEDOUT" ||
          causeCode === "ECONNRESET" ||
          message.includes("fetch failed") ||
          message.includes("ETIMEDOUT");

        if (isTransient) {
          retryAfterByMatchId.set(row.id, Date.now() + RETRY_COOLDOWN_MS);
          markPending(row.id).catch(() => {});
          log("DL", `Match ${row.id} re-queued (${causeCode || message.slice(0, 40)}).`);
        } else {
          console.error(`[DL] Match ${row.id} error:`, message);
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
  // Auto-accept incoming friend requests (bot never sends requests)
  if (relationship === SteamUser.EFriendRelationship.RequestRecipient) {
    client.addFriend(steamId);
    log("Steam", `Accepted friend request from ${steamId}.`);
  }
  // When a new friend is added, clear skip-cache and deliver tips immediately
  if (relationship === SteamUser.EFriendRelationship.Friend) {
    const sid = typeof steamId.getSteamID64 === "function"
      ? steamId.getSteamID64()
      : String(steamId);
    pendingFriendRequests.delete(sid);
    log("Tip", `${sid} is now a friend — delivering pending tips.`);
    sendPendingMessages().catch(() => {});
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
  const msg = error.message || String(error);
  console.error("[Steam] Client error:", msg);
  steamReady = false;
  gcReady = false;
  if (!disconnectedSince) {
    disconnectedSince = Date.now();
  }

  // LogonSessionReplaced means another login kicked us — exit immediately
  // so pm2 restarts fresh (reconnecting after this error always fails).
  if (msg === "LogonSessionReplaced" || error.eresult === 34) {
    log("Steam", "Session replaced by another login. Exiting for pm2 restart in 30s...");
    setTimeout(() => process.exit(1), 30_000);
    return;
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

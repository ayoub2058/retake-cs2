import { getSteamSessionFromCookies } from "@/lib/steamSessionServer";
import { supabaseAdmin } from "@/lib/supabaseAdmin";
import { RoundTimeline } from "@/app/_components/RoundTimeline";
import { MapIcon } from "@/app/_components/MapIcon";
import { cookies } from "next/headers";
import {
  getLocale,
  getTranslations,
  normalizeLanguage,
  LANG_COOKIE,
} from "@/lib/i18n";

const formatDate = (value: string | null, locale: string, fallback: string) => {
  if (!value) {
    return fallback;
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return fallback;
  }
  return new Intl.DateTimeFormat(locale, {
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(date);
};

const formatDuration = (value: number | null) => {
  if (!value || Number.isNaN(value)) {
    return "Unknown";
  }
  const totalSeconds = Math.max(0, Math.round(value));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}:${seconds.toString().padStart(2, "0")}`;
};

const extractMatchIdFromPath = (filePath: string | null) => {
  if (!filePath) {
    return null;
  }
  const match = filePath.match(/(\d+)/);
  if (!match) {
    return null;
  }
  const value = Number(match[1]);
  return Number.isNaN(value) ? null : value;
};

type MatchRow = {
  id: number;
  match_id: number;
  map_name: string | null;
  score_t: number | null;
  score_ct: number | null;
  winner: string | null;
  duration: number | null;
  match_date: string | null;
};

type PlayerRow = {
  steam_id: string;
  player_name: string | null;
  team_side: string | null;
  kills: number | null;
  deaths: number | null;
  assists: number | null;
  adr: number | null;
  hs_percent: number | null;
  opening_kills: number | null;
  opening_deaths: number | null;
  trade_kills: number | null;
  utility_damage: number | null;
};

type RoundRow = {
  id: number;
  match_id: number;
  round_number: number;
  winner_side: "CT" | "T" | null;
  reason: string | null;
  ct_score: number | null;
  t_score: number | null;
};

const extractMapFromPath = (filePath: string | null) => {
  if (!filePath) {
    return null;
  }
  const match = filePath.toLowerCase().match(/(de_[a-z0-9_]+)/);
  return match ? match[1] : null;
};


export default async function MatchDetailsPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const lang = normalizeLanguage((await cookies()).get(LANG_COOKIE)?.value);
  const t = getTranslations(lang);
  const locale = getLocale(lang);
  const { id: matchKey } = await params;
  const session = await getSteamSessionFromCookies();
  if (!session) {
    return (
      <div className="rounded-3xl glass-card p-8 text-white">
        <p className="text-xs uppercase tracking-[0.3em] text-[#67f5ff]">{t("matchDetails")}</p>
        <h1 className="mt-3 text-2xl font-semibold">{t("signInRequired")}</h1>
        <p className="mt-2 text-sm text-white/60">
          {t("signInToView")}
        </p>
      </div>
    );
  }


  const matchId = Number(matchKey);

  const baseSelect = "id, match_id, map_name, score_t, score_ct, winner, duration, match_date";
  const { data: matchRow, error: matchError } = await supabaseAdmin
    .from("matches")
    .select(baseSelect)
    .eq("match_id", matchKey)
    .maybeSingle();

  let resolvedMatch = matchRow ?? null;

  if (!resolvedMatch) {
    const { data: matchById } = await supabaseAdmin
      .from("matches")
      .select(baseSelect)
      .eq("id", matchId)
      .maybeSingle();
    resolvedMatch = matchById ?? null;
  }

  if (!resolvedMatch) {
    const { data: queueRow } = await supabaseAdmin
      .from("matches_to_download")
      .select("file_path")
      .eq("id", Number.isNaN(matchId) ? -1 : matchId)
      .maybeSingle();

    const derivedMatchId = extractMatchIdFromPath(queueRow?.file_path ?? null);
    if (derivedMatchId) {
      const { data: derivedMatchRow } = await supabaseAdmin
        .from("matches")
        .select(baseSelect)
        .eq("match_id", String(derivedMatchId))
        .maybeSingle();
      resolvedMatch = derivedMatchRow ?? null;
    }
  }

  if (matchError || !resolvedMatch) {
    return (
      <div className="rounded-3xl glass-card p-8 text-white">
        <p className="text-sm uppercase tracking-[0.3em] text-[#67f5ff]">{t("matchDetails")}</p>
        <h1 className="mt-3 text-2xl font-semibold">{t("matchNotFound")}</h1>
        <p className="mt-2 text-sm text-white/60">
          {t("matchNotFoundDesc")}
        </p>
        {matchError ? (
          <p className="mt-4 text-xs text-rose-300">{matchError.message}</p>
        ) : null}
      </div>
    );
  }

  const { data: players, error: playersError } = await supabaseAdmin
    .from("player_match_stats")
    .select(
      "steam_id, player_name, team_side, kills, deaths, assists, adr, hs_percent, opening_kills, opening_deaths, trade_kills, utility_damage"
    )
    .eq("match_id", resolvedMatch.id)
    .order("adr", { ascending: false });

  const { data: rounds } = await supabaseAdmin
    .from("rounds")
    .select("id, match_id, round_number, winner_side, reason, ct_score, t_score")
    .eq("match_id", resolvedMatch.id)
    .order("round_number", { ascending: true });

  if (playersError) {
    return (
      <div className="rounded-3xl glass-card p-8 text-white">
        <p className="text-sm uppercase tracking-[0.3em] text-[#67f5ff]">{t("matchDetails")}</p>
        <h1 className="mt-3 text-2xl font-semibold">{t("statsUnavailable")}</h1>
        <p className="mt-2 text-sm text-white/60">
          {t("statsUnavailableDesc")}
        </p>
      </div>
    );
  }

  const scoreText =
    resolvedMatch.score_ct !== null && resolvedMatch.score_t !== null
      ? `${resolvedMatch.score_ct}-${resolvedMatch.score_t}`
      : t("scoreUnavailable");

  const sortedRounds = (rounds ?? []).slice().sort((a: RoundRow, b: RoundRow) => {
    return a.round_number - b.round_number;
  });

  const currentUserId = session.steamId;
  const ctPlayers = (players ?? []).filter((player) => player.team_side === "CT");
  const tPlayers = (players ?? []).filter((player) => player.team_side === "T");
  const unknownPlayers = (players ?? []).filter(
    (player) => player.team_side !== "CT" && player.team_side !== "T"
  );
  const allPlayers = players ?? [];
  const mvp = [...allPlayers].sort((a, b) => {
    const adrA = a.adr ?? 0;
    const adrB = b.adr ?? 0;
    if (adrA !== adrB) {
      return adrB - adrA;
    }
    const kdA = (a.kills ?? 0) / Math.max(1, a.deaths ?? 0);
    const kdB = (b.kills ?? 0) / Math.max(1, b.deaths ?? 0);
    return kdB - kdA;
  })[0];

  const winnerLabel = mvp
    ? `${mvp.player_name || mvp.steam_id} ${t("team")}`
    : resolvedMatch.winner || t("unknown");

  let displayMap = resolvedMatch.map_name;
  if (!displayMap) {
    const { data: queueRow } = await supabaseAdmin
      .from("matches_to_download")
      .select("file_path")
      .eq("id", matchId)
      .maybeSingle();
    displayMap = extractMapFromPath(queueRow?.file_path ?? null);
  }

  const renderTable = (rows: PlayerRow[], label: string) => {
    if (!rows.length) {
      return null;
    }
    return (
      <div className="rounded-2xl glass-card">
        <div className="px-6 py-5 text-[11px] uppercase tracking-[0.35em] text-white/50 sm:px-8 sm:py-6">
          {label}
        </div>
        <div className="divide-y divide-white/5">
          {rows.map((player) => {
            const isCurrentUser = player.steam_id === currentUserId;
            const isMvp = player.steam_id === mvp?.steam_id;
            const rowClass = isCurrentUser
              ? "bg-[#d5ff4c]/10 text-white"
              : "text-zinc-200";
            const kills = player.kills ?? 0;
            const deaths = player.deaths ?? 0;
            const kdText = `${kills}/${deaths}`;
            const openingText = `${player.opening_kills ?? 0}/${player.opening_deaths ?? 0}`;
            return (
              <div key={player.steam_id} className={rowClass}>
                <div className="flex flex-wrap items-center justify-between gap-3 px-6 py-5 sm:px-8 sm:py-6">
                  <div className="flex items-center gap-2 text-base font-semibold">
                    <span>{player.player_name || player.steam_id}</span>
                    {isMvp ? (
                      <span className="rounded-full bg-[#d5ff4c]/20 px-2.5 py-1 text-[10px] uppercase tracking-[0.2em] text-[#d5ff4c]">
                        {t("mvp")}
                      </span>
                    ) : null}
                  </div>
                  <div className="rounded-full border border-white/10 bg-white/5 px-4 py-2 text-xs uppercase tracking-[0.2em] text-white/70">
                    {t("kdLabel")} {kdText}
                  </div>
                </div>
                <div className="grid gap-4 px-6 pb-6 text-sm sm:grid-cols-2 sm:px-8 lg:grid-cols-3">
                  <div className="rounded-xl border border-white/10 bg-black/40 px-5 py-4">
                    <p className="text-[10px] uppercase tracking-[0.25em] text-white/45">{t("adrLabel")}</p>
                    <p className="mt-2 text-xl font-semibold text-white">
                      {(player.adr ?? 0).toFixed(1)}
                    </p>
                  </div>
                  <div className="rounded-xl border border-white/10 bg-black/40 px-5 py-4">
                    <p className="text-[10px] uppercase tracking-[0.25em] text-white/45">{t("hsLabel")}</p>
                    <p className="mt-2 text-xl font-semibold text-white">
                      {(player.hs_percent ?? 0).toFixed(1)}
                    </p>
                  </div>
                  <div className="rounded-xl border border-white/10 bg-black/40 px-5 py-4">
                    <p className="text-[10px] uppercase tracking-[0.25em] text-white/45">{t("assists")}</p>
                    <p className="mt-2 text-xl font-semibold text-white">
                      {player.assists ?? 0}
                    </p>
                  </div>
                  <div className="rounded-xl border border-white/10 bg-black/40 px-5 py-4">
                    <p className="text-[10px] uppercase tracking-[0.25em] text-white/45">{t("openingKD")}</p>
                    <p className="mt-2 text-xl font-semibold text-white">
                      {openingText}
                    </p>
                  </div>
                  <div className="rounded-xl border border-white/10 bg-black/40 px-5 py-4">
                    <p className="text-[10px] uppercase tracking-[0.25em] text-white/45">{t("utilityDmg")}</p>
                    <p className="mt-2 text-xl font-semibold text-white">
                      {(player.utility_damage ?? 0).toFixed(1)}
                    </p>
                  </div>
                  <div className="rounded-xl border border-white/10 bg-black/40 px-5 py-4">
                    <p className="text-[10px] uppercase tracking-[0.25em] text-white/45">{t("tradeKills")}</p>
                    <p className="mt-2 text-xl font-semibold text-white">
                      {player.trade_kills ?? 0}
                    </p>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    );
  };

  return (
    <div className="w-full max-w-none space-y-6 sm:space-y-8">
      <div className="w-full max-w-none rounded-3xl glass-card p-6 sm:p-10">
        <div className="flex flex-col gap-6 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <p className="text-xs uppercase tracking-[0.35em] text-[#67f5ff]">
              {t("matchDetails")}
            </p>
            <div className="mt-3 flex items-center gap-4">
              <MapIcon mapName={displayMap} />
              <h1 className="text-2xl font-semibold sm:text-3xl">
                {displayMap || t("unknownMap")}
              </h1>
            </div>
            <p className="mt-2 text-sm text-white/60">
              {formatDate(resolvedMatch.match_date, locale, t("unknown"))} · {t("duration")}{" "}
              {formatDuration(resolvedMatch.duration)}
            </p>
          </div>
          <div className="w-full rounded-2xl border border-white/10 bg-white/5 px-5 py-4 text-center sm:w-auto">
            <p className="text-xs uppercase tracking-[0.3em] text-white/50">{t("finalScore")}</p>
            <p className="mt-2 text-2xl font-semibold text-[#d5ff4c]">{scoreText}</p>
            <p className="mt-1 text-xs text-white/50">
              {t("winner")}: {winnerLabel}
            </p>
          </div>
        </div>
      </div>

      {sortedRounds.length ? (
        <div className="w-full max-w-none rounded-3xl glass-card p-6 sm:p-10">
          <div className="mb-4">
            <p className="text-xs uppercase tracking-[0.35em] text-[#7dd3fc]">
              {t("roundTimeline")}
            </p>
          </div>
          <div className="my-6">
            <RoundTimeline rounds={sortedRounds} />
          </div>
        </div>
      ) : null}

      <div className="w-full max-w-none rounded-3xl glass-card p-6 sm:p-10">
        <div className="mb-6 flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <p className="text-xs uppercase tracking-[0.35em] text-[#d5ff4c]">
              {t("scoreboard")}
            </p>
            <h2 className="mt-2 text-2xl font-semibold sm:text-3xl">{t("fullMatchStats")}</h2>
          </div>
          {mvp ? (
            <div className="rounded-2xl border border-white/10 bg-white/5 px-5 py-4 text-sm text-white/70">
              {t("mvp")}: <span className="font-semibold text-white">{mvp.player_name || mvp.steam_id}</span>
            </div>
          ) : null}
        </div>

        <div className="grid gap-6 lg:grid-cols-2">
          {renderTable(ctPlayers, t("ctSquad"))}
          {renderTable(tPlayers, t("tSquad"))}
        </div>
        {unknownPlayers.length ? (
          <div className="mt-6">
            {renderTable(unknownPlayers, t("unassigned"))}
          </div>
        ) : null}
      </div>
    </div>
  );
}

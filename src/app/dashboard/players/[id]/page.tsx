import { PlayerStats } from "@/app/_components/PlayerStats";
import { WinRate } from "@/app/_components/WinRate";
import { supabaseAdmin } from "@/lib/supabaseAdmin";
import { cookies } from "next/headers";
import { getLocale, getTranslations, normalizeLanguage, LANG_COOKIE } from "@/lib/i18n";

type LeetifyMatch = Record<string, unknown>;

const LEETIFY_MATCHES_URL = "https://api-public.cs-prod.leetify.com/v3/profile/matches";
const LEETIFY_PROFILE_URL = "https://api-public.cs-prod.leetify.com/v3/profile";

const toStringOrNull = (value: unknown) => {
  if (value === null || value === undefined) {
    return null;
  }
  const str = String(value).trim();
  return str.length ? str : null;
};

const toNumberOrNull = (value: unknown) => {
  if (value === null || value === undefined) {
    return null;
  }
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
};

const formatDateLabel = (value: string | null, locale: string, fallback: string) => {
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

const getFaceitBadgeClass = (level: number) => {
  if (level >= 10) {
    return "bg-red-600 text-white";
  }
  if (level >= 5) {
    return "bg-orange-500 text-white";
  }
  return "bg-gray-500 text-white";
};

const fetchLeetifyMatches = async (steamId: string) => {
  const token = process.env.LEETIFY_API_TOKEN?.trim();
  if (!token) {
    return null;
  }
  const url = new URL(LEETIFY_MATCHES_URL);
  url.searchParams.set("steam64_id", steamId);
  const response = await fetch(url.toString(), {
    headers: {
      Authorization: `Bearer ${token}`,
      _leetify_key: token,
    },
    cache: "no-store",
  });
  if (!response.ok) {
    return null;
  }
  return (await response.json()) as Record<string, unknown> | unknown[];
};

const fetchLeetifyProfile = async (steamId: string) => {
  const token = process.env.LEETIFY_API_TOKEN?.trim();
  if (!token) {
    return null;
  }
  const url = new URL(LEETIFY_PROFILE_URL);
  url.searchParams.set("steam64_id", steamId);
  const response = await fetch(url.toString(), {
    headers: {
      Authorization: `Bearer ${token}`,
      _leetify_key: token,
    },
    cache: "no-store",
  });
  if (!response.ok) {
    return null;
  }
  return (await response.json()) as Record<string, unknown>;
};

const findPlayerStats = (match: Record<string, unknown>, steamId: string) => {
  const stats = match.stats;
  if (!Array.isArray(stats)) {
    return null;
  }
  for (const entry of stats) {
    if (!entry || typeof entry !== "object") {
      continue;
    }
    const item = entry as Record<string, unknown>;
    const id =
      toStringOrNull(item.steam64_id) ||
      toStringOrNull(item.steam_id) ||
      toStringOrNull(item.steamId) ||
      toStringOrNull(item.steamid) ||
      toStringOrNull(item.id);
    if (id === steamId) {
      return item;
    }
  }
  return null;
};

export default async function PlayerProfilePage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const lang = normalizeLanguage((await cookies()).get(LANG_COOKIE)?.value);
  const t = getTranslations(lang);
  const locale = getLocale(lang);
  const { id } = await params;
  const steamId = id;

  const { data: matches, error } = await supabaseAdmin
    .from("player_match_stats")
    .select("id")
    .eq("steam_id", steamId)
    .limit(1);

  if (error) {
    return (
      <div className="rounded-3xl glass-card p-8 text-white">
        <p className="text-xs uppercase tracking-[0.35em] text-[#67f5ff]">{t("playerProfile")}</p>
        <h1 className="mt-3 text-2xl font-semibold">{t("profileUnavailable")}</h1>
        <p className="mt-2 text-sm text-white/60">
          {t("failedLoadMatchHistory", { steamId })}
        </p>
      </div>
    );
  }

  const hasMatches = (matches ?? []).length > 0;

  if (!hasMatches) {
    const leetifyMatches = await fetchLeetifyMatches(steamId);
    const leetifyProfile = await fetchLeetifyProfile(steamId);
    const data = leetifyMatches as Record<string, unknown> | unknown[] | null;
    const matchesList = (() => {
      if (Array.isArray(data)) {
        return data as LeetifyMatch[];
      }
      if (data && typeof data === "object") {
        const container = data as Record<string, unknown>;
        if (Array.isArray(container.matches)) {
          return container.matches as LeetifyMatch[];
        }
        if (Array.isArray(container.data)) {
          return container.data as LeetifyMatch[];
        }
        if (Array.isArray(container.results)) {
          return container.results as LeetifyMatch[];
        }
      }
      return [] as LeetifyMatch[];
    })();

    const hasLeetifyProfile = Boolean(leetifyProfile);
    const hasLeetifyMatches = matchesList.length > 0;
    if (hasLeetifyProfile || hasLeetifyMatches) {
      const profileName = toStringOrNull(leetifyProfile?.name);
      const winRate = toNumberOrNull(leetifyProfile?.winrate);
      const totalMatches = toNumberOrNull(leetifyProfile?.total_matches);
      const firstMatch = toStringOrNull(leetifyProfile?.first_match_date);
      const ranks = leetifyProfile?.ranks as Record<string, unknown> | undefined;
      const premierRank = toNumberOrNull(ranks?.premier);
      const faceitRank = toNumberOrNull(ranks?.faceit);
      const rating = leetifyProfile?.rating as Record<string, unknown> | undefined;
      const aim = toNumberOrNull(rating?.aim);
      const positioning = toNumberOrNull(rating?.positioning);
      const utility = toNumberOrNull(rating?.utility);
      const stats = leetifyProfile?.stats as Record<string, unknown> | undefined;
      const reactionTime = toNumberOrNull(stats?.reaction_time_ms);
      const accuracyHead = toNumberOrNull(stats?.accuracy_head);
      return (
        <div className="w-full max-w-none space-y-6">
          <div className="rounded-3xl glass-card p-8 text-white">
            <p className="text-xs uppercase tracking-[0.35em] text-[#67f5ff]">{t("playerProfile")}</p>
            <h1 className="mt-3 text-2xl font-semibold">
              {profileName || t("leetifyPlayer")}
            </h1>
            <p className="mt-2 text-sm text-white/60">{t("steamIdLabel")}: {steamId}</p>
            <p className="mt-2 text-sm text-white/60">
              {t("notInDatabase")}
            </p>
            <p className="mt-2 text-xs uppercase tracking-[0.35em] text-[#7dd3fc]">
              {t("leetifyMatchHistory")}
            </p>
            {firstMatch ? (
              <p className="mt-2 text-xs text-white/50">
                {t("firstMatch")}: {formatDateLabel(firstMatch, locale, t("unknown"))}
              </p>
            ) : null}
          </div>

          {hasLeetifyProfile ? (
            <div className="grid gap-6 md:grid-cols-2 xl:grid-cols-4">
              {typeof totalMatches === "number" ? (
                <div className="rounded-2xl glass-card p-5 text-white">
                  <p className="text-xs uppercase tracking-[0.3em] text-white/60">{t("totalMatches")}</p>
                  <p className="mt-2 text-2xl font-semibold">{totalMatches}</p>
                </div>
              ) : null}
              {typeof winRate === "number" ? (
                <div className="rounded-2xl glass-card p-5 text-white">
                  <p className="text-xs uppercase tracking-[0.3em] text-white/60">{t("winRate")}</p>
                  <p className="mt-2 text-2xl font-semibold">{(winRate * 100).toFixed(1)}%</p>
                </div>
              ) : null}
              {typeof premierRank === "number" ? (
                <div className="rounded-2xl glass-card p-5 text-white">
                  <p className="text-xs uppercase tracking-[0.3em] text-white/60">{t("premier")}</p>
                  <p className="mt-2 text-2xl font-semibold">{premierRank}</p>
                </div>
              ) : null}
              {typeof faceitRank === "number" ? (
                <div className="rounded-2xl glass-card p-5 text-white">
                  <p className="text-xs uppercase tracking-[0.3em] text-white/60">{t("faceit")}</p>
                  <div className="mt-2 flex items-center gap-3">
                    <span
                      className={`flex h-8 w-8 items-center justify-center rounded-full text-sm font-bold ${getFaceitBadgeClass(faceitRank)}`}
                    >
                      {Math.round(faceitRank)}
                    </span>
                    <span className="text-2xl font-semibold">{t("level")}</span>
                  </div>
                </div>
              ) : null}
              {typeof aim === "number" ? (
                <div className="rounded-2xl glass-card p-5 text-white">
                  <p className="text-xs uppercase tracking-[0.3em] text-white/60">{t("aim")}</p>
                  <p className="mt-2 text-2xl font-semibold">{aim.toFixed(1)}</p>
                </div>
              ) : null}
              {typeof positioning === "number" ? (
                <div className="rounded-2xl glass-card p-5 text-white">
                  <p className="text-xs uppercase tracking-[0.3em] text-white/60">{t("positioning")}</p>
                  <p className="mt-2 text-2xl font-semibold">{positioning.toFixed(1)}</p>
                </div>
              ) : null}
              {typeof utility === "number" ? (
                <div className="rounded-2xl glass-card p-5 text-white">
                  <p className="text-xs uppercase tracking-[0.3em] text-white/60">{t("utility")}</p>
                  <p className="mt-2 text-2xl font-semibold">{utility.toFixed(1)}</p>
                </div>
              ) : null}
              {typeof reactionTime === "number" ? (
                <div className="rounded-2xl glass-card p-5 text-white">
                  <p className="text-xs uppercase tracking-[0.3em] text-white/60">{t("reactionTime")}</p>
                  <p className="mt-2 text-2xl font-semibold">{reactionTime.toFixed(0)}</p>
                </div>
              ) : null}
              {typeof accuracyHead === "number" ? (
                <div className="rounded-2xl border border-white/10 bg-black/60 p-5 text-white">
                  <p className="text-xs uppercase tracking-[0.3em] text-white/60">{t("hsAccuracy")}</p>
                  <p className="mt-2 text-2xl font-semibold">{accuracyHead.toFixed(1)}%</p>
                </div>
              ) : null}
            </div>
          ) : null}

          {hasLeetifyMatches ? (
            <div className="rounded-2xl glass-card p-5 text-white">
              <p className="text-xs uppercase tracking-[0.3em] text-white/60">{t("recentMatches")}</p>
              <div className="mt-4 divide-y divide-white/10">
                {matchesList.slice(0, 10).map((match, index) => {
                  const mapName =
                    toStringOrNull(match.map_name) ||
                    toStringOrNull(match.mapName) ||
                    toStringOrNull(match.map) ||
                    t("unknownMap");
                  let ctScore: string | null = null;
                  let tScore: string | null = null;
                  const teamScores = Array.isArray(match.team_scores)
                    ? (match.team_scores as Array<Record<string, unknown>>)
                    : [];
                  for (const teamScore of teamScores) {
                    const teamNumber = toNumberOrNull(teamScore.team_number);
                    const scoreValue = toNumberOrNull(teamScore.score);
                    if (teamNumber === 3 && scoreValue !== null) {
                      ctScore = String(scoreValue);
                    }
                    if (teamNumber === 2 && scoreValue !== null) {
                      tScore = String(scoreValue);
                    }
                  }
                  const scoreLabel =
                    ctScore && tScore ? `${ctScore}-${tScore}` : t("scoreUnavailable");
                  const startedAt =
                    toStringOrNull(match.started_at) ||
                    toStringOrNull(match.startedAt) ||
                    toStringOrNull(match.date) ||
                    toStringOrNull(match.created_at) ||
                    toStringOrNull(match.finished_at);
                  const playerStats = findPlayerStats(match, steamId);
                  const kills = toNumberOrNull(playerStats?.total_kills);
                  const deaths = toNumberOrNull(playerStats?.total_deaths);
                  const kdRatio = toNumberOrNull(playerStats?.kd_ratio);
                  const rounds = toNumberOrNull(playerStats?.rounds_count);
                  const totalDamage = toNumberOrNull(playerStats?.total_damage);
                  const dpr = toNumberOrNull(playerStats?.dpr);
                  const adr = totalDamage !== null && rounds ? totalDamage / rounds : dpr;
                  const hsKills = toNumberOrNull(playerStats?.total_hs_kills);
                  const accuracyHead = toNumberOrNull(playerStats?.accuracy_head);
                  const hs =
                    hsKills !== null && kills
                      ? (hsKills / kills) * 100
                      : accuracyHead !== null
                        ? accuracyHead * 100
                        : null;
                  const kd = kdRatio ??
                    (typeof kills === "number" && typeof deaths === "number"
                      ? deaths > 0
                        ? kills / deaths
                        : kills
                      : null);
                  return (
                    <div key={`${mapName}-${index}`} className="flex flex-wrap items-center justify-between gap-3 py-3">
                      <div>
                        <div className="text-sm font-semibold text-white">{mapName}</div>
                        <div className="text-xs text-white/50">
                          {formatDateLabel(startedAt, locale, t("unknown"))}
                        </div>
                        <div className="mt-2 flex flex-wrap gap-3 text-xs text-white/60">
                          {typeof adr === "number" ? <span>{t("adrLabel")} {adr.toFixed(1)}</span> : null}
                          {typeof kd === "number" ? <span>{t("kdLabel")} {kd.toFixed(2)}</span> : null}
                          {typeof hs === "number" ? <span>{t("hsLabel")} {hs.toFixed(1)}</span> : null}
                        </div>
                      </div>
                      <div className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-white/70">
                        {scoreLabel}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          ) : (
            <div className="rounded-2xl border border-white/10 bg-black/60 p-5 text-white">
              <p className="text-xs uppercase tracking-[0.3em] text-white/60">{t("recentMatches")}</p>
              <p className="mt-3 text-sm text-white/60">{t("noLeetifyMatches")}</p>
            </div>
          )}

          <a
            href={`https://leetify.com/public/profile/${encodeURIComponent(steamId)}`}
            target="_blank"
            rel="noreferrer"
            className="inline-flex items-center justify-center rounded-2xl border border-white/10 bg-white/5 px-5 py-3 text-sm font-semibold text-white transition hover:border-white/40 hover:bg-white/10"
          >
            {t("viewOnLeetify")}
          </a>
        </div>
      );
    }

    return (
      <div className="rounded-3xl glass-card p-8 text-white">
        <p className="text-xs uppercase tracking-[0.35em] text-[#67f5ff]">{t("playerProfile")}</p>
        <h1 className="mt-3 text-2xl font-semibold">{t("newPlayer")}</h1>
        <p className="mt-2 text-sm text-white/60">
          {t("steamIdLabel")}: {steamId}
        </p>
        <p className="mt-2 text-sm text-white/60">
          {t("notInDatabase")}
        </p>
        <p className="mt-4 text-sm text-white/60">{t("noMatchHistoryFound")}</p>
        <a
          href={`https://leetify.com/public/profile/${encodeURIComponent(steamId)}`}
          target="_blank"
          rel="noreferrer"
          className="mt-6 inline-flex items-center justify-center rounded-2xl border border-white/10 bg-white/5 px-5 py-3 text-sm font-semibold text-white transition hover:border-white/40 hover:bg-white/10"
        >
          {t("viewOnLeetify")}
        </a>
      </div>
    );
  }

  return (
    <div className="w-full max-w-none space-y-6">
      <div className="rounded-3xl glass-card p-6 text-white">
        <p className="text-xs uppercase tracking-[0.35em] text-[#67f5ff]">{t("playerProfile")}</p>
        <h1 className="mt-3 text-2xl font-semibold">{t("steamIdLabel")}: {steamId}</h1>
        <p className="mt-2 text-sm text-white/60">{t("localHistoryAvailable")}</p>
      </div>

      <div className="grid gap-6 lg:grid-cols-[280px,1fr]">
        <WinRate userId={steamId} />
        <PlayerStats userId={steamId} />
      </div>
    </div>
  );
}

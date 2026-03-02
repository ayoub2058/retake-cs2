import { redirect } from "next/navigation";
import { createServerSupabaseClient } from "@/lib/supabaseServer";
import { getSteamSessionFromCookies } from "@/lib/steamSessionServer";
import { MatchTable } from "@/app/_components/MatchTable";
import { BotSettings } from "@/app/_components/BotSettings";
import { RecentForm } from "@/app/_components/RecentForm";
import { QuickStatCard } from "@/app/_components/QuickStatCard";
import { cookies } from "next/headers";
import { getTranslations, normalizeLanguage, LANG_COOKIE } from "@/lib/i18n";

export default async function DashboardPage() {
  const lang = normalizeLanguage((await cookies()).get(LANG_COOKIE)?.value);
  const t = getTranslations(lang);
  const session = await getSteamSessionFromCookies();
  if (!session) {
    redirect("/");
  }

  const supabase = await createServerSupabaseClient();
  const { data: matches, error } = await supabase
    .from("matches_to_download")
    .select("id, created_at, status")
    .eq("user_id", session.steamId)
    .order("created_at", { ascending: false });

  if (error) {
    throw new Error("Failed to load matches.");
  }

  const rows = matches ?? [];
  const matchIds = rows.map((row) => row.id);
  let mapNameByMatchId = new Map<number, string | null>();
  let scoreByMatchId = new Map<number, { score_ct: number | null; score_t: number | null; winner: string | null }>();
  let userTeamByMatchId = new Map<number, string | null>();

  if (matchIds.length > 0) {
    const { data: mapRows } = await supabase
      .from("matches")
      .select("id, match_id, map_name, score_ct, score_t, winner")
      .in("match_id", matchIds.map((id) => String(id)));
    if (mapRows) {
      mapNameByMatchId = new Map(
        mapRows.map((row) => [Number(row.match_id), row.map_name ?? null])
      );
      scoreByMatchId = new Map(
        mapRows.map((row) => [
          Number(row.match_id),
          { score_ct: row.score_ct, score_t: row.score_t, winner: row.winner },
        ])
      );

      // Fetch the user's team side for each match
      const matchRowIds = mapRows.map((r) => r.id);
      if (matchRowIds.length > 0) {
        const { data: playerRows } = await supabase
          .from("player_match_stats")
          .select("match_id, team_side")
          .in("match_id", matchRowIds)
          .eq("steam_id", session.steamId);
        if (playerRows) {
          // map match_id (internal) back to matches_to_download id
          const internalToDownloadId = new Map(
            mapRows.map((r) => [r.id, Number(r.match_id)])
          );
          for (const pr of playerRows) {
            const downloadId = internalToDownloadId.get(pr.match_id);
            if (downloadId !== undefined) {
              userTeamByMatchId.set(downloadId, pr.team_side ?? null);
            }
          }
        }
      }
    }
  }

  const mappedRows = rows.map((row) => {
    const score = scoreByMatchId.get(row.id);
    const userTeam = userTeamByMatchId.get(row.id);
    let result: "win" | "loss" | "tie" | null = null;
    if (score?.winner && userTeam) {
      if (score.winner === "Tie") {
        result = "tie";
      } else if (score.winner === userTeam) {
        result = "win";
      } else {
        result = "loss";
      }
    }
    let scoreText: string | null = null;
    if (score?.score_ct != null && score?.score_t != null) {
      if (userTeam === "CT") {
        scoreText = `${score.score_ct}-${score.score_t}`;
      } else if (userTeam === "T") {
        scoreText = `${score.score_t}-${score.score_ct}`;
      } else {
        scoreText = `${score.score_ct}-${score.score_t}`;
      }
    }
    return {
      ...row,
      map_name: mapNameByMatchId.get(row.id) ?? null,
      result,
      score: scoreText,
      status: row.status ?? null,
    };
  });

  // ── Quick Stats computation ──
  let totalKills = 0;
  let totalDeaths = 0;
  let totalAdr = 0;
  let statsCount = 0;
  const mapWins: Record<string, number> = {};
  const mapTotal: Record<string, number> = {};

  if (matchIds.length > 0) {
    // Get the internal match IDs for this user
    const { data: mapRows } = await supabase
      .from("matches")
      .select("id, match_id, map_name, winner")
      .in("match_id", matchIds.map((id) => String(id)));

    const matchRowIds = (mapRows ?? []).map((r) => r.id);
    if (matchRowIds.length > 0) {
      const { data: pStats } = await supabase
        .from("player_match_stats")
        .select("match_id, kills, deaths, adr, team_side")
        .in("match_id", matchRowIds)
        .eq("steam_id", session.steamId);
      if (pStats) {
        for (const ps of pStats) {
          totalKills += ps.kills ?? 0;
          totalDeaths += ps.deaths ?? 0;
          totalAdr += ps.adr ?? 0;
          statsCount += 1;

          // Map win rate
          const matchInfo = (mapRows ?? []).find((m) => m.id === ps.match_id);
          if (matchInfo?.map_name) {
            const mapKey = matchInfo.map_name;
            mapTotal[mapKey] = (mapTotal[mapKey] ?? 0) + 1;
            if (matchInfo.winner && matchInfo.winner === ps.team_side) {
              mapWins[mapKey] = (mapWins[mapKey] ?? 0) + 1;
            }
          }
        }
      }
    }
  }

  const avgKd = totalDeaths > 0 ? (totalKills / totalDeaths).toFixed(2) : (totalKills > 0 ? totalKills.toFixed(2) : "—");
  const avgAdr = statsCount > 0 ? (totalAdr / statsCount).toFixed(1) : "—";
  const totalWins = mappedRows.filter((m) => m.result === "win").length;
  const winRateNum = rows.length > 0 ? Math.round((totalWins / rows.length) * 100) : 0;
  const winRateStr = rows.length > 0 ? `${winRateNum}%` : "—";

  // Best map
  let bestMap = "—";
  let bestMapRate = 0;
  for (const [map, total] of Object.entries(mapTotal)) {
    if (total < 1) continue;
    const rate = (mapWins[map] ?? 0) / total;
    if (rate > bestMapRate || (rate === bestMapRate && total > (mapTotal[bestMap] ?? 0))) {
      bestMapRate = rate;
      bestMap = map.replace(/^de_/, "").charAt(0).toUpperCase() + map.replace(/^de_/, "").slice(1);
    }
  }
  if (Object.keys(mapTotal).length === 0) bestMap = "—";

  // Map stats for MapWinRate component
  const mapStatsArray = Object.entries(mapTotal).map(([map, total]) => ({
    map,
    wins: mapWins[map] ?? 0,
    total,
    winRate: Math.round(((mapWins[map] ?? 0) / total) * 100),
  })).sort((a, b) => b.total - a.total);

  return (
    <div className="space-y-8 animate-fade-in">
      {/* Quick Stats Cards */}
      {rows.length > 0 && (
        <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
          <QuickStatCard
            label={t("totalMatches")}
            value={String(rows.length)}
            numericValue={rows.length}
            accent="bg-[#d5ff4c]/10"
            delay={0}
            icon={
              <svg className="h-5 w-5 text-[#d5ff4c]" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <rect x="3" y="3" width="7" height="7" rx="1" />
                <rect x="14" y="3" width="7" height="7" rx="1" />
                <rect x="3" y="14" width="7" height="7" rx="1" />
                <rect x="14" y="14" width="7" height="7" rx="1" />
              </svg>
            }
          />
          <QuickStatCard
            label={t("winRate")}
            value={winRateStr}
            numericValue={winRateNum}
            suffix="%"
            accent="bg-emerald-500/10"
            delay={80}
            icon={
              <svg className="h-5 w-5 text-emerald-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" />
                <polyline points="22 4 12 14.01 9 11.01" />
              </svg>
            }
          />
          <QuickStatCard
            label={t("avgKD")}
            value={avgKd}
            numericValue={totalDeaths > 0 ? totalKills / totalDeaths : (totalKills > 0 ? totalKills : undefined)}
            decimals={2}
            accent="bg-[#67f5ff]/10"
            delay={160}
            icon={
              <svg className="h-5 w-5 text-[#67f5ff]" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <circle cx="12" cy="12" r="10" /><line x1="14.31" y1="8" x2="20.05" y2="17.94" /><line x1="9.69" y1="8" x2="21.17" y2="8" /><line x1="7.38" y1="12" x2="13.12" y2="2.06" /><line x1="9.69" y1="16" x2="3.95" y2="6.06" /><line x1="14.31" y1="16" x2="2.83" y2="16" /><line x1="16.62" y1="12" x2="10.88" y2="21.94" />
              </svg>
            }
          />
          <QuickStatCard
            label={t("bestMap")}
            value={bestMap}
            accent="bg-[#a78bfa]/10"
            delay={240}
            icon={
              <svg className="h-5 w-5 text-[#a78bfa]" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <polygon points="1 6 1 22 8 18 16 22 23 18 23 2 16 6 8 2 1 6" />
                <line x1="8" y1="2" x2="8" y2="18" /><line x1="16" y1="6" x2="16" y2="22" />
              </svg>
            }
          />
        </div>
      )}

      <BotSettings userId={session.steamId} />

      <div className="rounded-3xl glass-card p-8">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <p className="text-[10px] uppercase tracking-[0.4em] text-[#d5ff4c]">
              {t("matchIntelligence")}
            </p>
            <h1 className="mt-3 text-3xl font-bold text-white">
              {t("yourMatchHistory")}
            </h1>
            <p className="mt-1 text-sm text-white/40">
              {rows.length} {rows.length === 1 ? "match" : "matches"} tracked
            </p>
          </div>
          <RecentForm results={mappedRows.map((m) => m.result)} />
        </div>

        {rows.length === 0 ? (
          <div className="mt-8 flex flex-col items-center justify-center rounded-2xl border border-dashed border-white/10 px-8 py-16 text-center">
            <div className="mb-4 flex h-16 w-16 items-center justify-center rounded-2xl border border-white/10 bg-white/5">
              <svg className="h-7 w-7 text-white/30" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
                <polyline points="17 8 12 3 7 8" />
                <line x1="12" y1="3" x2="12" y2="15" />
              </svg>
            </div>
            <p className="text-base font-medium text-white/70">{t("noMatchesFound")}</p>
            <p className="mt-2 max-w-md text-sm text-white/40">
              Play a competitive match and the bot will automatically download and analyze it for you.
            </p>
          </div>
        ) : (
          <MatchTable matches={mappedRows} />
        )}
      </div>

      {/* Map Win Rate Breakdown */}
      {mapStatsArray.length > 0 && (
        <div className="rounded-3xl glass-card p-8 animate-fade-in">
          <p className="text-[10px] uppercase tracking-[0.4em] text-[#a78bfa]">
            {t("mapBreakdown")}
          </p>
          <h2 className="mt-3 text-2xl font-bold text-white">{t("winRateByMap")}</h2>
          <div className="mt-6 grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            {mapStatsArray.map((mapStat) => {
              const displayName = mapStat.map.replace(/^de_/, "").charAt(0).toUpperCase() + mapStat.map.replace(/^de_/, "").slice(1);
              const barColor =
                mapStat.winRate >= 60 ? "bg-emerald-400" :
                mapStat.winRate >= 45 ? "bg-[#d5ff4c]" :
                "bg-rose-400";
              return (
                <div key={mapStat.map} className="rounded-xl border border-white/[0.06] bg-black/30 p-4">
                  <div className="flex items-center justify-between">
                    <span className="text-sm font-semibold text-white">{displayName}</span>
                    <span className="text-xs text-white/40">{mapStat.wins}W {mapStat.total - mapStat.wins}L</span>
                  </div>
                  <div className="mt-3 flex items-center gap-3">
                    <div className="h-2 flex-1 overflow-hidden rounded-full bg-white/10">
                      <div
                        className={`h-full rounded-full ${barColor} transition-all duration-700`}
                        style={{ width: `${mapStat.winRate}%` }}
                      />
                    </div>
                    <span className={`text-sm font-bold ${mapStat.winRate >= 50 ? "text-emerald-400" : "text-rose-400"}`}>
                      {mapStat.winRate}%
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}

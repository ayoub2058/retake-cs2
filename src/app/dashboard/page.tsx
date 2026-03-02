import { redirect } from "next/navigation";
import { createServerSupabaseClient } from "@/lib/supabaseServer";
import { getSteamSessionFromCookies } from "@/lib/steamSessionServer";
import { MatchTable } from "@/app/_components/MatchTable";
import { BotSettings } from "@/app/_components/BotSettings";
import { RecentForm } from "@/app/_components/RecentForm";
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
    .select("id, created_at")
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
    };
  });

  return (
    <div className="space-y-8 animate-fade-in">
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
    </div>
  );
}

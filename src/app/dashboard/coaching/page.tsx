import { redirect } from "next/navigation";
import { getSteamSessionFromCookies } from "@/lib/steamSessionServer";
import { createServerSupabaseClient } from "@/lib/supabaseServer";
import { supabaseAdmin } from "@/lib/supabaseAdmin";
import { MapIcon } from "@/app/_components/MapIcon";
import Link from "next/link";
import { cookies } from "next/headers";
import {
  getLocale,
  getTranslations,
  normalizeLanguage,
  LANG_COOKIE,
} from "@/lib/i18n";

const formatDate = (value: string | null, locale: string, fallback: string) => {
  if (!value) return fallback;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return fallback;
  return new Intl.DateTimeFormat(locale, {
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(date);
};

export default async function CoachingHistoryPage() {
  const lang = normalizeLanguage((await cookies()).get(LANG_COOKIE)?.value);
  const t = getTranslations(lang);
  const locale = getLocale(lang);
  const session = await getSteamSessionFromCookies();
  if (!session) redirect("/");

  const supabase = await createServerSupabaseClient();

  // Fetch all matches with coaching tips
  const { data: tipsRows } = await supabase
    .from("matches_to_download")
    .select("id, created_at, coach_tip, tip_image_url")
    .eq("user_id", session.steamId)
    .not("coach_tip", "is", null)
    .order("created_at", { ascending: false });

  const tips = tipsRows ?? [];

  // Get map names for these matches
  const tipIds = tips.map((t) => t.id);
  let mapByTipId = new Map<number, string | null>();
  let resultByTipId = new Map<number, "win" | "loss" | "tie" | null>();

  if (tipIds.length > 0) {
    const { data: matchRows } = await supabaseAdmin
      .from("matches")
      .select("match_id, map_name, winner")
      .in("match_id", tipIds.map((id) => String(id)));

    if (matchRows) {
      for (const row of matchRows) {
        mapByTipId.set(Number(row.match_id), row.map_name ?? null);
      }

      // Get user's team per match
      const matchInternalIds = matchRows.map((r) => r.match_id);
      // We need internal IDs for player_match_stats lookup
      const { data: matchIdRows } = await supabaseAdmin
        .from("matches")
        .select("id, match_id, winner")
        .in("match_id", tipIds.map((id) => String(id)));

      if (matchIdRows) {
        const internalIds = matchIdRows.map((r) => r.id);
        const { data: playerRows } = await supabaseAdmin
          .from("player_match_stats")
          .select("match_id, team_side")
          .in("match_id", internalIds)
          .eq("steam_id", session.steamId);

        if (playerRows) {
          const internalToDownload = new Map(matchIdRows.map((r) => [r.id, { downloadId: Number(r.match_id), winner: r.winner }]));
          for (const pr of playerRows) {
            const info = internalToDownload.get(pr.match_id);
            if (info) {
              if (info.winner === "Tie") resultByTipId.set(info.downloadId, "tie");
              else if (info.winner && info.winner === pr.team_side) resultByTipId.set(info.downloadId, "win");
              else if (info.winner) resultByTipId.set(info.downloadId, "loss");
            }
          }
        }
      }
    }
  }

  return (
    <div className="space-y-6 animate-fade-in">
      <div className="rounded-3xl glass-card p-8">
        <p className="text-[10px] uppercase tracking-[0.4em] text-[#a78bfa]">
          {t("coachingHistory")}
        </p>
        <h1 className="mt-3 text-3xl font-bold text-white">
          {t("aiCoachingTips")}
        </h1>
        <p className="mt-1 text-sm text-white/40">
          {tips.length} {tips.length === 1 ? "analysis" : "analyses"} generated
        </p>
      </div>

      {tips.length === 0 ? (
        <div className="rounded-3xl glass-card p-8">
          <div className="flex flex-col items-center justify-center rounded-2xl border border-dashed border-white/10 px-8 py-16 text-center">
            <div className="mb-4 flex h-16 w-16 items-center justify-center rounded-2xl border border-white/10 bg-white/5">
              <svg className="h-7 w-7 text-white/30" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <path d="M12 20h9" />
                <path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z" />
              </svg>
            </div>
            <p className="text-base font-medium text-white/70">{t("noCoachingTips")}</p>
            <p className="mt-2 max-w-md text-sm text-white/40">
              {t("noCoachingTipsDesc")}
            </p>
          </div>
        </div>
      ) : (
        <div className="space-y-4">
          {tips.map((tip, index) => {
            const mapName = mapByTipId.get(tip.id) ?? null;
            const result = resultByTipId.get(tip.id) ?? null;
            const preview = tip.coach_tip
              ? tip.coach_tip.length > 250
                ? tip.coach_tip.slice(0, 250) + "..."
                : tip.coach_tip
              : "";
            const resultBadge = result === "win"
              ? "bg-emerald-500/15 border-emerald-500/20 text-emerald-400"
              : result === "loss"
                ? "bg-rose-500/15 border-rose-500/20 text-rose-400"
                : result === "tie"
                  ? "bg-amber-500/15 border-amber-500/20 text-amber-400"
                  : null;
            const resultLabel = result === "win" ? "WIN" : result === "loss" ? "LOSS" : result === "tie" ? "TIE" : null;

            return (
              <div
                key={tip.id}
                className="rounded-2xl glass-card p-6 animate-fade-in"
                style={{ animationDelay: `${index * 60}ms` }}
              >
                <div className="flex flex-wrap items-center justify-between gap-3 mb-4">
                  <div className="flex items-center gap-3">
                    <MapIcon mapName={mapName} />
                    <div>
                      <div className="flex items-center gap-2">
                        <span className="text-sm font-semibold text-white">
                          {mapName
                            ? mapName.split("_").map((s) => s.charAt(0).toUpperCase() + s.slice(1)).join(" ")
                            : t("unknownMap")}
                        </span>
                        {resultBadge && resultLabel && (
                          <span className={`inline-block rounded-md border px-2 py-0.5 text-[9px] font-bold uppercase tracking-[0.15em] ${resultBadge}`}>
                            {resultLabel}
                          </span>
                        )}
                      </div>
                      <p className="text-xs text-white/40">
                        {formatDate(tip.created_at, locale, t("unknown"))}
                      </p>
                    </div>
                  </div>
                  <Link
                    href={`/dashboard/matches/${tip.id}`}
                    className="inline-flex items-center gap-2 rounded-lg border border-white/10 bg-white/[0.04] px-4 py-2 text-[10px] font-semibold uppercase tracking-[0.2em] text-white/70 transition-all hover:border-[#a78bfa]/30 hover:bg-[#a78bfa]/10 hover:text-[#a78bfa]"
                  >
                    {t("viewMatch")}
                    <svg className="h-3 w-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <path d="M5 12h14" /><path d="m12 5 7 7-7 7" />
                    </svg>
                  </Link>
                </div>
                <div className="rounded-xl border border-white/[0.06] bg-black/30 px-5 py-4">
                  <p className="whitespace-pre-line text-sm leading-relaxed text-white/70">
                    {preview}
                  </p>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

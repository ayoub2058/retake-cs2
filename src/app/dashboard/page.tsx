import { redirect } from "next/navigation";
import { createServerSupabaseClient } from "@/lib/supabaseServer";
import { getSteamSessionFromCookies } from "@/lib/steamSessionServer";
import { MatchTable } from "@/app/_components/MatchTable";
import { BotSettings } from "@/app/_components/BotSettings";
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
  if (matchIds.length > 0) {
    const { data: mapRows } = await supabase
      .from("matches")
      .select("match_id, map_name")
      .in("match_id", matchIds.map((id) => String(id)));
    if (mapRows) {
      mapNameByMatchId = new Map(
        mapRows.map((row) => [Number(row.match_id), row.map_name ?? null])
      );
    }
  }
  const mappedRows = rows.map((row) => ({
    ...row,
    map_name: mapNameByMatchId.get(row.id) ?? null,
  }));

  return (
    <div className="space-y-10">
      <BotSettings userId={session.steamId} />

      <div className="rounded-3xl glass-card p-8">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <p className="text-xs uppercase tracking-[0.35em] text-[#d5ff4c]">
              {t("matchIntelligence")}
            </p>
            <h1 className="mt-3 text-3xl font-semibold text-white">
              {t("yourMatchHistory")}
            </h1>
          </div>
        </div>

        {rows.length === 0 ? (
          <div className="mt-8 rounded-2xl glass-card px-6 py-8 text-zinc-300">
            {t("noMatchesFound")}
          </div>
        ) : (
          <MatchTable matches={mappedRows} />
        )}
      </div>
    </div>
  );
}

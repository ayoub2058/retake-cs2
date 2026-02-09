"use client";

import { useEffect, useMemo, useState } from "react";
import { createBrowserSupabaseClient } from "@/lib/supabaseClient";
import { useI18n } from "@/app/_components/I18nProvider";

type WinRateProps = {
  userId: string;
  limit?: number;
};

type PlayerMatchStat = {
  match_id: number | null;
  player_team: string | null;
  created_at: string;
};

type MatchRow = {
  id: number;
  winner: string | null;
};

export function WinRate({ userId, limit = 20 }: WinRateProps) {
  const { t } = useI18n();
  const [stats, setStats] = useState<PlayerMatchStat[]>([]);
  const [matchWinnersById, setMatchWinnersById] = useState<Record<number, string>>({});
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    let isMounted = true;
    const fetchWinRate = async () => {
      try {
        const supabase = createBrowserSupabaseClient();
        const statsResult = await supabase
          .from("player_match_stats")
          .select("match_id, player_team, created_at")
          .eq("steam_id", userId)
          .order("created_at", { ascending: false })
          .limit(limit);

        if (statsResult.error) {
          throw statsResult.error;
        }

        const ordered = [...(statsResult.data ?? [])].reverse();
        const matchIds = Array.from(
          new Set(
            ordered
              .map((item) => item.match_id)
              .filter((value): value is number => typeof value === "number")
          )
        );
        const winnersById: Record<number, string> = {};
        if (matchIds.length) {
          const matchesResult = await supabase
            .from("matches")
            .select("id, winner")
            .in("id", matchIds);
          if (matchesResult.error) {
            throw matchesResult.error;
          }
          for (const match of (matchesResult.data ?? []) as MatchRow[]) {
            if (typeof match.id === "number" && typeof match.winner === "string") {
              winnersById[match.id] = match.winner;
            }
          }
        }

        if (isMounted) {
          setStats(ordered);
          setMatchWinnersById(winnersById);
        }
      } catch (error) {
        console.error("Failed to load win rate", error);
      } finally {
        if (isMounted) {
          setIsLoading(false);
        }
      }
    };

    if (userId) {
      fetchWinRate();
    } else {
      setIsLoading(false);
    }

    return () => {
      isMounted = false;
    };
  }, [limit, userId]);

  const summary = useMemo(() => {
    if (!stats.length) {
      return { winRate: "N/A", winRatePercent: null as number | null };
    }
    let wins = 0;
    for (const item of stats) {
      if (!item.match_id || !item.player_team) {
        continue;
      }
      const winner = matchWinnersById[item.match_id];
      if (winner && winner === item.player_team) {
        wins += 1;
      }
    }
    const winRatePercent = (wins / stats.length) * 100;
    return { winRate: `${Math.round(winRatePercent)}%`, winRatePercent };
  }, [matchWinnersById, stats]);

  return (
    <div className="rounded-3xl glass-card p-6">
      <p className="text-xs uppercase tracking-[0.35em] text-[#7dd3fc]">{t("winRate")}</p>
      <div className="mt-3 flex items-end justify-between">
        <div className="text-3xl font-semibold">
          {isLoading ? "..." : summary.winRate}
        </div>
        <div
          className={
            "text-sm" +
            (summary.winRatePercent === null
              ? " text-white/40"
              : summary.winRatePercent > 50
                ? " text-emerald-400"
                : summary.winRatePercent < 50
                  ? " text-rose-400"
                  : " text-white/60")
          }
        >
          {summary.winRatePercent === null
            ? t("noMatches")
            : summary.winRatePercent > 50
              ? t("aboveFifty")
              : summary.winRatePercent < 50
                ? t("belowFifty")
                : t("evenWinRate")}
        </div>
      </div>
    </div>
  );
}

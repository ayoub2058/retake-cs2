"use client";

import { useEffect, useMemo, useState } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  RadarChart,
  Radar,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
} from "recharts";
import { createBrowserSupabaseClient } from "@/lib/supabaseClient";
import { useI18n } from "@/app/_components/I18nProvider";

type PlayerStatsProps = {
  userId: string;
};

type PlayerMatchStat = {
  created_at: string;
  match_id: number | null;
  player_team: string | null;
  kills: number | null;
  deaths: number | null;
  adr: number | null;
  hs_percent: number | null;
  opening_kills: number | null;
  utility_damage: number | null;
};

type ChartPoint = {
  dateLabel: string;
  adr: number;
  kills: number;
  deaths: number;
};

const formatDateLabel = (value: string) => {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "--";
  }
  return `${date.getMonth() + 1}/${date.getDate()}`;
};

const clamp = (value: number, min: number, max: number) => {
  return Math.min(Math.max(value, min), max);
};

export function PlayerStats({ userId }: PlayerStatsProps) {
  const { t } = useI18n();
  const [stats, setStats] = useState<PlayerMatchStat[]>([]);
  const [avatarUrl, setAvatarUrl] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [matchWinnersById, setMatchWinnersById] = useState<Record<number, string>>({});

  useEffect(() => {
    let isMounted = true;
    const fetchStats = async () => {
      try {
        const supabase = createBrowserSupabaseClient();
        const [statsResult, userResult] = await Promise.all([
          supabase
            .from("player_match_stats")
            .select(
              "created_at, match_id, player_team, kills, deaths, adr, hs_percent, opening_kills, utility_damage"
            )
            .eq("steam_id", userId)
            .order("created_at", { ascending: false })
            .limit(20),
          supabase
            .from("users")
            .select("avatar_url")
            .eq("steam_id", userId)
            .maybeSingle(),
        ]);

        if (statsResult.error) {
          throw statsResult.error;
        }
        if (userResult.error) {
          throw userResult.error;
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
          for (const match of matchesResult.data ?? []) {
            if (typeof match.id === "number" && typeof match.winner === "string") {
              winnersById[match.id] = match.winner;
            }
          }
        }
        if (isMounted) {
          setStats(ordered);
          setAvatarUrl(userResult.data?.avatar_url ?? null);
          setMatchWinnersById(winnersById);
        }
      } catch (err) {
        console.error("Failed to load player stats", err);
      } finally {
        if (isMounted) {
          setIsLoading(false);
        }
      }
    };

    if (userId) {
      fetchStats();
    } else {
      setIsLoading(false);
    }

    return () => {
      isMounted = false;
    };
  }, [userId]);

  const summary = useMemo(() => {
    if (!stats.length) {
      return {
        avgAdr: 0,
        kd: 0,
        winRate: "N/A",
        winRatePercent: null as number | null,
        avgHs: 0,
        avgOpening: 0,
        avgUtility: 0,
        avgDeaths: 0,
      };
    }
    const totals = stats.reduce(
      (acc, item) => {
        acc.kills += item.kills ?? 0;
        acc.deaths += item.deaths ?? 0;
        acc.adr += item.adr ?? 0;
        acc.hs += item.hs_percent ?? 0;
        acc.opening += item.opening_kills ?? 0;
        acc.utility += item.utility_damage ?? 0;
        return acc;
      },
      { kills: 0, deaths: 0, adr: 0, hs: 0, opening: 0, utility: 0 }
    );
    const avgAdr = totals.adr / stats.length;
    const avgHs = totals.hs / stats.length;
    const avgOpening = totals.opening / stats.length;
    const avgUtility = totals.utility / stats.length;
    const avgDeaths = totals.deaths / stats.length;
    const kd = totals.deaths ? totals.kills / totals.deaths : totals.kills;
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
    return {
      avgAdr,
      kd,
      winRate: `${Math.round(winRatePercent)}%`,
      winRatePercent,
      avgHs,
      avgOpening,
      avgUtility,
      avgDeaths,
    };
  }, [matchWinnersById, stats]);

  const chartData = useMemo<ChartPoint[]>(() => {
    return stats.map((item) => ({
      dateLabel: formatDateLabel(item.created_at),
      adr: item.adr ?? 0,
      kills: item.kills ?? 0,
      deaths: item.deaths ?? 0,
    }));
  }, [stats]);

  const radarData = useMemo(() => {
    const aim = clamp(summary.avgHs, 0, 100);
    const aggression = clamp((summary.avgOpening / 5) * 100, 0, 100);
    const utility = clamp((summary.avgUtility / 250) * 100, 0, 100);
    const survival = clamp(100 - summary.avgDeaths * 5, 0, 100);
    const impact = clamp((summary.avgAdr / 120) * 100, 0, 100);
    return [
      { stat: t("aim"), value: aim },
      { stat: t("aggression"), value: aggression },
      { stat: t("utility"), value: utility },
      { stat: t("survival"), value: survival },
      { stat: t("impact"), value: impact },
    ];
  }, [summary, t]);

  const CustomTooltip = ({ active, payload }: any) => {
    if (!active || !payload?.length) {
      return null;
    }
    const point = payload[0].payload as ChartPoint;
    return (
      <div className="rounded-lg border border-white/10 bg-black/80 px-3 py-2 text-xs text-white">
        <div className="font-semibold">
          {t("adrLabel")}: {point.adr.toFixed(1)}
        </div>
        <div>
          {t("kdLabel")}: {point.kills} / {point.deaths}
        </div>
      </div>
    );
  };

  return (
    <div className="rounded-3xl glass-card p-8">
      <div className="flex flex-wrap items-center justify-between gap-6">
        <div className="flex items-center gap-4">
          <div className="h-14 w-14 overflow-hidden rounded-2xl border border-white/20 bg-black/40">
            {avatarUrl ? (
              <img
                src={avatarUrl}
                alt="Profile"
                className="h-full w-full object-cover"
              />
            ) : (
              <div className="flex h-full w-full items-center justify-center text-xs text-white/60">
                {t("naLabel")}
              </div>
            )}
          </div>
          <div>
            <p className="text-xs uppercase tracking-[0.35em] text-[#67f5ff]">
              {t("lifetimeSummary")}
            </p>
            <h2 className="mt-2 text-2xl font-semibold">{t("yourPerformance")}</h2>
            <p className="mt-1 text-sm text-white/60">
              {t("avgKD")} {summary.kd.toFixed(2)} · {t("avgADR")}{" "}
              {summary.avgAdr.toFixed(1)} · {t("winRate")}{" "}
              <span
                className={
                  summary.winRatePercent === null
                    ? "text-white/60"
                    : summary.winRatePercent > 50
                      ? "text-emerald-400"
                      : summary.winRatePercent < 50
                        ? "text-rose-400"
                        : "text-white/80"
                }
              >
                {summary.winRate}
              </span>
            </p>
          </div>
        </div>
        <div className="rounded-2xl border border-white/10 bg-black/40 px-4 py-3 text-xs text-white/60">
          {isLoading
            ? t("loadingStats")
            : t("lastMatches", { count: stats.length })}
        </div>
      </div>

      <div className="mt-10 grid gap-10 lg:grid-cols-2">
        <div className="rounded-2xl glass-card p-6">
          <div className="mb-4 flex items-center justify-between">
            <div>
              <p className="text-xs uppercase tracking-[0.35em] text-[#67f5ff]">
                {t("adrTrend")}
              </p>
              <h3 className="mt-2 text-lg font-semibold">{t("last20Matches")}</h3>
            </div>
          </div>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <XAxis dataKey="dateLabel" stroke="#6ee7f5" tick={{ fill: "#9ae6f5" }} />
                <YAxis stroke="#6ee7f5" tick={{ fill: "#9ae6f5" }} />
                <Tooltip content={<CustomTooltip />} />
                <Line
                  type="monotone"
                  dataKey="adr"
                  stroke="#22d3ee"
                  strokeWidth={3}
                  dot={{ stroke: "#22d3ee", strokeWidth: 2 }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="rounded-2xl glass-card p-6">
          <div className="mb-4">
            <p className="text-xs uppercase tracking-[0.35em] text-[#7dff6b]">
              {t("playstyleRadar")}
            </p>
            <h3 className="mt-2 text-lg font-semibold">{t("coreAttributes")}</h3>
          </div>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <RadarChart data={radarData}>
                <PolarGrid stroke="#1f2937" />
                <PolarAngleAxis dataKey="stat" tick={{ fill: "#d1fae5" }} />
                <PolarRadiusAxis tick={{ fill: "#9ca3af" }} domain={[0, 100]} />
                <Radar
                  dataKey="value"
                  stroke="#34d399"
                  fill="#34d399"
                  fillOpacity={0.25}
                />
              </RadarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  );
}

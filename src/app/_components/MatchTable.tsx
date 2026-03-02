"use client";

import { MapIcon } from "@/app/_components/MapIcon";
import Link from "next/link";
import { useI18n } from "@/app/_components/I18nProvider";

type MatchRow = {
  id: number;
  map_name?: string | null | undefined;
  created_at: string | null;
  result?: "win" | "loss" | "tie" | null;
  score?: string | null;
  status?: string | null;
};

const statusConfig: Record<string, { label: string; color: string; dot: string; animate?: boolean }> = {
  pending: { label: "Queued", color: "text-white/50 bg-white/[0.06] border-white/10", dot: "bg-white/40" },
  downloading: { label: "Downloading", color: "text-sky-300 bg-sky-500/10 border-sky-500/20", dot: "bg-sky-400", animate: true },
  parsing: { label: "Analyzing", color: "text-amber-300 bg-amber-500/10 border-amber-500/20", dot: "bg-amber-400", animate: true },
  done: { label: "Ready", color: "text-emerald-300 bg-emerald-500/10 border-emerald-500/20", dot: "bg-emerald-400" },
  error: { label: "Error", color: "text-rose-300 bg-rose-500/10 border-rose-500/20", dot: "bg-rose-400" },
};

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

const formatMapName = (mapName: string | null | undefined) => {
  if (!mapName) {
    return "-";
  }
  return mapName
    .split("_")
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
    .join(" ");
};


const resultConfig = {
  win: { label: "WIN", color: "text-emerald-400", bg: "bg-emerald-500/15 border-emerald-500/20" },
  loss: { label: "LOSS", color: "text-rose-400", bg: "bg-rose-500/15 border-rose-500/20" },
  tie: { label: "TIE", color: "text-amber-400", bg: "bg-amber-500/15 border-amber-500/20" },
} as const;


export const MatchTable = ({ matches }: { matches: MatchRow[] }) => {
  const { t, locale } = useI18n();
  return (
    <div className="mt-8 overflow-hidden rounded-2xl glass-card">
      <table className="w-full border-collapse text-left text-sm">
        <thead className="border-b border-white/[0.06] bg-white/[0.03] text-[10px] uppercase tracking-[0.3em] text-white/40">
          <tr>
            <th className="px-6 py-4 font-medium">{t("date")}</th>
            <th className="px-6 py-4 font-medium">{t("map")}</th>
            <th className="px-6 py-4 font-medium text-center">Result</th>
            <th className="px-6 py-4 font-medium text-center">{t("statusLabel")}</th>
            <th className="px-6 py-4 text-right font-medium">{t("action")}</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/[0.04]">
          {matches.map((match, index) => {
            const cfg = match.result ? resultConfig[match.result] : null;
            return (
              <tr
                key={match.id}
                className="text-zinc-200 transition-colors hover:bg-white/[0.03]"
                style={{ animationDelay: `${index * 50}ms` }}
              >
                <td className="px-6 py-4 text-white/70">
                  {formatDate(match.created_at, locale, t("unknown"))}
                </td>
                <td className="px-6 py-4">
                  <div className="flex items-center gap-3">
                    <MapIcon mapName={match.map_name ?? null} />
                    <span className="font-medium">{formatMapName(match.map_name)}</span>
                  </div>
                </td>
                <td className="px-6 py-4 text-center">
                  {cfg ? (
                    <div className="inline-flex items-center gap-2">
                      <span className={`inline-block rounded-md border px-2.5 py-1 text-[10px] font-bold uppercase tracking-[0.15em] ${cfg.bg} ${cfg.color}`}>
                        {cfg.label}
                      </span>
                      {match.score ? (
                        <span className="text-xs font-semibold text-white/60">{match.score}</span>
                      ) : null}
                    </div>
                  ) : (
                    <span className="text-xs text-white/30">—</span>
                  )}
                </td>
                <td className="px-6 py-4 text-center">
                  {(() => {
                    const sc = match.status ? statusConfig[match.status] : null;
                    if (!sc) return <span className="text-xs text-white/30">—</span>;
                    return (
                      <span className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.1em] ${sc.color}`}>
                        <span className={`inline-block h-1.5 w-1.5 rounded-full ${sc.dot} ${sc.animate ? "animate-pulse" : ""}`} />
                        {sc.label}
                      </span>
                    );
                  })()}
                </td>
                <td className="px-6 py-4 text-right">
                  <Link
                    href={`/dashboard/matches/${match.id}`}
                    className="inline-flex items-center gap-2 rounded-lg border border-white/10 bg-white/[0.04] px-4 py-2 text-[10px] font-semibold uppercase tracking-[0.2em] text-white/70 transition-all hover:border-[#d5ff4c]/30 hover:bg-[#d5ff4c]/10 hover:text-[#d5ff4c]"
                  >
                    {t("viewDetails")}
                    <svg className="h-3 w-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <path d="M5 12h14" />
                      <path d="m12 5 7 7-7 7" />
                    </svg>
                  </Link>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
};

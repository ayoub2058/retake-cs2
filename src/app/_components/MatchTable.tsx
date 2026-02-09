"use client";

import { MapIcon } from "@/app/_components/MapIcon";
import Link from "next/link";
import { useI18n } from "@/app/_components/I18nProvider";

type MatchRow = {
  id: number;
  map_name?: string | null;
  created_at: string | null;
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


export const MatchTable = ({ matches }: { matches: MatchRow[] }) => {
  const { t, locale } = useI18n();
  return (
    <div className="mt-8 overflow-hidden rounded-2xl glass-card">
      <table className="w-full border-collapse text-left text-sm">
        <thead className="bg-white/5 text-xs uppercase tracking-[0.2em] text-zinc-400">
          <tr>
            <th className="px-6 py-4">{t("date")}</th>
            <th className="px-6 py-4">{t("map")}</th>
            <th className="px-6 py-4 text-right">{t("action")}</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/5">
          {matches.map((match) => {
            return (
              <tr key={match.id} className="text-zinc-200">
                <td className="px-6 py-4">
                  {formatDate(match.created_at, locale, t("unknown"))}
                </td>
                <td className="px-6 py-4">
                  <div className="flex items-center gap-3">
                    <MapIcon mapName={match.map_name} />
                    <span>{formatMapName(match.map_name)}</span>
                  </div>
                </td>
                <td className="px-6 py-4 text-right">
                  <Link
                    href={`/dashboard/matches/${match.id}`}
                    className="inline-flex items-center rounded-md border border-white/20 px-4 py-2 text-xs font-semibold uppercase tracking-[0.2em] text-white/80 transition hover:border-white/60 hover:text-white"
                  >
                    {t("viewDetails")}
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

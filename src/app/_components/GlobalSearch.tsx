"use client";

import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { Loader2, Search } from "lucide-react";
import { useI18n } from "@/app/_components/I18nProvider";

type SearchResult = {
  id: string | null;
  name: string | null;
  avatar: string | null;
  rank: string | null;
  hasStats?: boolean;
};

type GlobalSearchProps = {
  placeholder?: string;
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

export function GlobalSearch({ placeholder }: GlobalSearchProps) {
  const router = useRouter();
  const { t } = useI18n();
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [warning, setWarning] = useState<string | null>(null);
  const [isOpen, setIsOpen] = useState(false);

  const trimmedQuery = useMemo(() => query.trim(), [query]);

  const runSearch = async (value: string) => {
    if (!value) {
      return;
    }
    setIsLoading(true);
    setError(null);
    setWarning(null);
    try {
      const response = await fetch(`/api/steam/search?q=${encodeURIComponent(value)}`);
      const data = await response.json();
      if (!response.ok) {
        setError(data?.error || t("searchFailed"));
        setResults([]);
        setIsOpen(true);
        return;
      }
      const players = Array.isArray(data?.players) ? data.players : [];
      const playersWithFlags = await Promise.all(
        players.map(async (player: SearchResult) => {
          if (!player.id) {
            return player;
          }
          const statsResponse = await fetch(
            `/api/steam/has-stats?steam_id=${encodeURIComponent(player.id)}`
          );
          const statsData = await statsResponse.json();
          return {
            ...player,
            hasStats: Boolean(statsData?.hasStats),
          };
        })
      );
      setResults(playersWithFlags);
      setWarning(typeof data?.warning === "string" ? data.warning : null);
      setIsOpen(true);
    } catch (err) {
      console.error("Global search failed:", err);
      setError(t("searchFailed"));
      setResults([]);
      setIsOpen(true);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    if (trimmedQuery.length < 2) {
      setResults([]);
      setIsOpen(false);
      setError(null);
      setWarning(null);
      return;
    }
    const handle = setTimeout(() => {
      runSearch(trimmedQuery);
    }, 450);
    return () => clearTimeout(handle);
  }, [trimmedQuery]);

  const handleKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Enter") {
      event.preventDefault();
      runSearch(trimmedQuery);
    }
  };

  const handleSelect = (playerId: string | null) => {
    if (!playerId) {
      return;
    }
    router.push(`/dashboard/players/${encodeURIComponent(playerId)}`);
    setQuery("");
    setResults([]);
    setIsOpen(false);
    setWarning(null);
  };

  return (
    <div className="relative w-full max-w-xl">
      <div className="flex items-center gap-3 rounded-2xl border border-white/10 bg-black/50 px-4 py-3 text-sm text-white/80 shadow-[0_20px_40px_rgba(0,0,0,0.35)]">
        <Search className="h-4 w-4 text-white/50" />
        <input
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={placeholder ?? t("searchPlaceholder")}
          className="flex-1 bg-transparent text-sm text-white outline-none placeholder:text-white/40"
        />
        {isLoading ? <Loader2 className="h-4 w-4 animate-spin text-white/60" /> : null}
      </div>

      {isOpen ? (
        <div className="absolute left-0 right-0 top-full z-20 mt-2 overflow-hidden rounded-2xl border border-white/10 bg-black/90 shadow-[0_30px_60px_rgba(0,0,0,0.45)]">
          {error ? (
            <div className="px-4 py-3 text-sm text-rose-300">{error}</div>
          ) : null}
          {!error && warning ? (
            <div className="px-4 py-3 text-sm text-amber-300">{warning}</div>
          ) : null}
          {!error && !results.length && !isLoading ? (
            <div className="px-4 py-3 text-sm text-white/60">{t("noPlayersFound")}</div>
          ) : null}
          {results.map((player) => (
            <button
              key={`${player.id ?? "unknown"}-${player.name ?? "player"}`}
              type="button"
              onClick={() => handleSelect(player.id)}
              className="flex w-full items-center gap-3 border-b border-white/5 px-4 py-3 text-left text-sm transition hover:bg-white/5"
            >
              <div className="h-9 w-9 overflow-hidden rounded-full border border-white/10 bg-white/5">
                {player.avatar ? (
                  <img
                    src={player.avatar}
                    alt={player.name ?? t("playerUnknown")}
                    className="h-full w-full object-cover"
                  />
                ) : (
                  <div className="flex h-full w-full items-center justify-center text-[10px] text-white/40">
                    {t("naLabel")}
                  </div>
                )}
              </div>
              <div className="flex-1">
                <div className="font-semibold text-white">
                  {player.name ?? t("playerUnknown")}
                </div>
                {player.rank ? (
                  (() => {
                    const level = Number(player.rank);
                    if (Number.isFinite(level) && level >= 1 && level <= 10) {
                      return (
                        <div className="mt-1 flex items-center gap-2 text-xs text-white/50">
                          <span>{t("faceit")}</span>
                          <span
                            className={`flex h-8 w-8 items-center justify-center rounded-full text-xs font-bold ${getFaceitBadgeClass(level)}`}
                          >
                            {Math.round(level)}
                          </span>
                        </div>
                      );
                    }
                    return (
                      <div className="text-xs text-white/50">
                        {t("rankLabel")}: {player.rank}
                      </div>
                    );
                  })()
                ) : null}
                {player.hasStats === true ? (
                  <span className="mt-1 inline-flex w-fit rounded-full bg-emerald-500/20 px-2 py-0.5 text-[10px] uppercase tracking-[0.2em] text-emerald-300">
                    {t("statsAvailable")}
                  </span>
                ) : null}
                {player.hasStats === false ? (
                  <span className="mt-1 inline-flex w-fit rounded-full bg-white/10 px-2 py-0.5 text-[10px] uppercase tracking-[0.2em] text-white/50">
                    {t("steamProfileOnly")}
                  </span>
                ) : null}
              </div>
            </button>
          ))}
        </div>
      ) : null}
    </div>
  );
}

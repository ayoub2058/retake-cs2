"use client";

type MatchResult = "win" | "loss" | "tie" | null;

type RecentFormProps = {
  results: MatchResult[];
};

const DOT_COLORS: Record<string, { bg: string; ring: string }> = {
  win: { bg: "bg-emerald-400", ring: "ring-emerald-400/30" },
  loss: { bg: "bg-rose-400", ring: "ring-rose-400/30" },
  tie: { bg: "bg-amber-400", ring: "ring-amber-400/30" },
};

function computeStreak(results: MatchResult[]): { type: "W" | "L" | "T"; count: number } | null {
  const filtered = results.filter((r) => r !== null);
  if (!filtered.length) return null;
  const first = filtered[0];
  let count = 0;
  for (const r of filtered) {
    if (r === first) count++;
    else break;
  }
  const type = first === "win" ? "W" : first === "loss" ? "L" : "T";
  return { type, count };
}

const STREAK_COLOR: Record<string, string> = {
  W: "text-emerald-400 border-emerald-500/20 bg-emerald-500/10",
  L: "text-rose-400 border-rose-500/20 bg-rose-500/10",
  T: "text-amber-400 border-amber-500/20 bg-amber-500/10",
};

export function RecentForm({ results }: RecentFormProps) {
  const last5 = results.slice(0, 5);
  const streak = computeStreak(results);

  if (!last5.length) return null;

  return (
    <div className="flex items-center gap-4">
      {/* Form dots */}
      <div className="flex items-center gap-1.5">
        <span className="mr-1 text-[10px] uppercase tracking-[0.3em] text-white/40">Form</span>
        {last5.map((r, i) => {
          const colors = r ? DOT_COLORS[r] : { bg: "bg-white/20", ring: "ring-white/10" };
          return (
            <div
              key={i}
              className={`h-3 w-3 rounded-full ${colors.bg} ring-2 ${colors.ring} transition-transform hover:scale-125`}
              title={r ? r.toUpperCase() : "?"}
            />
          );
        })}
      </div>

      {/* Streak badge */}
      {streak && streak.count >= 2 && (
        <div className={`inline-flex items-center gap-1 rounded-lg border px-2.5 py-1 text-[10px] font-bold uppercase tracking-[0.15em] ${STREAK_COLOR[streak.type]}`}>
          <svg className="h-3 w-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" />
          </svg>
          {streak.count}{streak.type} Streak
        </div>
      )}
    </div>
  );
}

"use client";

import { useState, useMemo } from "react";

/**
 * Parses an AI coaching tip into structured sections and renders them
 * with visual hierarchy: section headers, round cards, mistake/fix highlights.
 */

type Section = {
  emoji: string;
  title: string;
  content: string;
  rounds: RoundEntry[];
};

type RoundEntry = {
  roundLabel: string;
  body: string;
  mistakes: string[];
  fixes: string[];
};

// Common section header patterns (emoji + title)
const SECTION_RE =
  /^([\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}\u{FE00}-\u{FE0F}\u{200D}\u{1F1E0}-\u{1F1FF}]+)\s*(.+)$/mu;

// Round label inside death analysis, e.g. "Round 2 –", "Round 15 –"
const ROUND_RE = /^(Round\s+\d+(?:\s*[–\-]\s*))/i;

// "Mistake:" or "Fix:" inline markers
const MISTAKE_RE = /(?:^|\.\s+)(Mistake:\s*[^.]+\.?)/gi;
const FIX_RE = /(?:^|\.\s+)(Fix:\s*[^.]+\.?)/gi;

function parseTip(raw: string): Section[] {
  const lines = raw.split("\n");
  const sections: Section[] = [];
  let currentSection: Section | null = null;
  let currentRound: RoundEntry | null = null;

  const flushRound = () => {
    if (currentRound && currentSection) {
      // Extract mistakes and fixes from body
      const mMatches = [...currentRound.body.matchAll(MISTAKE_RE)];
      const fMatches = [...currentRound.body.matchAll(FIX_RE)];
      currentRound.mistakes = mMatches.map((m) => m[1].replace(/^Mistake:\s*/i, "").trim());
      currentRound.fixes = fMatches.map((m) => m[1].replace(/^Fix:\s*/i, "").trim());
      currentSection.rounds.push(currentRound);
      currentRound = null;
    }
  };

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    // Check for section header (emoji + title)
    const sectionMatch = trimmed.match(SECTION_RE);
    if (
      sectionMatch &&
      trimmed.length < 120 &&
      !trimmed.match(ROUND_RE)
    ) {
      flushRound();
      currentSection = {
        emoji: sectionMatch[1],
        title: sectionMatch[2].replace(/[\u200F\u200E]/g, "").trim(),
        content: "",
        rounds: [],
      };
      sections.push(currentSection);
      continue;
    }

    // Check for round entry
    const roundMatch = trimmed.match(ROUND_RE);
    if (roundMatch && currentSection) {
      flushRound();
      currentRound = {
        roundLabel: roundMatch[1].replace(/[–\-]\s*$/, "").trim(),
        body: trimmed.slice(roundMatch[0].length).trim(),
        mistakes: [],
        fixes: [],
      };
      continue;
    }

    // Continuation of current round
    if (currentRound) {
      currentRound.body += " " + trimmed;
      continue;
    }

    // General content in current section
    if (currentSection) {
      currentSection.content += (currentSection.content ? "\n" : "") + trimmed;
    } else {
      // No section yet — create a default one
      currentSection = {
        emoji: "📋",
        title: "Analysis",
        content: trimmed,
        rounds: [],
      };
      sections.push(currentSection);
    }
  }

  flushRound();

  // If parsing found no sections, create a single fallback section
  if (sections.length === 0 && raw.trim()) {
    sections.push({
      emoji: "📋",
      title: "Analysis",
      content: raw.trim(),
      rounds: [],
    });
  }

  return sections;
}

// Highlight "Mistake:" and "Fix:" inline
function highlightBody(body: string) {
  const parts: { text: string; type: "normal" | "mistake" | "fix" }[] = [];
  let remaining = body;

  // Split on Mistake: and Fix: markers
  const combinedRe = /(Mistake:\s*[^.]+\.?)|(Fix:\s*[^.]+\.?)/gi;
  let lastIdx = 0;
  let match: RegExpExecArray | null;

  while ((match = combinedRe.exec(remaining)) !== null) {
    if (match.index > lastIdx) {
      parts.push({ text: remaining.slice(lastIdx, match.index), type: "normal" });
    }
    const matched = match[0];
    const type = matched.toLowerCase().startsWith("mistake") ? "mistake" : "fix";
    parts.push({ text: matched, type });
    lastIdx = match.index + matched.length;
  }

  if (lastIdx < remaining.length) {
    parts.push({ text: remaining.slice(lastIdx), type: "normal" });
  }

  return parts;
}

function RoundCard({ round, index }: { round: RoundEntry; index: number }) {
  const [expanded, setExpanded] = useState(true);
  const bodyParts = useMemo(() => highlightBody(round.body), [round.body]);

  return (
    <div className="rounded-xl border border-white/[0.06] bg-black/20 overflow-hidden transition-all">
      <button
        type="button"
        onClick={() => setExpanded(!expanded)}
        className="flex w-full items-center gap-3 px-4 py-3 text-left transition hover:bg-white/[0.03]"
      >
        <span className="flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-lg bg-rose-500/10 text-[11px] font-bold text-rose-400">
          {round.roundLabel.replace(/^Round\s*/i, "R")}
        </span>
        <span className="flex-1 min-w-0 text-sm text-white/70 truncate">
          {round.body.slice(0, 100)}
          {round.body.length > 100 ? "..." : ""}
        </span>
        <svg
          className={`h-4 w-4 flex-shrink-0 text-white/30 transition-transform ${expanded ? "rotate-180" : ""}`}
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <polyline points="6 9 12 15 18 9" />
        </svg>
      </button>

      {expanded && (
        <div className="border-t border-white/[0.04] px-4 py-4 space-y-3 animate-fade-in">
          <p className="text-sm leading-relaxed text-white/70">
            {bodyParts.map((part, i) => {
              if (part.type === "mistake") {
                return (
                  <span
                    key={i}
                    className="inline-block mt-2 rounded-lg border border-rose-500/20 bg-rose-500/[0.07] px-3 py-1.5 text-[13px] text-rose-300"
                  >
                    <svg
                      className="mr-1.5 inline h-3.5 w-3.5 -translate-y-[1px] text-rose-400"
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="2"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    >
                      <circle cx="12" cy="12" r="10" />
                      <line x1="15" y1="9" x2="9" y2="15" />
                      <line x1="9" y1="9" x2="15" y2="15" />
                    </svg>
                    {part.text}
                  </span>
                );
              }
              if (part.type === "fix") {
                return (
                  <span
                    key={i}
                    className="inline-block mt-2 rounded-lg border border-emerald-500/20 bg-emerald-500/[0.07] px-3 py-1.5 text-[13px] text-emerald-300"
                  >
                    <svg
                      className="mr-1.5 inline h-3.5 w-3.5 -translate-y-[1px] text-emerald-400"
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="2"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    >
                      <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" />
                      <polyline points="22 4 12 14.01 9 11.01" />
                    </svg>
                    {part.text}
                  </span>
                );
              }
              return <span key={i}>{part.text}</span>;
            })}
          </p>
        </div>
      )}
    </div>
  );
}

export function FormattedCoachingTip({
  text,
  previewLength,
}: {
  text: string;
  previewLength?: number;
}) {
  const [showAll, setShowAll] = useState(false);
  const sections = useMemo(() => parseTip(text), [text]);
  const totalRounds = sections.reduce((sum, s) => sum + s.rounds.length, 0);

  // For preview mode (coaching history page), we show a shorter version
  if (previewLength && !showAll) {
    const preview =
      text.length > previewLength
        ? text.slice(0, previewLength) + "..."
        : text;
    // Just find the first section for preview
    const firstSection = sections[0];

    return (
      <div className="space-y-3">
        {firstSection && (
          <div className="flex items-center gap-2 mb-2">
            <span className="text-lg">{firstSection.emoji}</span>
            <span className="text-xs font-semibold uppercase tracking-[0.2em] text-white/50">
              {firstSection.title}
            </span>
            {totalRounds > 0 && (
              <span className="ml-auto text-[10px] rounded-full bg-white/[0.06] px-2 py-0.5 text-white/40">
                {totalRounds} rounds analyzed
              </span>
            )}
          </div>
        )}
        <p className="text-sm leading-relaxed text-white/60">
          {firstSection?.content
            ? firstSection.content.length > previewLength
              ? firstSection.content.slice(0, previewLength) + "..."
              : firstSection.content
            : preview}
        </p>
        {text.length > previewLength && (
          <button
            type="button"
            onClick={() => setShowAll(true)}
            className="inline-flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-[0.2em] text-[#a78bfa] transition hover:text-[#c4b5fd]"
          >
            Expand full analysis
            <svg
              className="h-3 w-3"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <polyline points="6 9 12 15 18 9" />
            </svg>
          </button>
        )}
      </div>
    );
  }

  return (
    <div className="space-y-5">
      {sections.map((section, si) => (
        <div key={si} className="space-y-3">
          {/* Section header */}
          <div className="flex items-center gap-2.5 pt-1">
            <span className="text-xl leading-none">{section.emoji}</span>
            <h3 className="text-sm font-bold uppercase tracking-[0.12em] text-white/90">
              {section.title}
            </h3>
            {section.rounds.length > 0 && (
              <span className="ml-auto inline-flex items-center gap-1 rounded-full bg-rose-500/10 px-2.5 py-0.5 text-[10px] font-semibold text-rose-400">
                <svg className="h-3 w-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <circle cx="12" cy="12" r="10" /><polyline points="12 6 12 12 16 14" />
                </svg>
                {section.rounds.length} rounds
              </span>
            )}
          </div>

          {/* Section narrative content */}
          {section.content && (
            <div className="rounded-xl border border-white/[0.05] bg-white/[0.02] px-5 py-4">
              <p className="text-[13px] leading-[1.8] text-white/70 whitespace-pre-line">
                {section.content}
              </p>
            </div>
          )}

          {/* Round-by-round cards */}
          {section.rounds.length > 0 && (
            <div className="space-y-2">
              {section.rounds.map((round, ri) => (
                <RoundCard key={ri} round={round} index={ri} />
              ))}
            </div>
          )}
        </div>
      ))}

      {/* Collapse button if in expanded preview mode */}
      {previewLength && showAll && (
        <button
          type="button"
          onClick={() => setShowAll(false)}
          className="inline-flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-[0.2em] text-white/40 transition hover:text-white/70"
        >
          Show less
          <svg
            className="h-3 w-3 rotate-180"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <polyline points="6 9 12 15 18 9" />
          </svg>
        </button>
      )}
    </div>
  );
}

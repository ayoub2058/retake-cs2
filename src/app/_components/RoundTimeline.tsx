"use client";

import React from "react";
import { Bomb, ShieldCheck, Skull } from "lucide-react";
import { useI18n } from "@/app/_components/I18nProvider";

type Round = {
  round_number: number;
  winner_side: "CT" | "T" | null;
  reason: string | null;
  ct_score: number | null;
  t_score: number | null;
};

type RoundTimelineProps = {
  rounds: Round[];
};

const getWinnerLabel = (
  side: Round["winner_side"],
  labels: { ct: string; t: string; unknown: string }
) => {
  if (side === "CT") {
    return labels.ct;
  }
  if (side === "T") {
    return labels.t;
  }
  return labels.unknown;
};

const getReasonLabel = (reason: string | null, fallback: string) => {
  if (!reason) {
    return fallback;
  }
  return reason.replace(/_/g, " ");
};

const getScoreLabel = (ctScore: number | null, tScore: number | null) => {
  const ctLabel = typeof ctScore === "number" ? ctScore : "?";
  const tLabel = typeof tScore === "number" ? tScore : "?";
  return `${ctLabel} - ${tLabel}`;
};

const getRoundIcon = (reason: string | null) => {
  const value = (reason || "").toLowerCase();
  if (value.includes("bomb") && (value.includes("defuse") || value.includes("defused"))) {
    return ShieldCheck;
  }
  if (value.includes("bomb") && (value.includes("explode") || value.includes("exploded"))) {
    return Bomb;
  }
  if (value.includes("elimination") || value.includes("time") || value.includes("timeout")) {
    return Skull;
  }
  return Skull;
};

export function RoundTimeline({ rounds }: RoundTimelineProps) {
  const { t } = useI18n();
  if (!rounds.length) {
    return null;
  }

  return (
    <div className="flex h-14 w-full items-stretch overflow-hidden rounded-xl border border-white/[0.06] bg-black/50">
      {rounds.map((round, index) => {
        const isHalftimeGap = round.round_number === 12;
        const winnerLabel = getWinnerLabel(round.winner_side, {
          ct: "CT",
          t: "T",
          unknown: t("unknown"),
        });
        const reasonLabel = getReasonLabel(round.reason, t("unknown"));
        const scoreLabel = getScoreLabel(round.ct_score, round.t_score);
        const Icon = getRoundIcon(round.reason);
        const colorClass =
          round.winner_side === "CT"
            ? "bg-cyan-500"
            : round.winner_side === "T"
              ? "bg-yellow-500"
              : "bg-white/30";

        return (
          <div
            key={`${round.round_number}-${index}`}
            className={
              "group relative flex flex-1 items-center justify-center transition-all duration-200 hover:flex-[1.3] hover:scale-y-105" +
              (isHalftimeGap ? " ml-0.5 mr-0.5 border-l border-white/10" : "")
            }
          >
            <div className={`flex h-full w-full items-center justify-center ${colorClass} transition-all duration-200 group-hover:brightness-110`}>
              <Icon className="h-4 w-4 text-black/70" />
            </div>
            <div className="pointer-events-none absolute bottom-full left-1/2 z-10 mb-3 w-44 -translate-x-1/2 rounded-xl border border-white/10 bg-black/95 px-4 py-3 text-xs text-white opacity-0 shadow-2xl transition-opacity group-hover:opacity-100">
              <div className="mb-1 font-bold text-white">
                {t("round")} {round.round_number}
              </div>
              <div className="text-white/60">
                {t("winner")}: <span className="text-white/90">{winnerLabel}</span>
              </div>
              <div className="text-white/60">
                {t("reason")}: <span className="text-white/90">{reasonLabel}</span>
              </div>
              <div className="mt-1 font-mono text-sm font-semibold text-[#d5ff4c]">
                {scoreLabel}
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

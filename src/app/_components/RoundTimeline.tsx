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
    <div className="flex h-12 w-full items-stretch overflow-hidden rounded-2xl border border-white/10 bg-black/40">
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
              "group relative flex flex-1 items-center justify-center transition-[flex,transform] duration-150 hover:flex-[1.2] hover:scale-y-110" +
              (isHalftimeGap ? " mr-1" : "")
            }
          >
            <div className={`flex h-full w-full items-center justify-center ${colorClass}`}>
              <Icon className="h-4 w-4 text-black/80" />
            </div>
            <div className="pointer-events-none absolute bottom-full left-1/2 z-10 mb-2 w-40 -translate-x-1/2 rounded-lg border border-white/10 bg-black/90 px-3 py-2 text-xs text-white opacity-0 shadow-lg transition-opacity group-hover:opacity-100">
              <div className="font-semibold">
                {t("round")} {round.round_number}
              </div>
              <div>
                {t("winner")}: {winnerLabel}
              </div>
              <div>
                {t("reason")}: {reasonLabel}
              </div>
              <div>
                {t("score")}: {scoreLabel}
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

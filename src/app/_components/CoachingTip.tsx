"use client";

import { useState } from "react";

type CoachingTipProps = {
  coachTip: string | null;
  tipImageUrl: string | null;
};

export function CoachingTip({ coachTip, tipImageUrl }: CoachingTipProps) {
  const [imageExpanded, setImageExpanded] = useState(false);
  const [showFull, setShowFull] = useState(false);

  if (!coachTip && !tipImageUrl) return null;

  const isLong = (coachTip?.length ?? 0) > 1000;
  const displayTip = isLong && !showFull ? coachTip!.slice(0, 1000) + "..." : coachTip;

  return (
    <div className="w-full max-w-none space-y-6">
      {/* Stats Card Image */}
      {tipImageUrl && (
        <div className="rounded-3xl glass-card p-6 sm:p-10 animate-fade-in">
          <div className="mb-5 flex items-center gap-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-[#d5ff4c]/10">
              <svg className="h-5 w-5 text-[#d5ff4c]" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <rect x="3" y="3" width="18" height="18" rx="2" ry="2" />
                <circle cx="8.5" cy="8.5" r="1.5" />
                <polyline points="21 15 16 10 5 21" />
              </svg>
            </div>
            <div>
              <p className="text-xs uppercase tracking-[0.35em] text-[#d5ff4c]">Performance Card</p>
              <p className="text-[10px] text-white/40">AI-generated match summary</p>
            </div>
          </div>
          <div
            className={`overflow-hidden rounded-2xl border border-white/10 transition-all duration-500 cursor-pointer ${
              imageExpanded ? "max-h-[2000px]" : "max-h-[400px]"
            }`}
            onClick={() => setImageExpanded(!imageExpanded)}
          >
            <img
              src={tipImageUrl}
              alt="Match Stats Card"
              className="w-full object-cover"
              loading="lazy"
            />
          </div>
          {!imageExpanded && (
            <button
              type="button"
              onClick={() => setImageExpanded(true)}
              className="mt-3 text-xs text-white/40 hover:text-white/70 transition"
            >
              Click to expand full card
            </button>
          )}
        </div>
      )}

      {/* AI Coaching Analysis */}
      {coachTip && (
        <div className="rounded-3xl glass-card p-6 sm:p-10 animate-fade-in">
          <div className="mb-6 flex items-center gap-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-[#a78bfa]/10">
              <svg className="h-5 w-5 text-[#a78bfa]" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M12 20h9" />
                <path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z" />
              </svg>
            </div>
            <div>
              <p className="text-xs uppercase tracking-[0.35em] text-[#a78bfa]">AI Coach Analysis</p>
              <p className="text-[10px] text-white/40">Personalized tactical coaching</p>
            </div>
          </div>

          <div className="coaching-tip-body rounded-2xl border border-white/[0.06] bg-black/30 px-6 py-5">
            <p className="whitespace-pre-line text-sm leading-relaxed text-white/80">
              {displayTip}
            </p>
            {isLong && (
              <button
                type="button"
                onClick={() => setShowFull(!showFull)}
                className="mt-4 inline-flex items-center gap-1.5 rounded-lg border border-white/10 bg-white/[0.04] px-4 py-2 text-[10px] font-semibold uppercase tracking-[0.2em] text-[#a78bfa] transition hover:border-[#a78bfa]/30 hover:bg-[#a78bfa]/10"
              >
                {showFull ? "Show Less" : "Read Full Analysis"}
                <svg className={`h-3 w-3 transition-transform ${showFull ? "rotate-180" : ""}`} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <polyline points="6 9 12 15 18 9" />
                </svg>
              </button>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

"use client";

import { AnimatedNumber } from "@/app/_components/AnimatedNumber";

type QuickStatCardProps = {
  label: string;
  value: string;
  numericValue?: number;
  decimals?: number;
  suffix?: string;
  accent: string;
  icon: React.ReactNode;
  delay: number;
};

export function QuickStatCard({
  label,
  value,
  numericValue,
  decimals = 0,
  suffix = "",
  accent,
  icon,
  delay,
}: QuickStatCardProps) {
  return (
    <div
      className="stat-card rounded-2xl px-5 py-5 animate-fade-in"
      style={{ animationDelay: `${delay}ms` }}
    >
      <div className="flex items-center gap-3">
        <div className={`flex h-10 w-10 items-center justify-center rounded-xl ${accent}`}>
          {icon}
        </div>
        <div className="min-w-0 flex-1">
          <p className="text-[10px] uppercase tracking-[0.35em] text-white/40">{label}</p>
          <p className="mt-1 text-xl font-bold text-white">
            {numericValue != null ? (
              <AnimatedNumber
                value={numericValue}
                decimals={decimals}
                suffix={suffix}
                duration={1400}
              />
            ) : (
              value
            )}
          </p>
        </div>
      </div>
    </div>
  );
}

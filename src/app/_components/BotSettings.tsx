"use client";

import { useEffect, useState } from "react";
import { createBrowserSupabaseClient } from "@/lib/supabaseClient";
import { useI18n } from "@/app/_components/I18nProvider";

type BotSettingsProps = {
  userId: string;
};

export function BotSettings({ userId }: BotSettingsProps) {
  const { t } = useI18n();
  const [language, setLanguage] = useState("english");
  const [coachStyle, setCoachStyle] = useState("narrative");
  const [isSaving, setIsSaving] = useState(false);
  const [isLoading, setIsLoading] = useState(true);

  const languageOptions = [
    { value: "english", label: t("languageEnglish") },
    { value: "arabic", label: t("languageArabic") },
  ];

  const styleOptions = [
    { value: "narrative", label: t("narrativeStyle") },
    { value: "short", label: t("shortStyle") },
    { value: "stats_only", label: t("statsOnlyStyle") },
  ];

  useEffect(() => {
    let isMounted = true;
    const fetchSettings = async () => {
      try {
        const supabase = createBrowserSupabaseClient();
        const { data, error } = await supabase
          .from("users")
          .select("language, coach_style")
          .eq("steam_id", userId)
          .maybeSingle();
        if (error) {
          throw error;
        }
        if (isMounted && data) {
          setLanguage((data.language as string) || "english");
          setCoachStyle((data.coach_style as string) || "narrative");
        }
      } catch (err) {
        console.error("Failed to load bot settings", err);
      } finally {
        if (isMounted) {
          setIsLoading(false);
        }
      }
    };
    if (userId) {
      fetchSettings();
    } else {
      setIsLoading(false);
    }
    return () => {
      isMounted = false;
    };
  }, [userId]);

  const handleSave = async () => {
    if (!userId) {
      return;
    }
    setIsSaving(true);
    try {
      const supabase = createBrowserSupabaseClient();
      const { error } = await supabase
        .from("users")
        .update({ language, coach_style: coachStyle })
        .eq("steam_id", userId);
      if (error) {
        throw error;
      }
      alert(t("settingsSaved"));
    } catch (err) {
      console.error("Failed to save settings", err);
      alert(t("settingsSaveFailed"));
    } finally {
      setIsSaving(false);
    }
  };

  return (
    <div className="rounded-3xl glass-card p-8 animate-fade-in">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <p className="text-[10px] uppercase tracking-[0.4em] text-[#d5ff4c]">
            {t("coachSettings")}
          </p>
          <h2 className="mt-3 text-2xl font-bold text-white">
            {t("customizeCoach")}
          </h2>
        </div>
        <a
          href="https://steamcommunity.com/profiles/76561199559179562/"
          target="_blank"
          rel="noreferrer"
          className="group rounded-xl border border-white/10 bg-white/[0.04] px-5 py-3 text-sm font-semibold text-white transition-all hover:border-white/20 hover:bg-white/[0.08]"
        >
          <span className="flex items-center gap-3">
            <span className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-white/10 bg-white/[0.06] transition group-hover:border-white/20">
              <img
                src="/images/Steam_(service)-Logo.wine.svg"
                alt="Steam"
                className="h-5 w-5"
              />
            </span>
            {t("addBot")}
          </span>
        </a>
      </div>

      <div className="mt-8 grid gap-6 md:grid-cols-2">
        <div className="space-y-2">
          <div className="text-[10px] uppercase tracking-[0.35em] text-white/40">
            {t("languageLabel")}
          </div>
          <div className="lang-select-shell">
            <select
              value={language}
              onChange={(event) => setLanguage(event.target.value)}
              disabled={isLoading}
              className="lang-select"
            >
              {languageOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </div>
        </div>

        <div className="space-y-2">
          <div className="text-[10px] uppercase tracking-[0.35em] text-white/40">
            {t("coachStyle")}
          </div>
          <div className="lang-select-shell">
            <select
              value={coachStyle}
              onChange={(event) => setCoachStyle(event.target.value)}
              disabled={isLoading}
              className="lang-select"
            >
              {styleOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </div>
        </div>
      </div>

      <div className="mt-8 flex items-center justify-between rounded-xl border border-white/[0.04] bg-white/[0.02] px-5 py-4">
        <p className="text-sm text-white/50">
          {isLoading
            ? t("loadingSettings")
            : t("preferencesSaved")}
        </p>
        <button
          type="button"
          onClick={handleSave}
          disabled={isSaving || isLoading}
          className="rounded-lg bg-[#d5ff4c] px-6 py-2.5 text-sm font-bold text-black transition-all hover:bg-[#c4f03c] hover:shadow-[0_0_20px_rgba(213,255,76,0.25)] disabled:cursor-not-allowed disabled:opacity-40"
        >
          {isSaving ? t("saving") : t("saveSettings")}
        </button>
      </div>
    </div>
  );
}

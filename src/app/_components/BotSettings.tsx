"use client";

import { useEffect, useState } from "react";
import { createBrowserSupabaseClient } from "@/lib/supabaseClient";
import { useI18n } from "@/app/_components/I18nProvider";

type BotSettingsProps = {
  userId: string;
};

function Toggle({
  checked,
  onChange,
  disabled,
}: {
  checked: boolean;
  onChange: (v: boolean) => void;
  disabled?: boolean;
}) {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={checked}
      disabled={disabled}
      onClick={() => onChange(!checked)}
      className={`relative inline-flex h-6 w-11 shrink-0 cursor-pointer items-center rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[#d5ff4c] disabled:cursor-not-allowed disabled:opacity-40 ${
        checked ? "bg-[#d5ff4c]" : "bg-white/10"
      }`}
    >
      <span
        className={`pointer-events-none inline-block h-4 w-4 rounded-full shadow-lg ring-0 transition duration-200 ease-in-out ${
          checked
            ? "translate-x-5 bg-black"
            : "translate-x-0.5 bg-white/60"
        }`}
      />
    </button>
  );
}

export function BotSettings({ userId }: BotSettingsProps) {
  const { t } = useI18n();
  const [language, setLanguage] = useState("english");
  const [coachStyle, setCoachStyle] = useState("narrative");
  const [sendCard, setSendCard] = useState(true);
  const [sendTip, setSendTip] = useState(true);
  const [sendLink, setSendLink] = useState(true);
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
          .select("language, coach_style, bot_send_card, bot_send_tip, bot_send_link")
          .eq("steam_id", userId)
          .maybeSingle();
        if (error) {
          throw error;
        }
        if (isMounted && data) {
          setLanguage((data.language as string) || "english");
          setCoachStyle((data.coach_style as string) || "narrative");
          setSendCard(data.bot_send_card !== false);
          setSendTip(data.bot_send_tip !== false);
          setSendLink(data.bot_send_link !== false);
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
        .update({
          language,
          coach_style: coachStyle,
          bot_send_card: sendCard,
          bot_send_tip: sendTip,
          bot_send_link: sendLink,
        })
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

  const toggleItems = [
    {
      key: "card",
      label: t("sendGameCard"),
      desc: t("sendGameCardDesc"),
      checked: sendCard,
      onChange: setSendCard,
      icon: (
        <svg className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
          <rect x="2" y="3" width="20" height="14" rx="2" />
          <path d="M8 21h8" />
          <path d="M12 17v4" />
        </svg>
      ),
    },
    {
      key: "tip",
      label: t("sendCoachTip"),
      desc: t("sendCoachTipDesc"),
      checked: sendTip,
      onChange: setSendTip,
      icon: (
        <svg className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
          <path d="M12 20h9" />
          <path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z" />
        </svg>
      ),
    },
    {
      key: "link",
      label: t("sendMatchLink"),
      desc: t("sendMatchLinkDesc"),
      checked: sendLink,
      onChange: setSendLink,
      icon: (
        <svg className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
          <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
          <polyline points="15 3 21 3 21 9" />
          <line x1="10" y1="14" x2="21" y2="3" />
        </svg>
      ),
    },
  ];

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

      {/* Language & Style selects */}
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

      {/* Bot message toggles */}
      <div className="mt-8">
        <div className="mb-4">
          <p className="text-[10px] uppercase tracking-[0.35em] text-[#67f5ff]/70">
            {t("botSendPrefs")}
          </p>
          <p className="mt-1 text-xs text-white/40">
            {t("botSendPrefsDesc")}
          </p>
        </div>
        <div className="space-y-3">
          {toggleItems.map((item) => (
            <div
              key={item.key}
              className="flex items-center justify-between rounded-xl border border-white/[0.06] bg-white/[0.02] px-5 py-4 transition-colors hover:bg-white/[0.04]"
            >
              <div className="flex items-center gap-4">
                <span className={`text-white/40 transition ${item.checked ? "text-[#d5ff4c]" : ""}`}>
                  {item.icon}
                </span>
                <div>
                  <p className="text-sm font-medium text-white">{item.label}</p>
                  <p className="text-xs text-white/40">{item.desc}</p>
                </div>
              </div>
              <Toggle
                checked={item.checked}
                onChange={item.onChange}
                disabled={isLoading}
              />
            </div>
          ))}
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

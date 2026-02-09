"use client";

import { createContext, useCallback, useContext, useMemo, useState } from "react";
import {
  LANG_COOKIE,
  Language,
  TranslationKey,
  getDirection,
  getLocale,
  getTranslations,
  normalizeLanguage,
} from "@/lib/i18n";

type I18nContextValue = {
  lang: Language;
  dir: "ltr" | "rtl";
  locale: string;
  t: (key: TranslationKey, vars?: Record<string, string | number>) => string;
  setLanguage: (lang: Language) => void;
};

const I18nContext = createContext<I18nContextValue | null>(null);

export const I18nProvider = ({
  lang,
  children,
}: {
  lang: Language;
  children: React.ReactNode;
}) => {
  const [currentLang, setCurrentLang] = useState<Language>(normalizeLanguage(lang));
  const [isSwitching, setIsSwitching] = useState(false);
  const t = useMemo(() => getTranslations(currentLang), [currentLang]);
  const dir = getDirection(currentLang);
  const locale = getLocale(currentLang);

  const setLanguage = useCallback((next: Language) => {
    const nextLang = normalizeLanguage(next);
    if (nextLang === currentLang) {
      return;
    }
    const nextDir = getDirection(nextLang);
    setCurrentLang(nextLang);
    document.cookie = `${LANG_COOKIE}=${nextLang}; path=/; max-age=31536000`;
    document.documentElement.lang = nextLang === "ar" ? "ar" : "en";
    document.documentElement.dir = nextDir;
    document.body.classList.add("lang-switching");
    setIsSwitching(true);
    window.setTimeout(() => {
      window.location.reload();
    }, 1000);
  }, [currentLang]);

  return (
    <I18nContext.Provider value={{ lang: currentLang, dir, locale, t, setLanguage }}>
      {isSwitching ? (
        <div className="lang-switch-overlay" aria-live="polite" role="status">
          <img
            src="/map_icon/De_dust2.png"
            alt="Loading"
            className="lang-switch-icon"
          />
        </div>
      ) : null}
      {children}
    </I18nContext.Provider>
  );
};

export const useI18n = () => {
  const context = useContext(I18nContext);
  if (!context) {
    throw new Error("useI18n must be used within I18nProvider");
  }
  return context;
};

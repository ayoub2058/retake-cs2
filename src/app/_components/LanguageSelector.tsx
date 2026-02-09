"use client";

import { useI18n } from "@/app/_components/I18nProvider";
import { Language } from "@/lib/i18n";

const options: Array<{ value: Language }> = [
  { value: "en" },
  { value: "ar" },
];

export const LanguageSelector = () => {
  const { lang, setLanguage, t } = useI18n();

  return (
    <div className="space-y-2">
      <div className="text-[10px] uppercase tracking-[0.35em] text-white/50">
        {t("languageLabel")}
      </div>
      <div className="lang-select-shell">
        <select
          value={lang}
          onChange={(event) => setLanguage(event.target.value as Language)}
          className="lang-select"
        >
          {options.map((option) => (
            <option key={option.value} value={option.value}>
              {option.value === "ar" ? t("languageArabic") : t("languageEnglish")}
            </option>
          ))}
        </select>
      </div>
    </div>
  );
};

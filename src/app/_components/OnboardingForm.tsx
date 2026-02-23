"use client";

import { useFormState, useFormStatus } from "react-dom";
import { useI18n } from "@/app/_components/I18nProvider";

type ActionState = {
  error: string | null;
};

type OnboardingFormProps = {
  action: (state: ActionState, formData: FormData) => Promise<ActionState>;
  username: string;
  helpUrl: string;
};

const initialState: ActionState = { error: null };

const SubmitButton = () => {
  const { pending } = useFormStatus();
  const { t } = useI18n();

  return (
    <button
      type="submit"
      className="mt-6 w-full rounded-xl bg-[#d5ff4c] px-6 py-4 text-sm font-bold uppercase tracking-[0.15em] text-black transition-all hover:bg-[#c1eb3d] hover:shadow-[0_0_25px_rgba(213,255,76,0.25)] disabled:cursor-not-allowed disabled:opacity-40"
      disabled={pending}
    >
      {pending ? t("savingCode") : t("saveCode")}
    </button>
  );
};

export const OnboardingForm = ({ action, username, helpUrl }: OnboardingFormProps) => {
  const { t } = useI18n();
  const [state, formAction] = useFormState(action, initialState);

  return (
    <div className="w-full max-w-xl rounded-3xl glass-card p-10">
      <div className="mb-8">
        <div className="mb-4 inline-flex items-center gap-2 rounded-full border border-[#d5ff4c]/20 bg-[#d5ff4c]/5 px-4 py-1.5">
          <span className="text-[10px] uppercase tracking-[0.3em] text-[#d5ff4c]">
            {t("secureMatchTracking")}
          </span>
        </div>
        <h1 className="text-3xl font-bold text-white">
          {t("oneLastStep", { username })}
        </h1>
        <p className="mt-3 text-sm leading-relaxed text-white/50">
          {t("trackMatchesDescription")}
        </p>
      </div>

      <form action={formAction} className="space-y-5">
        <div>
          <label className="mb-2 block text-[10px] uppercase tracking-[0.3em] text-white/40">
            {t("gameAuthCode")}
          </label>
          <input
            name="auth_code"
            placeholder="AAAA-AAAAA-AAAA"
            className="w-full rounded-xl border border-white/[0.08] bg-black/50 px-4 py-3.5 text-sm text-white outline-none ring-1 ring-transparent transition-all focus:border-[#d5ff4c]/50 focus:ring-[#d5ff4c]/30 focus:shadow-[0_0_15px_rgba(213,255,76,0.1)] placeholder:text-white/20"
            autoComplete="off"
            inputMode="text"
            pattern="[A-Za-z0-9]{4}-[A-Za-z0-9]{5}-[A-Za-z0-9]{4}"
            title="Format: AAAA-AAAAA-AAAA"
            required
          />
        </div>
        <div>
          <label className="mb-2 block text-[10px] uppercase tracking-[0.3em] text-white/40">
            {t("matchToken")}
          </label>
          <input
            name="last_known_match_code"
            placeholder="CSGO-AAAAA-BBBBB-CCCCC-DDDDD-EEEEE"
            className="w-full rounded-xl border border-white/[0.08] bg-black/50 px-4 py-3.5 text-sm text-white outline-none ring-1 ring-transparent transition-all focus:border-[#d5ff4c]/50 focus:ring-[#d5ff4c]/30 focus:shadow-[0_0_15px_rgba(213,255,76,0.1)] placeholder:text-white/20"
            autoComplete="off"
            inputMode="text"
            pattern="CSGO-[A-Za-z0-9]{5}(-[A-Za-z0-9]{5}){4}"
            title="Format: CSGO-AAAAA-BBBBB-CCCCC-DDDDD-EEEEE"
            required
          />
        </div>
        {state.error ? (
          <div className="flex items-center gap-2 rounded-lg border border-rose-500/20 bg-rose-500/5 px-4 py-3 text-sm text-rose-300">
            <svg className="h-4 w-4 flex-shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="12" cy="12" r="10" />
              <line x1="15" y1="9" x2="9" y2="15" />
              <line x1="9" y1="9" x2="15" y2="15" />
            </svg>
            {state.error}
          </div>
        ) : null}
        <a
          href={helpUrl}
          className="inline-flex items-center gap-1 text-xs uppercase tracking-[0.2em] text-white/40 transition hover:text-[#67f5ff]"
          target="_blank"
          rel="noreferrer"
        >
          <svg className="h-3 w-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <circle cx="12" cy="12" r="10" />
            <path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3" />
            <line x1="12" y1="17" x2="12.01" y2="17" />
          </svg>
          {t("getCodeHere")}
        </a>
        <SubmitButton />
      </form>
    </div>
  );
};

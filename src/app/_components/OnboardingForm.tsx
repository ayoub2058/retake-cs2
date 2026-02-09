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
      className="mt-4 w-full rounded-md bg-[#d5ff4c] px-6 py-3 text-sm font-semibold uppercase tracking-[0.2em] text-black transition hover:bg-[#c1eb3d] disabled:cursor-not-allowed disabled:opacity-60"
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
    <div className="w-full max-w-xl rounded-2xl glass-card p-8">
      <div className="mb-6">
        <p className="text-xs uppercase tracking-[0.35em] text-[#d5ff4c]">
          {t("secureMatchTracking")}
        </p>
        <h1 className="mt-3 text-3xl font-semibold text-white">
          {t("oneLastStep", { username })}
        </h1>
        <p className="mt-3 text-sm text-zinc-300">
          {t("trackMatchesDescription")}
        </p>
      </div>

      <form action={formAction} className="space-y-3">
        <label className="block text-xs uppercase tracking-[0.25em] text-zinc-400">
          {t("gameAuthCode")}
        </label>
        <input
          name="auth_code"
          placeholder="AAAA-AAAAA-AAAA"
          className="w-full rounded-md border border-white/10 bg-black/40 px-4 py-3 text-sm text-white outline-none ring-1 ring-transparent transition focus:border-[#d5ff4c] focus:ring-[#d5ff4c]"
          autoComplete="off"
          inputMode="text"
          pattern="[A-Za-z0-9]{4}-[A-Za-z0-9]{5}-[A-Za-z0-9]{4}"
          title="Format: AAAA-AAAAA-AAAA"
          required
        />
        <label className="mt-4 block text-xs uppercase tracking-[0.25em] text-zinc-400">
          {t("matchToken")}
        </label>
        <input
          name="last_known_match_code"
          placeholder="CSGO-AAAAA-BBBBB-CCCCC-DDDDD-EEEEE"
          className="w-full rounded-md border border-white/10 bg-black/40 px-4 py-3 text-sm text-white outline-none ring-1 ring-transparent transition focus:border-[#d5ff4c] focus:ring-[#d5ff4c]"
          autoComplete="off"
          inputMode="text"
          pattern="CSGO-[A-Za-z0-9]{5}(-[A-Za-z0-9]{5}){4}"
          title="Format: CSGO-AAAAA-BBBBB-CCCCC-DDDDD-EEEEE"
          required
        />
        {state.error ? (
          <p className="text-sm text-red-400">{state.error}</p>
        ) : null}
        <a
          href={helpUrl}
          className="inline-flex items-center text-xs uppercase tracking-[0.2em] text-zinc-400 transition hover:text-white"
          target="_blank"
          rel="noreferrer"
        >
          {t("getCodeHere")}
        </a>
        <SubmitButton />
      </form>
    </div>
  );
};

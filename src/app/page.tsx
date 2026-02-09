import { redirect } from "next/navigation";
import { createServerSupabaseClient } from "@/lib/supabaseServer";
import { getSteamSessionFromCookies } from "@/lib/steamSessionServer";
import { OnboardingForm } from "@/app/_components/OnboardingForm";
import { cookies } from "next/headers";
import { getTranslations, normalizeLanguage, LANG_COOKIE } from "@/lib/i18n";

type ActionState = {
  error: string | null;
};

const CODE_PATTERN = /^[A-Z0-9]{4}-[A-Z0-9]{5}-[A-Z0-9]{4}$/;
const KNOWN_CODE_PATTERN = /^CSGO-([A-Z0-9]{5}-){4}[A-Z0-9]{5}$/i;


async function saveAuthCode(
  _state: ActionState,
  formData: FormData
): Promise<ActionState> {
  "use server";

  const lang = normalizeLanguage((await cookies()).get(LANG_COOKIE)?.value);
  const t = getTranslations(lang);

  const session = await getSteamSessionFromCookies();
  if (!session) {
    return { error: t("errorSignedIn") };
  }

  const codeRaw = formData.get("auth_code");
  const code = typeof codeRaw === "string" ? codeRaw.trim().toUpperCase() : "";
  const knownRaw = formData.get("last_known_match_code");
  const knownCode = typeof knownRaw === "string" ? knownRaw.trim() : "";

  if (!CODE_PATTERN.test(code)) {
    return { error: t("errorInvalidCode") };
  }

  if (!KNOWN_CODE_PATTERN.test(knownCode)) {
    return {
      error: t("errorInvalidToken"),
    };
  }

  const supabase = await createServerSupabaseClient();
  const { data: userRow, error: userError } = await supabase
    .from("users")
    .select("steam_id")
    .eq("steam_id", session.steamId)
    .maybeSingle();

  if (userError || !userRow) {
    return { error: t("errorUserNotFound") };
  }

  const { error: updateError } = await supabase
    .from("users")
    .update({ auth_code: code, last_known_match_code: knownCode })
    .eq("steam_id", session.steamId);

  if (updateError) {
    return { error: t("errorSaveAuthFailed") };
  }

  redirect("/dashboard");
}

const GuestView = ({ t }: { t: ReturnType<typeof getTranslations> }) => {
  return (
    <div className="relative min-h-screen overflow-hidden bg-[#0b0d0f] text-white">
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_top,_rgba(213,255,76,0.12),transparent_45%),radial-gradient(circle_at_25%_80%,_rgba(0,135,255,0.12),transparent_45%)]" />
      <div className="absolute inset-0 bg-[linear-gradient(120deg,rgba(255,255,255,0.06),transparent_45%,rgba(255,255,255,0.04))] opacity-60" />
      <main className="relative z-10 flex min-h-screen items-center justify-center px-6 py-16">
        <div className="w-full max-w-4xl">
          <p className="text-xs uppercase tracking-[0.35em] text-[#d5ff4c]">
            {t("guestTitle")}
          </p>
          <h1 className="mt-5 text-4xl font-semibold leading-tight text-white sm:text-5xl">
            {t("guestSubtitle")}
          </h1>
          <p className="mt-4 max-w-2xl text-base text-zinc-300">
            {t("guestDescription")}
          </p>
          <div className="mt-8 flex flex-wrap gap-4">
            <a
              href="/api/auth/steam"
              className="inline-flex items-center justify-center rounded-md bg-[#d5ff4c] px-6 py-3 text-sm font-semibold uppercase tracking-[0.2em] text-black transition hover:bg-[#c1eb3d]"
            >
              {t("signInWithSteam")}
            </a>
            <div className="rounded-md border border-white/15 bg-black/40 px-5 py-3 text-sm text-zinc-300">
              {t("instantReplays")}
            </div>
          </div>
        </div>
      </main>
    </div>
  );
};

const OnboardingView = ({
  username,
  t,
}: {
  username: string;
  t: ReturnType<typeof getTranslations>;
}) => {
  return (
    <div className="relative min-h-screen overflow-hidden bg-[#0b0d0f] text-white">
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_top,_rgba(213,255,76,0.12),transparent_45%),radial-gradient(circle_at_25%_80%,_rgba(0,135,255,0.12),transparent_45%)]" />
      <div className="absolute inset-0 bg-[linear-gradient(120deg,rgba(255,255,255,0.06),transparent_45%,rgba(255,255,255,0.04))] opacity-60" />
      <main className="relative z-10 flex min-h-screen items-center justify-center px-6 py-16">
        <OnboardingForm
          action={saveAuthCode}
          username={username}
          helpUrl="https://help.steampowered.com/en/wizard/HelpWithGameIssue/?appid=730&issueid=128"
        />
      </main>
    </div>
  );
};

export default async function Home() {
  const lang = normalizeLanguage((await cookies()).get(LANG_COOKIE)?.value);
  const t = getTranslations(lang);
  const session = await getSteamSessionFromCookies();
  if (!session) {
    return <GuestView t={t} />;
  }

  const supabase = await createServerSupabaseClient();
  const { data: userRow } = await supabase
    .from("users")
    .select("steam_id, username, auth_code, last_known_match_code")
    .eq("steam_id", session.steamId)
    .maybeSingle();

  const authCode = userRow?.auth_code?.trim();
  const lastKnown = userRow?.last_known_match_code?.trim();

  if (authCode && lastKnown) {
    redirect("/dashboard");
  }

  return <OnboardingView username={userRow?.username || session.username} t={t} />;
}

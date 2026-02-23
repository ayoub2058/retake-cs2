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
      <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_top_center,_rgba(213,255,76,0.1),transparent_50%),radial-gradient(ellipse_at_20%_80%,_rgba(0,135,255,0.08),transparent_45%)]" />
      <div className="absolute inset-0 bg-[linear-gradient(160deg,rgba(255,255,255,0.04),transparent_40%,rgba(255,255,255,0.02))] opacity-80" />
      <main className="relative z-10 flex min-h-screen items-center justify-center px-6 py-16">
        <div className="w-full max-w-4xl animate-fade-in">
          <div className="mb-6 inline-flex items-center gap-2 rounded-full border border-[#d5ff4c]/20 bg-[#d5ff4c]/5 px-4 py-1.5">
            <span className="status-dot online" />
            <span className="text-[10px] uppercase tracking-[0.3em] text-[#d5ff4c]">
              {t("guestTitle")}
            </span>
          </div>
          <h1 className="text-4xl font-bold leading-tight text-white sm:text-5xl lg:text-6xl">
            {t("guestSubtitle")}
          </h1>
          <p className="mt-5 max-w-2xl text-base leading-relaxed text-white/50">
            {t("guestDescription")}
          </p>
          <div className="mt-10 flex flex-wrap gap-4">
            <a
              href="/api/auth/steam"
              className="inline-flex items-center justify-center gap-2 rounded-xl bg-[#d5ff4c] px-8 py-4 text-sm font-bold uppercase tracking-[0.15em] text-black transition-all hover:bg-[#c1eb3d] hover:shadow-[0_0_30px_rgba(213,255,76,0.3)]"
            >
              <svg className="h-5 w-5" viewBox="0 0 24 24" fill="currentColor">
                <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z" />
              </svg>
              {t("signInWithSteam")}
            </a>
            <div className="flex items-center rounded-xl border border-white/10 bg-white/[0.03] px-6 py-4 text-sm text-white/50">
              <svg className="mr-2 h-4 w-4 text-[#67f5ff]" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" />
              </svg>
              {t("instantReplays")}
            </div>
          </div>

          {/* Feature cards */}
          <div className="mt-16 grid gap-4 sm:grid-cols-3">
            <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-6 backdrop-blur-sm transition hover:border-white/10">
              <div className="mb-3 flex h-10 w-10 items-center justify-center rounded-lg bg-[#d5ff4c]/10">
                <svg className="h-5 w-5 text-[#d5ff4c]" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
                  <polyline points="7 10 12 15 17 10" />
                  <line x1="12" y1="15" x2="12" y2="3" />
                </svg>
              </div>
              <h3 className="text-sm font-semibold text-white">Auto Downloads</h3>
              <p className="mt-1.5 text-xs leading-relaxed text-white/40">Replays downloaded automatically after each match</p>
            </div>
            <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-6 backdrop-blur-sm transition hover:border-white/10">
              <div className="mb-3 flex h-10 w-10 items-center justify-center rounded-lg bg-[#67f5ff]/10">
                <svg className="h-5 w-5 text-[#67f5ff]" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M18 20V10" />
                  <path d="M12 20V4" />
                  <path d="M6 20v-6" />
                </svg>
              </div>
              <h3 className="text-sm font-semibold text-white">Deep Analytics</h3>
              <p className="mt-1.5 text-xs leading-relaxed text-white/40">ADR, K/D, opening duels, utility damage, and more</p>
            </div>
            <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-6 backdrop-blur-sm transition hover:border-white/10">
              <div className="mb-3 flex h-10 w-10 items-center justify-center rounded-lg bg-[#a78bfa]/10">
                <svg className="h-5 w-5 text-[#a78bfa]" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M12 20h9" />
                  <path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z" />
                </svg>
              </div>
              <h3 className="text-sm font-semibold text-white">AI Coaching</h3>
              <p className="mt-1.5 text-xs leading-relaxed text-white/40">Personalized tips delivered via Steam chat</p>
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
      <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_top_center,_rgba(213,255,76,0.08),transparent_50%),radial-gradient(ellipse_at_20%_80%,_rgba(0,135,255,0.06),transparent_45%)]" />
      <main className="relative z-10 flex min-h-screen items-center justify-center px-6 py-16">
        <div className="animate-fade-in">
          <OnboardingForm
            action={saveAuthCode}
            username={username}
            helpUrl="https://help.steampowered.com/en/wizard/HelpWithGameIssue/?appid=730&issueid=128"
          />
        </div>
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

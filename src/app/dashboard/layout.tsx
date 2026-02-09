import Link from "next/link";
import { cookies } from "next/headers";
import { GlobalSearch } from "@/app/_components/GlobalSearch";
import { LanguageSelector } from "@/app/_components/LanguageSelector";
import { getTranslations, normalizeLanguage, LANG_COOKIE } from "@/lib/i18n";

export default async function DashboardLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const lang = normalizeLanguage((await cookies()).get(LANG_COOKIE)?.value);
  const t = getTranslations(lang);
  return (
    <div className="flex h-screen w-full overflow-hidden text-white">
      <aside className="h-full w-64 flex-shrink-0 border-r border-white/10 bg-black/60 backdrop-blur-md">
        <div className="flex h-full flex-col px-6 py-10">
          <img
            src="/images/Sans%20titre-1.png?v=2"
            alt="Counter-Strike 2"
            className="mb-6 mt-0 h-auto w-full max-h-16 rounded-xl border border-lime-300/60 bg-black/20 p-2 opacity-95 drop-shadow-[0_0_22px_rgba(163,230,53,0.45)] brightness-120 saturate-170 hue-rotate-[95deg] transition hover:opacity-100 object-contain"
          />
          <p className="text-xs uppercase tracking-[0.35em] text-[#67f5ff]">
            {t("controlRoom")}
          </p>
          <nav className="mt-8 flex flex-col gap-3 text-sm">
            <Link
              href="/dashboard"
              className="rounded-2xl border border-white/10 bg-white/10 px-4 py-3 transition hover:border-white/40 hover:bg-white/15"
            >
              {t("matchHistory")}
            </Link>
            <Link
              href="/dashboard/stats"
              className="rounded-2xl border border-white/10 bg-white/10 px-4 py-3 transition hover:border-white/40 hover:bg-white/15"
            >
              {t("playerStats")}
            </Link>
          </nav>
          <div className="mt-auto pt-6">
            <LanguageSelector />
          </div>
        </div>
      </aside>
      <main className="relative h-full flex-1 overflow-y-auto">
        <div className="mx-auto w-full max-w-[1920px] p-6">
          <div className="mb-8">
            <GlobalSearch />
          </div>
          {children}
        </div>
      </main>
    </div>
  );
}

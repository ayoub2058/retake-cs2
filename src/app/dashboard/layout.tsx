import Link from "next/link";
import { cookies } from "next/headers";
import { GlobalSearch } from "@/app/_components/GlobalSearch";
import { LanguageSelector } from "@/app/_components/LanguageSelector";
import { MobileSidebarToggle } from "@/app/_components/MobileSidebar";
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
      {/* Sidebar — hidden on mobile */}
      <aside className="group/sidebar hidden h-full w-64 flex-shrink-0 border-r border-white/[0.06] bg-gradient-to-b from-black/80 via-black/70 to-black/80 backdrop-blur-xl lg:block">
        <div className="flex h-full flex-col px-5 py-8">
          {/* Logo */}
          <div className="mb-8 flex items-center gap-3">
            <img
              src="/images/retakeai-icon.png"
              alt="RetakeAI"
              className="h-11 w-11 rounded-xl object-cover ring-1 ring-lime-400/30 drop-shadow-[0_0_14px_rgba(163,230,53,0.3)] transition hover:ring-lime-400/50 hover:drop-shadow-[0_0_22px_rgba(163,230,53,0.5)]"
            />
            <div>
              <h2 className="text-sm font-bold tracking-wide text-white">RetakeAI</h2>
              <p className="text-[10px] uppercase tracking-[0.25em] text-white/40">CS2 Intelligence</p>
            </div>
          </div>

          {/* Section label */}
          <p className="mb-4 text-[10px] uppercase tracking-[0.35em] text-[#67f5ff]/70">
            {t("controlRoom")}
          </p>

          {/* Navigation */}
          <nav className="flex flex-col gap-1.5 text-sm">
            <Link
              href="/dashboard"
              className="group flex items-center gap-3 rounded-xl border border-transparent px-4 py-3 text-white/80 transition-all hover:border-white/10 hover:bg-white/[0.06] hover:text-white"
            >
              <svg className="h-4 w-4 text-white/40 transition group-hover:text-[#d5ff4c]" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <rect x="3" y="3" width="7" height="7" rx="1" />
                <rect x="14" y="3" width="7" height="7" rx="1" />
                <rect x="3" y="14" width="7" height="7" rx="1" />
                <rect x="14" y="14" width="7" height="7" rx="1" />
              </svg>
              {t("matchHistory")}
            </Link>
            <Link
              href="/dashboard/stats"
              className="group flex items-center gap-3 rounded-xl border border-transparent px-4 py-3 text-white/80 transition-all hover:border-white/10 hover:bg-white/[0.06] hover:text-white"
            >
              <svg className="h-4 w-4 text-white/40 transition group-hover:text-[#67f5ff]" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M18 20V10" />
                <path d="M12 20V4" />
                <path d="M6 20v-6" />
              </svg>
              {t("playerStats")}
            </Link>
          </nav>

          {/* Divider */}
          <div className="my-6 h-px bg-gradient-to-r from-transparent via-white/10 to-transparent" />

          {/* Language selector at bottom */}
          <div className="mt-auto pt-4">
            <LanguageSelector />
          </div>

          {/* Version */}
          <p className="mt-4 text-center text-[9px] uppercase tracking-[0.3em] text-white/20">
            v2.0
          </p>
        </div>
      </aside>

      {/* Main content */}
      <main className="relative h-full flex-1 overflow-y-auto">
        <div className="mx-auto w-full max-w-[1920px] p-6 lg:p-8">
          {/* Top bar with search */}
          <div className="mb-8 flex items-center justify-between gap-4">
            <div className="flex items-center gap-3 flex-1 min-w-0">
              <MobileSidebarToggle />
              <div className="flex-1 min-w-0">
                <GlobalSearch />
              </div>
            </div>
            <div className="hidden items-center gap-2 md:flex">
              <span className="status-dot online" />
              <span className="text-xs text-white/50">Connected</span>
            </div>
          </div>
          {children}
        </div>
      </main>
    </div>
  );
}

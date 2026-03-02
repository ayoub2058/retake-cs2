"use client";

import { useState, useEffect } from "react";

export function MobileSidebarToggle() {
  const [open, setOpen] = useState(false);

  useEffect(() => {
    const handleResize = () => {
      if (window.innerWidth >= 1024) setOpen(false);
    };
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  useEffect(() => {
    document.body.style.overflow = open ? "hidden" : "";
    return () => {
      document.body.style.overflow = "";
    };
  }, [open]);

  return (
    <>
      {/* Hamburger button — visible only on mobile */}
      <button
        type="button"
        onClick={() => setOpen(true)}
        className="lg:hidden flex h-10 w-10 items-center justify-center rounded-xl border border-white/10 bg-white/[0.04] text-white/70 transition hover:bg-white/[0.08] hover:text-white"
        aria-label="Open menu"
      >
        <svg className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <line x1="3" y1="6" x2="21" y2="6" />
          <line x1="3" y1="12" x2="21" y2="12" />
          <line x1="3" y1="18" x2="21" y2="18" />
        </svg>
      </button>

      {/* Backdrop */}
      {open && (
        <div
          className="fixed inset-0 z-40 bg-black/60 backdrop-blur-sm lg:hidden"
          onClick={() => setOpen(false)}
        />
      )}

      {/* Slide-in sidebar panel */}
      <div
        className={`fixed inset-y-0 left-0 z-50 w-72 transform border-r border-white/[0.06] bg-gradient-to-b from-black/95 via-black/90 to-black/95 backdrop-blur-xl transition-transform duration-300 lg:hidden ${
          open ? "translate-x-0" : "-translate-x-full"
        }`}
      >
        <div className="flex h-full flex-col px-5 py-8">
          {/* Close button */}
          <button
            type="button"
            onClick={() => setOpen(false)}
            className="absolute right-4 top-4 flex h-8 w-8 items-center justify-center rounded-lg text-white/50 transition hover:bg-white/10 hover:text-white"
            aria-label="Close menu"
          >
            <svg className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <line x1="18" y1="6" x2="6" y2="18" />
              <line x1="6" y1="6" x2="18" y2="18" />
            </svg>
          </button>

          {/* Logo */}
          <div className="mb-8 flex items-center gap-3">
            <img
              src="/images/retakeai-icon.png"
              alt="RetakeAI"
              className="h-11 w-11 rounded-xl object-cover ring-1 ring-lime-400/30 drop-shadow-[0_0_14px_rgba(163,230,53,0.3)]"
            />
            <div>
              <h2 className="text-sm font-bold tracking-wide text-white">RetakeAI</h2>
              <p className="text-[10px] uppercase tracking-[0.25em] text-white/40">CS2 Intelligence</p>
            </div>
          </div>

          {/* Navigation — uses <a> to force full page nav & close sidebar */}
          <nav className="flex flex-col gap-1.5 text-sm">
            <a
              href="/dashboard"
              onClick={() => setOpen(false)}
              className="group flex items-center gap-3 rounded-xl border border-transparent px-4 py-3 text-white/80 transition-all hover:border-white/10 hover:bg-white/[0.06] hover:text-white"
            >
              <svg className="h-4 w-4 text-white/40 transition group-hover:text-[#d5ff4c]" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <rect x="3" y="3" width="7" height="7" rx="1" />
                <rect x="14" y="3" width="7" height="7" rx="1" />
                <rect x="3" y="14" width="7" height="7" rx="1" />
                <rect x="14" y="14" width="7" height="7" rx="1" />
              </svg>
              Match History
            </a>
            <a
              href="/dashboard/stats"
              onClick={() => setOpen(false)}
              className="group flex items-center gap-3 rounded-xl border border-transparent px-4 py-3 text-white/80 transition-all hover:border-white/10 hover:bg-white/[0.06] hover:text-white"
            >
              <svg className="h-4 w-4 text-white/40 transition group-hover:text-[#67f5ff]" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M18 20V10" />
                <path d="M12 20V4" />
                <path d="M6 20v-6" />
              </svg>
              Player Stats
            </a>
          </nav>
        </div>
      </div>
    </>
  );
}

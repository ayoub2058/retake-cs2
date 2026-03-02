"use client";

import { useEffect, useState, useCallback, type ReactNode } from "react";

type ToastType = "success" | "error" | "info";

type ToastData = {
  id: number;
  message: string;
  type: ToastType;
};

let toastId = 0;
let addToastGlobal: ((message: string, type?: ToastType) => void) | null = null;

/** Fire a toast from anywhere */
export const toast = {
  success: (msg: string) => addToastGlobal?.(msg, "success"),
  error: (msg: string) => addToastGlobal?.(msg, "error"),
  info: (msg: string) => addToastGlobal?.(msg, "info"),
};

const ICON: Record<ToastType, ReactNode> = {
  success: (
    <svg className="h-4 w-4 text-emerald-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="20 6 9 17 4 12" />
    </svg>
  ),
  error: (
    <svg className="h-4 w-4 text-rose-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="10" />
      <line x1="15" y1="9" x2="9" y2="15" />
      <line x1="9" y1="9" x2="15" y2="15" />
    </svg>
  ),
  info: (
    <svg className="h-4 w-4 text-[#67f5ff]" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="10" />
      <line x1="12" y1="16" x2="12" y2="12" />
      <line x1="12" y1="8" x2="12.01" y2="8" />
    </svg>
  ),
};

const BORDER_COLOR: Record<ToastType, string> = {
  success: "border-emerald-500/30",
  error: "border-rose-500/30",
  info: "border-[#67f5ff]/30",
};

export function ToastProvider() {
  const [toasts, setToasts] = useState<ToastData[]>([]);

  const addToast = useCallback((message: string, type: ToastType = "info") => {
    const id = ++toastId;
    setToasts((prev) => [...prev, { id, message, type }]);
    setTimeout(() => {
      setToasts((prev) => prev.filter((t) => t.id !== id));
    }, 3500);
  }, []);

  useEffect(() => {
    addToastGlobal = addToast;
    return () => {
      addToastGlobal = null;
    };
  }, [addToast]);

  if (!toasts.length) return null;

  return (
    <div className="fixed bottom-6 right-6 z-[9999] flex flex-col-reverse gap-2">
      {toasts.map((t) => (
        <div
          key={t.id}
          className={`animate-slide-up flex items-center gap-3 rounded-xl border ${BORDER_COLOR[t.type]} bg-black/80 px-5 py-3.5 text-sm text-white shadow-2xl backdrop-blur-lg`}
        >
          {ICON[t.type]}
          <span>{t.message}</span>
        </div>
      ))}
    </div>
  );
}

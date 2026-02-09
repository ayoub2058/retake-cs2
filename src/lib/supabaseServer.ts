import { createServerClient } from "@supabase/ssr";
import { cookies } from "next/headers";

const getSupabaseUrl = (): string => {
  return (
    process.env.NEXT_PUBLIC_SUPABASE_URL ||
    process.env.SUPABASE_URL ||
    ""
  );
};

const getSupabaseAnonKey = (): string => {
  return (
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ||
    process.env.SUPABASE_ANON_KEY ||
    ""
  );
};

const resolveCookieStore = async () => {
  const cookieStore = cookies();
  if (typeof (cookieStore as Promise<unknown>)?.then === "function") {
    return await cookieStore;
  }
  return cookieStore;
};

type CookieEntry = { name: string; value: string };

const safeGetAll = (cookieStore: { getAll?: () => unknown[] }): CookieEntry[] => {
  if (typeof cookieStore.getAll === "function") {
    const items = cookieStore.getAll();
    if (Array.isArray(items)) {
      return items
        .map((item) => {
          if (!item || typeof item !== "object") {
            return null;
          }
          const record = item as { name?: unknown; value?: unknown };
          if (typeof record.name !== "string" || typeof record.value !== "string") {
            return null;
          }
          return { name: record.name, value: record.value };
        })
        .filter((item): item is CookieEntry => Boolean(item));
    }
  }
  return [];
};

export const createServerSupabaseClient = async () => {
  const supabaseUrl = getSupabaseUrl();
  const supabaseAnonKey = getSupabaseAnonKey();

  if (!supabaseUrl || !supabaseAnonKey) {
    throw new Error(
      "SUPABASE_URL/NEXT_PUBLIC_SUPABASE_URL and SUPABASE_ANON_KEY/NEXT_PUBLIC_SUPABASE_ANON_KEY must be set"
    );
  }

  const cookieStore = await resolveCookieStore();

  return createServerClient(supabaseUrl, supabaseAnonKey, {
    cookies: {
      getAll() {
        return safeGetAll(cookieStore);
      },
      setAll(cookiesToSet) {
        cookiesToSet.forEach(({ name, value, options }) => {
          cookieStore.set(name, value, options);
        });
      },
    },
  });
};

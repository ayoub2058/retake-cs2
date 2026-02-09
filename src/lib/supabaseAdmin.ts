import { createClient } from "@supabase/supabase-js";
import type { SupabaseClient } from "@supabase/supabase-js";

const supabaseUrl = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL;
const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const hasConfig = Boolean(supabaseUrl && serviceRoleKey);

const createAdminClient = () =>
  createClient(supabaseUrl as string, serviceRoleKey as string, {
    auth: {
      persistSession: false,
    },
  });

const createMissingConfigProxy = () =>
  new Proxy(
    {},
    {
      get() {
        throw new Error(
          "Supabase admin env vars are missing. Set SUPABASE_URL (or NEXT_PUBLIC_SUPABASE_URL) and SUPABASE_SERVICE_ROLE_KEY."
        );
      },
    }
  ) as SupabaseClient;

export const supabaseAdmin: SupabaseClient = hasConfig
  ? createAdminClient()
  : createMissingConfigProxy();

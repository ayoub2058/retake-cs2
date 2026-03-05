-- Track when tip_sent was set to true so recovery doesn't reset freshly-claimed tips
ALTER TABLE public.matches_to_download
ADD COLUMN IF NOT EXISTS tip_claimed_at timestamptz;

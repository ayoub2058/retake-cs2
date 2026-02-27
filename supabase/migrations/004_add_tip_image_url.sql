-- Add tip_image_url column to store the stats card screenshot URL
ALTER TABLE public.matches_to_download
ADD COLUMN IF NOT EXISTS tip_image_url TEXT;

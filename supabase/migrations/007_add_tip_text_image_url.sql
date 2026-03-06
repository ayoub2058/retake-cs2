-- Store Arabic coaching tip as an image URL (for better readability in Steam chat)
ALTER TABLE public.matches_to_download
ADD COLUMN IF NOT EXISTS tip_text_image_url TEXT;

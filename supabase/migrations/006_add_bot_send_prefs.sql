-- User preferences for what the Steam bot sends
ALTER TABLE public.users
ADD COLUMN IF NOT EXISTS bot_send_card boolean NOT NULL DEFAULT true,
ADD COLUMN IF NOT EXISTS bot_send_tip boolean NOT NULL DEFAULT true,
ADD COLUMN IF NOT EXISTS bot_send_link boolean NOT NULL DEFAULT true;

import type { NextApiRequest, NextApiResponse } from "next";
import { serialize } from "cookie";
import { configureSteamPassport } from "@/lib/steamPassport";
import { supabaseAdmin } from "@/lib/supabaseAdmin";
import { encodeSteamSession } from "@/lib/steamSession";

type SteamProfile = {
  displayName?: string;
  photos?: Array<{ value?: string }>;
  _json?: {
    personaname?: string;
    avatarfull?: string;
  };
};

const extractSteamId = (claimedId: string | undefined): string | null => {
  if (!claimedId) {
    return null;
  }
  const match = claimedId.match(/https?:\/\/steamcommunity\.com\/openid\/id\/(\d{17})/);
  return match ? match[1] : null;
};

const getUsername = (profile: SteamProfile): string => {
  if (profile.displayName) {
    return profile.displayName;
  }
  const jsonProfile = profile._json as { personaname?: string } | undefined;
  return jsonProfile?.personaname ?? "Unknown";
};

const getAvatarUrl = (profile: SteamProfile): string | null => {
  const photo = profile.photos?.[0]?.value;
  if (photo) {
    return photo;
  }
  const jsonProfile = profile._json as { avatarfull?: string } | undefined;
  return jsonProfile?.avatarfull ?? null;
};

export default function handler(req: NextApiRequest, res: NextApiResponse) {
  const passport = configureSteamPassport(req);
  const reqAny = req as unknown as any;
  const resAny = res as unknown as any;

  return passport.initialize()(reqAny, resAny, () => {
    passport.authenticate(
      "steam",
      { session: false },
      async (error: unknown, profile: SteamProfile | false) => {
        if (error || !profile) {
          res.status(401).json({ error: "Steam authentication failed." });
          return;
        }

        const claimedId =
          typeof req.query["openid.claimed_id"] === "string"
            ? req.query["openid.claimed_id"]
            : undefined;
        const steamId = extractSteamId(claimedId);

        if (!steamId) {
          res.status(400).json({ error: "Missing or invalid SteamID." });
          return;
        }

        const username = getUsername(profile);
        const avatarUrl = getAvatarUrl(profile);

        const { error: upsertError } = await supabaseAdmin
          .from("users")
          .upsert(
            {
              steam_id: steamId,
              username,
              avatar_url: avatarUrl,
            },
            { onConflict: "steam_id" }
          );

        if (upsertError) {
          res.status(500).json({ error: "Failed to upsert user." });
          return;
        }

        const sessionValue = encodeSteamSession({
          steamId,
          username,
        });
        res.setHeader(
          "Set-Cookie",
          serialize("steam_session", sessionValue, {
            httpOnly: true,
            sameSite: "lax",
            secure: process.env.NODE_ENV === "production",
            path: "/",
            maxAge: 60 * 60 * 24 * 30,
          })
        );

        res.redirect(302, "/");
      }
    )(reqAny, resAny, () => {
      res.status(500).end("Steam authentication callback failed.");
    });
  });
}

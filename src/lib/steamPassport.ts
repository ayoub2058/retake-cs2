import passport from "passport";
import { Strategy as SteamStrategy } from "passport-steam";
import type { NextApiRequest } from "next";

const getBaseUrl = (req: NextApiRequest): string => {
  const forwardedProto = req.headers["x-forwarded-proto"];
  const protocol =
    typeof forwardedProto === "string"
      ? forwardedProto.split(",")[0]
      : "http";
  const host = req.headers.host;
  if (!host) {
    throw new Error("Missing Host header");
  }
  return `${protocol}://${host}`;
};

export const configureSteamPassport = (req: NextApiRequest) => {
  const apiKey = process.env.STEAM_API_KEY;
  if (!apiKey) {
    throw new Error("STEAM_API_KEY must be set");
  }

  const baseUrl = getBaseUrl(req);

  passport.use(
    new SteamStrategy(
      {
        returnURL: `${baseUrl}/api/auth/steam/callback`,
        realm: baseUrl,
        apiKey,
      },
      (_identifier, profile, done) => {
        done(null, profile);
      }
    )
  );

  return passport;
};

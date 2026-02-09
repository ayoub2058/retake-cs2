import crypto from "crypto";

export type SteamSession = {
  steamId: string;
  username: string;
};

const getSessionSecret = () => process.env.SESSION_SECRET || "";

const sign = (payload: string, secret: string) => {
  return crypto.createHmac("sha256", secret).update(payload).digest("hex");
};

export const encodeSteamSession = (session: SteamSession): string => {
  const secret = getSessionSecret();
  if (!secret) {
    throw new Error("SESSION_SECRET must be set");
  }

  const payload = Buffer.from(JSON.stringify(session)).toString("base64url");
  const signature = sign(payload, secret);
  return `${payload}.${signature}`;
};

export const decodeSteamSession = (value: string | undefined): SteamSession | null => {
  if (!value) {
    return null;
  }
  const secret = getSessionSecret();
  if (!secret) {
    return null;
  }

  const [payload, signature] = value.split(".");
  if (!payload || !signature) {
    return null;
  }

  const expected = sign(payload, secret);
  if (!crypto.timingSafeEqual(Buffer.from(signature), Buffer.from(expected))) {
    return null;
  }

  try {
    const parsed = JSON.parse(Buffer.from(payload, "base64url").toString("utf8"));
    if (
      !parsed ||
      typeof parsed.steamId !== "string" ||
      typeof parsed.username !== "string"
    ) {
      return null;
    }
    return { steamId: parsed.steamId, username: parsed.username };
  } catch {
    return null;
  }
};

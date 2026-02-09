import { NextRequest, NextResponse } from "next/server";

type SteamPlayer = {
  id: string;
  name: string;
  avatar: string | null;
};

const isSteamId64 = (value: string) => /^765\d{14}$/.test(value);

const extractFromProfileUrl = (value: string) => {
  try {
    const url = new URL(value);
    if (!url.hostname.includes("steamcommunity.com")) {
      return null;
    }
    const parts = url.pathname.split("/").filter(Boolean);
    if (parts.length < 2) {
      return null;
    }
    const [kind, idPart] = parts;
    if (kind === "profiles" && isSteamId64(idPart)) {
      return { steamId: idPart, vanity: null };
    }
    if (kind === "id" && idPart) {
      return { steamId: null, vanity: idPart };
    }
  } catch {
    return null;
  }
  return null;
};

const fetchJson = async (url: string) => {
  const response = await fetch(url);
  if (!response.ok) {
    return null;
  }
  try {
    return await response.json();
  } catch {
    return null;
  }
};

export async function GET(request: NextRequest) {
  const url = new URL(request.url);
  const query = url.searchParams.get("q")?.trim();
  if (!query) {
    return NextResponse.json(
      { error: "Missing query parameter 'q'." },
      { status: 400 }
    );
  }

  const apiKey = process.env.STEAM_API_KEY?.trim();
  if (!apiKey) {
    return NextResponse.json(
      { error: "STEAM_API_KEY is not configured." },
      { status: 500 }
    );
  }

  const extracted = extractFromProfileUrl(query);
  let steamId = extracted?.steamId ?? query;
  const vanityCandidate = extracted?.vanity ?? (!isSteamId64(query) ? query : null);

  if (!isSteamId64(steamId)) {
    if (!vanityCandidate) {
      return NextResponse.json(
        { players: [], error: "No Steam ID found for that profile URL." },
        { status: 404 }
      );
    }
    const vanityUrl = `http://api.steampowered.com/ISteamUser/ResolveVanityURL/v0001/?key=${apiKey}&vanityurl=${encodeURIComponent(vanityCandidate)}`;
    const vanityResponse = await fetchJson(vanityUrl);
    const resolvedId = vanityResponse?.response?.steamid;
    if (typeof resolvedId !== "string" || !isSteamId64(resolvedId)) {
      return NextResponse.json(
        { players: [], error: "No Steam ID found for that vanity URL." },
        { status: 404 }
      );
    }
    steamId = resolvedId;
  }

  const summaryUrl = `http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key=${apiKey}&steamids=${steamId}`;
  const summaryResponse = await fetchJson(summaryUrl);
  const players = summaryResponse?.response?.players;
  if (!Array.isArray(players) || players.length === 0) {
    return NextResponse.json(
      { players: [], error: "No player summary found." },
      { status: 404 }
    );
  }

  const player = players[0] as Record<string, unknown>;
  const result: SteamPlayer = {
    id: String(player.steamid ?? steamId),
    name: String(player.personaname ?? "Unknown"),
    avatar: typeof player.avatarfull === "string" ? player.avatarfull : null,
  };

  return NextResponse.json({ players: [result] });
}

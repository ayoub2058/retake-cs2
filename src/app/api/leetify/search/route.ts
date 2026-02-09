import { NextRequest, NextResponse } from "next/server";

type LeetifyPlayer = {
  id: string | null;
  name: string | null;
  avatar: string | null;
  rank: string | null;
};

const LEETIFY_PROFILE_URL = "https://api-public.cs-prod.leetify.com/v3/profile";

const toStringOrNull = (value: unknown) => {
  if (value === null || value === undefined) {
    return null;
  }
  const str = String(value).trim();
  return str.length ? str : null;
};

const extractPlayer = (payload: unknown): LeetifyPlayer | null => {
  const data = payload as Record<string, unknown> | unknown[] | null;
  if (!data || typeof data !== "object") {
    return null;
  }
  const item = data as Record<string, unknown>;
  const id =
    toStringOrNull(item.steam64_id) ||
    toStringOrNull(item.steam_id) ||
    toStringOrNull(item.steamId) ||
    toStringOrNull(item.steamid) ||
    toStringOrNull(item.id);
  const name =
    toStringOrNull(item.name) ||
    toStringOrNull(item.username) ||
    toStringOrNull(item.display_name) ||
    toStringOrNull(item.displayName);
  const avatar =
    toStringOrNull(item.avatar_url) ||
    toStringOrNull(item.avatar) ||
    toStringOrNull(item.avatarUrl);
  const ranks = item.ranks as Record<string, unknown> | undefined;
  const premier = ranks?.premier as Record<string, unknown> | undefined;
  const faceit = ranks?.faceit as Record<string, unknown> | undefined;
  const rank =
    toStringOrNull(ranks?.leetify) ||
    toStringOrNull(premier?.rating ?? premier) ||
    toStringOrNull(faceit?.rating ?? faceit);
  return { id, name, avatar, rank };
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

  const token = process.env.LEETIFY_API_TOKEN?.trim();
  if (!token) {
    return NextResponse.json(
      { error: "LEETIFY_API_TOKEN is not configured." },
      { status: 500 }
    );
  }

  let response: Response;
  try {
    const profileUrl = new URL(LEETIFY_PROFILE_URL);
    profileUrl.searchParams.set("steam64_id", query);
    response = await fetch(profileUrl, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${token}`,
        _leetify_key: token,
      },
    });
  } catch (error) {
    console.error("Leetify search request failed:", error);
    return NextResponse.json(
      { error: "Leetify request failed." },
      { status: 502 }
    );
  }

  const rawText = await response.text();
  const trimmedText = rawText.trim();

  if (response.status === 401) {
    console.warn("Leetify auth failed:", trimmedText || "(empty body)");
    return NextResponse.json(
      { error: "Leetify authorization failed.", details: trimmedText || null },
      { status: 401 }
    );
  }

  if (response.status === 404) {
    const profileByIdUrl = new URL(LEETIFY_PROFILE_URL);
    profileByIdUrl.searchParams.set("id", query);
    const fallbackResponse = await fetch(profileByIdUrl, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${token}`,
        _leetify_key: token,
      },
    });
    if (fallbackResponse.ok) {
      const fallbackPayload = await fallbackResponse.json();
      const fallbackPlayer = extractPlayer(fallbackPayload);
      return NextResponse.json({ players: fallbackPlayer ? [fallbackPlayer] : [] });
    }

    return NextResponse.json({
      players: [
        {
          id: query,
          name: `Steam64 ${query}`,
          avatar: null,
          rank: null,
        },
      ],
      warning: "Player not found or profile is private on Leetify.",
      details: trimmedText || null,
    });
  }

  if (!response.ok) {
    console.warn("Leetify request failed:", response.status, trimmedText || "(empty body)");
    return NextResponse.json(
      { error: "Leetify request failed.", details: trimmedText || null },
      { status: 502 }
    );
  }
  let payload: unknown;
  try {
    payload = rawText ? JSON.parse(rawText) : [];
  } catch (error) {
    console.error("Leetify response parse failed:", error);
    return NextResponse.json(
      { error: "Invalid response from Leetify." },
      { status: 502 }
    );
  }

  const player = extractPlayer(payload);
  return NextResponse.json({ players: player ? [player] : [] });
}

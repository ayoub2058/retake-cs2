import { NextRequest, NextResponse } from "next/server";
import { getSteamSessionFromCookies } from "@/lib/steamSessionServer";
import { createServerSupabaseClient } from "@/lib/supabaseServer";

const CODE_PATTERN = /^[A-Z0-9]{4}-[A-Z0-9]{5}-[A-Z0-9]{4}$/;
const MATCH_TOKEN_PATTERN = /^CSGO-[A-Za-z0-9]{5}(-[A-Za-z0-9]{5}){4}$/;

/**
 * POST /api/extension/save-codes
 *
 * Accepts { auth_code, last_known_match_code } from the Chrome extension.
 * The user must be authenticated (Steam session cookie).
 */
export async function POST(request: NextRequest) {
  try {
    const session = await getSteamSessionFromCookies();
    if (!session) {
      return NextResponse.json(
        { error: "Not authenticated. Please sign in to RetakeAI first." },
        { status: 401 }
      );
    }

    const body = await request.json();
    const authCode =
      typeof body.auth_code === "string"
        ? body.auth_code.trim().toUpperCase()
        : "";
    const matchToken =
      typeof body.last_known_match_code === "string"
        ? body.last_known_match_code.trim()
        : "";

    if (!authCode && !matchToken) {
      return NextResponse.json(
        { error: "No codes provided." },
        { status: 400 }
      );
    }

    if (authCode && !CODE_PATTERN.test(authCode)) {
      return NextResponse.json(
        { error: "Invalid auth code format. Expected XXXX-XXXXX-XXXX." },
        { status: 400 }
      );
    }

    if (matchToken && !MATCH_TOKEN_PATTERN.test(matchToken)) {
      return NextResponse.json(
        {
          error:
            "Invalid match token format. Expected CSGO-XXXXX-XXXXX-XXXXX-XXXXX-XXXXX.",
        },
        { status: 400 }
      );
    }

    const supabase = await createServerSupabaseClient();

    const updatePayload: Record<string, string> = {};
    if (authCode) updatePayload.auth_code = authCode;
    if (matchToken) updatePayload.last_known_match_code = matchToken;

    const { error: updateError } = await supabase
      .from("users")
      .update(updatePayload)
      .eq("steam_id", session.steamId);

    if (updateError) {
      console.error("Extension save-codes error:", updateError);
      return NextResponse.json(
        { error: "Failed to save codes." },
        { status: 500 }
      );
    }

    return NextResponse.json({
      success: true,
      message: "Codes saved successfully!",
      saved: Object.keys(updatePayload),
    });
  } catch (err) {
    console.error("Extension save-codes unexpected error:", err);
    return NextResponse.json(
      { error: "Internal server error." },
      { status: 500 }
    );
  }
}

/**
 * GET /api/extension/save-codes
 *
 * Returns current auth status so the extension can check
 * if the user is signed in and has codes already.
 */
export async function GET() {
  try {
    const session = await getSteamSessionFromCookies();
    if (!session) {
      return NextResponse.json({
        authenticated: false,
        hasCodes: false,
      });
    }

    const supabase = await createServerSupabaseClient();
    const { data: user } = await supabase
      .from("users")
      .select("auth_code, last_known_match_code")
      .eq("steam_id", session.steamId)
      .maybeSingle();

    return NextResponse.json({
      authenticated: true,
      steamId: session.steamId,
      hasCodes: Boolean(user?.auth_code && user?.last_known_match_code),
      hasAuthCode: Boolean(user?.auth_code),
      hasMatchToken: Boolean(user?.last_known_match_code),
    });
  } catch {
    return NextResponse.json({
      authenticated: false,
      hasCodes: false,
    });
  }
}

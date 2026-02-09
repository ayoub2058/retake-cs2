import { NextRequest, NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabaseAdmin";

export async function GET(request: NextRequest) {
  const url = new URL(request.url);
  const steamId = url.searchParams.get("steam_id")?.trim();
  if (!steamId) {
    return NextResponse.json(
      { error: "Missing query parameter 'steam_id'." },
      { status: 400 }
    );
  }

  const { data, error } = await supabaseAdmin
    .from("player_match_stats")
    .select("id")
    .eq("steam_id", steamId)
    .limit(1);

  if (error) {
    return NextResponse.json(
      { error: "Failed to check stats availability." },
      { status: 500 }
    );
  }

  return NextResponse.json({ hasStats: (data ?? []).length > 0 });
}

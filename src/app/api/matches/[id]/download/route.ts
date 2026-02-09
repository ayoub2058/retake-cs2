import { NextResponse } from "next/server";
import fs from "node:fs";
import path from "node:path";
import { Readable } from "node:stream";
import { getSteamSessionFromCookies } from "@/lib/steamSessionServer";
import { supabaseAdmin } from "@/lib/supabaseAdmin";

const downloadsDir = path.join(process.cwd(), "downloads");

export async function GET(
  _request: Request,
  { params }: { params: { id: string } | Promise<{ id: string }> }
) {
  const { id } = await params;
  const session = await getSteamSessionFromCookies();
  if (!session) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const matchId = Number(id);
  if (!Number.isFinite(matchId)) {
    return NextResponse.json({ error: "Invalid match id" }, { status: 400 });
  }

  const { data: matchRow, error } = await supabaseAdmin
    .from("matches_to_download")
    .select("id, user_id, status, file_path")
    .eq("id", matchId)
    .maybeSingle();

  if (error || !matchRow || String(matchRow.user_id) !== session.steamId) {
    return NextResponse.json({ error: "Not found" }, { status: 404 });
  }

  if (matchRow.status !== "downloaded" || !matchRow.file_path) {
    return NextResponse.json({ error: "Replay not available" }, { status: 404 });
  }

  const resolvedPath = path.resolve(matchRow.file_path);
  if (!resolvedPath.startsWith(downloadsDir)) {
    return NextResponse.json({ error: "Invalid file path" }, { status: 400 });
  }

  if (!fs.existsSync(resolvedPath)) {
    return NextResponse.json({ error: "File missing" }, { status: 404 });
  }

  const stat = fs.statSync(resolvedPath);
  const buffer = fs.readFileSync(resolvedPath);

  return new Response(buffer, {
    headers: {
      "Content-Type": "application/octet-stream",
      "Content-Length": String(stat.size),
      "Content-Disposition": `attachment; filename="${path.basename(
        resolvedPath
      )}"`,
    },
  });
}

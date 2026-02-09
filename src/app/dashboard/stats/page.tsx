import { redirect } from "next/navigation";
import { getSteamSessionFromCookies } from "@/lib/steamSessionServer";
import { PlayerStats } from "@/app/_components/PlayerStats";

export default async function PlayerStatsPage() {
  const session = await getSteamSessionFromCookies();
  if (!session) {
    redirect("/");
  }

  return <PlayerStats userId={session.steamId} />;
}

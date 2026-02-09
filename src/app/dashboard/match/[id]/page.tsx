import { redirect } from "next/navigation";

export default function MatchRedirectPage({
  params,
}: {
  params: { id: string };
}) {
  redirect(`/dashboard/matches/${params.id}`);
}

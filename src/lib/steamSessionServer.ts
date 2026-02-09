import { cookies } from "next/headers";
import { decodeSteamSession } from "@/lib/steamSession";

const resolveCookieStore = async () => {
  const cookieStore = cookies();
  if (typeof (cookieStore as Promise<unknown>)?.then === "function") {
    return await cookieStore;
  }
  return cookieStore;
};

export const getSteamSessionFromCookies = async () => {
  const cookieStore = await resolveCookieStore();
  const getCookie = cookieStore?.get?.bind(cookieStore);
  const sessionValue = getCookie ? getCookie("steam_session")?.value : undefined;
  return decodeSteamSession(sessionValue);
};

import type { NextApiRequest, NextApiResponse } from "next";
import { configureSteamPassport } from "@/lib/steamPassport";

export default function handler(req: NextApiRequest, res: NextApiResponse) {
  const passport = configureSteamPassport(req);
  const reqAny = req as unknown as any;
  const resAny = res as unknown as any;

  return passport.initialize()(reqAny, resAny, () => {
    passport.authenticate("steam", { session: false })(reqAny, resAny, () => {
      res.status(500).end("Steam authentication failed to start.");
    });
  });
}

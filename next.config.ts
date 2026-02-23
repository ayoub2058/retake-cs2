import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactCompiler: true,
  images: {
    remotePatterns: [
      { protocol: "https", hostname: "avatars.steamstatic.com" },
      { protocol: "https", hostname: "steamcdn-a.akamaihd.net" },
      { protocol: "https", hostname: "cdn.cloudflare.steamstatic.com" },
    ],
  },
  // On Vercel the downloads/ folder doesn't exist – disable the download route
  serverExternalPackages: ["pg"],
};

export default nextConfig;

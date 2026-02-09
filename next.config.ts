import type { NextConfig } from "next";

const allowBuildErrors = process.env.ALLOW_BUILD_ERRORS === "true";

const nextConfig: NextConfig = {
  typescript: {
    // !! WARN !!
    // Dangerously allow production builds to successfully complete even if
    // your project has type errors.
    ignoreBuildErrors: allowBuildErrors,
  },
};

export default nextConfig;

import type { NextConfig } from "next";

const allowBuildErrors = process.env.ALLOW_BUILD_ERRORS === "true";

const nextConfig: NextConfig = {
  eslint: {
    // Warning: This allows production builds to successfully complete even if
    // your project has ESLint errors.
    ignoreDuringBuilds: allowBuildErrors,
  },
  typescript: {
    // !! WARN !!
    // Dangerously allow production builds to successfully complete even if
    // your project has type errors.
    ignoreBuildErrors: allowBuildErrors,
  },
};

export default nextConfig;

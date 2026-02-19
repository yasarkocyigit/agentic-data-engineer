import type { NextConfig } from "next";

const BACKEND_URL = process.env.BACKEND_URL || 'http://localhost:8000';
const GITEA_URL = process.env.GITEA_DIRECT_URL || 'http://localhost:3030';

const nextConfig: NextConfig = {
  reactCompiler: true,
  async rewrites() {
    return {
      beforeFiles: [
        // ── FastAPI backend: all Gitea API proxy endpoints ──
        { source: '/api/gitea/:path*', destination: `${BACKEND_URL}/gitea/:path*` },
        // ── Gitea direct proxy (static assets, UI pages) ──
        { source: '/gitea-proxy/:path*', destination: `${GITEA_URL}/:path*` },
        { source: '/assets/:path*', destination: `${GITEA_URL}/assets/:path*` },
        { source: '/avatars/:path*', destination: `${GITEA_URL}/avatars/:path*` },
        { source: '/-/:path*', destination: `${GITEA_URL}/-/:path*` },
      ],
      afterFiles: [],
      fallback: [
        { source: '/user/:path*', destination: `${GITEA_URL}/user/:path*` },
        { source: '/explore/:path*', destination: `${GITEA_URL}/explore/:path*` },
        { source: '/admin/:path*', destination: `${GITEA_URL}/admin/:path*` },
        { source: '/api/v1/:path*', destination: `${GITEA_URL}/api/v1/:path*` },
        { source: '/repo/:path*', destination: `${GITEA_URL}/repo/:path*` },
      ],
    };
  },
};

export default nextConfig;

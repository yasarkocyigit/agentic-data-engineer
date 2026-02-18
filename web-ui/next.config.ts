import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactCompiler: true,
  async rewrites() {
    const giteaDest = 'http://localhost:3030';
    return {
      beforeFiles: [
        // Gitea explicit proxy
        { source: '/gitea-proxy/:path*', destination: `${giteaDest}/:path*` },
        // Gitea static assets (CSS, JS, images, fonts)
        { source: '/assets/:path*', destination: `${giteaDest}/assets/:path*` },
        // Gitea avatars
        { source: '/avatars/:path*', destination: `${giteaDest}/avatars/:path*` },
        // Gitea internal paths
        { source: '/-/:path*', destination: `${giteaDest}/-/:path*` },
      ],
      afterFiles: [],
      fallback: [
        // Gitea user pages (login, settings, etc.)
        { source: '/user/:path*', destination: `${giteaDest}/user/:path*` },
        // Gitea explore
        { source: '/explore/:path*', destination: `${giteaDest}/explore/:path*` },
        // Gitea repo pages (when accessed via admin/reponame)
        { source: '/admin/:path*', destination: `${giteaDest}/admin/:path*` },
        // Gitea API
        { source: '/api/v1/:path*', destination: `${giteaDest}/api/v1/:path*` },
        // Gitea repo creation
        { source: '/repo/:path*', destination: `${giteaDest}/repo/:path*` },
      ],
    };
  },
};

export default nextConfig;

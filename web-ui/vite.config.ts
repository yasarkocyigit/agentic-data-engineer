import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';

export default defineConfig({
    plugins: [react()],
    resolve: {
        alias: {
            '@': path.resolve(__dirname, './src'),
        },
    },
    server: {
        port: 3010,
        proxy: {
            // ─── FastAPI Backend ───
            '/api': {
                target: 'http://localhost:8000',
                changeOrigin: true,
                ws: true,
            },
            // ─── Gitea Proxy ───
            '/gitea-proxy': {
                target: 'http://localhost:3030',
                changeOrigin: true,
                rewrite: (path) => path.replace(/^\/gitea-proxy/, ''),
            },
            '/assets': {
                target: 'http://localhost:3030',
                changeOrigin: true,
            },
            '/avatars': {
                target: 'http://localhost:3030',
                changeOrigin: true,
            },
            '/-': {
                target: 'http://localhost:3030',
                changeOrigin: true,
            },
        },
    },
});

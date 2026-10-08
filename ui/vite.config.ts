import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'
import { VitePWA } from 'vite-plugin-pwa'

// Dev: vite on 5173, /api proxied to the backend (uv run pmkt ui) or the
// contract-exact mock (npm run mock), both defaulting to 127.0.0.1:8100.
// Override with PMKT_API_TARGET (e.g. when the mock runs on another port):
//   PMKT_API_TARGET=http://127.0.0.1:8199 npm run dev
export default defineConfig({
  plugins: [
    react(),
    VitePWA({
      registerType: 'autoUpdate',
      devOptions: { enabled: true },
      // Precaches only the built JS/CSS/HTML shell — never /api/* (no
      // runtimeCaching rules here), so installed clients always hit the
      // current local dataset instead of cached market prices.
      manifest: {
        name: 'Prediction Markets Research',
        short_name: 'PM Research',
        description: 'Research interface for wallet skill, selected-trader flow and paper decisions.',
        start_url: '/',
        display: 'standalone',
        background_color: '#f2f3f6',
        theme_color: '#5a62d2',
      },
    }),
  ],
  server: {
    proxy: {
      '/api': {
        target: process.env.PMKT_API_TARGET ?? 'http://127.0.0.1:8100',
        changeOrigin: false,
      },
    },
  },
  test: {
    environment: 'node',
    include: ['src/**/*.test.ts'],
  },
})

import { loadEnv } from "vite";
import { defineConfig } from "vitest/config";   // typed superset of vite's defineConfig (allows the `test` key)
import react from "@vitejs/plugin-react";

// Dev server proxies /api, /health and WebSocket /api/v1/ws to the FastAPI backend, so the browser stays
// same-origin and no CORS is needed. Override the target with VITE_DEV_PROXY_TARGET.
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const target = env.VITE_DEV_PROXY_TARGET || "http://localhost:8000";
  return {
    plugins: [react()],
    server: { port: 5173, proxy: { "/api": { target, ws: true, changeOrigin: true }, "/health": { target, changeOrigin: true } } },
    test: { environment: "node", include: ["src/**/*.test.ts"] },
  };
});

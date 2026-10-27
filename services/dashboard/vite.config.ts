import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// In development the API runs on :8000. In the container, nginx does the same job (nginx.conf).
export default defineConfig({
  plugins: [react()],
  // MapLibre alone is about 1 MB minified; the default 500 kB warning would always fire.
  build: { chunkSizeWarningLimit: 2000 },
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        rewrite: (path) => path.replace(/^\/api/, ""),
      },
      "/ws": { target: "ws://localhost:8000", ws: true },
    },
  },
});

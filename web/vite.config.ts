import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Static SPA. Relative base so the built dist/ can be opened from any path or hosted on any
// sub-path (e.g. GitHub Pages) without rewriting asset URLs. Benchmark data is served as static
// JSON from public/data/ (produced by `npm run data`).
export default defineConfig({
  base: "./",
  plugins: [react()],
  build: {
    outDir: "dist",
    chunkSizeWarningLimit: 1500,
  },
});

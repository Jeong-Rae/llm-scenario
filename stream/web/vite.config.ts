import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  root: "web",
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      "/chat": {
        target: "http://localhost:8080",
        changeOrigin: true,
      },
      "/conversation": {
        target: "http://localhost:8080",
        changeOrigin: true,
      },
    },
  },
});

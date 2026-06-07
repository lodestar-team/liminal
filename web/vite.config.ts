import { defineConfig } from "vite";

// The jco-generated component glue has a Node-only `await import('node:fs/promises')`
// branch that never runs in the browser (it uses `fetch` there). Stub the
// specifier so the bundler can resolve it.
export default defineConfig({
  base: "./",
  resolve: {
    alias: {
      "node:fs/promises": new URL("./src/stub-empty.js", import.meta.url).pathname,
    },
  },
  build: {
    target: "es2022", // top-level await + modern wasm
  },
});

# Customs — in-browser demo

The actual compiled Customs WASIp2 components running **in the browser**, producing the same
routing as the native `liminal` host. Live: [web-nbgn.vercel.app](https://web-nbgn.vercel.app)

## How it works

- The Rust components (`decoder`, `screener`, `enricher`) are compiled to `wasm32-wasip2`, then
  transpiled to browser-runnable JS with [jco](https://github.com/bytecodealliance/jco) into
  [`src/gen/`](./src/gen).
- [`src/host.ts`](./src/host.ts) is a ~80-line TypeScript port of the host's routing logic
  (`liminal-host/src/runtime.rs`): it feeds the fixture transfers through each component's real
  `transform`, reads the screener's `"tag"` discriminant, and routes accordingly.
- The `liminal:node/store` import (W4 key-value) is satisfied by [`src/kv.js`](./src/kv.js); the
  WASI `node:fs` fallback is stubbed (the browser path uses `fetch`).

So it's not a mock — the genuine compliance components execute client-side. The "screening outage"
toggle models what the live `screener-http` does when its provider is unreachable (fail-closed).

## Develop / build

```bash
npm install
npm run dev       # vite dev server
npm run build     # → dist/ (static; deploys to Vercel as a Vite app)
```

## Regenerating the components

After changing/rebuilding the Rust components (`just build` at the repo root), re-transpile:

```bash
for c in decoder screener enricher; do
  npx @bytecodealliance/jco transpile ../examples/customs/$c.wasm \
    -o src/gen/$c --no-typescript --map 'liminal:node/store=../../kv.js'
done
```

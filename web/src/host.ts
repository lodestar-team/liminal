// A browser port of the Liminal host's routing — the ~same logic as
// liminal-host/src/runtime.rs, but in TS, driving the REAL compiled components
// (jco-transpiled). Each `node.transform` call below executes the actual
// decoder/screener/enricher wasm.

// @ts-ignore - jco-generated JS, no types
import { node as decoder } from "./gen/decoder/decoder.js";
// @ts-ignore
import { node as screener } from "./gen/screener/screener.js";
// @ts-ignore
import { node as enricher } from "./gen/enricher/enricher.js";
import { _reset as resetKv } from "./kv.js";

const enc = (o: unknown) => new TextEncoder().encode(JSON.stringify(o));
const dec = (u8: Uint8Array) => JSON.parse(new TextDecoder().decode(u8));

export type Verdict = "cleared" | "flagged" | "indeterminate";

export interface Routed {
  tx: string;
  token: string;
  from: string;
  to: string;
  value: string;
  verdict: Verdict;
  counterparty?: string;
  destinations: string[];
  /** true when the destination set includes the system-of-record writer */
  reachedWriter: boolean;
}

export interface RunResult {
  rows: Routed[];
  failClosed: boolean;
}

/**
 * Run the Customs pipeline over the fixture logs, using the real components.
 *
 * `failClosed` simulates a screening outage: every transfer is treated as
 * indeterminate → held, nothing written. (In the offline component the screen
 * is a compiled-in list; this models what the live `screener-http` does when
 * its provider is unreachable — see the fail-closed integration test.)
 */
export function runPipeline(logs: unknown[], failClosed = false): RunResult {
  resetKv();
  const rows: Routed[] = [];

  for (const log of logs) {
    const transfers: Uint8Array[] = decoder.transform(enc(log));
    for (const tBytes of transfers) {
      const t = dec(tBytes);

      let verdictTag: Verdict = "indeterminate";
      let counterparty: string | undefined;
      let verdictBytes: Uint8Array | undefined;

      if (!failClosed) {
        verdictBytes = screener.transform(tBytes)[0];
        const v = dec(verdictBytes!);
        verdictTag = v.tag;
        counterparty = v.counterparty;
      }

      let destinations: string[];
      if (verdictTag === "cleared") {
        enricher.transform(verdictBytes!); // real enrichment runs
        destinations = ["sink-sor", "sink-kafka"];
      } else if (verdictTag === "flagged") {
        destinations = ["sink-quarantine"];
      } else {
        destinations = ["sink-hold"];
      }

      rows.push({
        tx: t.tx_hash,
        token: t.token,
        from: t.from,
        to: t.to,
        value: t.value,
        verdict: verdictTag,
        counterparty,
        destinations,
        reachedWriter: destinations.includes("sink-sor"),
      });
    }
  }

  return { rows, failClosed };
}

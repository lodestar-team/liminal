// In-browser key-value store backing the screener's `liminal:node/store` import.
// The native host namespaces keys per component; here the screener is the only
// kv user, so a single Map suffices. (get returns the bytes or undefined.)
const store = new Map();
const k = (key) => key; // single namespace in the browser demo
export function get(key) { return store.has(k(key)) ? store.get(k(key)) : undefined; }
export function set(key, value) { store.set(k(key), value); }
function del(key) { store.delete(k(key)); }
export { del as delete };
export function exists(key) { return store.has(k(key)); }
export function _reset() { store.clear(); }

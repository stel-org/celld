// Adapted from Base Worker's `runtime/js/90_deno_ns.js` at
// a18ce33715e30cd2b0d99c7e322ef11a65490e4d.  That runtime maps
// Deno.memoryUsage() to `op_runtime_memory_usage`; celld supplies the same
// public result through a lexical V8 callback instead of deno_core ops.
//
// This deliberately creates only the reviewed V0 subset of `Deno`. It does
// not claim or emulate the rest of the Deno namespace.
if (Object.prototype.hasOwnProperty.call(globalThis, "Deno")) {
  throw new Error("BaseWorker profile cannot install Deno over an existing global");
}

const memoryUsage = () => JSON.parse(__base_worker_memory_usage());

Object.defineProperty(globalThis, "Deno", {
  value: { memoryUsage },
  writable: false,
  enumerable: true,
  configurable: false,
});

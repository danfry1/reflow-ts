/**
 * Browser equivalents of the `node:timers` functions the engine imports. A
 * macrotask after pending callbacks stands in for `setImmediate`.
 */
export function setImmediate(callback: () => void): ReturnType<typeof globalThis.setTimeout> {
  return globalThis.setTimeout(callback, 0)
}

export const setTimeout = globalThis.setTimeout.bind(globalThis)

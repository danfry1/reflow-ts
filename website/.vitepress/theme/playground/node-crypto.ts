/**
 * The parts of `node:crypto` the engine and dashboard use, for running them
 * in the browser on the home page. Only the docs build maps `node:crypto`
 * here; the library itself is unchanged.
 */
export function randomUUID(): string {
  return globalThis.crypto.randomUUID()
}

/** Random bytes with the one encoding the dashboard asks for. */
export function randomBytes(size: number): { toString(encoding: 'base64'): string } {
  const bytes = globalThis.crypto.getRandomValues(new Uint8Array(size))
  return { toString: () => btoa(String.fromCharCode(...bytes)) }
}

/** Used only to name schedules, which the playground does not create. */
export function createHash(): never {
  throw new Error('createHash is not available in the docs playground')
}

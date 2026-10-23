/**
 * The parts of `node:crypto` the engine uses, for running it in the browser
 * on the home page. Only the docs build maps `node:crypto` here; the library
 * itself is unchanged.
 */
export function randomUUID(): string {
  return globalThis.crypto.randomUUID()
}

/** Used only to name schedules, which the playground does not create. */
export function createHash(): never {
  throw new Error('createHash is not available in the docs playground')
}

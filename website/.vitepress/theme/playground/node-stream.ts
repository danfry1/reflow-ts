/**
 * Stand-ins for the `node:stream` modules the dashboard's Node adapter
 * imports. The playground serves the dashboard through its Fetch handler, so
 * the adapter — and these — are never called in the browser.
 */
export class Readable {
  static fromWeb(): never {
    throw new Error('node:stream is not available in the docs playground')
  }
}

export function pipeline(): never {
  throw new Error('node:stream is not available in the docs playground')
}

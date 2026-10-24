import { timingSafeEqual } from 'node:crypto'
import { ConfigError } from '../core/errors'

/** Credentials for {@link basicAuth}. */
export interface BasicAuthOptions {
  username: string
  /** Read it from the environment, not from source: `process.env.REFLOW_DASHBOARD_PASSWORD`. */
  password: string | undefined
  /** Shown by some browsers in the sign-in prompt. Defaults to `"Reflow"`. */
  realm?: string
}

/**
 * An `authorize` that asks for a username and password, with the browser's
 * own sign-in prompt — for an app with no login of its own to reuse.
 *
 * The browser sends the password with every request, encoded but not
 * encrypted, so serve the dashboard over HTTPS anywhere but localhost.
 *
 * @example
 * ```ts
 * createDashboard({
 *   storage,
 *   authorize: basicAuth({ username: 'admin', password: process.env.REFLOW_DASHBOARD_PASSWORD }),
 * })
 * ```
 *
 * @throws {ConfigError} if the username or password is empty or missing, so
 *   an unset environment variable fails at startup rather than locking
 *   everyone out — or letting an empty password in.
 */
export function basicAuth(options: BasicAuthOptions): (request: Request) => true | Response {
  const { username, password } = options
  if (username === '' || username.includes(':')) {
    throw new ConfigError('basicAuth: username must be non-empty and must not contain ":"')
  }
  if (password === undefined || password === '') {
    throw new ConfigError('basicAuth: password is empty or missing; is its environment variable set?')
  }
  const expected = Buffer.from(`${username}:${password}`, 'utf8')
  const realm = (options.realm ?? 'Reflow').replaceAll('"', '')
  const challenge = `Basic realm="${realm}", charset="UTF-8"`

  return (request) => {
    const header = request.headers.get('authorization') ?? ''
    const match = /^Basic\s+([A-Za-z0-9+/=]+)\s*$/i.exec(header)
    if (match?.[1] !== undefined && matches(Buffer.from(match[1], 'base64'), expected)) {
      return true
    }
    return new Response('Sign in to view this dashboard', {
      status: 401,
      headers: {
        'www-authenticate': challenge,
        'content-type': 'text/plain; charset=utf-8',
        'cache-control': 'no-store',
        'x-content-type-options': 'nosniff',
      },
    })
  }
}

/**
 * Whether `sent` equals `expected`, in a time that depends only on the
 * length of `expected`, so timing reveals nothing about the password.
 */
function matches(sent: Buffer, expected: Buffer): boolean {
  const padded = Buffer.alloc(expected.length)
  sent.copy(padded)
  return timingSafeEqual(padded, expected) && sent.length === expected.length
}

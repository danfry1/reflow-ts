import { describe, it, expect } from 'vitest'
import { ConfigError } from '../../core/errors'
import { MemoryStorage } from '../../storage/memory'
import { basicAuth, createDashboard } from '../index'

const ORIGIN = 'http://dash.test'

function signedIn(credentials: string): Request {
  return new Request(`${ORIGIN}/`, { headers: { authorization: `Basic ${Buffer.from(credentials, 'utf8').toString('base64')}` } })
}

describe('basicAuth', () => {
  const authorize = basicAuth({ username: 'admin', password: 'correct horse' })

  it('lets the right username and password through', () => {
    expect(authorize(signedIn('admin:correct horse'))).toBe(true)
  })

  it('accepts a password that contains a colon', () => {
    expect(basicAuth({ username: 'admin', password: 'a:b' })(signedIn('admin:a:b'))).toBe(true)
  })

  it('accepts non-ASCII credentials sent as UTF-8', () => {
    expect(basicAuth({ username: 'zoë', password: 'pässwörd' })(signedIn('zoë:pässwörd'))).toBe(true)
  })

  it('asks the browser to sign in when no credentials are sent', async () => {
    const response = authorize(new Request(`${ORIGIN}/`))
    if (response === true) throw new Error('expected a challenge')
    expect(response.status).toBe(401)
    expect(response.headers.get('www-authenticate')).toBe('Basic realm="Reflow", charset="UTF-8"')
    expect(response.headers.get('cache-control')).toBe('no-store')
    expect(await response.text()).toBe('Sign in to view this dashboard')
  })

  it.each([
    ['a wrong password', signedIn('admin:wrong')],
    ['a wrong username', signedIn('root:correct horse')],
    ['no colon', signedIn('admincorrect horse')],
    ['a bearer token', new Request(`${ORIGIN}/`, { headers: { authorization: 'Bearer correct horse' } })],
    ['a malformed header', new Request(`${ORIGIN}/`, { headers: { authorization: 'Basic not base64!' } })],
  ])('challenges %s', (_, request) => {
    const response = authorize(request)
    expect(response === true ? 'let through' : response.status).toBe(401)
  })

  it('names the realm in the challenge, without quotes that would break the header', () => {
    const response = basicAuth({ username: 'a', password: 'b', realm: 'Ops "prod"' })(new Request(`${ORIGIN}/`))
    expect(response === true ? null : response.headers.get('www-authenticate')).toBe('Basic realm="Ops prod", charset="UTF-8"')
  })

  it.each([
    ['an unset password', { username: 'admin', password: undefined }, 'basicAuth: password is empty or missing; is its environment variable set?'],
    ['an empty password', { username: 'admin', password: '' }, 'basicAuth: password is empty or missing; is its environment variable set?'],
    ['an empty username', { username: '', password: 'x' }, 'basicAuth: username must be non-empty and must not contain ":"'],
    ['a username with a colon', { username: 'a:b', password: 'x' }, 'basicAuth: username must be non-empty and must not contain ":"'],
  ])('refuses %s at startup', (_, options, message) => {
    expect(() => basicAuth(options)).toThrow(new ConfigError(message))
    expect(() => basicAuth(options)).toThrow(ConfigError)
  })

  it('guards the page and the API when passed to createDashboard', async () => {
    const dashboard = createDashboard({ storage: new MemoryStorage(), authorize })
    const page = await dashboard(new Request(`${ORIGIN}/`))
    const api = await dashboard(new Request(`${ORIGIN}/api/overview`))
    expect([page.status, page.headers.get('www-authenticate')]).toStrictEqual([401, 'Basic realm="Reflow", charset="UTF-8"'])
    expect([api.status, api.headers.get('www-authenticate')]).toStrictEqual([401, 'Basic realm="Reflow", charset="UTF-8"'])
    expect((await dashboard(signedIn('admin:correct horse'))).status).toBe(200)
  })
})

describe('authorize returning a Response', () => {
  it('sends that response instead of the dashboard', async () => {
    const dashboard = createDashboard({
      storage: new MemoryStorage(),
      authorize: () => Response.redirect(`${ORIGIN}/login`, 302),
    })
    const response = await dashboard(new Request(`${ORIGIN}/api/overview`))
    expect([response.status, response.headers.get('location')]).toStrictEqual([302, `${ORIGIN}/login`])
  })

  it('waits for an async one', async () => {
    const dashboard = createDashboard({
      storage: new MemoryStorage(),
      authorize: async () => new Response('Forbidden', { status: 403 }),
    })
    const response = await dashboard(new Request(`${ORIGIN}/`))
    expect([response.status, await response.text()]).toStrictEqual([403, 'Forbidden'])
  })
})

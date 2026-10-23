/**
 * A web dashboard for inspecting runs, served from inside your own app.
 *
 * `createDashboard()` returns a standard Fetch API handler — `(Request) =>
 * Promise<Response>` — so it mounts as-is in Bun.serve, Deno, Hono, or a
 * Next.js route handler; `toNodeHandler()` adapts it for `node:http` and
 * Express. The page is one self-contained document with no external
 * requests, so it works offline and behind a strict network policy.
 *
 * @example
 * ```ts
 * import { createDashboard } from 'reflow-ts/dashboard'
 *
 * const dashboard = createDashboard({
 *   storage,
 *   engine, // optional: enables cancel / resume and live updates
 *   authorize: (request) => request.headers.get('authorization') === `Bearer ${process.env.ADMIN_TOKEN}`,
 * })
 *
 * Bun.serve({ port: 4400, fetch: dashboard })
 * ```
 */

import { randomBytes } from 'node:crypto'
import type { Engine } from '../core/engine'
import { toError } from '../core/errors'
import {
  RUN_STATUSES,
  type ListRunsFilter,
  type RunStatus,
  type StepResult,
  type StorageAdapter,
  type WorkflowRun,
} from '../core/types'
import { renderPage } from './page'

export { toNodeHandler } from './node'

/** Configuration for {@link createDashboard}. */
export interface DashboardOptions {
  /** Where runs are read from — the same storage your engine uses. */
  storage: StorageAdapter
  /**
   * Decide whether a request may see the dashboard. Called for every request,
   * page and API alike; return `false` to answer 401.
   *
   * Required, because run inputs and outputs are your application's data.
   * Check something the request *carries* — a session cookie, a bearer token —
   * not where it comes from: a page on another site can reach a loopback port
   * through DNS rebinding. To serve it with no check at all, pass `() => true`
   * together with {@link DashboardOptions.allowedHosts}.
   */
  authorize(request: Request): boolean | Promise<boolean>
  /**
   * The `Host` values the dashboard answers to, such as `['localhost:4400']`
   * or `['admin.example.com']`. Requests for any other host get 421.
   *
   * Set it whenever `authorize` does not check credentials. Otherwise a site
   * the viewer visits can re-point its own domain at the dashboard's address
   * (DNS rebinding) and read it as same-origin. Also list the public host when
   * a proxy in front rewrites `Host`, so actions arriving from it are accepted.
   */
  allowedHosts?: readonly string[]
  /**
   * The engine running these workflows. Optional: with it the dashboard can
   * cancel and resume runs and update live as they progress; without it the
   * dashboard is read-only and refreshes by polling.
   */
  engine?: Pick<Engine, 'cancel' | 'resume' | 'stream'>
  /**
   * The path the dashboard is mounted at, when the handler receives full
   * request paths (Bun.serve, Hono, Next.js). Leave it as `/` when the host
   * strips the mount prefix itself, as Express's `app.use(prefix, …)` does.
   */
  basePath?: string
  /** Shown in the page title and header. Defaults to `"Reflow"`. */
  title?: string
}

/** A Fetch API request handler. */
export type DashboardHandler = (request: Request) => Promise<Response>

/** How often an idle live-update connection sends a keep-alive comment. */
const HEARTBEAT_MS = 25_000

/** Largest page of runs the API returns at once. */
const MAX_PAGE_SIZE = 200

/** Sent by the page on every state-changing request; see {@link isSameOriginAction}. */
const ACTION_HEADER = 'x-reflow-dashboard'

/**
 * Create the dashboard's request handler.
 *
 * Every response carries headers that keep the page from being framed, from
 * loading anything external, or from leaking its URL to other sites.
 */
export function createDashboard(options: DashboardOptions): DashboardHandler {
  const { storage, engine, authorize } = options
  const allowedHosts = options.allowedHosts?.map((host) => host.toLowerCase())
  const basePath = normalizeBasePath(options.basePath ?? '/')
  const title = options.title ?? 'Reflow'

  return async (request) => {
    const url = new URL(request.url)
    const route = routeOf(url.pathname, basePath)
    if (route === null) {
      return text(404, 'Not found')
    }

    if (allowedHosts && !allowedHosts.includes(url.host.toLowerCase())) {
      return text(421, 'This dashboard does not answer for that host')
    }

    let authorized: boolean
    try {
      authorized = await authorize(request)
    } catch {
      // Deliberately generic: the caller is unauthenticated, and an
      // authorize() error message can echo what they sent.
      return text(500, 'Could not check authorization')
    }
    if (!authorized) {
      return text(401, 'Not authorized to view this dashboard')
    }

    // Relative URLs in the page resolve against a trailing slash, so the page
    // must be served from one wherever the dashboard is mounted.
    if (route === '' && !url.pathname.endsWith('/')) {
      return new Response(null, { status: 308, headers: { location: `${url.pathname}/${url.search}` } })
    }

    try {
      return await handle(request, url, route)
    } catch (error) {
      return json(500, { error: toError(error).message })
    }
  }

  async function handle(request: Request, url: URL, route: string): Promise<Response> {
    const segments = decodeSegments(route)
    if (segments === null) {
      return text(400, 'Malformed path')
    }
    const [head, second, third, fourth] = segments
    const method = request.method

    if (segments.length === 0) {
      return method === 'GET' ? page() : methodNotAllowed('GET')
    }

    if (head !== 'api') {
      return text(404, 'Not found')
    }

    if (second === 'overview' && segments.length === 2) {
      if (method !== 'GET') return methodNotAllowed('GET')
      return json(200, {
        title,
        counts: await storage.countRuns(),
        actions: engine !== undefined,
        live: engine !== undefined,
      })
    }

    if (second === 'runs' && segments.length === 2) {
      if (method !== 'GET') return methodNotAllowed('GET')
      return listRuns(url.searchParams)
    }

    if (second === 'runs' && third !== undefined && segments.length === 3) {
      if (method !== 'GET') return methodNotAllowed('GET')
      const run = await storage.getRun(third)
      if (!run) return json(404, { error: `No run with id "${third}"` })
      return json(200, { run, steps: await storage.getStepResults(third) })
    }

    if (second === 'runs' && third !== undefined && (fourth === 'cancel' || fourth === 'resume') && segments.length === 4) {
      if (method !== 'POST') return methodNotAllowed('POST')
      if (!isSameOriginAction(request, url, allowedHosts)) {
        return json(403, { error: 'Actions must come from the dashboard page itself' })
      }
      if (!engine) {
        return json(403, { error: 'This dashboard is read-only: pass `engine` to createDashboard() to enable actions' })
      }
      const done = fourth === 'cancel' ? await engine.cancel(third) : await engine.resume(third)
      return json(200, { done })
    }

    if (second === 'schedules' && segments.length === 2) {
      if (method !== 'GET') return methodNotAllowed('GET')
      return json(200, { schedules: await storage.listSchedules() })
    }

    if (second === 'live' && segments.length === 2) {
      if (method !== 'GET') return methodNotAllowed('GET')
      if (!engine) return json(404, { error: 'Live updates need `engine` passed to createDashboard()' })
      return liveUpdates(request, engine)
    }

    return json(404, { error: `No such endpoint: ${route}` })
  }

  async function listRuns(params: URLSearchParams): Promise<Response> {
    const filter: ListRunsFilter = { limit: 50 }

    const status = params.get('status')
    if (status !== null) {
      if (!isRunStatus(status)) return json(400, { error: `Unknown status "${status}"` })
      filter.status = status
    }

    const workflow = params.get('workflow')
    if (workflow !== null && workflow !== '') {
      filter.workflow = workflow
    }

    const limit = params.get('limit')
    if (limit !== null) {
      const parsed = Number(limit)
      if (!Number.isInteger(parsed) || parsed < 1 || parsed > MAX_PAGE_SIZE) {
        return json(400, { error: `limit must be an integer from 1 to ${MAX_PAGE_SIZE}` })
      }
      filter.limit = parsed
    }

    const before = params.get('before')
    const beforeId = params.get('beforeId')
    if (before !== null) {
      const parsed = Number(before)
      if (!Number.isFinite(parsed)) return json(400, { error: 'before must be a timestamp' })
      filter.before = parsed
      if (beforeId !== null) filter.beforeId = beforeId
    }

    const runs = await storage.listRuns(filter)
    const last = runs.at(-1)
    const next = last !== undefined && runs.length === filter.limit
      ? { before: last.createdAt, beforeId: last.id }
      : null

    // Each row draws its run as a line, which needs the steps' timing but not
    // their outputs. Inputs and outputs can be large; the list sends neither,
    // and the detail endpoint returns them for the one run being looked at.
    const steps = await Promise.all(runs.map((run) => storage.getStepResults(run.id)))
    return json(200, {
      runs: runs.map((run, index) => ({ ...summarize(run), steps: (steps[index] ?? []).map(outline) })),
      next,
    })
  }

  function page(): Response {
    const nonce = randomBytes(16).toString('base64')
    return new Response(renderPage({ title, nonce }), {
      status: 200,
      headers: {
        ...SECURITY_HEADERS,
        'content-type': 'text/html; charset=utf-8',
        'content-security-policy': [
          "default-src 'none'",
          `script-src 'nonce-${nonce}'`,
          `style-src 'nonce-${nonce}'`,
          "connect-src 'self'",
          'img-src data:',
          "base-uri 'none'",
          "form-action 'none'",
          "frame-ancestors 'none'",
        ].join('; '),
      },
    })
  }
}

/** A run without its input and output, for the list view. */
function summarize(run: WorkflowRun): Omit<WorkflowRun, 'input' | 'output'> {
  const { input: _input, output: _output, ...summary } = run
  return summary
}

/** A step without its output: what a run's line needs and nothing more. */
function outline(step: StepResult): Omit<StepResult, 'output' | 'id' | 'runId'> {
  const { output: _output, id: _id, runId: _runId, ...rest } = step
  return rest
}

/**
 * Stream a notice to the page whenever a run changes, as server-sent events.
 *
 * Only the run id and event type are sent; the page re-reads what it shows,
 * so nothing here depends on events arriving or being complete. That is what
 * lets a slow client be handled without either cost a buffer would bring:
 * notices it cannot take yet are dropped and folded into one `resync` once it
 * catches up, so a stalled tab neither grows server memory nor — since the
 * engine stream is never left to fill — slows the engine down.
 */
function liveUpdates(request: Request, engine: Pick<Engine, 'stream'>): Response {
  const events = engine.stream()
  const encoder = new TextEncoder()
  let missed = false
  // Set once the stream is closed, errored, or cancelled by the client; the
  // controller accepts no further calls after any of the three.
  let settled = false
  let heartbeat: ReturnType<typeof setInterval> | null = null

  const stopHeartbeat = () => {
    if (heartbeat) {
      clearInterval(heartbeat)
      heartbeat = null
    }
  }

  const body = new ReadableStream<Uint8Array>({
    // Not async: the stream only starts calling pull() — which delivers the
    // resync — once start() returns, so the event loop runs detached.
    start(controller) {
      const close = () => {
        void events[Symbol.asyncDispose]()
      }
      request.signal.addEventListener('abort', close, { once: true })

      const send = (chunk: string) => {
        if (!settled && (controller.desiredSize ?? 0) > 0) {
          controller.enqueue(encoder.encode(chunk))
          return true
        }
        return false
      }

      controller.enqueue(encoder.encode(': connected\n\n'))
      // A comment every so often lets either end notice a dead connection.
      heartbeat = setInterval(() => send(': ping\n\n'), HEARTBEAT_MS)
      void (async () => {
        for await (const event of events) {
          if (!send(`data: ${JSON.stringify({ type: event.type, runId: event.runId })}\n\n`)) {
            missed = true
          }
        }
      })().then(
        () => {
          if (!settled) {
            settled = true
            controller.close()
          }
        },
        (error: unknown) => {
          if (!settled) {
            settled = true
            controller.error(error)
          }
        },
      ).finally(() => {
        stopHeartbeat()
        request.signal.removeEventListener('abort', close)
      })
    },
    pull(controller) {
      if (missed && !settled) {
        missed = false
        controller.enqueue(encoder.encode(`data: ${JSON.stringify({ type: 'resync' })}\n\n`))
      }
    },
    async cancel() {
      settled = true
      stopHeartbeat()
      await events[Symbol.asyncDispose]()
    },
  })

  return new Response(body, {
    status: 200,
    headers: {
      ...SECURITY_HEADERS,
      'content-type': 'text/event-stream; charset=utf-8',
      'cache-control': 'no-store',
      connection: 'keep-alive',
    },
  })
}

/**
 * Whether a state-changing request came from the dashboard page.
 *
 * A cross-site form or script cannot set a custom header without the browser
 * first asking this server's permission, which it never grants, so requiring
 * one rules out cross-site request forgery. A present `Origin` must also name
 * this host, or one in `allowedHosts`. Hosts are compared rather than whole
 * origins so that a TLS-terminating proxy, which turns `https://` into
 * `http://` on the way in, does not block every action.
 */
function isSameOriginAction(request: Request, url: URL, allowedHosts: readonly string[] | undefined): boolean {
  if (request.headers.get(ACTION_HEADER) !== '1') {
    return false
  }
  const origin = request.headers.get('origin')
  if (origin === null) {
    return true
  }
  let originHost: string
  try {
    originHost = new URL(origin).host.toLowerCase()
  } catch {
    // `Origin: null` and other opaque origins name no host to trust.
    return false
  }
  return originHost === url.host.toLowerCase() || (allowedHosts?.includes(originHost) ?? false)
}

/** Split a route into decoded segments, or `null` if one is not valid percent-encoding. */
function decodeSegments(route: string): string[] | null {
  const segments: string[] = []
  for (const segment of route.split('/')) {
    if (segment.length === 0) continue
    try {
      segments.push(decodeURIComponent(segment))
    } catch {
      return null
    }
  }
  return segments
}

/** The path below `basePath`, or `null` when the request is outside the dashboard. */
function routeOf(pathname: string, basePath: string): string | null {
  if (pathname === basePath.slice(0, -1)) {
    return ''
  }
  return pathname.startsWith(basePath) ? pathname.slice(basePath.length) : null
}

/** `'/admin/reflow'` → `'/admin/reflow/'`; always a leading and trailing slash. */
function normalizeBasePath(basePath: string): string {
  const trimmed = basePath.replace(/^\/+|\/+$/g, '')
  return trimmed === '' ? '/' : `/${trimmed}/`
}

function isRunStatus(value: string): value is RunStatus {
  return (RUN_STATUSES as readonly string[]).includes(value)
}

const SECURITY_HEADERS = {
  'x-content-type-options': 'nosniff',
  'referrer-policy': 'no-referrer',
  'x-frame-options': 'DENY',
} as const

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...SECURITY_HEADERS, 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' },
  })
}

function text(status: number, body: string): Response {
  return new Response(body, {
    status,
    headers: { ...SECURITY_HEADERS, 'content-type': 'text/plain; charset=utf-8', 'cache-control': 'no-store' },
  })
}

function methodNotAllowed(allow: string): Response {
  return new Response(`Use ${allow} for this endpoint`, {
    status: 405,
    headers: { ...SECURITY_HEADERS, allow, 'content-type': 'text/plain; charset=utf-8' },
  })
}

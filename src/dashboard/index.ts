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
 * import { basicAuth, createDashboard } from 'reflow-ts/dashboard'
 *
 * const dashboard = createDashboard({
 *   storage,
 *   engine, // optional: enables cancel / resume and live updates
 *   authorize: basicAuth({ username: 'admin', password: process.env.REFLOW_DASHBOARD_PASSWORD }),
 * })
 *
 * Bun.serve({ port: 4400, fetch: dashboard })
 * ```
 */

import { randomBytes } from 'node:crypto'
import type { Engine } from '../core/engine'
import { assertNever, errorMessage, toError } from '../core/errors'
import {
  RUN_STATUSES,
  type ListRunsFilter,
  type PersistedValue,
  type RunCounts,
  type RunStatus,
  type StepResult,
  type StorageAdapter,
  type WorkflowRun,
} from '../core/types'
import type { ExecutionUnit } from '../core/workflow'
import { renderPage } from './page'
import { RecentReads } from './recent'

export { toNodeHandler } from './node'
export { basicAuth, type BasicAuthOptions } from './auth'

/** Configuration for {@link createDashboard}. */
export interface DashboardOptions {
  /** Where runs are read from — the same storage your engine uses. */
  storage: StorageAdapter
  /**
   * Decide whether a request may see the dashboard. Called for every request,
   * page and API alike; return `false` to answer 401, or a `Response` to send
   * it instead — a redirect to your login page, say. {@link basicAuth} is a
   * ready-made one that asks for a password.
   *
   * Required, because run inputs and outputs are your application's data.
   * Check something the request *carries* — a session cookie, a bearer token —
   * not where it comes from: a page on another site can reach a loopback port
   * through DNS rebinding. To serve it with no check at all, pass `() => true`
   * together with {@link DashboardOptions.allowedHosts}.
   */
  authorize(request: Request): boolean | Response | Promise<boolean | Response>
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
  /**
   * The workflows your engine runs — the same list passed to `createEngine`.
   * Optional: with it the dashboard lists every workflow with its steps in
   * order, including steps a run has not reached yet; without it, it lists
   * the workflows seen in recent runs, and only the steps that have run.
   */
  workflows?: readonly DescribedWorkflow[]
}

/** The part of a workflow the dashboard reads: its name and its steps. */
export interface DescribedWorkflow {
  readonly name: string
  readonly executionUnits: readonly ExecutionUnit[]
  /** The workflow's run label — see `createWorkflow({ label })`. */
  label?(input: PersistedValue): string
}

/** One entry in a workflow's outline, as the page draws it. */
type OutlineUnit =
  | { readonly kind: 'step'; readonly name: string }
  | { readonly kind: 'parallel'; readonly branches: readonly string[] }
  | { readonly kind: 'sleep'; readonly name: string; readonly durationMs: number }
  | { readonly kind: 'wait'; readonly name: string; readonly timeoutMs: number | null }

/** How many recent runs are read for workflow names when no definitions are given. */
const RECENT_RUNS_FOR_NAMES = 500

/** How many of a workflow's latest runs its insights are read from. */
const INSIGHT_SAMPLE = 200
const ACTIVITY_MINUTES = 60
/** How long one computation of a workflow's insights is shared between requests. */
const INSIGHT_TTL_MS = 2_000
/** How long run counts are shared between requests. */
const COUNTS_TTL_MS = 1_000

/** One page of the run list, as the API sends it. */
interface RunPage {
  readonly runs: readonly (Omit<WorkflowRun, 'input' | 'output'> & {
    readonly label: string | null
    readonly steps: readonly Omit<StepResult, 'output' | 'id' | 'runId'>[]
  })[]
  readonly next: { readonly before: number; readonly beforeId: string } | null
}

/** Most distinct reads of one kind held at once. */
const RECENT_CAPACITY = 64
const MINUTE_MS = 60_000

/** One step of a workflow, and how its recent runs fared there. */
interface PipelineStage {
  readonly name: string
  /** The step names it covers: one, or each branch of a parallel group. */
  readonly steps: readonly string[]
  /** Runs that got as far as this step. */
  reached: number
  completed: number
  failed: number
  skipped: number
  /** Runs paused or executing at this step right now. */
  here: number
  /** Of those paused here, the ones backing off before another attempt. */
  retrying: number
}

/** Failed runs that stopped at the same step with the same message. */
interface FailureGroup {
  readonly stepName: string
  readonly message: string
  readonly code: string | null
  count: number
  lastAt: number
  /** Attempts the step made in the first run of the group. */
  readonly attempts: number
}

interface WorkflowInsights {
  /** How many runs these figures come from. */
  readonly sample: number
  /** Whether that is every run of the workflow. */
  readonly complete: boolean
  /** When the oldest of them was enqueued. */
  readonly since: number | null
  readonly pipeline: readonly PipelineStage[]
  readonly failures: readonly FailureGroup[]
  readonly activity: {
    readonly start: number
    readonly bucketMs: number
    readonly started: number[]
    readonly completed: number[]
    readonly failed: number[]
    /** The counts are complete from this time on; before it they are unknown, not zero. */
    readonly coveredFrom: number
  }
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
  const outlines = new Map((options.workflows ?? []).map((workflow) => [workflow.name, outlineOf(workflow)]))
  const labellers = new Map((options.workflows ?? []).map((workflow) => [workflow.name, workflow]))
  // Counts and insights scan many rows; several viewers, or one refreshing
  // on every change, share a recent result rather than each reading again.
  const insightCache = new RecentReads<WorkflowInsights>(INSIGHT_TTL_MS, RECENT_CAPACITY)
  const countCache = new RecentReads<RunCounts>(COUNTS_TTL_MS, RECENT_CAPACITY)
  // A page of runs, shared between identical requests the same way.
  const pageCache = new RecentReads<RunPage>(COUNTS_TTL_MS, RECENT_CAPACITY)

  /**
   * A run's name for people: what its workflow's `label` returns, else its
   * idempotency key, else nothing — the page then shows its short id. Input
   * is never shown here unless the workflow's author chose to.
   */
  function labelOf(run: WorkflowRun): string | null {
    const workflow = labellers.get(run.workflow)
    let label: string | null = run.idempotencyKey
    if (workflow?.label) {
      try {
        const named: unknown = workflow.label(run.input)
        label = isThenable(named)
          ? 'label() returned a Promise; it must return a string'
          : String(named)
      } catch (error) {
        // Shown in place of the label, so a broken labeller is visible
        // rather than quietly falling back.
        label = `label() threw: ${errorMessage(toError(error))}`
      }
    }
    if (label === null || label === '') return null
    return label.length > LABEL_LENGTH ? `${label.slice(0, LABEL_LENGTH - 1)}…` : label
  }

  return async (request) => {
    const url = new URL(request.url)
    const route = routeOf(url.pathname, basePath)
    if (route === null) {
      return text(404, 'Not found')
    }

    if (allowedHosts && !allowedHosts.includes(url.host.toLowerCase())) {
      return text(421, 'This dashboard does not answer for that host')
    }

    let authorized: boolean | Response
    try {
      authorized = await authorize(request)
    } catch {
      // Deliberately generic: the caller is unauthenticated, and an
      // authorize() error message can echo what they sent.
      return text(500, 'Could not check authorization')
    }
    if (authorized instanceof Response) {
      return authorized
    }
    // Only an explicit `true` lets a request in. Anything else — a Response
    // built by another fetch implementation, which is not this runtime's
    // Response but is truthy, say — is refused rather than read as consent.
    if (authorized !== true) {
      return authorized === false
        ? text(401, 'Not authorized to view this dashboard')
        : text(500, 'authorize() must return true, false, or a Response')
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
        counts: await countCache.shared('all', () => storage.countRuns()),
        actions: engine !== undefined,
        live: engine !== undefined,
      })
    }

    if (second === 'workflows' && segments.length === 2) {
      if (method !== 'GET') return methodNotAllowed('GET')
      return listWorkflows()
    }

    // The workflow goes in the query, not the path: a name such as `..`
    // would otherwise be resolved away by the browser before it was sent.
    if (second === 'insights' && segments.length === 2) {
      if (method !== 'GET') return methodNotAllowed('GET')
      const name = url.searchParams.get('workflow')
      if (name === null) return json(400, { error: 'insights needs a workflow' })
      return json(200, await insightsFor(name))
    }

    if (second === 'runs' && segments.length === 2) {
      if (method !== 'GET') return methodNotAllowed('GET')
      return listRuns(url.searchParams)
    }

    if (second === 'runs' && third !== undefined && segments.length === 3) {
      if (method !== 'GET') return methodNotAllowed('GET')
      const run = await storage.getRun(third)
      if (!run) return json(404, { error: `No run with id "${third}"` })
      return json(200, {
        run,
        label: labelOf(run),
        steps: await storage.getStepResults(third),
        outline: outlines.get(run.workflow) ?? null,
      })
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

  /**
   * Every workflow with its run counts. Names come from the definitions when
   * given; otherwise from recent runs, since storage keeps no list of them.
   */
  async function listWorkflows(): Promise<Response> {
    const known = outlines.size > 0
    const names = known
      ? [...outlines.keys()]
      : [...new Set((await storage.listRuns({ limit: RECENT_RUNS_FOR_NAMES })).map((run) => run.workflow))].sort()
    const workflows = await Promise.all(names.map(async (name) => ({
      name,
      steps: outlines.get(name) ?? null,
      counts: await countCache.shared(`workflow:${name}`, () => storage.countRuns({ workflow: name })),
    })))
    return json(200, { workflows, source: known ? 'definitions' : 'recent-runs' })
  }

  /**
   * How a workflow's recent runs are going: how far they get through its
   * steps and where they are now, what they fail with, and how many started,
   * completed, and failed each minute of the last hour.
   *
   * Read from the latest {@link INSIGHT_SAMPLE} runs rather than aggregated
   * in storage, which keeps no per-step counts; the page says so.
   */
  /** Several runs' steps: in one read where the storage offers it. */
  async function stepsOf(runs: readonly WorkflowRun[]): Promise<Map<string, StepResult[]>> {
    if (storage.getStepResultsForRuns) {
      return storage.getStepResultsForRuns(runs.map((run) => run.id))
    }
    const lists = await Promise.all(runs.map((run) => storage.getStepResults(run.id)))
    return new Map(runs.map((run, index) => [run.id, lists[index] ?? []]))
  }

  function insightsFor(name: string): Promise<WorkflowInsights> {
    return insightCache.shared(name, () => computeInsights(name))
  }

  async function computeInsights(name: string): Promise<WorkflowInsights> {
    const runs = await storage.listRuns({ workflow: name, limit: INSIGHT_SAMPLE })
    const stepMap = await stepsOf(runs)
    const stepsByRun = runs.map((run) => stepMap.get(run.id) ?? [])
    const outlined = outlines.get(name)
    const units = outlined ? outlined.map(unitNames) : namesInOrder(stepsByRun)
    const now = Date.now()

    const pipeline = units.map((names): PipelineStage => ({
      name: names.join(' + '),
      steps: names,
      reached: 0, completed: 0, failed: 0, skipped: 0, here: 0, retrying: 0,
    }))
    const failures = new Map<string, FailureGroup>()

    runs.forEach((run, index) => {
      const steps = new Map((stepsByRun[index] ?? []).map((step) => [step.name, step]))
      // Only a run that can still move on is anywhere "now": a cancelled run
      // keeps the step rows it had, sleeping or waiting, but is going nowhere.
      const paused = run.status === 'sleeping' || run.status === 'waiting'
      let current = -1
      units.forEach((names, position) => {
        const stage = pipeline[position]
        const members = names.map((member) => steps.get(member)).filter((step) => step !== undefined)
        if (!stage || members.length === 0) return
        stage.reached++
        if (members.some((step) => step.status === 'failed')) stage.failed++
        else if (members.every((step) => step.status === 'skipped')) stage.skipped++
        else if (members.every((step) => step.status === 'completed' || step.status === 'completed-early' || step.status === 'skipped')) stage.completed++
        if (paused && members.some((step) => step.status === 'retrying')) stage.retrying++
        if (paused && members.some((step) => step.status === 'retrying' || step.status === 'sleeping' || step.status === 'waiting')) stage.here++
        // A running run is executing in the first stage it has not finished:
        // one partly saved, such as a parallel group with a branch still going,
        // or else the one after the last it saved.
        const finished = members.length === names.length && members.every((step) => step.status !== 'running')
        current = finished ? position + 1 : position
      })
      const executing = pipeline[current === -1 ? 0 : current]
      if (run.status === 'running' && executing) executing.here++

      if (run.status === 'failed' && run.error) {
        const key = `${run.error.stepName}\u0000${run.error.message}`
        const group = failures.get(key)
        const attempts = steps.get(run.error.stepName)?.attempts ?? 0
        if (group) {
          group.count++
          group.lastAt = Math.max(group.lastAt, run.finishedAt ?? run.updatedAt)
        } else {
          failures.set(key, {
            stepName: run.error.stepName, message: run.error.message, code: run.error.code,
            count: 1, lastAt: run.finishedAt ?? run.updatedAt, attempts,
          })
        }
      }
    })

    const activity = await activityFor(name, runs, now)
    return {
      sample: runs.length,
      complete: runs.length < INSIGHT_SAMPLE,
      since: runs.at(-1)?.createdAt ?? null,
      pipeline,
      failures: [...failures.values()].sort((a, b) => b.count - a.count || b.lastAt - a.lastAt),
      activity,
    }
  }

  /**
   * Runs started, completed, and failed per minute over the last hour. Exact
   * when the storage counts them; otherwise counted from the sample, and
   * marked as covering only the stretch of time the sample spans, so a busy
   * workflow's chart is not read as an hour with nothing in it.
   */
  async function activityFor(name: string, runs: readonly WorkflowRun[], now: number): Promise<WorkflowInsights['activity']> {
    const start = Math.floor(now / MINUTE_MS) * MINUTE_MS - (ACTIVITY_MINUTES - 1) * MINUTE_MS
    const window = { start, bucketMs: MINUTE_MS }
    if (storage.runActivity) {
      const counts = await storage.runActivity({ workflow: name, since: start, bucketMs: MINUTE_MS, buckets: ACTIVITY_MINUTES })
      return { ...window, ...counts, coveredFrom: start }
    }
    const oldest = runs.at(-1)?.createdAt ?? now
    const coveredFrom = runs.length < INSIGHT_SAMPLE ? start : Math.max(start, oldest)
    const bucket = (at: number | null): number => at === null || at < coveredFrom ? -1 : Math.min(ACTIVITY_MINUTES - 1, Math.floor((at - start) / MINUTE_MS))
    const counts = {
      started: new Array<number>(ACTIVITY_MINUTES).fill(0),
      completed: new Array<number>(ACTIVITY_MINUTES).fill(0),
      failed: new Array<number>(ACTIVITY_MINUTES).fill(0),
    }
    for (const run of runs) {
      const started = bucket(run.createdAt)
      if (started >= 0) counts.started[started] = (counts.started[started] ?? 0) + 1
      const finished = bucket(run.finishedAt)
      if (finished >= 0 && run.status === 'completed') counts.completed[finished] = (counts.completed[finished] ?? 0) + 1
      if (finished >= 0 && run.status === 'failed') counts.failed[finished] = (counts.failed[finished] ?? 0) + 1
    }
    return { ...window, ...counts, coveredFrom }
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

    return json(200, await pageCache.shared(JSON.stringify(filter), () => readPage(filter)))
  }

  async function readPage(filter: ListRunsFilter): Promise<RunPage> {
    const runs = await storage.listRuns(filter)
    const last = runs.at(-1)
    const next = last !== undefined && runs.length === filter.limit
      ? { before: last.createdAt, beforeId: last.id }
      : null

    // A run's row and detail need its steps' timing but not their outputs.
    // Inputs and outputs can be large; the list sends neither, and the detail
    // endpoint returns them for the one run being looked at.
    const steps = await stepsOf(runs)
    return {
      runs: runs.map((run) => ({
        ...summarize(run),
        label: labelOf(run),
        steps: (steps.get(run.id) ?? []).map(outline),
      })),
      next,
    }
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

/** The step names a unit covers: one, or each branch of a parallel group. */
function unitNames(unit: OutlineUnit): readonly string[] {
  return unit.kind === 'parallel' ? unit.branches : [unit.name]
}

/** Without definitions, steps in the order runs first recorded them. */
function namesInOrder(stepsByRun: readonly (readonly StepResult[])[]): string[][] {
  const seen = new Set<string>()
  const order: string[][] = []
  for (const steps of stepsByRun) {
    for (const step of steps) {
      if (!seen.has(step.name)) {
        seen.add(step.name)
        order.push([step.name])
      }
    }
  }
  return order
}

/** Whether a value is a Promise or something awaitable like one. */
function isThenable(value: unknown): boolean {
  return typeof value === 'object' && value !== null && 'then' in value && typeof value.then === 'function'
}

/** A workflow's units as names and kinds, without handlers or schemas. */
function outlineOf(workflow: DescribedWorkflow): OutlineUnit[] {
  return workflow.executionUnits.map((unit): OutlineUnit => {
    switch (unit.kind) {
      case 'step':
        return { kind: 'step', name: unit.definition.name }
      case 'parallel':
        return { kind: 'parallel', branches: unit.branches.map((branch) => branch.name) }
      case 'sleep':
        return { kind: 'sleep', name: unit.name, durationMs: unit.durationMs }
      case 'waitForEvent':
        return { kind: 'wait', name: unit.name, timeoutMs: unit.timeoutMs ?? null }
      default:
        return assertNever(unit, 'execution unit')
    }
  })
}

/** Longest label sent for a run; its full input is in the run's detail. */
const LABEL_LENGTH = 80

/** A step without its output: what a run's row needs and nothing more. */
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

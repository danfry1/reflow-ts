import { describe, it, expect, beforeEach } from 'vitest'
import { createServer } from 'node:http'
import { z } from 'zod'
import { createEngine, createWorkflow } from '../../index'
import { MemoryStorage } from '../../storage/memory'
import { delegatingAdapter } from '../../__tests__/helpers'
import { createDashboard, toNodeHandler, type DashboardOptions } from '../index'

const ORIGIN = 'http://dash.test'

const wf = createWorkflow({ name: 'order', input: z.object({ id: z.string() }) })
  .step('charge', async ({ input }) => {
    if (input.id === 'bad') throw new Error('card declined')
    return { charged: input.id }
  })

const waiter = createWorkflow({ name: 'approval', input: z.object({}) }).waitForEvent('approved')

function get(path: string, init?: RequestInit): Request {
  return new Request(ORIGIN + path, init)
}

function post(path: string, headers: Record<string, string> = { 'x-reflow-dashboard': '1' }): Request {
  return new Request(ORIGIN + path, { method: 'POST', headers })
}

describe('createDashboard', () => {
  let storage: MemoryStorage
  let engine: ReturnType<typeof makeEngine>

  function makeEngine() {
    return createEngine({ storage, workflows: [wf, waiter], concurrency: 5 })
  }

  function dashboard(options: Partial<DashboardOptions> = {}) {
    return createDashboard({ storage, authorize: () => true, ...options })
  }

  beforeEach(async () => {
    storage = new MemoryStorage()
    await storage.initialize()
    engine = makeEngine()
  })

  describe('access', () => {
    it('answers 401 to a request authorize() rejects, page and API alike', async () => {
      const seen: string[] = []
      const handler = dashboard({ authorize: (request) => {
        seen.push(new URL(request.url).pathname)
        return false
      } })

      expect((await handler(get('/'))).status).toBe(401)
      expect((await handler(get('/api/overview'))).status).toBe(401)
      expect(seen).toStrictEqual(['/', '/api/overview'])
    })

    it('answers 500 without detail when authorize() throws', async () => {
      const handler = dashboard({ authorize: () => {
        throw new Error('<script>alert(1)</script> is not a token')
      } })

      const response = await handler(get('/api/overview'))

      expect(response.status).toBe(500)
      expect(await response.text()).toBe('Could not check authorization')
      expect(response.headers.get('x-content-type-options')).toBe('nosniff')
    })

    it('answers only to allowed hosts, so DNS rebinding cannot reach it', async () => {
      const handler = dashboard({ allowedHosts: ['localhost:4400'] })

      const rebound = await handler(new Request('http://attacker.example:4400/api/runs'))
      const local = await handler(new Request('http://LOCALHOST:4400/api/runs'))

      expect(rebound.status).toBe(421)
      expect(local.status).toBe(200)
    })

    it('waits for an async authorize()', async () => {
      const handler = dashboard({ authorize: async () => true })

      expect((await handler(get('/api/overview'))).status).toBe(200)
    })
  })

  describe('page', () => {
    it('serves the page with a nonce-locked content security policy', async () => {
      const response = await dashboard()(get('/'))
      const html = await response.text()
      const policy = response.headers.get('content-security-policy') ?? ''
      const nonce = /script-src 'nonce-([^']+)'/.exec(policy)?.[1]

      expect(response.status).toBe(200)
      expect(response.headers.get('content-type')).toBe('text/html; charset=utf-8')
      expect(policy).toContain("default-src 'none'")
      expect(policy).toContain("frame-ancestors 'none'")
      expect(nonce).toMatch(/^[A-Za-z0-9+/=]{24}$/)
      expect(html).toContain(`<script nonce="${nonce}">`)
      expect(html).toContain(`<style nonce="${nonce}">`)
    })

    it('uses a fresh nonce for every response', async () => {
      const handler = dashboard()
      const first = (await handler(get('/'))).headers.get('content-security-policy')
      const second = (await handler(get('/'))).headers.get('content-security-policy')

      expect(first).not.toBe(second)
    })

    it('loads nothing from anywhere else', async () => {
      const html = await (await dashboard()(get('/'))).text()

      expect(html).not.toMatch(/\b(src|href)="https?:/)
    })

    it('escapes the configured title', async () => {
      const html = await (await dashboard({ title: '<b>Ops</b> & co' })(get('/'))).text()

      expect(html).toContain('<title>&lt;b&gt;Ops&lt;/b&gt; &amp; co</title>')
      expect(html).not.toContain('<b>Ops</b>')
    })
  })

  describe('mount path', () => {
    it('redirects the bare mount path to its trailing-slash form', async () => {
      const response = await dashboard({ basePath: '/admin/reflow' })(get('/admin/reflow?x=1'))

      expect(response.status).toBe(308)
      expect(response.headers.get('location')).toBe('/admin/reflow/?x=1')
    })

    it('serves the API below the mount path and nothing outside it', async () => {
      const handler = dashboard({ basePath: '/admin/reflow/' })

      expect((await handler(get('/admin/reflow/api/overview'))).status).toBe(200)
      expect((await handler(get('/api/overview'))).status).toBe(404)
      expect((await handler(get('/admin/reflowx/api/overview'))).status).toBe(404)
    })
  })

  describe('reading', () => {
    it('reports counts, and whether actions and live updates are available', async () => {
      await engine.enqueue('order', { id: 'a' })

      const readOnly = await (await dashboard()(get('/api/overview'))).json()
      const withEngine = await (await dashboard({ engine })(get('/api/overview'))).json()

      expect(readOnly).toMatchObject({ title: 'Reflow', actions: false, live: false })
      expect(readOnly.counts.pending).toBe(1)
      expect(withEngine).toMatchObject({ actions: true, live: true })
    })

    it('lists runs without their inputs and outputs', async () => {
      await engine.enqueue('order', { id: 'secret-ish' })
      await engine.tick()

      const body = await (await dashboard()(get('/api/runs'))).json()

      expect(body.runs).toHaveLength(1)
      expect(body.runs[0]).toMatchObject({ workflow: 'order', status: 'completed' })
      expect(body.runs[0]).not.toHaveProperty('input')
      expect(body.runs[0]).not.toHaveProperty('output')
      expect(body.next).toBeNull()
    })

    it("lists each run with its steps' timing, but not their outputs", async () => {
      await engine.enqueue('order', { id: 'o1' })
      await engine.tick()

      const body = await (await dashboard()(get('/api/runs'))).json()
      const steps = body.runs[0].steps

      expect(steps.map((step: { name: string; status: string }) => [step.name, step.status]))
        .toStrictEqual([['charge', 'completed']])
      expect(steps[0].history).toHaveLength(1)
      expect(steps[0]).toHaveProperty('startedAt')
      expect(steps[0]).not.toHaveProperty('output')
    })

    it('lists the given workflows with their steps in order and their run counts', async () => {
      const staged = createWorkflow({ name: 'staged', input: z.object({}) })
        .step('first', async () => ({}))
        .parallel({ left: async () => ({}), right: async () => ({}) })
        .sleep('pause', 60_000)
        .waitForEvent('approved', { timeoutMs: 5_000 })
      await engine.enqueue('order', { id: 'o1' })
      await engine.tick()

      const body = await (await dashboard({ workflows: [wf, staged] })(get('/api/workflows'))).json()

      expect(body.source).toBe('definitions')
      expect(body.workflows.map((workflow: { name: string }) => workflow.name)).toStrictEqual(['order', 'staged'])
      expect(body.workflows[0].counts.completed).toBe(1)
      expect(body.workflows[1].steps).toStrictEqual([
        { kind: 'step', name: 'first' },
        { kind: 'parallel', branches: ['left', 'right'] },
        { kind: 'sleep', name: 'pause', durationMs: 60_000 },
        { kind: 'wait', name: 'approved', timeoutMs: 5_000 },
      ])
    })

    it('lists a workflow written as a function without an outline, as its steps are known only once runs reach them', async () => {
      const fn = createWorkflow({ name: 'as-function', input: z.object({}) }, async ({ step }) => {
        await step.run('only', async () => 1)
      })

      const body = await (await dashboard({ workflows: [wf, fn] })(get('/api/workflows'))).json()

      expect(body.workflows.map((workflow: { name: string; steps: unknown }) => [workflow.name, workflow.steps !== null]))
        .toStrictEqual([['order', true], ['as-function', false]])
    })

    it('lists the workflows seen in recent runs when given no definitions', async () => {
      await engine.enqueue('order', { id: 'o1' })
      await engine.enqueue('approval', {})

      const body = await (await dashboard()(get('/api/workflows'))).json()

      expect(body.source).toBe('recent-runs')
      expect(body.workflows.map((workflow: { name: string; steps: unknown }) => [workflow.name, workflow.steps]))
        .toStrictEqual([['approval', null], ['order', null]])
    })

    it("names runs by their workflow's label, else their idempotency key, else not at all", async () => {
      const labelled = createWorkflow({ name: 'labelled', input: z.object({ userId: z.string() }), label: (input) => `user ${input.userId}` })
        .step('a', async () => ({}))
      const broken = createWorkflow({ name: 'broken', input: z.object({}), label: () => { throw new Error('no name') } })
        .step('a', async () => ({}))
      const labelling = createEngine({ storage, workflows: [wf, labelled, broken] })
      const named = await labelling.enqueue('labelled', { userId: 'u_42' })
      const keyed = await labelling.enqueue('order', { id: 'secret' }, { idempotencyKey: 'order-1001' })
      const plain = await labelling.enqueue('order', { id: 'secret' })
      const failing = await labelling.enqueue('broken', {})
      const long = await labelling.enqueue('labelled', { userId: 'x'.repeat(200) })

      const handler = dashboard({ workflows: [wf, labelled, broken] })
      const listed = await (await handler(get('/api/runs'))).json()
      const labels = new Map(listed.runs.map((run: { id: string; label: string | null }) => [run.id, run.label]))
      const detail = await (await handler(get(`/api/runs/${named.id}`))).json()

      expect(labels.get(named.id)).toBe('user u_42')
      expect(labels.get(keyed.id)).toBe('order-1001')
      expect(labels.get(plain.id)).toBeNull()
      expect(labels.get(failing.id)).toBe('label() threw: no name')
      expect(labels.get(long.id)).toHaveLength(80)
      expect(detail.label).toBe('user u_42')
      expect(detail.outline).toStrictEqual([{ kind: 'step', name: 'a' }])
    })

    it("sums up a workflow's recent runs: how far they got, what they failed with, and when", async () => {
      const pipeline = createWorkflow({ name: 'pipeline', input: z.object({ fail: z.string().optional() }) })
        .step('fetch', async () => ({}))
        .step('charge', async ({ input }) => {
          if (input.fail) throw new Error(input.fail)
          return {}
        })
        .waitForEvent('shipped')
      const local = createEngine({ storage, workflows: [pipeline] })
      for (const fail of [undefined, undefined, 'card declined', 'card declined', 'gateway down']) {
        await local.enqueue('pipeline', fail ? { fail } : {})
      }
      for (let index = 0; index < 5; index++) await local.tick()

      const body = await (await dashboard({ workflows: [pipeline] })(get('/api/insights?workflow=pipeline'))).json()

      expect({ sample: body.sample, complete: body.complete }).toStrictEqual({ sample: 5, complete: true })
      expect(body.pipeline.map((stage: Record<string, unknown>) => [stage.name, stage.reached, stage.completed, stage.failed, stage.here]))
        .toStrictEqual([['fetch', 5, 5, 0, 0], ['charge', 5, 2, 3, 0], ['shipped', 2, 0, 0, 2]])
      expect(body.failures.map((group: Record<string, unknown>) => [group.stepName, group.message, group.count]))
        .toStrictEqual([['charge', 'card declined', 2], ['charge', 'gateway down', 1]])
      expect(body.activity.started.reduce((sum: number, n: number) => sum + n, 0)).toBe(5)
      expect(body.activity.failed.reduce((sum: number, n: number) => sum + n, 0)).toBe(3)
    })

    it("reads every listed run's steps in one call when the storage offers it", async () => {
      await engine.enqueue('order', { id: 'o1' })
      await engine.enqueue('order', { id: 'o2' })
      await engine.tick()
      let single = 0
      let batched = 0
      const counting = delegatingAdapter(storage, {
        getStepResults: (runId) => { single++; return storage.getStepResults(runId) },
        getStepResultsForRuns: (runIds) => { batched++; return storage.getStepResultsForRuns(runIds) },
      })

      const body = await (await createDashboard({ storage: counting, authorize: () => true })(get('/api/runs'))).json()

      expect(body.runs).toHaveLength(2)
      expect({ single, batched }).toStrictEqual({ single: 0, batched: 1 })
    })

    it('shares one computation of insights between requests close together', async () => {
      await engine.enqueue('order', { id: 'o1' })
      let lists = 0
      const counting = delegatingAdapter(storage, { listRuns: (filter) => { lists++; return storage.listRuns(filter) } })
      const handler = createDashboard({ storage: counting, authorize: () => true, workflows: [wf] })

      await handler(get('/api/insights?workflow=order'))
      await handler(get('/api/insights?workflow=order'))

      expect(lists).toBe(1)
    })

    it('marks the minutes a sample of runs does not reach as uncounted, without runActivity', async () => {
      const now = Date.now()
      for (let index = 0; index < 210; index++) {
        await storage.createRun({
          id: `r${index}`, workflow: 'order', input: { id: String(index) }, idempotencyKey: null, status: 'completed',
          createdAt: now - (210 - index) * 1_000, updatedAt: now, startedAt: null, finishedAt: now - (210 - index) * 1_000,
          output: undefined, error: null, wakeAt: null, waitingFor: null,
        })
      }
      // The helper adapter forwards only the required methods, so no runActivity.
      const plain = delegatingAdapter(storage)

      const body = await (await createDashboard({ storage: plain, authorize: () => true, workflows: [wf] })(get('/api/insights?workflow=order'))).json()

      const oldestSampled = now - 200 * 1_000
      expect(body.activity.coveredFrom).toBe(oldestSampled)
      expect(body.activity.started.reduce((sum: number, n: number) => sum + n, 0)).toBe(200)
    })

    it('counts only runs that can still move on as being at a step now', async () => {
      const napper = createWorkflow({ name: 'napper', input: z.object({}) }).step('first', async () => ({})).sleep('nap', 60_000)
      const local = createEngine({ storage, workflows: [napper] })
      const kept = await local.enqueue('napper', {})
      const cancelled = await local.enqueue('napper', {})
      await local.tick()
      await local.tick()
      await local.cancel(cancelled.id)

      const body = await (await dashboard({ workflows: [napper] })(get('/api/insights?workflow=napper'))).json()

      expect(kept.id).not.toBe(cancelled.id)
      expect(body.pipeline.map((stage: { name: string; here: number }) => [stage.name, stage.here])).toStrictEqual([['first', 0], ['nap', 1]])
    })

    it('places a running run at a parallel group a branch of which is still going', async () => {
      const fanout = createWorkflow({ name: 'fanout', input: z.object({}) })
        .step('first', async () => ({}))
        .parallel({ left: async () => ({}), right: async () => ({}) })
        .step('last', async () => ({}))
      const now = Date.now()
      await storage.createRun({
        id: 'r1', workflow: 'fanout', input: {}, idempotencyKey: null, status: 'running',
        createdAt: now, updatedAt: now, startedAt: now, finishedAt: null, output: undefined, error: null, wakeAt: null, waitingFor: null,
      })
      const step = (name: string) => ({
        id: `r1-${name}`, runId: 'r1', name, status: 'completed' as const, output: {}, error: null, attempts: 1,
        createdAt: now, updatedAt: now, startedAt: now, history: [],
      })
      await storage.saveStepResult(step('first'))
      await storage.saveStepResult(step('left'))

      const body = await (await dashboard({ workflows: [fanout] })(get('/api/insights?workflow=fanout'))).json()

      expect(body.pipeline.map((stage: { name: string; here: number }) => [stage.name, stage.here]))
        .toStrictEqual([['first', 0], ['left + right', 1], ['last', 0]])
      expect(body.pipeline[1].steps).toStrictEqual(['left', 'right'])
    })

    it('says so when a label() returns a Promise instead of a string', async () => {
      // The types forbid an async label; JavaScript callers are not held to them.
      const promising = Object.defineProperty(
        createWorkflow({ name: 'promising', input: z.object({}) }).step('a', async () => ({})),
        'label',
        { value: async () => 'never shown' },
      )
      await createEngine({ storage, workflows: [promising] }).enqueue('promising', {})

      const body = await (await dashboard({ workflows: [promising] })(get('/api/runs'))).json()

      expect(body.runs[0].label).toBe('label() returned a Promise; it must return a string')
    })

    it('serves insights for a workflow whose name a path would resolve away', async () => {
      const dots = createWorkflow({ name: '..', input: z.object({}) }).step('a', async () => ({}))
      await createEngine({ storage, workflows: [dots] }).enqueue('..', {})

      const response = await dashboard({ workflows: [dots] })(get(`/api/insights?workflow=${encodeURIComponent('..')}`))

      expect(response.status).toBe(200)
      expect((await response.json()).sample).toBe(1)
    })

    it('filters runs by status and workflow', async () => {
      await engine.enqueue('order', { id: 'ok' })
      await engine.enqueue('order', { id: 'bad' })
      await engine.enqueue('approval', {})
      await engine.tick()
      const handler = dashboard()

      const failed = await (await handler(get('/api/runs?status=failed'))).json()
      const approvals = await (await handler(get('/api/runs?workflow=approval'))).json()

      expect(failed.runs.map((run: { error: { message: string } }) => run.error.message)).toStrictEqual(['card declined'])
      expect(approvals.runs.map((run: { status: string }) => run.status)).toStrictEqual(['waiting'])
    })

    it('pages through runs with the returned cursor', async () => {
      for (const id of ['a', 'b', 'c']) {
        await engine.enqueue('order', { id })
      }
      const handler = dashboard()

      const first = await (await handler(get('/api/runs?limit=2'))).json()
      const cursor = new URLSearchParams({ limit: '2', before: String(first.next.before), beforeId: first.next.beforeId })
      const second = await (await handler(get(`/api/runs?${cursor.toString()}`))).json()

      const ids = [...first.runs, ...second.runs].map((run: { id: string }) => run.id)
      expect(new Set(ids).size).toBe(3)
      expect(second.next).toBeNull()
    })

    it('rejects an unknown status or an out-of-range limit', async () => {
      const handler = dashboard()

      expect((await handler(get('/api/runs?status=paused'))).status).toBe(400)
      expect((await handler(get('/api/runs?limit=0'))).status).toBe(400)
      expect((await handler(get('/api/runs?limit=201'))).status).toBe(400)
      expect((await handler(get('/api/runs?before=soon'))).status).toBe(400)
    })

    it('returns one run with its steps, or 404', async () => {
      const run = await engine.enqueue('order', { id: 'a' })
      await engine.tick()
      const handler = dashboard()

      const found = await (await handler(get(`/api/runs/${run.id}`))).json()
      const missing = await handler(get('/api/runs/nope'))

      expect(found.run).toMatchObject({ id: run.id, input: { id: 'a' }, output: { charged: 'a' } })
      expect(found.steps.map((step: { name: string }) => step.name)).toStrictEqual(['charge'])
      expect(missing.status).toBe(404)
    })

    it('decodes run ids in the path, and rejects malformed escapes', async () => {
      await storage.createRun({
        id: 'run with spaces', workflow: 'order', input: { id: 'x' }, idempotencyKey: null, status: 'pending',
        createdAt: 1, updatedAt: 1, startedAt: null, finishedAt: null, output: undefined, error: null, wakeAt: null, waitingFor: null,
      })
      const handler = dashboard()

      expect((await handler(get('/api/runs/run%20with%20spaces'))).status).toBe(200)
      expect((await handler(get('/api/runs/%E0%A4%A'))).status).toBe(400)
    })

    it('lists schedules', async () => {
      await engine.schedule('order', { id: 'nightly' }, { cron: '0 3 * * *' }, { key: 'nightly-order' })

      const body = await (await dashboard()(get('/api/schedules'))).json()

      expect(body.schedules).toMatchObject([{ key: 'nightly-order', recurrence: { kind: 'cron', expression: '0 3 * * *' } }])
    })

    it('answers 405 to the wrong method and 404 to an unknown endpoint', async () => {
      const handler = dashboard()

      expect((await handler(post('/api/overview'))).status).toBe(405)
      expect((await handler(get('/api/nothing'))).status).toBe(404)
    })

    it('answers 500 with the message when storage fails', async () => {
      const broken = delegatingAdapter(storage, {
        countRuns: async () => {
          throw new Error('disk gone')
        },
      })
      const response = await createDashboard({ storage: broken, authorize: () => true })(get('/api/overview'))

      expect(response.status).toBe(500)
      expect(await response.json()).toStrictEqual({ error: 'disk gone' })
    })
  })

  describe('actions', () => {
    it('cancels and resumes a run through the engine', async () => {
      const run = await engine.enqueue('approval', {})
      await engine.tick()
      const handler = dashboard({ engine })

      const cancelled = await handler(post(`/api/runs/${run.id}/cancel`))
      expect(await cancelled.json()).toStrictEqual({ done: true })
      expect((await storage.getRun(run.id))?.status).toBe('cancelled')

      const resumed = await handler(post(`/api/runs/${run.id}/resume`))
      expect(await resumed.json()).toStrictEqual({ done: true })
      expect((await storage.getRun(run.id))?.status).toBe('pending')
    })

    it('refuses actions without the dashboard header', async () => {
      const run = await engine.enqueue('approval', {})

      const response = await dashboard({ engine })(post(`/api/runs/${run.id}/cancel`, {}))

      expect(response.status).toBe(403)
      expect((await storage.getRun(run.id))?.status).toBe('pending')
    })

    it('refuses actions from another origin', async () => {
      const run = await engine.enqueue('approval', {})

      const response = await dashboard({ engine })(
        post(`/api/runs/${run.id}/cancel`, { 'x-reflow-dashboard': '1', origin: 'https://evil.example' }),
      )

      expect(response.status).toBe(403)
      expect((await storage.getRun(run.id))?.status).toBe('pending')
    })

    it('accepts actions from the same host over https, as through a TLS-terminating proxy', async () => {
      const run = await engine.enqueue('approval', {})

      const response = await dashboard({ engine })(
        post(`/api/runs/${run.id}/cancel`, { 'x-reflow-dashboard': '1', origin: 'https://dash.test' }),
      )

      expect(response.status).toBe(200)
    })

    it('accepts actions from an allowed public host when a proxy rewrites Host', async () => {
      const run = await engine.enqueue('approval', {})
      const handler = dashboard({ engine, allowedHosts: ['dash.test', 'admin.example.com'] })

      const fromPublic = await handler(
        post(`/api/runs/${run.id}/cancel`, { 'x-reflow-dashboard': '1', origin: 'https://admin.example.com' }),
      )

      expect(fromPublic.status).toBe(200)
    })

    it('refuses actions from an opaque origin', async () => {
      const run = await engine.enqueue('approval', {})

      const response = await dashboard({ engine })(
        post(`/api/runs/${run.id}/cancel`, { 'x-reflow-dashboard': '1', origin: 'null' }),
      )

      expect(response.status).toBe(403)
    })

    it('is read-only without an engine', async () => {
      const run = await engine.enqueue('approval', {})

      const response = await dashboard()(post(`/api/runs/${run.id}/cancel`))

      expect(response.status).toBe(403)
      expect((await storage.getRun(run.id))?.status).toBe('pending')
    })

    it('does not change state on GET', async () => {
      const run = await engine.enqueue('approval', {})

      const response = await dashboard({ engine })(get(`/api/runs/${run.id}/cancel`))

      expect(response.status).toBe(405)
      expect((await storage.getRun(run.id))?.status).toBe('pending')
    })
  })

  describe('live updates', () => {
    it('is unavailable without an engine', async () => {
      expect((await dashboard()(get('/api/live'))).status).toBe(404)
    })

    it('drops notices a stalled client cannot take, and sends one resync when it catches up', async () => {
      const response = await dashboard({ engine })(get('/api/live'))
      const reader = response.body?.getReader()
      const decoder = new TextDecoder()
      if (!reader) throw new Error('expected a streaming body')
      await reader.read()

      // Many changes while the client reads nothing.
      for (const id of ['a', 'b', 'c']) {
        await engine.enqueue('order', { id })
      }
      await engine.tick()

      const first = decoder.decode((await reader.read()).value)
      const second = decoder.decode((await reader.read()).value)
      expect(first).toMatch(/^data: \{"type":"runStart"/)
      expect(second).toBe(`data: ${JSON.stringify({ type: 'resync' })}\n\n`)
      await reader.cancel()
    })

    it('streams a notice naming each run that changes', async () => {
      const aborted = new AbortController()
      const response = await dashboard({ engine })(get('/api/live', { signal: aborted.signal }))
      const reader = response.body?.getReader()
      const decoder = new TextDecoder()
      if (!reader) throw new Error('expected a streaming body')

      expect(response.headers.get('content-type')).toBe('text/event-stream; charset=utf-8')
      expect(decoder.decode((await reader.read()).value)).toBe(': connected\n\n')

      const run = await engine.enqueue('order', { id: 'a' })
      await engine.tick()
      const notice = decoder.decode((await reader.read()).value)

      expect(notice).toBe(`data: ${JSON.stringify({ type: 'runStart', runId: run.id })}\n\n`)
      aborted.abort()
      await reader.cancel()
    })
  })
})

describe('toNodeHandler', () => {
  it('serves the dashboard over node:http', async () => {
    const storage = new MemoryStorage()
    await storage.initialize()
    const server = createServer(toNodeHandler(createDashboard({ storage, authorize: () => true })))
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const address = server.address()
    if (address === null || typeof address === 'string') throw new Error('expected a TCP address')
    const { port } = address

    try {
      const page = await fetch(`http://127.0.0.1:${port}/`)
      const overview = await fetch(`http://127.0.0.1:${port}/api/overview`)

      expect(page.status).toBe(200)
      expect(page.headers.get('content-security-policy')).toContain("default-src 'none'")
      expect(await overview.json()).toMatchObject({ actions: false })
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })

  it('ends the response, rather than crashing, when a body fails mid-stream', async () => {
    const failing = async () => new Response(new ReadableStream<Uint8Array>({
      pull(controller) {
        controller.enqueue(new TextEncoder().encode('partial'))
        controller.error(new Error('body failed'))
      },
    }))
    const server = createServer(toNodeHandler(failing))
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const address = server.address()
    if (address === null || typeof address === 'string') throw new Error('expected a TCP address')

    try {
      const response = await fetch(`http://127.0.0.1:${address.port}/`)
      await expect(response.text()).rejects.toBeInstanceOf(TypeError)
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })

  it('answers 500 without detail when the handler throws', async () => {
    const server = createServer(toNodeHandler(async () => {
      throw new Error('secret internals')
    }))
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
    const address = server.address()
    if (address === null || typeof address === 'string') throw new Error('expected a TCP address')

    try {
      const response = await fetch(`http://127.0.0.1:${address.port}/`)
      expect(response.status).toBe(500)
      expect(await response.text()).toBe('Internal error')
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()))
    }
  })
})

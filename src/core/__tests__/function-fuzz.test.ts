/**
 * Randomised tests of workflows written as a function.
 *
 * Each case generates a workflow from a seed — steps that succeed, fail once
 * and retry, or always fail inside a `try`; sleeps; waits for events, with and
 * without timeouts; groups run with `Promise.all` and `Promise.race`; and
 * awaits of ordinary promises between them — and drives one run of it to the
 * end through engines that crash: an engine dies at a random storage call,
 * every call it makes from then on hanging, as a process that stopped would
 * leave them, and a fresh engine reclaims the run once its lease has expired.
 * Events are sent at random moments and time moves forward between engines.
 *
 * Whatever the crashes and timings, every execution of the function must see
 * what earlier ones saw — each operation's result, each race's winner, each
 * caught failure — no step's handler may run again once its result has been
 * seen, and the run must complete, leaving nothing looking under way.
 *
 * `REFLOW_FUZZ_RUNS` sets how many seeds to try (default 150), and
 * `REFLOW_FUZZ_SEED` runs one seed, to look into a failure.
 */
import { afterEach, describe, expect, it, vi } from 'vitest'
import { z } from 'zod'
import { createEngine, createWorkflow, StepFailedError, type StorageAdapter, type WorkflowSteps } from '../../index'
import { MemoryStorage } from '../../storage/memory'
import { SQLiteStorage } from '../../storage/sqlite-node'

const HOUR = 3_600_000

/** Storage calls an engine may die at, where a crash splits two writes that belong together. */
const CRASH_POINTS = ['saveStepResult', 'consumeEvent', 'sleepRun', 'waitRun', 'completeRun', 'completeRun', 'failRun', 'getStepResults']
const LEASE_MS = 60_000

/** A small, fast, seeded random number generator (mulberry32). */
function random(seed: number): () => number {
  let state = seed >>> 0
  return () => {
    state = (state + 0x6d2b79f5) >>> 0
    let t = state
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4_294_967_296
  }
}

/** One operation in a generated workflow. */
type Op =
  | { readonly t: 'run'; readonly name: string; readonly latency: number; readonly outcome: 'ok' | 'flaky' | 'fail' }
  | { readonly t: 'sleep'; readonly name: string; readonly minutes: number }
  | { readonly t: 'wait'; readonly name: string; readonly event: EventName; readonly timeoutMinutes: number | null }
  | { readonly t: 'all'; readonly items: readonly Op[] }
  | {
      readonly t: 'race'
      readonly id: string
      readonly items: readonly Op[]
      /** Per item: ms to wait before starting the next, and extra `.then`s chained on it. */
      readonly stagger: readonly number[]
      readonly chain: readonly number[]
    }
  | { readonly t: 'try'; readonly id: string; readonly op: Op }
  | { readonly t: 'gap'; readonly ms: number }
  /** Started and never awaited: it must neither hold the run nor be left looking under way. */
  | { readonly t: 'detach'; readonly name: string; readonly sleep: boolean }

const EVENTS = ['e0', 'e1', 'e2', 'e3'] as const
type EventName = (typeof EVENTS)[number]

/** Generate a workflow's operations from `next`, deterministically. */
function generate(next: () => number): Op[] {
  let counter = 0
  const id = (prefix: string) => `${prefix}${counter++}`
  const pick = <T,>(items: readonly T[]): T => items[Math.floor(next() * items.length)] as T

  const leaf = (allowFailure: boolean): Op => {
    const roll = next()
    if (roll < 0.5) {
      const outcome = allowFailure ? pick(['ok', 'ok', 'flaky', 'fail'] as const) : pick(['ok', 'ok', 'flaky'] as const)
      return { t: 'run', name: id('s'), latency: Math.floor(next() * 4), outcome }
    }
    if (roll < 0.75) return { t: 'sleep', name: id('z'), minutes: 1 + Math.floor(next() * 60) }
    return { t: 'wait', name: id('w'), event: pick(EVENTS), timeoutMinutes: next() < 0.5 ? 1 + Math.floor(next() * 90) : null }
  }

  const op = (depth: number): Op => {
    const roll = next()
    if (depth < 2 && roll < 0.15) return { t: 'all', items: Array.from({ length: 2 + Math.floor(next() * 3) }, () => op(depth + 1)) }
    // A race between operations only: a race against other promises is not replayed deterministically.
    if (depth < 2 && roll < 0.27) {
      const items = Array.from({ length: 2 + Math.floor(next() * 2) }, () => leaf(false))
      return {
        t: 'race',
        id: id('r'),
        items,
        // Chained on to different depths, and with the first sometimes
        // started a moment before the rest, as real code does: neither may
        // change the winner on replay. (Awaiting other work after starting
        // two or more of them is outside what replay can promise: which had
        // settled by the race then depends on the clock.)
        stagger: items.map((_, index) => (index === 0 && next() < 0.4 ? 1 + Math.floor(next() * 5) : 0)),
        chain: items.map(() => Math.floor(next() * 4)),
      }
    }
    if (roll < 0.37) return { t: 'try', id: id('c'), op: leaf(true) }
    return leaf(false)
  }

  const ops: Op[] = []
  for (let index = 0, length = 3 + Math.floor(next() * 6); index < length; index++) {
    ops.push(op(0))
    if (next() < 0.2) ops.push({ t: 'gap', ms: Math.floor(next() * 3) })
    if (next() < 0.1) ops.push({ t: 'detach', name: id('d'), sleep: next() < 0.7 })
  }
  return ops
}

const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

/** What every execution of one run has seen, and the rules it broke. */
interface Observer {
  /** Each key's value, as the first execution to see it saw it. */
  readonly seen: Map<string, string>
  readonly violations: string[]
  /** The engine now driving the run; handlers of engines that died are not checked. */
  generation: number
  /** How much of what the cases should exercise they did, summed over seeds. */
  readonly coverage: Record<'executions' | 'deaths' | 'races' | 'caught' | 'timeouts' | 'events', number>
}

/** Record `key` as seeing `value`, which must match what earlier executions saw. */
function observe(observer: Observer, key: string, value: unknown): void {
  const encoded = JSON.stringify(value) ?? 'undefined'
  const before = observer.seen.get(key)
  if (before === undefined) {
    observer.seen.set(key, encoded)
  } else if (before !== encoded) {
    observer.violations.push(`${key} was ${before}, then ${encoded}`)
  }
}

/** Run one generated operation with `step`, recording what the function sees. */
async function execute(
  op: Op,
  step: WorkflowSteps<Record<EventName, { n: number }>>,
  observer: Observer,
  generation: number,
): Promise<unknown> {
  switch (op.t) {
    case 'run': {
      const value = await step.run(op.name, {
        retry: { maxAttempts: op.outcome === 'flaky' ? 2 : 1, initialDelay: 0 },
        handler: async ({ attempt }) => {
          if (generation === observer.generation && observer.seen.has(op.name)) {
            observer.violations.push(`${op.name} ran again after its result was seen`)
          }
          await delay(op.latency)
          if (op.outcome === 'fail' || (op.outcome === 'flaky' && attempt === 1)) throw new Error(`${op.name} failed`)
          return `${op.name} done`
        },
      })
      observe(observer, op.name, value)
      return value
    }
    case 'sleep':
      await step.sleep(op.name, `${op.minutes}m`)
      observe(observer, op.name, 'slept')
      return 'slept'
    case 'wait': {
      const payload = op.timeoutMinutes === null
        ? await step.waitForEvent(op.name, { event: op.event })
        : await step.waitForEvent(op.name, { event: op.event, timeout: `${op.timeoutMinutes}m` })
      if (payload === null) observer.coverage.timeouts++
      else observer.coverage.events++
      observe(observer, op.name, payload)
      return payload
    }
    case 'all':
      return await Promise.all(op.items.map((item) => execute(item, step, observer, generation)))
    case 'race': {
      observer.coverage.races++
      const started: Promise<unknown>[] = []
      for (const [index, item] of op.items.entries()) {
        let promise = execute(item, step, observer, generation)
        for (let link = 0; link < (op.chain[index] ?? 0); link++) promise = promise.then((value) => value)
        started.push(promise)
        const wait = op.stagger[index] ?? 0
        if (wait > 0) await delay(wait)
      }
      const winner = await Promise.race(started)
      observe(observer, op.id, winner)
      return winner
    }
    case 'try':
      try {
        const value = await execute(op.op, step, observer, generation)
        observe(observer, op.id, 'ok')
        return value
      } catch (error) {
        if (!(error instanceof StepFailedError)) throw error
        observer.coverage.caught++
        observe(observer, op.id, `caught ${error.message}`)
        return null
      }
    case 'gap':
      await delay(op.ms)
      return null
    case 'detach':
      // Deliberately not awaited, nor chained on.
      if (op.sleep) void step.sleep(op.name, '3h')
      else void step.run(op.name, async () => 'detached')
      return null
  }
}

/**
 * `storage` as one engine sees it, dying at call `dieAt`: from then on every
 * call hangs, as calls from a process that stopped never complete.
 */
function mortal(
  storage: StorageAdapter,
  death: { readonly atCall: number } | { readonly atMethod: string; readonly occurrence: number } | null,
): { readonly storage: StorageAdapter; readonly died: Promise<void> } {
  let calls = 0
  let ofMethod = 0
  let die: () => void = () => {}
  const died = new Promise<void>((resolve) => { die = resolve })
  const wrapped = new Proxy(storage, {
    get(target, property, receiver) {
      const value: unknown = Reflect.get(target, property, receiver)
      if (typeof value !== 'function') return value
      return (...args: unknown[]) => {
        calls++
        if (death && 'atMethod' in death && property === death.atMethod) ofMethod++
        const dies = death !== null && ('atCall' in death ? calls >= death.atCall : property === death.atMethod && ofMethod >= death.occurrence)
        if (dies) {
          die()
          return new Promise(() => {})
        }
        return Reflect.apply(value, target, args)
      }
    },
  })
  return { storage: wrapped, died }
}

/** Drive one generated workflow's run to its end, returning what went wrong. */
async function fuzz(seed: number, makeStorage: () => StorageAdapter, coverage: Observer['coverage']): Promise<string[]> {
  const next = random(seed)
  const ops = generate(next)
  const observer: Observer = { seen: new Map(), violations: [], generation: 0, coverage }
  const events = { e0: z.object({ n: z.number() }), e1: z.object({ n: z.number() }), e2: z.object({ n: z.number() }), e3: z.object({ n: z.number() }) }
  const wf = createWorkflow({ name: 'fuzzed', input: z.object({}), events }, async ({ step }) => {
    const generation = observer.generation
    coverage.executions++
    for (const op of ops) await execute(op, step, observer, generation)
    return 'done'
  })

  const storage = makeStorage()
  await storage.initialize()
  const api = createEngine({ storage, workflows: [wf], runLeaseDurationMs: LEASE_MS })
  const run = await api.enqueue('fuzzed', {})
  let sent = 0
  const sendSome = async (all: boolean) => {
    for (const event of EVENTS) {
      if (all || next() < 0.3) await api.sendEvent(run.id, event, { n: sent++ })
    }
  }

  for (let round = 0; round < 80; round++) {
    observer.generation++
    // Most engines live; some die at a random point; the last rounds never die,
    // so every run can reach its end.
    const roll = next()
    const death = round >= 50 || roll >= 0.5
      ? null
      : roll < 0.25
        ? { atCall: 1 + Math.floor(next() * 40) }
        : { atMethod: CRASH_POINTS[Math.floor(next() * CRASH_POINTS.length)] ?? 'saveStepResult', occurrence: 1 + Math.floor(next() * 3) }
    const { storage: seen, died } = mortal(storage, death)
    // Some engines' hooks are slow, so a step's result is handed over well
    // after it is saved — which must not change the order results arrive in.
    const hookDelays = next() < 0.4 ? Array.from({ length: 64 }, () => Math.floor(next() * 6)) : null
    let hookCall = 0
    const engine = createEngine({
      storage: seen,
      workflows: [wf],
      runLeaseDurationMs: LEASE_MS,
      hooks: hookDelays ? { onStepComplete: () => delay(hookDelays[hookCall++ % hookDelays.length] ?? 0) } : undefined,
    })
    if (next() < 0.5) await sendSome(round >= 50)
    // Sometimes events arrive through the engine running the run, mid-tick.
    const midTick = next() < 0.3 ? Math.floor(next() * 8) : null
    const ticking = engine.tick()
    if (midTick !== null) {
      void delay(midTick).then(async () => {
        for (const event of EVENTS) await engine.sendEvent(run.id, event, { n: sent++ }).catch(() => false)
      })
    }
    const outcome = await Promise.race([
      ticking.then(() => 'ticked' as const),
      died.then(() => 'died' as const),
      delay(5_000).then(() => 'stuck' as const),
    ])
    if (outcome === 'stuck') return [`seed ${seed}: an engine did not finish a tick: ${JSON.stringify(ops)}`]
    // Let what a dead engine still had running reach the storage call that hangs.
    if (outcome === 'died') {
      coverage.deaths++
      await delay(20)
    }
    if (next() < 0.5) await sendSome(round >= 50)

    const current = await storage.getRun(run.id)
    if (current?.status === 'completed' || current?.status === 'failed') break
    // On, past every sleep, timeout, and lease.
    vi.setSystemTime(Date.now() + 2 * HOUR)
  }

  const finished = await storage.getRun(run.id)
  const problems = observer.violations.map((violation) => `seed ${seed}: ${violation}`)
  if (finished?.status !== 'completed') {
    problems.push(`seed ${seed}: run ended ${finished?.status ?? 'missing'} ${JSON.stringify(finished?.error)}: ${JSON.stringify(ops)}`)
  }
  const unfinished = (await storage.getStepResults(run.id)).filter((step) => ['sleeping', 'waiting', 'retrying', 'running', 'pending'].includes(step.status))
  if (unfinished.length > 0) {
    problems.push(`seed ${seed}: left ${unfinished.map((step) => `${step.name}:${step.status}`).join(', ')}`)
  }
  return problems
}

const runs = Number(process.env['REFLOW_FUZZ_RUNS'] ?? 150)
const only = process.env['REFLOW_FUZZ_SEED']
const seeds = only === undefined ? Array.from({ length: runs }, (_, index) => index + 1) : [Number(only)]

describe('workflows written as a function, fuzzed', () => {
  afterEach(() => {
    vi.useRealTimers()
  })

  const coverage = (): Observer['coverage'] => ({ executions: 0, deaths: 0, races: 0, caught: 0, timeouts: 0, events: 0 })

  it(`replays consistently through crashes, on MemoryStorage (${seeds.length} seeds)`, async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const problems: string[] = []
    const covered = coverage()
    for (const seed of seeds) problems.push(...await fuzz(seed, () => new MemoryStorage(), covered))
    expect(problems).toStrictEqual([])
    // The cases did what they are for: runs crashed and were replayed, and
    // races, caught failures, timeouts, and events were all seen.
    if (only === undefined) {
      expect(covered.executions).toBeGreaterThan(seeds.length * 2)
      for (const key of ['deaths', 'races', 'caught', 'timeouts', 'events'] as const) {
        expect([key, covered[key] > 0]).toStrictEqual([key, true])
      }
    }
  }, 600_000)

  it('replays consistently through crashes, on SQLite (a sample of seeds)', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const problems: string[] = []
    for (const seed of seeds.filter((seed) => seed % 5 === 0)) problems.push(...await fuzz(seed, () => new SQLiteStorage(':memory:'), coverage()))
    expect(problems).toStrictEqual([])
  }, 600_000)
})

import type {
  StepResult,
  StorageAdapter,
  WorkflowRun,
  WorkflowSchedule,
} from '../core/types'

/**
 * The behavioural contract every `StorageAdapter` must satisfy, as a list of
 * runtime-agnostic cases.
 *
 * Deliberately free of any test-framework import. The Bun adapter cannot load
 * under the Node-based Vitest run, so without this the only adapter backing a
 * documented "works on Bun" claim would be the least tested one — which is how
 * `bun:sqlite` reporting affected-row counts differently went unnoticed until it
 * had broken every heartbeat and claim guard on that adapter.
 *
 * Cases assert through plain throws so the same array can be driven by Vitest
 * for the Node-loadable adapters and by a Bun script for `sqlite-bun`.
 * Adapter-specific concerns — WAL pragmas, column migrations, driver error
 * codes — stay in each adapter's own suite; this covers only what they must
 * agree on.
 */

/** A single contract case. Receives a freshly initialized, empty storage. */
export interface ConformanceCase {
  readonly name: string
  run(storage: StorageAdapter): Promise<void>
}

class ConformanceFailure extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'ConformanceFailure'
  }
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new ConformanceFailure(message)
  }
}

function assertEqual(actual: unknown, expected: unknown, message: string): void {
  const a = JSON.stringify(actual)
  const b = JSON.stringify(expected)
  if (a !== b) {
    throw new ConformanceFailure(`${message} — expected ${b}, got ${a}`)
  }
}

function makeRun(overrides: Partial<WorkflowRun> = {}): WorkflowRun {
  const now = Date.now()
  return {
    id: 'run_1',
    workflow: 'test',
    input: {},
    idempotencyKey: null,
    status: 'pending',
    createdAt: now,
    updatedAt: now,
    startedAt: null,
    finishedAt: null,
    output: undefined,
    error: null,
    wakeAt: null,
    waitingFor: null,
    ...overrides,
  }
}

function makeStep(overrides: Partial<StepResult> = {}): StepResult {
  const now = Date.now()
  return {
    id: 'step_1',
    runId: 'run_1',
    name: 'a',
    status: 'completed',
    output: { ok: true },
    error: null,
    attempts: 1,
    createdAt: now,
    updatedAt: now,
    startedAt: now,
    history: [],
    ...overrides,
  }
}

function makeSchedule(overrides: Partial<WorkflowSchedule> = {}): WorkflowSchedule {
  const now = Date.now()
  return {
    key: 'nightly',
    workflow: 'test',
    input: { olderThanDays: 30 },
    recurrence: { kind: 'interval', intervalMs: 60_000 },
    nextRunAt: now + 60_000,
    createdAt: now,
    updatedAt: now,
    ...overrides,
  }
}

/** Claim a run, asserting that it succeeded. */
async function claim(storage: StorageAdapter, names: readonly string[] = ['test']) {
  const claimed = await storage.claimNextRun(names)
  assert(claimed, 'expected a claimable run')
  return claimed
}

export const storageConformanceCases: readonly ConformanceCase[] = [
  // -------------------------------------------------------------------------
  // Runs and idempotency
  // -------------------------------------------------------------------------
  {
    name: 'createRun stores a run and reports it as created',
    async run(storage) {
      const { run, created } = await storage.createRun(makeRun())
      assert(created, 'a fresh run must report created: true')
      assertEqual(run.id, 'run_1', 'stored run id')
      assertEqual((await storage.getRun('run_1'))?.status, 'pending', 'stored status')
    },
  },
  {
    name: 'getRun returns null for an unknown id',
    async run(storage) {
      assertEqual(await storage.getRun('missing'), null, 'unknown run')
    },
  },
  {
    name: 'createRun round-trips input through serialization',
    async run(storage) {
      const input = { s: 'x', n: 1, b: true, nil: null, nested: { list: [1, 2] } }
      await storage.createRun(makeRun({ input }))
      assertEqual((await storage.getRun('run_1'))?.input, input, 'round-tripped input')
    },
  },
  {
    name: 'createRun returns the existing run for a reused idempotency key',
    async run(storage) {
      await storage.createRun(makeRun({ id: 'a', idempotencyKey: 'k' }))
      const second = await storage.createRun(makeRun({ id: 'b', idempotencyKey: 'k' }))

      assert(!second.created, 'a reused key must report created: false')
      assertEqual(second.run.id, 'a', 'must return the original run')
    },
  },
  {
    name: 'idempotency keys are scoped per workflow',
    async run(storage) {
      await storage.createRun(makeRun({ id: 'a', workflow: 'one', idempotencyKey: 'k' }))
      const second = await storage.createRun(
        makeRun({ id: 'b', workflow: 'two', idempotencyKey: 'k' }),
      )

      assert(second.created, 'the same key under a different workflow is a distinct run')
    },
  },

  // -------------------------------------------------------------------------
  // Claiming and leases
  // -------------------------------------------------------------------------
  {
    name: 'claimNextRun marks the run running and issues a lease',
    async run(storage) {
      await storage.createRun(makeRun())
      const claimed = await claim(storage)

      assertEqual(claimed.status, 'running', 'claimed status')
      assert(typeof claimed.leaseId === 'string' && claimed.leaseId.length > 0, 'lease id issued')
    },
  },
  {
    name: 'a claimed run cannot be claimed again',
    async run(storage) {
      // Depends on reading the UPDATE's affected-row count — the exact thing
      // that silently differed on bun:sqlite.
      await storage.createRun(makeRun())
      await claim(storage)

      assertEqual(await storage.claimNextRun(['test']), null, 'double claim must be refused')
    },
  },
  {
    name: 'claimNextRun only considers the named workflows',
    async run(storage) {
      await storage.createRun(makeRun({ workflow: 'other' }))

      assertEqual(await storage.claimNextRun(['test']), null, 'must not claim another workflow')
      assert(await storage.claimNextRun(['other']), 'must claim its own workflow')
    },
  },
  {
    name: 'claimNextRun returns null for an empty workflow list',
    async run(storage) {
      await storage.createRun(makeRun())
      assertEqual(await storage.claimNextRun([]), null, 'no names means nothing claimable')
    },
  },
  {
    name: 'claimNextRun reclaims a stale running run',
    async run(storage) {
      await storage.createRun(makeRun({ status: 'running', updatedAt: 1 }))
      const reclaimed = await storage.claimNextRun(['test'], 10)

      assert(reclaimed, 'a run older than staleBefore must be reclaimable')
      assertEqual(reclaimed.id, 'run_1', 'reclaimed run id')
    },
  },
  {
    name: 'claimNextRun claims pending runs in the order they became pending',
    async run(storage) {
      const now = Date.now()
      await storage.createRun(makeRun({ id: 'newer', createdAt: now - 1_000, updatedAt: now - 1_000 }))
      await storage.createRun(makeRun({ id: 'older', createdAt: now - 2_000, updatedAt: now - 2_000 }))

      assertEqual((await claim(storage)).id, 'older', 'first claim')
      assertEqual((await claim(storage)).id, 'newer', 'second claim')
    },
  },
  {
    name: 'claimNextRun does not starve a woken sleeper behind newer pending runs',
    async run(storage) {
      await storage.createRun(makeRun({ id: 'sleeper', createdAt: 1, updatedAt: 1 }))
      const sleeper = await claim(storage)
      await storage.sleepRun('sleeper', sleeper.leaseId, Date.now() - 60_000)
      await storage.createRun(makeRun({ id: 'fresh' }))

      assertEqual(
        (await claim(storage)).id,
        'sleeper',
        'a run due a minute ago must be claimed before one enqueued just now',
      )
    },
  },
  {
    name: 'claimNextRun orders a woken sleeper by its wake time, not ahead of older pending runs',
    async run(storage) {
      const now = Date.now()
      await storage.createRun(makeRun({ id: 'sleeper' }))
      const sleeper = await claim(storage)
      await storage.sleepRun('sleeper', sleeper.leaseId, now - 1_000)
      await storage.createRun(makeRun({ id: 'waiting-longer', createdAt: now - 60_000, updatedAt: now - 60_000 }))

      assertEqual((await claim(storage)).id, 'waiting-longer', 'first claim')
      assertEqual((await claim(storage)).id, 'sleeper', 'second claim')
    },
  },
  {
    name: 'claimNextRun does not starve a stale run behind newer pending runs',
    async run(storage) {
      const now = Date.now()
      // Last heartbeat two minutes ago with a 30s lease: due for 90s.
      await storage.createRun(makeRun({ id: 'stale', status: 'running', updatedAt: now - 120_000 }))
      await storage.createRun(makeRun({ id: 'fresh' }))

      const reclaimed = await storage.claimNextRun(['test'], now - 30_000)
      assertEqual(reclaimed?.id, 'stale', 'a run whose lease expired 90s ago must be claimed first')
    },
  },
  {
    name: 'heartbeatRun succeeds with the held lease and fails once it is lost',
    async run(storage) {
      await storage.createRun(makeRun())
      const claimed = await claim(storage)

      assert(await storage.heartbeatRun('run_1', claimed.leaseId), 'held lease must heartbeat')
      assert(
        !(await storage.heartbeatRun('run_1', 'not-the-lease')),
        'a foreign lease must not heartbeat',
      )
    },
  },
  {
    name: 'claimNextRun records when a run first started and keeps it across reclaims',
    async run(storage) {
      const now = Date.now()
      await storage.createRun(makeRun({ status: 'running', startedAt: 5, updatedAt: now - 120_000 }))

      const reclaimed = await storage.claimNextRun(['test'], now - 30_000)

      assertEqual(reclaimed?.startedAt, 5, 'claimed run startedAt')
      assertEqual((await storage.getRun('run_1'))?.startedAt, 5, 'stored startedAt')
    },
  },
  {
    name: 'claimNextRun sets startedAt on a run that has never started',
    async run(storage) {
      const before = Date.now()
      await storage.createRun(makeRun())
      const claimed = await claim(storage)

      assert(claimed.startedAt !== null && claimed.startedAt >= before, 'startedAt must be set on first claim')
      assertEqual((await storage.getRun('run_1'))?.startedAt, claimed.startedAt, 'stored startedAt')
    },
  },
  {
    name: 'completeRun records the output and finish time under the held lease only',
    async run(storage) {
      await storage.createRun(makeRun())
      const claimed = await claim(storage)

      assert(!(await storage.completeRun('run_1', 'not-the-lease', { n: 1 })), 'a foreign lease must not complete')
      assert(await storage.completeRun('run_1', claimed.leaseId, { n: 1 }), 'the held lease must complete')

      const run = await storage.getRun('run_1')
      assertEqual(run?.status, 'completed', 'status')
      assertEqual(run?.output, { n: 1 }, 'output')
      assert(run?.finishedAt !== null && run?.finishedAt !== undefined, 'finishedAt must be set')
      assert(!(await storage.heartbeatRun('run_1', claimed.leaseId)), 'completing must release the lease')
    },
  },
  {
    name: 'completeRun distinguishes a null output from no output',
    async run(storage) {
      await storage.createRun(makeRun({ id: 'nulled' }))
      await storage.createRun(makeRun({ id: 'pending' }))
      const claimed = await claim(storage)
      await storage.completeRun(claimed.id, claimed.leaseId, null)

      assertEqual((await storage.getRun('nulled'))?.output, null, 'a completed null output')
      assert((await storage.getRun('pending'))?.output === undefined, 'an unfinished run has no output')
    },
  },
  {
    name: 'failRun records the error and finish time under the held lease only',
    async run(storage) {
      await storage.createRun(makeRun())
      const claimed = await claim(storage)
      const error = { stepName: 'charge', message: 'card declined', code: 'E_DECLINED' }

      assert(!(await storage.failRun('run_1', 'not-the-lease', error)), 'a foreign lease must not fail the run')
      assert(await storage.failRun('run_1', claimed.leaseId, error), 'the held lease must fail the run')

      const run = await storage.getRun('run_1')
      assertEqual(run?.status, 'failed', 'status')
      assertEqual(run?.error, error, 'error')
      assert(run?.finishedAt !== null && run?.finishedAt !== undefined, 'finishedAt must be set')
    },
  },
  {
    name: 'cancelRun cancels a live run, held or not, but never a finished one',
    async run(storage) {
      await storage.createRun(makeRun({ id: 'pending' }))
      await storage.createRun(makeRun({ id: 'done', status: 'completed' }))

      assert(await storage.cancelRun('pending'), 'a pending run must cancel')
      assertEqual((await storage.getRun('pending'))?.status, 'cancelled', 'status')
      assert((await storage.getRun('pending'))?.finishedAt !== null, 'finishedAt must be set')
      assert(!(await storage.cancelRun('pending')), 'an already-cancelled run must not cancel again')
      assert(!(await storage.cancelRun('done')), 'a completed run must not cancel')
      assertEqual((await storage.getRun('done'))?.status, 'completed', 'completed run untouched')
      assert(!(await storage.cancelRun('missing')), 'a missing run must not cancel')
    },
  },
  {
    name: 'cancelRun releases the lease of a running run',
    async run(storage) {
      await storage.createRun(makeRun())
      const claimed = await claim(storage)

      assert(await storage.cancelRun('run_1'), 'a running run must cancel')
      assert(!(await storage.heartbeatRun('run_1', claimed.leaseId)), 'the lease must be released')
      assert(!(await storage.completeRun('run_1', claimed.leaseId, null)), 'the former holder must not complete it')
    },
  },
  {
    name: 'waitRun records the awaited event and deadline, and claiming clears them',
    async run(storage) {
      await storage.createRun(makeRun())
      const claimed = await claim(storage)
      await storage.waitRun('run_1', claimed.leaseId, 'approved', 1_000)

      const waiting = await storage.getRun('run_1')
      assertEqual(waiting?.waitingFor, 'approved', 'waitingFor')
      assertEqual(waiting?.wakeAt, 1_000, 'wakeAt')

      const woken = await claim(storage)
      assertEqual([woken.waitingFor, woken.wakeAt], [null, null], 'claimed run')
      const stored = await storage.getRun('run_1')
      assertEqual([stored?.waitingFor, stored?.wakeAt], [null, null], 'stored after claim')
    },
  },
  {
    name: 'sleepRun records the wake time',
    async run(storage) {
      await storage.createRun(makeRun())
      const claimed = await claim(storage)
      await storage.sleepRun('run_1', claimed.leaseId, Date.now() + 60_000)

      const run = await storage.getRun('run_1')
      assert(run?.wakeAt !== null && run?.wakeAt !== undefined && run.wakeAt > Date.now(), 'wakeAt')
      assertEqual(run?.waitingFor, null, 'a sleep awaits no event')
    },
  },
  {
    name: 'requeueRun clears the finish time and error but keeps the start time',
    async run(storage) {
      await storage.createRun(makeRun())
      const claimed = await claim(storage)
      await storage.failRun('run_1', claimed.leaseId, { stepName: 'a', message: 'boom', code: null })

      assert(await storage.requeueRun('run_1'), 'a failed run must requeue')

      const run = await storage.getRun('run_1')
      assertEqual([run?.finishedAt, run?.error], [null, null], 'finish fields')
      assertEqual(run?.startedAt, claimed.startedAt, 'startedAt')
    },
  },
  {
    name: 'requeueRun discards a step that was waiting out a retry, so it starts over',
    async run(storage) {
      await storage.createRun(makeRun({ status: 'cancelled' }))
      await storage.saveStepResult(makeStep({ id: 'done', name: 'a', status: 'completed' }))
      await storage.saveStepResult(makeStep({ id: 'backing-off', name: 'b', status: 'retrying', attempts: 1, output: 0 }))

      assert(await storage.requeueRun('run_1'), 'a cancelled run must requeue')

      assertEqual(
        (await storage.getStepResults('run_1')).map((step) => step.name),
        ['a'],
        'remaining steps',
      )
    },
  },
  {
    name: 'countRuns counts every status, including those with no runs',
    async run(storage) {
      await storage.createRun(makeRun({ id: 'a' }))
      await storage.createRun(makeRun({ id: 'b' }))
      await storage.createRun(makeRun({ id: 'c', status: 'failed' }))
      await storage.createRun(makeRun({ id: 'd', workflow: 'other' }))

      assertEqual(
        await storage.countRuns(),
        { pending: 3, running: 0, sleeping: 0, waiting: 0, completed: 0, failed: 1, cancelled: 0 },
        'all workflows',
      )
      assertEqual(
        await storage.countRuns({ workflow: 'other' }),
        { pending: 1, running: 0, sleeping: 0, waiting: 0, completed: 0, failed: 0, cancelled: 0 },
        'one workflow',
      )
    },
  },
  {
    name: 'deleteRuns removes finished runs older than the cutoff, with their steps and events',
    async run(storage) {
      await storage.createRun(makeRun({ id: 'old', status: 'completed', finishedAt: 100 }))
      await storage.createRun(makeRun({ id: 'recent', status: 'completed', finishedAt: 900 }))
      await storage.createRun(makeRun({ id: 'live', status: 'pending' }))
      await storage.saveStepResult(makeStep({ id: 's', runId: 'old' }))
      await storage.deliverEvent('old', 'e', null)

      const deleted = await storage.deleteRuns({ finishedBefore: 500, statuses: ['completed'], limit: 10 })

      assertEqual(deleted, 1, 'deleted count')
      assertEqual(await storage.getRun('old'), null, 'old run removed')
      assertEqual(await storage.getStepResults('old'), [], 'its steps removed')
      assertEqual(await storage.takeEvent('old', 'e'), null, 'its events removed')
      assert(await storage.getRun('recent'), 'a run finished after the cutoff stays')
      assert(await storage.getRun('live'), 'a live run stays')
    },
  },
  {
    name: 'deleteRuns honours the status filter and the limit, oldest first',
    async run(storage) {
      await storage.createRun(makeRun({ id: 'failed', status: 'failed', finishedAt: 100 }))
      await storage.createRun(makeRun({ id: 'c2', status: 'completed', finishedAt: 300 }))
      await storage.createRun(makeRun({ id: 'c1', status: 'completed', finishedAt: 200 }))

      assertEqual(await storage.deleteRuns({ finishedBefore: 1_000, statuses: ['completed'], limit: 1 }), 1, 'limited')
      assertEqual(await storage.getRun('c1'), null, 'the oldest completed run goes first')
      assert(await storage.getRun('c2'), 'the limit holds back the next')
      assert(await storage.getRun('failed'), 'an unlisted status stays')
    },
  },
  {
    name: 'saveStepResult round-trips the step start time',
    async run(storage) {
      await storage.createRun(makeRun())
      await storage.saveStepResult(makeStep({ startedAt: 42 }))

      assertEqual((await storage.getStepResults('run_1'))[0]?.startedAt, 42, 'startedAt')
    },
  },
  {
    name: 'saveStepResult round-trips the attempt history, in order',
    async run(storage) {
      const history = [
        { startedAt: 10, finishedAt: 12, error: 'gateway timed out' },
        { startedAt: 1_012, finishedAt: 1_015, error: null },
      ]
      await storage.createRun(makeRun())
      await storage.saveStepResult(makeStep({ attempts: 2, history }))

      const [step] = await storage.getStepResults('run_1')
      assertEqual(step?.history, history, 'history')

      // Mutating what was read must not reach the stored record.
      step?.history.push({ startedAt: 0, finishedAt: 0, error: null })
      assertEqual((await storage.getStepResults('run_1'))[0]?.history, history, 'history after mutating a read')
    },
  },

  {
    name: 'listRuns returns runs newest first',
    async run(storage) {
      await storage.createRun(makeRun({ id: 'a', createdAt: 1 }))
      await storage.createRun(makeRun({ id: 'b', createdAt: 2 }))
      await storage.createRun(makeRun({ id: 'c', createdAt: 3 }))

      assertEqual(
        (await storage.listRuns()).map((run) => run.id),
        ['c', 'b', 'a'],
        'reverse-chronological order',
      )
    },
  },
  {
    name: 'listRuns filters by status and workflow',
    async run(storage) {
      await storage.createRun(makeRun({ id: 'a', status: 'failed' }))
      await storage.createRun(makeRun({ id: 'b', workflow: 'other' }))

      assertEqual((await storage.listRuns({ status: 'failed' })).map((r) => r.id), ['a'], 'by status')
      assertEqual((await storage.listRuns({ workflow: 'other' })).map((r) => r.id), ['b'], 'by workflow')
    },
  },
  {
    name: 'listRuns paginates exactly when runs share a createdAt',
    async run(storage) {
      // The tie-break is the point: ordering by createdAt alone would make a
      // cursor either skip or repeat rows that landed in the same millisecond.
      for (const id of ['a', 'b', 'c']) {
        await storage.createRun(makeRun({ id, createdAt: 1 }))
      }

      const first = await storage.listRuns({ limit: 2 })
      assertEqual(first.length, 2, 'first page size')

      const cursor = first[first.length - 1]
      assert(cursor, 'expected a cursor row')
      const second = await storage.listRuns({ limit: 2, before: cursor.createdAt, beforeId: cursor.id })

      const seen = [...first, ...second].map((run) => run.id)
      assertEqual(new Set(seen).size, 3, 'every run seen exactly once across pages')
    },
  },
  {
    name: 'requeueRun resets a failed run and discards only its failed steps',
    async run(storage) {
      await storage.createRun(makeRun({ status: 'failed' }))
      await storage.saveStepResult(makeStep({ id: 's1', name: 'a', status: 'completed' }))
      await storage.saveStepResult(makeStep({ id: 's2', name: 'b', status: 'failed', output: null }))

      assert(await storage.requeueRun('run_1'), 'a failed run must be requeueable')
      assertEqual((await storage.getRun('run_1'))?.status, 'pending', 'reset to pending')

      const steps = await storage.getStepResults('run_1')
      assertEqual(steps.map((step) => step.name), ['a'], 'completed steps kept, failed discarded')
    },
  },
  {
    name: 'requeueRun refuses a run that is not in a resumable state',
    async run(storage) {
      await storage.createRun(makeRun({ status: 'running' }))

      assert(!(await storage.requeueRun('run_1')), 'a running run is not resumable')
      assert(!(await storage.requeueRun('missing')), 'a missing run is not resumable')
    },
  },

  // -------------------------------------------------------------------------
  // Step results
  // -------------------------------------------------------------------------
  {
    name: 'saveStepResult persists a step and getStepResults reads it back',
    async run(storage) {
      await storage.createRun(makeRun())
      assert(await storage.saveStepResult(makeStep()), 'save must succeed')

      const steps = await storage.getStepResults('run_1')
      assertEqual(steps.length, 1, 'step count')
      assertEqual(steps[0]?.output, { ok: true }, 'step output round-trip')
    },
  },
  {
    name: 'saveStepResult upserts by row id rather than appending',
    async run(storage) {
      // Row identity is what stops a re-executed step showing up twice in
      // getRunStatus(); every adapter must agree on it.
      await storage.createRun(makeRun())
      await storage.saveStepResult(makeStep({ status: 'failed', output: null }))
      await storage.saveStepResult(makeStep({ status: 'completed', output: { ok: true } }))

      const steps = await storage.getStepResults('run_1')
      assertEqual(steps.length, 1, 'same id must update in place')
      assertEqual(steps[0]?.status, 'completed', 'latest status wins')
    },
  },
  {
    name: 'saveStepResult with a stale lease is refused',
    async run(storage) {
      await storage.createRun(makeRun())
      await claim(storage)

      assert(
        !(await storage.saveStepResult(makeStep(), 'not-the-lease')),
        'a foreign lease must not persist a step',
      )
    },
  },
  {
    name: 'getStepResults returns an empty list for a run with no steps',
    async run(storage) {
      await storage.createRun(makeRun())
      assertEqual(await storage.getStepResults('run_1'), [], 'no steps yet')
    },
  },

  // -------------------------------------------------------------------------
  // Sleeping and waiting
  // -------------------------------------------------------------------------
  {
    name: 'sleepRun suspends the run and releases it at the wake time',
    async run(storage) {
      await storage.createRun(makeRun())
      const claimed = await claim(storage)

      assert(await storage.sleepRun('run_1', claimed.leaseId, Date.now() - 1), 'sleep must succeed')
      assertEqual((await storage.getRun('run_1'))?.status, 'sleeping', 'suspended status')

      const woken = await storage.claimNextRun(['test'])
      assert(woken, 'an elapsed sleep must be reclaimable')
      assert(woken.leaseId !== claimed.leaseId, 'reclaiming must issue a fresh lease')
    },
  },
  {
    name: 'a sleeping run is not claimable before its wake time',
    async run(storage) {
      await storage.createRun(makeRun())
      const claimed = await claim(storage)
      await storage.sleepRun('run_1', claimed.leaseId, Date.now() + 60_000)

      assertEqual(await storage.claimNextRun(['test']), null, 'must stay asleep')
    },
  },
  {
    name: 'sleepRun requires the held lease',
    async run(storage) {
      await storage.createRun(makeRun())
      await claim(storage)

      assert(
        !(await storage.sleepRun('run_1', 'not-the-lease', Date.now() + 1000)),
        'a foreign lease must not suspend the run',
      )
    },
  },
  {
    name: 'deliverEvent buffers a payload that takeEvent then consumes once',
    async run(storage) {
      await storage.createRun(makeRun())

      assert(await storage.deliverEvent('run_1', 'e', { n: 1 }), 'delivery to an existing run')
      const taken = await storage.takeEvent('run_1', 'e')
      assertEqual(taken?.payload, { n: 1 }, 'buffered payload')
      assertEqual(await storage.takeEvent('run_1', 'e'), null, 'an event is consumed once')
    },
  },
  {
    name: 'deliverEvent reports a missing run',
    async run(storage) {
      assert(!(await storage.deliverEvent('missing', 'e', {})), 'unknown run')
    },
  },
  {
    name: 'takeEvent consumes the oldest matching event first',
    async run(storage) {
      await storage.createRun(makeRun())
      await storage.deliverEvent('run_1', 'e', { n: 1 })
      await storage.deliverEvent('run_1', 'e', { n: 2 })

      assertEqual((await storage.takeEvent('run_1', 'e'))?.payload, { n: 1 }, 'first in, first out')
      assertEqual((await storage.takeEvent('run_1', 'e'))?.payload, { n: 2 }, 'then the second')
    },
  },
  {
    name: 'takeEvent does not cross event names or runs',
    async run(storage) {
      await storage.createRun(makeRun())
      await storage.deliverEvent('run_1', 'e', { n: 1 })

      assertEqual(await storage.takeEvent('run_1', 'other'), null, 'other event name')
      assertEqual(await storage.takeEvent('other-run', 'e'), null, 'other run')
    },
  },
  {
    name: 'waitRun suspends the run until an event wakes it',
    async run(storage) {
      await storage.createRun(makeRun())
      const claimed = await claim(storage)

      assert(await storage.waitRun('run_1', claimed.leaseId, 'e', null), 'wait must succeed')
      assertEqual((await storage.getRun('run_1'))?.status, 'waiting', 'waiting status')
      assertEqual(await storage.claimNextRun(['test']), null, 'not claimable while waiting')

      await storage.deliverEvent('run_1', 'e', { n: 1 })
      assertEqual((await storage.claimNextRun(['test']))?.id, 'run_1', 'delivery must wake it')
    },
  },
  {
    name: 'waitRun leaves the run reclaimable when a matching event is already buffered',
    async run(storage) {
      // Closes the race where an event lands between the caller's takeEvent
      // check and the suspend. Getting this wrong strands the run until timeout.
      await storage.createRun(makeRun())
      const claimed = await claim(storage)
      await storage.deliverEvent('run_1', 'e', { n: 1 })

      await storage.waitRun('run_1', claimed.leaseId, 'e', null)

      assertEqual(
        (await storage.claimNextRun(['test']))?.id,
        'run_1',
        'a buffered event must leave the run immediately reclaimable',
      )
    },
  },
  {
    name: 'a waiting run with a deadline becomes claimable once it passes',
    async run(storage) {
      await storage.createRun(makeRun())
      const claimed = await claim(storage)
      await storage.waitRun('run_1', claimed.leaseId, 'e', Date.now() - 1)

      assertEqual((await storage.claimNextRun(['test']))?.id, 'run_1', 'elapsed deadline')
    },
  },

  // -------------------------------------------------------------------------
  // Schedules
  // -------------------------------------------------------------------------
  {
    name: 'upsertSchedule registers a schedule that listSchedules returns',
    async run(storage) {
      await storage.upsertSchedule(makeSchedule())

      const schedules = await storage.listSchedules()
      assertEqual(schedules.length, 1, 'schedule count')
      assertEqual(schedules[0]?.input, { olderThanDays: 30 }, 'schedule input round-trip')
    },
  },
  {
    name: 'upsertSchedule preserves the cadence when the interval is unchanged',
    async run(storage) {
      // A service redeploying more often than its schedule fires would never
      // run if re-registration reset the clock.
      const first = await storage.upsertSchedule(makeSchedule({ nextRunAt: 5_000 }))
      const second = await storage.upsertSchedule(
        makeSchedule({ nextRunAt: 900_000, input: { olderThanDays: 90 } }),
      )

      assertEqual(second.nextRunAt, first.nextRunAt, 'cadence preserved')
      assertEqual(second.input, { olderThanDays: 90 }, 'input still updated')
    },
  },
  {
    name: 'upsertSchedule resets the cadence when the interval changes',
    async run(storage) {
      await storage.upsertSchedule(makeSchedule({ nextRunAt: 5_000 }))
      const changed = await storage.upsertSchedule(
        makeSchedule({ recurrence: { kind: 'interval', intervalMs: 120_000 }, nextRunAt: 900_000 }),
      )

      assertEqual(changed.nextRunAt, 900_000, 'cadence reset')
      assertEqual(changed.recurrence, { kind: 'interval', intervalMs: 120_000 }, 'new recurrence stored')
    },
  },
  {
    name: 'claimDueSchedule advances past now and reports the occurrence it fired for',
    async run(storage) {
      await storage.upsertSchedule(makeSchedule({ nextRunAt: 1_000, recurrence: { kind: 'interval', intervalMs: 100 } }))

      const claimed = await storage.claimDueSchedule(['test'], 1_250)
      assertEqual(claimed?.nextRunAt, 1_000, 'reports the due occurrence')
      assertEqual((await storage.listSchedules())[0]?.nextRunAt, 1_300, 'advances past now')
    },
  },
  {
    name: 'an occurrence can only be claimed once',
    async run(storage) {
      // The atomicity that stops N instances firing one occurrence N times.
      await storage.upsertSchedule(makeSchedule({ nextRunAt: 1_000, recurrence: { kind: 'interval', intervalMs: 100_000 } }))

      assert(await storage.claimDueSchedule(['test'], 1_000), 'first claim succeeds')
      assertEqual(await storage.claimDueSchedule(['test'], 1_000), null, 'second finds nothing due')
    },
  },
  {
    name: 'claimDueSchedule ignores schedules that are not yet due',
    async run(storage) {
      await storage.upsertSchedule(makeSchedule({ nextRunAt: 10_000 }))
      assertEqual(await storage.claimDueSchedule(['test'], 9_999), null, 'not yet due')
    },
  },
  {
    name: 'claimDueSchedule only considers the named workflows',
    async run(storage) {
      await storage.upsertSchedule(makeSchedule({ workflow: 'elsewhere', nextRunAt: 1_000 }))

      assertEqual(await storage.claimDueSchedule(['test'], 5_000), null, 'other workflow skipped')
      assert(await storage.claimDueSchedule(['elsewhere'], 5_000), 'own workflow claimed')
    },
  },
  {
    name: 'claimDueSchedule returns null for an empty workflow list',
    async run(storage) {
      await storage.upsertSchedule(makeSchedule({ nextRunAt: 1_000 }))
      assertEqual(await storage.claimDueSchedule([], 5_000), null, 'no names means nothing claimable')
    },
  },
  {
    name: 'upsertSchedule round-trips a cron recurrence',
    async run(storage) {
      await storage.upsertSchedule(makeSchedule({
        recurrence: { kind: 'cron', expression: '0 9 * * 1-5' },
        nextRunAt: 5_000,
      }))

      const stored = (await storage.listSchedules())[0]
      assertEqual(stored?.recurrence, { kind: 'cron', expression: '0 9 * * 1-5' }, 'cron recurrence')
    },
  },
  {
    name: 'upsertSchedule preserves the cadence when the cron expression is unchanged',
    async run(storage) {
      const cron = { kind: 'cron', expression: '0 9 * * 1-5' } as const
      const first = await storage.upsertSchedule(makeSchedule({ recurrence: cron, nextRunAt: 5_000 }))
      const second = await storage.upsertSchedule(makeSchedule({ recurrence: cron, nextRunAt: 900_000 }))

      assertEqual(second.nextRunAt, first.nextRunAt, 'cadence preserved')
    },
  },
  {
    name: 'upsertSchedule resets the cadence when the cron expression changes',
    async run(storage) {
      await storage.upsertSchedule(makeSchedule({
        recurrence: { kind: 'cron', expression: '0 9 * * *' },
        nextRunAt: 5_000,
      }))
      const changed = await storage.upsertSchedule(makeSchedule({
        recurrence: { kind: 'cron', expression: '0 10 * * *' },
        nextRunAt: 900_000,
      }))

      assertEqual(changed.nextRunAt, 900_000, 'cadence reset')
    },
  },
  {
    name: 'switching between interval and cron resets the cadence',
    async run(storage) {
      // The two recurrence kinds are distinct cadences even when the stored
      // columns for the other kind happen to be null on both sides.
      await storage.upsertSchedule(makeSchedule({
        recurrence: { kind: 'interval', intervalMs: 60_000 },
        nextRunAt: 5_000,
      }))
      const changed = await storage.upsertSchedule(makeSchedule({
        recurrence: { kind: 'cron', expression: '0 9 * * *' },
        nextRunAt: 900_000,
      }))

      assertEqual(changed.nextRunAt, 900_000, 'cadence reset')
      assertEqual(changed.recurrence, { kind: 'cron', expression: '0 9 * * *' }, 'recurrence replaced')
    },
  },
  {
    name: 'claimDueSchedule advances a cron schedule to its next occurrence',
    async run(storage) {
      // 09:00 daily; due at 2026-03-10T09:00Z, claimed a minute later.
      await storage.upsertSchedule(makeSchedule({
        recurrence: { kind: 'cron', expression: '0 9 * * *' },
        nextRunAt: Date.parse('2026-03-10T09:00:00Z'),
      }))

      const claimed = await storage.claimDueSchedule(['test'], Date.parse('2026-03-10T09:01:00Z'))
      assertEqual(claimed?.nextRunAt, Date.parse('2026-03-10T09:00:00Z'), 'reports the due occurrence')
      assertEqual(
        (await storage.listSchedules())[0]?.nextRunAt,
        Date.parse('2026-03-11T09:00:00Z'),
        'advances to tomorrow, not by a fixed interval',
      )
    },
  },
  {
    name: 'deleteSchedule reports whether it removed anything',
    async run(storage) {
      await storage.upsertSchedule(makeSchedule())

      assert(await storage.deleteSchedule('nightly'), 'existing schedule removed')
      assert(!(await storage.deleteSchedule('nightly')), 'second delete reports nothing')
      assertEqual(await storage.listSchedules(), [], 'list is empty')
    },
  },
  {
    name: 'listSchedules is ordered by key',
    async run(storage) {
      await storage.upsertSchedule(makeSchedule({ key: 'c' }))
      await storage.upsertSchedule(makeSchedule({ key: 'a' }))
      await storage.upsertSchedule(makeSchedule({ key: 'b' }))

      assertEqual(
        (await storage.listSchedules()).map((schedule) => schedule.key),
        ['a', 'b', 'c'],
        'key order',
      )
    },
  },
]

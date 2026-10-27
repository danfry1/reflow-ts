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
  /**
   * Whether the case applies to this adapter — false for a case covering an
   * optional method the adapter does not implement. A runner can report the
   * case as skipped; `run` also passes without checking anything in that case.
   */
  appliesTo?(storage: StorageAdapter): boolean
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
    name: 'claimNextRun skips a workflow already running as many runs as its limit',
    appliesTo: (storage) => storage.supportsConcurrencyLimits === true,
    async run(storage) {
      if (storage.supportsConcurrencyLimits !== true) return
      const now = Date.now()
      const limits = new Map([['test', 2]])
      await storage.createRun(makeRun({ id: 'a', createdAt: now - 3_000, updatedAt: now - 3_000 }))
      await storage.createRun(makeRun({ id: 'b', createdAt: now - 2_000, updatedAt: now - 2_000 }))
      await storage.createRun(makeRun({ id: 'c', createdAt: now - 1_000, updatedAt: now - 1_000 }))
      await storage.createRun(makeRun({ id: 'other', workflow: 'other', createdAt: now, updatedAt: now }))

      assertEqual((await storage.claimNextRun(['test'], now - 30_000, limits))?.id, 'a', 'first place')
      const second = await storage.claimNextRun(['test'], now - 30_000, limits)
      assertEqual(second?.id, 'b', 'second place')
      assertEqual(await storage.claimNextRun(['test'], now - 30_000, limits), null, 'no third run while two are running')
      assertEqual(
        (await storage.claimNextRun(['test', 'other'], now - 30_000, limits))?.id,
        'other',
        'a workflow with no limit is still claimed',
      )

      assert(second, 'second claim')
      await storage.completeRun(second.id, second.leaseId, null)
      assertEqual((await storage.claimNextRun(['test'], now - 30_000, limits))?.id, 'c', 'a finished run frees its place')
    },
  },
  {
    name: 'claimNextRun does not count a run whose lease has gone stale toward its limit',
    appliesTo: (storage) => storage.supportsConcurrencyLimits === true,
    async run(storage) {
      if (storage.supportsConcurrencyLimits !== true) return
      const now = Date.now()
      // Its process died: the lease was last renewed a minute ago.
      await storage.createRun(makeRun({ id: 'stale', status: 'running', createdAt: now - 120_000, updatedAt: now - 60_000 }))
      await storage.createRun(makeRun({ id: 'next', createdAt: now - 1_000, updatedAt: now - 1_000 }))

      const claimed = await storage.claimNextRun(['test'], now - 30_000, new Map([['test', 1]]))
      assertEqual(claimed?.id, 'stale', 'the stale run is reclaimed, in its own place')
      assertEqual(await storage.claimNextRun(['test'], now - 30_000, new Map([['test', 1]])), null, 'and now holds the only place')
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
    name: "getStepResultsForRuns returns each run's steps in order, and an empty list for the rest",
    appliesTo: (storage) => storage.getStepResultsForRuns !== undefined,
    async run(storage) {
      if (!storage.getStepResultsForRuns) return
      await storage.createRun(makeRun({ id: 'a' }))
      await storage.createRun(makeRun({ id: 'b' }))
      await storage.saveStepResult(makeStep({ id: 'a2', runId: 'a', name: 'second', createdAt: 2 }))
      await storage.saveStepResult(makeStep({ id: 'a1', runId: 'a', name: 'first', createdAt: 1 }))

      const byRun = await storage.getStepResultsForRuns(['a', 'b', 'missing'])

      assertEqual([...byRun.keys()].sort(), ['a', 'b', 'missing'], 'every run asked for')
      assertEqual(byRun.get('a')?.map((step) => step.name), ['first', 'second'], 'steps in creation order')
      assertEqual(byRun.get('b'), [], 'a run with no steps')
      assertEqual(byRun.get('missing'), [], 'a run that does not exist')
    },
  },
  {
    name: 'runActivity counts runs started, completed, and failed per bucket',
    appliesTo: (storage) => storage.runActivity !== undefined,
    async run(storage) {
      if (!storage.runActivity) return
      const finished = (id: string, workflow: string, status: 'completed' | 'failed', createdAt: number, finishedAt: number) =>
        storage.createRun(makeRun({ id, workflow, status, createdAt, updatedAt: finishedAt, finishedAt }))
      await finished('a', 'w', 'completed', 1_000, 1_500)
      await finished('b', 'w', 'failed', 1_900, 2_100)
      await finished('c', 'other', 'completed', 1_000, 1_100)
      await storage.createRun(makeRun({ id: 'd', workflow: 'w', createdAt: 2_500 }))
      await storage.createRun(makeRun({ id: 'early', workflow: 'w', createdAt: 999 }))
      await storage.createRun(makeRun({ id: 'late', workflow: 'w', createdAt: 3_000 }))

      const activity = await storage.runActivity({ workflow: 'w', since: 1_000, bucketMs: 1_000, buckets: 2 })

      assertEqual(activity, { started: [2, 1], completed: [1, 0], failed: [0, 1] }, 'activity for w')
      const all = await storage.runActivity({ since: 1_000, bucketMs: 1_000, buckets: 2 })
      assertEqual(all.started, [3, 1], 'every workflow')
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
    name: 'requeueRun keeps the failed steps it is told to keep',
    appliesTo: (storage) => storage.supportsKeepFailed === true,
    async run(storage) {
      if (storage.supportsKeepFailed !== true) return
      await storage.createRun(makeRun({ status: 'failed' }))
      await storage.saveStepResult(makeStep({ id: 's1', name: 'a', status: 'completed' }))
      await storage.saveStepResult(makeStep({ id: 's2', name: 'caught', status: 'failed', output: null }))
      await storage.saveStepResult(makeStep({ id: 's3', name: 'fatal', status: 'failed', output: null }))
      await storage.saveStepResult(makeStep({ id: 's4', name: 'backing-off', status: 'retrying', output: 0 }))

      assert(await storage.requeueRun('run_1', { keepFailed: ['caught', 'backing-off'] }), 'a failed run must requeue')

      const steps = await storage.getStepResults('run_1')
      assertEqual(
        steps.map((step) => `${step.name}:${step.status}`),
        ['a:completed', 'caught:failed'],
        'only the named failed step is kept; a retrying step is discarded whatever its name',
      )
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
    name: 'consumeEvent takes the oldest event and saves it as the step, under the held lease',
    appliesTo: (storage) => storage.consumeEvent !== undefined,
    async run(storage) {
      if (!storage.consumeEvent) return
      await storage.createRun(makeRun())
      const claimed = await claim(storage)
      await storage.deliverEvent('run_1', 'e', { n: 1 })
      await storage.deliverEvent('run_1', 'e', { n: 2 })
      const { output: _unused, ...step } = makeStep({ name: 'e', attempts: 0 })

      assertEqual(
        await storage.consumeEvent(step, claimed.leaseId),
        { status: 'consumed', payload: { n: 1 } },
        'the oldest event is consumed',
      )
      const [saved] = await storage.getStepResults('run_1')
      assertEqual([saved?.name, saved?.status, saved?.output], ['e', 'completed', { n: 1 }], 'saved with the payload as its output')
      assertEqual((await storage.takeEvent('run_1', 'e'))?.payload, { n: 2 }, 'only that event is removed')
      assertEqual(await storage.consumeEvent(step, claimed.leaseId), { status: 'none' }, 'nothing left to consume')
    },
  },
  {
    name: 'consumeEvent takes the named event for a step named apart from it',
    appliesTo: (storage) => storage.consumeEvent !== undefined && storage.supportsEventNames === true,
    async run(storage) {
      if (!storage.consumeEvent || storage.supportsEventNames !== true) return
      await storage.createRun(makeRun())
      const claimed = await claim(storage)
      await storage.deliverEvent('run_1', 'approved', { round: 1 })
      await storage.deliverEvent('run_1', 'approved', { round: 2 })
      const { output: _unused, ...first } = makeStep({ id: 'w1', name: 'approval-1', attempts: 0 })
      const { output: _unused2, ...second } = makeStep({ id: 'w2', name: 'approval-2', attempts: 0 })

      assertEqual(await storage.consumeEvent(first, claimed.leaseId, 'approved'), { status: 'consumed', payload: { round: 1 } }, 'first wait takes the oldest')
      assertEqual(await storage.consumeEvent(second, claimed.leaseId, 'approved'), { status: 'consumed', payload: { round: 2 } }, 'second wait takes the next')
      assertEqual(
        (await storage.getStepResults('run_1')).map((step) => [step.name, step.output]),
        [['approval-1', { round: 1 }], ['approval-2', { round: 2 }]],
        'each saved under its own step name',
      )
    },
  },
  {
    name: 'waitRun waits for any of several events, and stays reclaimable when one is buffered',
    appliesTo: (storage) => storage.supportsEventNames === true,
    async run(storage) {
      if (storage.supportsEventNames !== true) return
      await storage.createRun(makeRun({ id: 'run_1' }))
      await storage.createRun(makeRun({ id: 'run_2' }))
      const first = await claim(storage)
      const second = await claim(storage)
      const [waitingId, bufferedId] = [first.id, second.id]
      await storage.deliverEvent(bufferedId, 'b', {})

      assert(await storage.waitRun(waitingId, first.leaseId, ['a', 'b'], null), 'wait must succeed')
      assert(await storage.waitRun(bufferedId, second.leaseId, ['a', 'b'], null), 'wait must succeed')
      assertEqual(
        [(await storage.getRun(waitingId))?.status, (await storage.getRun(waitingId))?.waitingFor],
        ['waiting', 'a, b'],
        'waiting for both, listed',
      )
      assertEqual((await storage.getRun(bufferedId))?.status, 'pending', 'an event for the second name is already buffered')

      await storage.deliverEvent(waitingId, 'b', {})
      assertEqual((await storage.getRun(waitingId))?.status, 'pending', 'either name wakes it')
    },
  },
  {
    name: 'saveStepResult keeps what kind of operation a step row records',
    async run(storage) {
      await storage.createRun(makeRun())
      await storage.saveStepResult(makeStep({ id: 's1', name: 'nap', status: 'sleeping', kind: 'sleep' }))
      await storage.saveStepResult(makeStep({ id: 's2', name: 'old', status: 'completed' }))

      assertEqual(
        (await storage.getStepResults('run_1')).map((step) => [step.name, step.kind ?? null]),
        [['nap', 'sleep'], ['old', null]],
        'the kind saved, or null for a row saved without one',
      )
    },
  },
  {
    name: 'waitRun and requeueRun take more event and step names than a statement can hold parameters for',
    appliesTo: (storage) => storage.supportsEventNames === true && storage.supportsKeepFailed === true,
    async run(storage) {
      if (storage.supportsEventNames !== true || storage.supportsKeepFailed !== true) return
      // Above SQLite's limit of 32,766 parameters in one statement.
      const names = Array.from({ length: 33_000 }, (_, index) => `e${index}`)
      await storage.createRun(makeRun())
      const claimed = await claim(storage)
      await storage.deliverEvent('run_1', 'e32999', {})

      assert(await storage.waitRun('run_1', claimed.leaseId, names, null), 'wait must succeed')
      assertEqual((await storage.getRun('run_1'))?.status, 'pending', 'the buffered event, last in the list, is found')

      const again = await claim(storage)
      await storage.saveStepResult(makeStep({ id: 'kept', name: 'e5', status: 'failed', output: null }), again.leaseId)
      await storage.saveStepResult(makeStep({ id: 'gone', name: 'fatal', status: 'failed', output: null }), again.leaseId)
      await storage.failRun('run_1', again.leaseId, { stepName: 'fatal', message: 'boom', code: null })
      assert(await storage.requeueRun('run_1', { keepFailed: names }), 'a failed run must requeue')
      assertEqual((await storage.getStepResults('run_1')).map((step) => step.name), ['e5'], 'kept among many names')
    },
  },
  {
    name: 'consumeEvent replaces the waiting step row it is given rather than adding one',
    appliesTo: (storage) => storage.consumeEvent !== undefined,
    async run(storage) {
      if (!storage.consumeEvent) return
      // A wait resumed by its event: the engine passes the id of the row the
      // step was saved under while it waited.
      await storage.createRun(makeRun())
      const claimed = await claim(storage)
      await storage.saveStepResult(makeStep({ name: 'e', status: 'waiting', output: null, attempts: 0 }), claimed.leaseId)
      await storage.deliverEvent('run_1', 'e', { n: 1 })
      const { output: _unused, ...step } = makeStep({ name: 'e', attempts: 0 })

      await storage.consumeEvent(step, claimed.leaseId)

      const steps = await storage.getStepResults('run_1')
      assertEqual(steps.map((saved) => [saved.id, saved.status, saved.output]), [['step_1', 'completed', { n: 1 }]], 'one row, now completed')
    },
  },
  {
    name: 'consumeEvent changes nothing once the lease is lost',
    appliesTo: (storage) => storage.consumeEvent !== undefined,
    async run(storage) {
      if (!storage.consumeEvent) return
      // The worker that lost the run must neither take its event nor record
      // the step, or the event is gone with no step to show for it.
      await storage.createRun(makeRun())
      await claim(storage)
      await storage.deliverEvent('run_1', 'e', { n: 1 })
      const { output: _unused, ...step } = makeStep({ name: 'e', attempts: 0 })

      assertEqual(await storage.consumeEvent(step, 'not-the-lease'), { status: 'lease-lost' }, 'a foreign lease is refused')
      assertEqual(await storage.getStepResults('run_1'), [], 'no step saved')
      assertEqual((await storage.takeEvent('run_1', 'e'))?.payload, { n: 1 }, 'the event is still buffered')
    },
  },
  {
    name: 'cached steps are stored, replaced by key, and read back expired or not',
    appliesTo: (storage) => storage.getCachedStep !== undefined && storage.putCachedStep !== undefined,
    async run(storage) {
      if (!storage.getCachedStep || !storage.putCachedStep) return
      const now = Date.now()
      const entry = { key: 'k1', workflow: 'test', stepName: 'a', output: { n: 1 }, createdAt: now, expiresAt: now + 1_000 }

      assertEqual(await storage.getCachedStep('k1'), null, 'nothing cached yet')
      await storage.putCachedStep(entry)
      assertEqual(await storage.getCachedStep('k1'), entry, 'stored as given')
      await storage.putCachedStep({ ...entry, output: { n: 2 }, createdAt: now - 5_000, expiresAt: now - 1 })
      assertEqual((await storage.getCachedStep('k1'))?.output, { n: 2 }, 'a second put under the key replaces the first')
      assertEqual((await storage.getCachedStep('k1'))?.expiresAt, now - 1, 'an expired entry is still returned; the engine decides')
    },
  },
  {
    name: 'deleteExpiredCachedSteps deletes entries expired by now and keeps the rest',
    appliesTo: (storage) => storage.deleteExpiredCachedSteps !== undefined && storage.putCachedStep !== undefined,
    async run(storage) {
      if (!storage.deleteExpiredCachedSteps || !storage.putCachedStep || !storage.getCachedStep) return
      const now = Date.now()
      const entry = (key: string, expiresAt: number) => ({ key, workflow: 'test', stepName: 'a', output: null, createdAt: now - 10_000, expiresAt })
      await storage.putCachedStep(entry('gone', now - 1))
      await storage.putCachedStep(entry('edge', now))
      await storage.putCachedStep(entry('live', now + 1))

      assertEqual(await storage.deleteExpiredCachedSteps(now, 1), 1, 'no more than the limit')
      assertEqual(await storage.deleteExpiredCachedSteps(now, 10), 1, 'the rest expired at or before now')
      assertEqual(await storage.getCachedStep('edge'), null, 'expiring exactly now counts as expired')
      assertEqual((await storage.getCachedStep('live'))?.key, 'live', 'a live entry is kept')
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

import { randomUUID } from 'node:crypto'
import type {
  ClaimedRun,
  CountRunsFilter,
  CreateRunResult,
  DeleteRunsFilter,
  PersistedValue,
  ListRunsFilter,
  RunActivity,
  RunActivityFilter,
  RunCounts,
  RunError,
  RunStatus,
  StepResult,
  StorageAdapter,
  TerminalRunStatus,
  WorkflowRun,
  WorkflowSchedule,
} from '../core/types'
import { emptyRunCounts } from './records'
import { assertNever, InternalError } from '../core/errors'
import { nextOccurrence, sameRecurrence } from '../core/schedule-timing'
import { clonePersistedValue } from './codec'

interface StoredRun extends WorkflowRun {
  leaseId: string | null
}

interface StoredEvent {
  eventName: string
  payload: PersistedValue
  createdAt: number
}

export class MemoryStorage implements StorageAdapter {
  private runs: Map<string, StoredRun> = new Map()
  private steps: Map<string, StepResult[]> = new Map()
  private events: Map<string, StoredEvent[]> = new Map()
  private schedules: Map<string, WorkflowSchedule> = new Map()

  async initialize(): Promise<void> {}

  async createRun(run: WorkflowRun): Promise<CreateRunResult> {
    if (run.idempotencyKey) {
      for (const existingRun of this.runs.values()) {
        if (
          existingRun.workflow === run.workflow
          && existingRun.idempotencyKey === run.idempotencyKey
        ) {
          return {
            run: cloneWorkflowRun(existingRun),
            created: false,
          }
        }
      }
    }

    const storedRun: StoredRun = {
      ...run,
      input: clonePersistedValue(run.input, 'Workflow input'),
      output: run.status === 'completed' ? clonePersistedValue(run.output, 'Run output') : undefined,
      error: run.error === null ? null : { ...run.error },
      leaseId: null,
    }

    this.runs.set(run.id, storedRun)
    return {
      run: cloneWorkflowRun(storedRun),
      created: true,
    }
  }

  async claimNextRun(workflowNames: readonly string[], staleBefore?: number): Promise<ClaimedRun | null> {
    if (workflowNames.length === 0) {
      return null
    }

    const now = Date.now()
    // Claim the run that has been due longest, so new pending runs cannot
    // starve a woken sleeper or a stale run. A single pass rather than a sort:
    // this runs on every claim, over every stored run. Ties go to the earlier
    // `createdAt`, then to insertion order.
    let run: StoredRun | undefined
    let runDueAt = Number.POSITIVE_INFINITY
    for (const candidate of this.runs.values()) {
      if (!workflowNames.includes(candidate.workflow)) {
        continue
      }

      const dueAt = claimableSince(candidate, now, staleBefore)
      if (
        dueAt !== null
        && (dueAt < runDueAt || (dueAt === runDueAt && run !== undefined && candidate.createdAt < run.createdAt))
      ) {
        run = candidate
        runDueAt = dueAt
      }
    }

    if (!run) {
      return null
    }

    run.status = 'running'
    run.updatedAt = now
    run.startedAt ??= now
    run.leaseId = randomUUID()
    run.wakeAt = null
    run.waitingFor = null

    return cloneClaimedRun(run)
  }

  async heartbeatRun(runId: string, leaseId: string): Promise<boolean> {
    const run = this.runs.get(runId)
    if (!run || run.status !== 'running' || run.leaseId !== leaseId) {
      return false
    }

    run.updatedAt = Date.now()
    return true
  }

  async sleepRun(runId: string, leaseId: string, wakeAt: number): Promise<boolean> {
    const run = this.runs.get(runId)
    if (!run || run.status !== 'running' || run.leaseId !== leaseId) {
      return false
    }

    run.status = 'sleeping'
    run.leaseId = null
    run.wakeAt = wakeAt
    run.updatedAt = Date.now()
    return true
  }

  async waitRun(runId: string, leaseId: string, eventName: string, wakeAt: number | null): Promise<boolean> {
    const run = this.runs.get(runId)
    if (!run || run.status !== 'running' || run.leaseId !== leaseId) {
      return false
    }

    // If a matching event is already buffered, stay reclaimable instead of
    // waiting — closes the deliver-during-suspend race.
    const buffered = (this.events.get(runId) ?? []).some((event) => event.eventName === eventName)
    run.status = buffered ? 'pending' : 'waiting'
    run.leaseId = null
    run.wakeAt = buffered ? null : wakeAt
    run.waitingFor = buffered ? null : eventName
    run.updatedAt = Date.now()
    return true
  }

  async deliverEvent(runId: string, eventName: string, payload: PersistedValue): Promise<boolean> {
    const run = this.runs.get(runId)
    if (!run) {
      return false
    }

    const list = this.events.get(runId) ?? []
    list.push({ eventName, payload: clonePersistedValue(payload, 'Event payload'), createdAt: Date.now() })
    this.events.set(runId, list)

    if (run.status === 'waiting') {
      run.status = 'pending'
      run.leaseId = null
      run.wakeAt = null
      run.waitingFor = null
      run.updatedAt = Date.now()
    }
    return true
  }

  async takeEvent(runId: string, eventName: string): Promise<{ payload: PersistedValue } | null> {
    const list = this.events.get(runId)
    if (!list) {
      return null
    }
    const index = list.findIndex((event) => event.eventName === eventName)
    if (index < 0) {
      return null
    }
    const [taken] = list.splice(index, 1)
    if (taken === undefined) {
      throw new InternalError(`Event "${eventName}" vanished from the buffer during take`)
    }
    return { payload: clonePersistedValue(taken.payload, 'Event payload') }
  }

  async getRun(runId: string): Promise<WorkflowRun | null> {
    const run = this.runs.get(runId)
    return run ? cloneWorkflowRun(run) : null
  }

  async listRuns(filter: ListRunsFilter = {}): Promise<WorkflowRun[]> {
    const { status, workflow, limit = 100, before, beforeId } = filter
    return Array.from(this.runs.values())
      .filter((run) => {
        if (status !== undefined && run.status !== status) {
          return false
        }
        if (workflow !== undefined && run.workflow !== workflow) {
          return false
        }
        if (before !== undefined) {
          // Keyset cursor over the (createdAt DESC, id DESC) order.
          const afterCursor = beforeId !== undefined
            ? run.createdAt < before || (run.createdAt === before && run.id < beforeId)
            : run.createdAt < before
          if (!afterCursor) {
            return false
          }
        }
        return true
      })
      .sort((left, right) =>
        right.createdAt - left.createdAt || (left.id < right.id ? 1 : left.id > right.id ? -1 : 0),
      )
      .slice(0, limit)
      .map(cloneWorkflowRun)
  }

  async requeueRun(runId: string): Promise<boolean> {
    const run = this.runs.get(runId)
    if (!run || (run.status !== 'failed' && run.status !== 'cancelled')) {
      return false
    }

    run.status = 'pending'
    run.leaseId = null
    run.updatedAt = Date.now()
    run.finishedAt = null
    run.output = undefined
    run.error = null

    const steps = this.steps.get(runId)
    if (steps) {
      this.steps.set(runId, steps.filter((step) => step.status !== 'failed' && step.status !== 'retrying'))
    }

    return true
  }

  async getStepResults(runId: string): Promise<StepResult[]> {
    // Ordered by creation time, as the contract says; insertion order only
    // matches it when steps are first saved in the order they began.
    return [...(this.steps.get(runId) ?? [])].sort((a, b) => a.createdAt - b.createdAt).map((step) => ({
      ...step,
      output: clonePersistedValue(step.output, 'Step output'),
      history: step.history.map((attempt) => ({ ...attempt })),
    }))
  }

  async saveStepResult(result: StepResult, leaseId?: string): Promise<boolean> {
    if (leaseId) {
      const run = this.runs.get(result.runId)
      if (!run || run.status !== 'running' || run.leaseId !== leaseId) {
        return false
      }
    }

    const existing = this.steps.get(result.runId) ?? []
    const idx = existing.findIndex((step) => step.id === result.id)
    const cloned = {
      ...result,
      output: clonePersistedValue(result.output, 'Step output'),
      history: result.history.map((attempt) => ({ ...attempt })),
    }

    if (idx >= 0) {
      existing[idx] = cloned
    } else {
      existing.push(cloned)
    }

    this.steps.set(result.runId, existing)
    return true
  }

  async completeRun(runId: string, leaseId: string, output: PersistedValue): Promise<boolean> {
    const run = this.heldRun(runId, leaseId)
    if (!run) {
      return false
    }

    // Serialize before transitioning, so an unpersistable output leaves the run
    // held and running rather than completed with nothing recorded.
    const stored = clonePersistedValue(output, 'Run output')
    finish(run, 'completed')
    run.output = stored
    return true
  }

  async failRun(runId: string, leaseId: string, error: RunError): Promise<boolean> {
    const run = this.heldRun(runId, leaseId)
    if (!run) {
      return false
    }

    finish(run, 'failed')
    run.error = { ...error }
    return true
  }

  async cancelRun(runId: string): Promise<boolean> {
    const run = this.runs.get(runId)
    if (!run || isTerminal(run.status)) {
      return false
    }

    finish(run, 'cancelled')
    return true
  }

  async getStepResultsForRuns(runIds: readonly string[]): Promise<Map<string, StepResult[]>> {
    const byRun = new Map<string, StepResult[]>()
    for (const runId of runIds) {
      byRun.set(runId, await this.getStepResults(runId))
    }
    return byRun
  }

  async runActivity(filter: RunActivityFilter): Promise<RunActivity> {
    const { since, bucketMs, buckets } = filter
    const activity: RunActivity = {
      started: new Array<number>(buckets).fill(0),
      completed: new Array<number>(buckets).fill(0),
      failed: new Array<number>(buckets).fill(0),
    }
    const bucketOf = (at: number | null): number | null => {
      if (at === null || at < since) return null
      const bucket = Math.floor((at - since) / bucketMs)
      return bucket < buckets ? bucket : null
    }
    for (const run of this.runs.values()) {
      if (filter.workflow !== undefined && run.workflow !== filter.workflow) continue
      const started = bucketOf(run.createdAt)
      if (started !== null) activity.started[started] = (activity.started[started] ?? 0) + 1
      const finished = bucketOf(run.finishedAt)
      if (finished === null) continue
      if (run.status === 'completed') activity.completed[finished] = (activity.completed[finished] ?? 0) + 1
      if (run.status === 'failed') activity.failed[finished] = (activity.failed[finished] ?? 0) + 1
    }
    return activity
  }

  async countRuns(filter: CountRunsFilter = {}): Promise<RunCounts> {
    const counts = emptyRunCounts()
    for (const run of this.runs.values()) {
      if (filter.workflow === undefined || run.workflow === filter.workflow) {
        counts[run.status]++
      }
    }
    return counts
  }

  async deleteRuns(filter: DeleteRunsFilter): Promise<number> {
    const doomed = Array.from(this.runs.values())
      .filter((run) =>
        run.finishedAt !== null
        && run.finishedAt < filter.finishedBefore
        && filter.statuses.some((status) => status === run.status),
      )
      .sort((left, right) => (left.finishedAt ?? 0) - (right.finishedAt ?? 0))
      .slice(0, Math.max(0, filter.limit))

    for (const run of doomed) {
      this.runs.delete(run.id)
      this.steps.delete(run.id)
      this.events.delete(run.id)
    }
    return doomed.length
  }

  private heldRun(runId: string, leaseId: string): StoredRun | null {
    const run = this.runs.get(runId)
    return run && run.status === 'running' && run.leaseId === leaseId ? run : null
  }

  async upsertSchedule(schedule: WorkflowSchedule): Promise<WorkflowSchedule> {
    const existing = this.schedules.get(schedule.key)

    const stored: WorkflowSchedule = {
      ...schedule,
      input: clonePersistedValue(schedule.input, 'Schedule input'),
      // A restarting process must rejoin the existing cadence rather than push
      // the next firing out by a full interval on every deploy.
      nextRunAt: existing && sameRecurrence(existing.recurrence, schedule.recurrence)
        ? existing.nextRunAt
        : schedule.nextRunAt,
      createdAt: existing?.createdAt ?? schedule.createdAt,
    }

    this.schedules.set(schedule.key, stored)
    return cloneSchedule(stored)
  }

  async claimDueSchedule(
    workflowNames: readonly string[],
    now: number,
  ): Promise<WorkflowSchedule | null> {
    const due = Array.from(this.schedules.values())
      .filter((schedule) => schedule.nextRunAt <= now && workflowNames.includes(schedule.workflow))
      .sort((left, right) => left.nextRunAt - right.nextRunAt)[0]

    if (!due) {
      return null
    }

    // Advance before returning: the caller treats a claim as exclusive, and in
    // this adapter the "transaction" is simply that nothing else runs between
    // these two statements.
    this.schedules.set(due.key, {
      ...due,
      nextRunAt: nextOccurrence(due.nextRunAt, due.recurrence, now),
      updatedAt: now,
    })

    return cloneSchedule(due)
  }

  async deleteSchedule(key: string): Promise<boolean> {
    return this.schedules.delete(key)
  }

  async listSchedules(): Promise<WorkflowSchedule[]> {
    return Array.from(this.schedules.values())
      .sort((left, right) => (left.key < right.key ? -1 : left.key > right.key ? 1 : 0))
      .map(cloneSchedule)
  }

  close(): void {}
}

function cloneWorkflowRun(run: StoredRun): WorkflowRun {
  return {
    id: run.id,
    workflow: run.workflow,
    input: clonePersistedValue(run.input, 'Workflow input'),
    idempotencyKey: run.idempotencyKey,
    status: run.status,
    createdAt: run.createdAt,
    updatedAt: run.updatedAt,
    startedAt: run.startedAt,
    finishedAt: run.finishedAt,
    output: clonePersistedValue(run.output, 'Run output'),
    error: run.error === null ? null : { ...run.error },
    wakeAt: run.wakeAt,
    waitingFor: run.waitingFor,
  }
}

function isTerminal(status: RunStatus): status is TerminalRunStatus {
  return status === 'completed' || status === 'failed' || status === 'cancelled'
}

/** Move a run to a terminal status, releasing everything that only a live run holds. */
function finish(run: StoredRun, status: TerminalRunStatus): void {
  const now = Date.now()
  run.status = status
  run.leaseId = null
  run.wakeAt = null
  run.waitingFor = null
  run.updatedAt = now
  run.finishedAt = now
}

/**
 * When `run` became claimable, or null if it is not claimable now.
 *
 * A pending run is due from the moment it became pending, a sleeping or
 * waiting run from its wake time, and a stale running run from the moment its
 * lease expired: `now - staleBefore` (the lease duration) after its last
 * heartbeat.
 */
function claimableSince(run: StoredRun, now: number, staleBefore: number | undefined): number | null {
  switch (run.status) {
    case 'pending':
      return run.updatedAt
    case 'sleeping':
    case 'waiting':
      return run.wakeAt !== null && run.wakeAt <= now ? run.wakeAt : null
    case 'running':
      return staleBefore !== undefined && run.updatedAt <= staleBefore
        ? run.updatedAt + (now - staleBefore)
        : null
    case 'completed':
    case 'failed':
    case 'cancelled':
      return null
    default:
      return assertNever(run.status, 'run status')
  }
}

function cloneClaimedRun(run: StoredRun): ClaimedRun {
  if (!run.leaseId) {
    throw new InternalError('Claimed run is missing a lease id')
  }

  return {
    ...cloneWorkflowRun(run),
    leaseId: run.leaseId,
  }
}

function cloneSchedule(schedule: WorkflowSchedule): WorkflowSchedule {
  return {
    ...schedule,
    input: clonePersistedValue(schedule.input, 'Schedule input'),
  }
}

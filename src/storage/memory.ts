import { randomUUID } from 'node:crypto'
import type {
  ClaimedRun,
  CreateRunResult,
  PersistedValue,
  RunStatus,
  StepResult,
  StorageAdapter,
  WorkflowRun,
  WorkflowSchedule,
} from '../core/types'
import { InternalError } from '../core/errors'
import { nextOccurrence } from '../core/schedule-timing'
import { clonePersistedValue } from './codec'

interface StoredRun extends WorkflowRun {
  leaseId: string | null
  wakeAt: number | null
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
      leaseId: null,
      wakeAt: null,
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
    const candidates = Array.from(this.runs.values())
      .filter((run) => {
        if (!workflowNames.includes(run.workflow)) {
          return false
        }

        if (run.status === 'pending') {
          return true
        }

        if (
          (run.status === 'sleeping' || run.status === 'waiting')
          && run.wakeAt !== null
          && run.wakeAt <= now
        ) {
          return true
        }

        return staleBefore !== undefined && run.status === 'running' && run.updatedAt <= staleBefore
      })
      .sort((left, right) => {
        // Pending runs are claimed before woken sleeping / stale running runs;
        // within a rank, oldest first. Rank-based so the comparator stays a
        // valid total order when both non-pending kinds are present.
        const rank = (status: RunStatus): number => (status === 'pending' ? 0 : 1)
        const rankDiff = rank(left.status) - rank(right.status)
        if (rankDiff !== 0) {
          return rankDiff
        }
        return left.createdAt - right.createdAt
      })

    const run = candidates[0]
    if (!run) {
      return null
    }

    run.status = 'running'
    run.updatedAt = Date.now()
    run.leaseId = randomUUID()
    run.wakeAt = null

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

  async getStepResults(runId: string): Promise<StepResult[]> {
    return (this.steps.get(runId) ?? []).map((step) => ({
      ...step,
      output: clonePersistedValue(step.output, 'Step output'),
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
    const cloned = { ...result, output: clonePersistedValue(result.output, 'Step output') }

    if (idx >= 0) {
      existing[idx] = cloned
    } else {
      existing.push(cloned)
    }

    this.steps.set(result.runId, existing)
    return true
  }

  async updateRunStatus(runId: string, status: RunStatus): Promise<boolean> {
    const run = this.runs.get(runId)
    if (!run) {
      return false
    }

    run.status = status
    run.updatedAt = Date.now()
    run.leaseId = null
    run.wakeAt = null

    return true
  }

  async updateClaimedRunStatus(runId: string, leaseId: string, status: RunStatus): Promise<boolean> {
    const run = this.runs.get(runId)
    if (!run || run.status !== 'running' || run.leaseId !== leaseId) {
      return false
    }

    run.status = status
    run.updatedAt = Date.now()

    if (status !== 'running') {
      run.leaseId = null
      run.wakeAt = null
    }

    return true
  }

  async upsertSchedule(schedule: WorkflowSchedule): Promise<WorkflowSchedule> {
    const existing = this.schedules.get(schedule.key)

    const stored: WorkflowSchedule = {
      ...schedule,
      input: clonePersistedValue(schedule.input, 'Schedule input'),
      // A restarting process must rejoin the existing cadence rather than push
      // the next firing out by a full interval on every deploy.
      nextRunAt: existing && existing.intervalMs === schedule.intervalMs
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
      nextRunAt: nextOccurrence(due.nextRunAt, due.intervalMs, now),
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

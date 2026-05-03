import { randomUUID } from 'node:crypto'
import type {
  ClaimedRun,
  CreateRunResult,
  RunStatus,
  StepResult,
  StorageAdapter,
  WorkflowRun,
} from '../core/types'
import { clonePersistedValue } from './codec'

interface StoredRun extends WorkflowRun {
  leaseId: string | null
}

export class MemoryStorage implements StorageAdapter {
  private runs: Map<string, StoredRun> = new Map()
  private steps: Map<string, StepResult[]> = new Map()
  private cacheIndex: Map<string, StepResult[]> = new Map()

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

    const candidates = Array.from(this.runs.values())
      .filter((run) => {
        if (!workflowNames.includes(run.workflow)) {
          return false
        }

        if (run.status === 'pending') {
          return true
        }

        return staleBefore !== undefined && run.status === 'running' && run.updatedAt <= staleBefore
      })
      .sort((left, right) => {
        if (left.status !== right.status) {
          return left.status === 'pending' ? -1 : 1
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

  async saveStepResult(result: StepResult, leaseId?: string, cacheKey?: string): Promise<boolean> {
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

    if (cacheKey && result.attempts > 0 && result.status === 'completed') {
      const indexKey = `${result.name}::${cacheKey}`
      const entries = this.cacheIndex.get(indexKey) ?? []
      entries.push({ ...result, output: clonePersistedValue(result.output, 'Step output') })
      this.cacheIndex.set(indexKey, entries)
    }

    return true
  }

  async getCachedStepResult(stepName: string, cacheKey: string, ttlMs?: number): Promise<StepResult | null> {
    const indexKey = `${stepName}::${cacheKey}`
    const entries = this.cacheIndex.get(indexKey) ?? []
    const cutoff = ttlMs !== undefined ? Date.now() - ttlMs : 0
    const valid = entries.filter((e) => e.updatedAt >= cutoff)
    if (valid.length === 0) return null
    const best = valid.reduce((a, b) => (a.updatedAt >= b.updatedAt ? a : b))
    return { ...best, output: clonePersistedValue(best.output, 'Step output') }
  }

  async updateRunStatus(runId: string, status: RunStatus): Promise<boolean> {
    const run = this.runs.get(runId)
    if (!run) {
      return false
    }

    run.status = status
    run.updatedAt = Date.now()
    run.leaseId = null

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
    }

    return true
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
    throw new Error('Claimed run is missing a lease id')
  }

  return {
    ...cloneWorkflowRun(run),
    leaseId: run.leaseId,
  }
}

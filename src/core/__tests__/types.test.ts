import { describe, it, expect } from 'vitest'
import type {
  RunStatus,
  StepStatus,
  WorkflowRun,
  StepResult,
  StorageAdapter,
  RetryConfig,
} from '../types'

describe('core types', () => {
  it('RunStatus accepts valid values', () => {
    const statuses: RunStatus[] = ['pending', 'running', 'completed', 'failed', 'cancelled']
    expect(statuses).toHaveLength(5)
  })

  it('StepStatus accepts valid values', () => {
    const statuses: StepStatus[] = ['pending', 'running', 'completed', 'completed-early', 'failed']
    expect(statuses).toHaveLength(5)
  })

  it('WorkflowRun has required fields', () => {
    const run: WorkflowRun = {
      id: 'run_1',
      workflow: 'test-workflow',
      input: { foo: 'bar' },
      idempotencyKey: null,
      status: 'pending',
      createdAt: Date.now(),
      updatedAt: Date.now(),
    }
    expect(run.id).toBe('run_1')
    expect(run.status).toBe('pending')
  })

  it('StepResult has required fields', () => {
    const step: StepResult = {
      id: 'step_1',
      runId: 'run_1',
      name: 'charge',
      status: 'completed',
      output: { chargeId: 'ch_123' },
      error: null,
      attempts: 1,
      createdAt: Date.now(),
      updatedAt: Date.now(),
    }
    expect(step.name).toBe('charge')
    expect(step.output).toEqual({ chargeId: 'ch_123' })
  })

  it('RetryConfig has expected shape', () => {
    const config: RetryConfig = {
      maxAttempts: 3,
      backoff: 'exponential',
    }
    expect(config.maxAttempts).toBe(3)
  })
})

import { describe, it, expect } from 'vitest'
import {
  createWorkflow,
  createEngine,
  ReflowError,
  ConfigError,
  WorkflowNotFoundError,
  DuplicateWorkflowError,
  DuplicateStepError,
  ValidationError,
  IdempotencyConflictError,
  SerializationError,
  StepTimeoutError,
  RunCancelledError,
  LeaseExpiredError,
} from '../index'

describe('public API', () => {
  it('exports createWorkflow', () => {
    expect(createWorkflow).toBeTypeOf('function')
  })

  it('exports createEngine', () => {
    expect(createEngine).toBeTypeOf('function')
  })

  it('exports typed error classes with correct hierarchy', () => {
    const errors = [
      ConfigError,
      WorkflowNotFoundError,
      DuplicateWorkflowError,
      DuplicateStepError,
      ValidationError,
      IdempotencyConflictError,
      SerializationError,
      StepTimeoutError,
      RunCancelledError,
      LeaseExpiredError,
    ]

    for (const ErrorClass of errors) {
      expect(ErrorClass.prototype).toBeInstanceOf(ReflowError)
      expect(ErrorClass.prototype).toBeInstanceOf(Error)
    }
  })

  it('error instances carry structured context', () => {
    const wfErr = new WorkflowNotFoundError('my-workflow')
    expect(wfErr.workflowName).toBe('my-workflow')
    expect(wfErr).toBeInstanceOf(ReflowError)
    expect(wfErr.message).toBe('Workflow "my-workflow" not found')

    const idempErr = new IdempotencyConflictError('my-workflow', 'key-1')
    expect(idempErr.workflowName).toBe('my-workflow')
    expect(idempErr.idempotencyKey).toBe('key-1')

    const valErr = new ValidationError('bad input', [{ message: 'required' }])
    expect(valErr.issues).toEqual([{ message: 'required' }])

    const timeoutErr = new StepTimeoutError(5000)
    expect(timeoutErr.timeoutMs).toBe(5000)

    const cancelErr = new RunCancelledError('run-123')
    expect(cancelErr.runId).toBe('run-123')

    const leaseErr = new LeaseExpiredError('run-456')
    expect(leaseErr.runId).toBe('run-456')

    const serErr = new SerializationError('bad value', '$.foo')
    expect(serErr.path).toBe('$.foo')

    const dupStepErr = new DuplicateStepError('my-wf', 'step-1')
    expect(dupStepErr.workflowName).toBe('my-wf')
    expect(dupStepErr.stepName).toBe('step-1')
  })
})

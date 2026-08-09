import { describe, it, expect, vi } from 'vitest'
import { z } from 'zod'
import { createWorkflow, createEngine } from '../../index'
import { MemoryStorage } from '../../storage/memory'
import {
  BranchFailedError,
  ConfigError,
  DuplicateStepError,
  DuplicateWorkflowError,
  IdempotencyConflictError,
  ParallelCompleteError,
  RunCancelledError,
  SerializationError,
  StepFailedError,
  ValidationError,
  WorkflowNotFoundError,
  HookError,
  InternalError,
  LeaseExpiredError,
  ReflowError,
  StepTimeoutError,
  ThrownValueError,
  WaitTimeoutError,
  assertNever,
  toError,
} from '../errors'

describe('error discriminants', () => {
  it('carries a stable code on every error', () => {
    expect(new ConfigError('bad').code).toBe('CONFIG')
    expect(new DuplicateStepError('wf', 'a').code).toBe('DUPLICATE_STEP')
    expect(new StepTimeoutError(50).code).toBe('STEP_TIMEOUT')
    expect(new WaitTimeoutError('approval', 50).code).toBe('WAIT_TIMEOUT')
    expect(new LeaseExpiredError('run-1').code).toBe('LEASE_EXPIRED')
    expect(new HookError('onRunStart hook').code).toBe('HOOK')
  })

  it('keeps instanceof working through the hierarchy', () => {
    const error = new DuplicateStepError('wf', 'a')

    expect(error).toBeInstanceOf(DuplicateStepError)
    expect(error).toBeInstanceOf(ReflowError)
    expect(error).toBeInstanceOf(Error)
    expect(error.name).toBe('DuplicateStepError')
  })

  it('exposes context as typed fields rather than only in the message', () => {
    const error = new DuplicateStepError('checkout', 'charge')

    expect(error.workflowName).toBe('checkout')
    expect(error.stepName).toBe('charge')
  })
})

describe('toError', () => {
  it('returns an Error unchanged', () => {
    const original = new RangeError('out of range')

    expect(toError(original)).toBe(original)
  })

  it('preserves a thrown non-Error value on both message and cause', () => {
    const error = toError('raw string error')

    expect(error).toBeInstanceOf(ThrownValueError)
    expect(error.message).toBe('raw string error')
    expect(error.cause).toBe('raw string error')
  })

  it('keeps a thrown object recoverable rather than flattening it', () => {
    const thrown = { status: 503, retryable: true }
    const error = toError(thrown)

    expect(error).toBeInstanceOf(ThrownValueError)
    expect((error as ThrownValueError).value).toStrictEqual(thrown)
  })
})

describe('serialization', () => {
  it('renders discriminant, context, and flattened cause as JSON', () => {
    const cause = new StepTimeoutError(50)
    const error = new HookError('onStepComplete hook', { cause })

    expect(error.toJSON()).toStrictEqual({
      name: 'HookError',
      code: 'HOOK',
      message: 'onStepComplete hook threw',
      context: { source: 'onStepComplete hook' },
      cause: {
        name: 'StepTimeoutError',
        code: 'STEP_TIMEOUT',
        message: 'Step timed out after 50ms',
        context: { timeoutMs: 50 },
      },
    })
  })

  it('omits cause when there is none', () => {
    expect(new ConfigError('bad').toJSON()).toStrictEqual({
      name: 'ConfigError',
      code: 'CONFIG',
      message: 'bad',
      context: {},
    })
  })

  it('survives a round trip through JSON.stringify', () => {
    const parsed: unknown = JSON.parse(JSON.stringify(new WaitTimeoutError('approval', 50)))

    expect(parsed).toStrictEqual({
      name: 'WaitTimeoutError',
      code: 'WAIT_TIMEOUT',
      message: 'Timed out after 50ms waiting for event "approval"',
      context: { eventName: 'approval', timeoutMs: 50 },
    })
  })
})

describe('assertNever', () => {
  it('throws an InternalError naming the unhandled value', () => {
    // Cast is the point of the test: proving the runtime guard still fires when
    // the compile-time exhaustiveness check has been subverted at a boundary.
    expect(() => assertNever('surprise' as never, 'execution unit')).toThrowError(InternalError)
    expect(() => assertNever('surprise' as never, 'execution unit')).toThrowError(
      /Unhandled execution unit: "surprise"/,
    )
  })
})

describe('observer failures reach onError', () => {
  it('reports a throwing lifecycle hook instead of swallowing it', async () => {
    const onError = vi.fn()

    const wf = createWorkflow({ name: 'hook-throws', input: z.object({}) })
      .step('a', async () => ({ ok: true }))

    const engine = createEngine({
      storage: new MemoryStorage(),
      workflows: [wf],
      hooks: {
        onStepComplete: () => {
          throw new Error('metrics backend down')
        },
        onError,
      },
    })

    const run = await engine.enqueue('hook-throws', {})
    await engine.tick()

    // The run is unaffected — observers cannot change its outcome.
    expect((await engine.getRunStatus(run.id))?.run.status).toBe('completed')

    const reported = onError.mock.calls.map(([error]) => error as HookError)
    const hookError = reported.find((error) => error.source === 'stepComplete hook')

    expect(hookError).toBeInstanceOf(HookError)
    expect(hookError?.code).toBe('HOOK')
    expect((hookError?.cause as Error).message).toBe('metrics backend down')
  })

  it('reports a throwing onFailure handler instead of swallowing it', async () => {
    const onError = vi.fn()

    const wf = createWorkflow({ name: 'compensation-throws', input: z.object({}) })
      .step('a', async () => {
        throw new Error('step exploded')
      })
      .onFailure(async () => {
        throw new Error('compensation exploded')
      })

    const engine = createEngine({
      storage: new MemoryStorage(),
      workflows: [wf],
      hooks: { onError },
    })

    const run = await engine.enqueue('compensation-throws', {})
    await engine.tick()

    expect((await engine.getRunStatus(run.id))?.run.status).toBe('failed')

    const reported = onError.mock.calls.map(([error]) => error as HookError)
    const failureError = reported.find((error) => error.source === 'onFailure handler')

    expect(failureError).toBeInstanceOf(HookError)
    expect((failureError?.cause as Error).message).toBe('compensation exploded')
  })
})

describe('every error serializes its own context', () => {
  // Rule: errors must be safe to log and carry machine-readable context. Each
  // entry pins the exact JSON a caller receives, so adding a field to an error
  // without deciding whether it belongs in logs fails here.
  const cases: ReadonlyArray<readonly [string, ReflowError, Record<string, unknown>]> = [
    ['ConfigError', new ConfigError('bad'), {}],
    ['WorkflowNotFoundError', new WorkflowNotFoundError('wf'), { workflowName: 'wf' }],
    ['DuplicateWorkflowError', new DuplicateWorkflowError('wf'), { workflowName: 'wf' }],
    ['DuplicateStepError', new DuplicateStepError('wf', 'a'), { workflowName: 'wf', stepName: 'a' }],
    ['ParallelCompleteError', new ParallelCompleteError('a'), { stepName: 'a' }],
    ['ValidationError', new ValidationError('bad', [{ message: 'required' }]), { issues: [{ message: 'required' }] }],
    ['IdempotencyConflictError', new IdempotencyConflictError('wf', 'k'), { workflowName: 'wf', idempotencyKey: 'k' }],
    ['SerializationError', new SerializationError('bad', 'output.a'), { path: 'output.a' }],
    ['StepTimeoutError', new StepTimeoutError(50), { timeoutMs: 50 }],
    ['WaitTimeoutError', new WaitTimeoutError('e', 50), { eventName: 'e', timeoutMs: 50 }],
    ['StepFailedError', new StepFailedError('a', 3), { stepName: 'a', attempts: 3 }],
    ['BranchFailedError', new BranchFailedError('b', new Error('boom'), 1), { branchName: 'b', attempts: 1 }],
    ['HookError', new HookError('onRunStart hook'), { source: 'onRunStart hook' }],
    ['ThrownValueError', new ThrownValueError(42), { value: 42 }],
    ['InternalError', new InternalError('invariant'), {}],
    ['RunCancelledError', new RunCancelledError('run-1'), { runId: 'run-1' }],
    ['LeaseExpiredError', new LeaseExpiredError('run-1'), { runId: 'run-1' }],
  ]

  it.each(cases)('%s exposes its context and a stable code', (name, error, context) => {
    const json = error.toJSON()

    expect(json.name).toBe(name)
    expect(json.code).toBe(error.code)
    expect(json.context).toStrictEqual(context)
    expect(json.message).toBe(error.message)
  })

  it('gives every error class a distinct code', () => {
    const codes = cases.map(([, error]) => error.code)

    expect(new Set(codes).size).toBe(codes.length)
  })
})

import { describe, it, expect, expectTypeOf } from 'vitest'
import { z } from 'zod'
import { createWorkflow, createEngine } from '../../index'
import { testEngine } from '../../test/index'
import { MemoryStorage } from '../../storage/memory'
import type { Workflow, FailureContext, StepContext } from '../../index'

describe('type safety', () => {
  const orderWorkflow = createWorkflow({
    name: 'order',
    input: z.object({ orderId: z.string(), amount: z.number() }),
  })
    .step('charge', async ({ input }) => ({ chargeId: `ch_${input.orderId}` }))
    .step('fulfill', async ({ prev }) => ({ trackingNumber: `TRK_${prev.chargeId}` }))

  const mathWorkflow = createWorkflow({
    name: 'math',
    input: z.object({ x: z.number() }),
  })
    .step('double', async ({ input }) => ({ doubled: input.x * 2 }))
    .step('add-ten', async ({ prev }) => ({ result: prev.doubled + 10 }))

  it('workflow name is a string literal type', () => {
    expectTypeOf(orderWorkflow.name).toEqualTypeOf<'order'>()
    expectTypeOf(mathWorkflow.name).toEqualTypeOf<'math'>()
  })

  it('workflow preserves input type through chaining', () => {
    type OrderInput = { orderId: string; amount: number }
    const wf = createWorkflow({
      name: 'test',
      input: z.object({ orderId: z.string(), amount: z.number() }),
    }).step('a', async ({ input }) => {
      expectTypeOf(input).toEqualTypeOf<OrderInput>()
      return { ok: true }
    })
    expect(wf.steps).toHaveLength(1)
  })

  it('prev type flows from previous step output', () => {
    createWorkflow({
      name: 'chain-test',
      input: z.object({ x: z.number() }),
    })
      .step('first', async ({ input }) => ({ a: input.x + 1 }))
      .step('second', async ({ prev, signal }) => {
        expectTypeOf(prev).toEqualTypeOf<{ a: number }>()
        expectTypeOf(signal).toEqualTypeOf<AbortSignal>()
        return { b: prev.a * 2 }
      })
  })

  it('first step prev is undefined', () => {
    createWorkflow({
      name: 'first-step',
      input: z.object({}),
    }).step('first', async ({ prev }) => {
      expectTypeOf(prev).toEqualTypeOf<undefined>()
      return {}
    })
  })

  it('engine.enqueue is type-safe on workflow name and input', () => {
    const storage = new MemoryStorage()
    const engine = createEngine({ storage, workflows: [orderWorkflow, mathWorkflow] })

    // Valid calls compile
    expectTypeOf(engine.enqueue).toBeCallableWith('order', { orderId: 'x', amount: 1 })
    expectTypeOf(engine.enqueue).toBeCallableWith('math', { x: 5 })
    expectTypeOf(engine.enqueue).toBeCallableWith('math', { x: 5 }, { idempotencyKey: 'dedupe-key' })

    // Only registered workflow names are accepted as first argument
    expectTypeOf(engine.enqueue).parameter(0).toEqualTypeOf<'order' | 'math'>()
  })

  it('onFailure handler receives typed input', () => {
    createWorkflow({
      name: 'typed-fail',
      input: z.object({ userId: z.string() }),
    })
      .step('a', async () => ({ ok: true }))
      .onFailure(async (ctx) => {
        expectTypeOf(ctx).toEqualTypeOf<FailureContext<{ userId: string }>>()
      })
  })

  it('testEngine.run is type-safe on workflow name and input', () => {
    const te = testEngine({ workflows: [orderWorkflow, mathWorkflow] })

    // Only valid workflow names are accepted
    expectTypeOf(te.run).parameter(0).toEqualTypeOf<'order' | 'math'>()

    // Valid calls compile
    expectTypeOf(te.run).toBeCallableWith('math', { x: 5 })
    expectTypeOf(te.run).toBeCallableWith('order', { orderId: 'x', amount: 1 })
  })

  it('testEngine returns typed step results', async () => {
    const te = testEngine({ workflows: [mathWorkflow] })
    const result = await te.run('math', { x: 5 })

    expectTypeOf(result.steps.double.status).toEqualTypeOf<'completed' | 'failed'>()
    expectTypeOf(result.steps.double.error).toEqualTypeOf<string | null>()

    if (result.steps.double.status === 'completed') {
      expectTypeOf(result.steps.double.output).toEqualTypeOf<{ doubled: number }>()
    } else {
      expectTypeOf(result.steps.double.output).toEqualTypeOf<null>()
    }

    if (result.steps['add-ten'].status === 'completed') {
      expectTypeOf(result.steps['add-ten'].output).toEqualTypeOf<{ result: number }>()
    } else {
      expectTypeOf(result.steps['add-ten'].output).toEqualTypeOf<null>()
    }

    expect(result.status).toBe('completed')
    expect(result.steps.double.output).toEqual({ doubled: 10 })
  })

  it('Workflow type params are accessible via InferInput and InferSteps', () => {
    type W = typeof orderWorkflow

    // The workflow type carries all the info
    expectTypeOf<W>().toMatchTypeOf<
      Workflow<'order', { orderId: string; amount: number }>
    >()
  })

  it('StepContext exposes signal, complete, and steps alongside input and prev', () => {
    expectTypeOf<StepContext<{ value: string }, { ok: boolean }>>().toEqualTypeOf<{
      input: { value: string }
      prev: { ok: boolean }
      signal: AbortSignal
      complete: (value?: import('../../core/types').PersistedValue) => never
      steps: Record<string, import('../../core/types').PersistedValue>
    }>()
  })

  it('steps context is typed with previous step outputs only', () => {
    createWorkflow({ name: 'steps-typed', input: z.object({ x: z.number() }) })
      .step('a', async ({ steps }) => {
        expectTypeOf(steps).toEqualTypeOf<{}>()
        return { fromA: 1 }
      })
      .step('b', async ({ steps }) => {
        expectTypeOf(steps.a).toEqualTypeOf<{ fromA: number }>()
        return { fromB: 2 }
      })
      .step('c', async ({ steps }) => {
        expectTypeOf(steps.a).toEqualTypeOf<{ fromA: number }>()
        expectTypeOf(steps.b).toEqualTypeOf<{ fromB: number }>()
        return {}
      })
  })
})

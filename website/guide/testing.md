# Testing

Reflow ships a test helper that runs a workflow in one pass, in memory, and returns typed results — no polling, no leases, no timing.

```typescript
import { testEngine } from 'reflow-ts/test'

const te = testEngine({ workflows: [orderWorkflow] })

const result = await te.run('order-fulfillment', { orderId: 'test', amount: 100 })

result.status              // 'completed' | 'failed'
result.output              // what the run produced, undefined if it failed
result.steps.charge.status // 'completed' | 'failed'
result.steps.charge.output // { chargeId: string } once status is 'completed', null if it failed
result.steps.charge.error  // string if it failed, null otherwise
```

`run()` enqueues the workflow into an in-memory engine, runs a single `tick()`, and returns the outcome. Step results are keyed by step name with their outputs typed from the workflow definition. For a [workflow written as a function](/guide/functions), `output` is typed from what the function returns. A step with [`when`](/api/workflow) that did not run reports `status: 'skipped'`.

Each step result is a union on `status`, so check `status` before reading `output` — TypeScript then types it as the step's output instead of `… | null`.

A workflow that suspends can't finish in one tick: a `.sleep()` that hasn't elapsed, or a `.waitForEvent()` whose event hasn't arrived. For those, `run()` throws `TestRunIncompleteError` carrying the run's `status`. Test them with a real engine instead (see below).

## In a test suite

```typescript
import { describe, it, expect } from 'vitest'
import { testEngine } from 'reflow-ts/test'

describe('order workflow', () => {
  it('charges and fulfills', async () => {
    const te = testEngine({ workflows: [orderWorkflow] })
    const result = await te.run('order-fulfillment', { orderId: 'ORD_1', amount: 100 })

    expect(result.status).toBe('completed')
    expect(result.steps.charge.output?.chargeId).toBeTruthy()
    expect(result.steps.fulfill.output?.trackingNumber).toBeTruthy()
  })
})
```

## Testing failures and behavior

Because the test engine runs real workflow logic, you can assert failure handling, retries, and early completion by controlling your step dependencies (mock the API, throw on the first call, etc.) and inspecting `result.status` and per-step `status` / `error`.

For tests that need full engine behavior — sleeps, waits, polling, concurrency, cancellation, crash recovery — construct a real `createEngine` with [`MemoryStorage`](/guide/storage) and drive it with `tick()` and `sendEvent()`.

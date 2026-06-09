# Testing

Reflow ships a test helper that runs a workflow to completion synchronously and returns typed results — no polling, no leases, no timing.

```typescript
import { testEngine } from 'reflow-ts/test'

const te = testEngine({ workflows: [orderWorkflow] })

const result = await te.run('order-fulfillment', { orderId: 'test', amount: 100 })

result.status              // 'completed' | 'failed'
result.steps.charge.output // { chargeId: string } — fully typed
result.steps.charge.status // 'completed' | 'failed'
result.steps.charge.error  // string | null
```

`run()` enqueues the workflow into an in-memory engine, runs a single `tick()`, and returns the outcome. Step results are keyed by step name with their outputs typed from the workflow definition.

## In a test suite

```typescript
import { describe, it, expect } from 'vitest'
import { testEngine } from 'reflow-ts/test'

describe('order workflow', () => {
  it('charges and fulfills', async () => {
    const te = testEngine({ workflows: [orderWorkflow] })
    const result = await te.run('order-fulfillment', { orderId: 'ORD_1', amount: 100 })

    expect(result.status).toBe('completed')
    expect(result.steps.charge.output.chargeId).toBeTruthy()
    expect(result.steps.fulfill.output.trackingNumber).toBeTruthy()
  })
})
```

## Testing failures and behavior

Because the test engine runs real workflow logic, you can assert failure handling, retries, and early completion by controlling your step dependencies (mock the API, throw on the first call, etc.) and inspecting `result.status` and per-step `status` / `error`.

For tests that need full engine behavior — polling, concurrency, cancellation, crash recovery — construct a real `createEngine` with [`MemoryStorage`](/guide/storage) and drive it with `tick()`.

# Early Completion

A step can finish the workflow early by calling `complete()`, skipping all remaining steps.

```typescript
const workflow = createWorkflow({ name: 'conditional', input: schema })
  .step('check', async ({ input, complete }) => {
    if (!input.eligible) {
      return complete({ reason: 'ineligible' })
    }
    return { eligible: true }
  })
  .step('process', async ({ prev }) => {
    // Only runs if 'check' did not call complete()
    return await doWork(prev)
  })
```

The optional value passed to `complete()` is persisted as that step's result and is visible via [`getRunStatus()`](/api/engine). Calling `complete()` with no argument finishes the run with an `undefined` step output.

::: tip Return the call
Write `return complete(...)`. `complete()` throws internally to unwind the handler, so anything after it won't run — returning it makes that explicit and keeps the types honest.
:::

## Crash-safe

Early completion is durable. If the engine crashes **after** saving the early-complete step but **before** marking the run completed, recovery detects the early-complete marker and finishes the run without re-executing later steps.

## Not in parallel branches

`complete()` only makes sense in sequential context. Calling it inside a [parallel branch](/guide/parallel) throws [`ParallelCompleteError`](/guide/error-handling) — there's no well-defined "rest of the workflow" to skip from within a concurrent group.

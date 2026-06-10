# Workflows

A workflow is a named sequence of steps with a validated input schema. Any [Standard Schema](https://github.com/standard-schema/standard-schema)-compatible library works (Zod, Valibot, ArkType, …).

```typescript
import { createWorkflow } from 'reflow-ts'
import { z } from 'zod'

const workflow = createWorkflow({
  name: 'send-welcome',
  input: z.object({ userId: z.string(), email: z.email() }),
})
  .step('create-account', async ({ input }) => {
    // input is typed as { userId: string; email: string }
    return { accountId: await createAccount(input.userId) }
  })
  .step('send-email', async ({ prev, input, signal }) => {
    // prev is typed as { accountId: string }
    await sendEmail(input.email, `Welcome! Your account: ${prev.accountId}`, { signal })
  })
```

## The step context

Each `.step()` handler receives a single context object:

| Field | Description |
|---|---|
| `input` | The validated workflow input. The same value for every step in the run. |
| `prev` | The return value of the previous step (`undefined` for the first step). |
| `steps` | Typed access to **all** previously completed step results by name, e.g. `steps.charge.chargeId`. |
| `signal` | An `AbortSignal`, aborted when the run is cancelled, its lease is lost, or the step times out. |
| `complete(value?)` | Finish the workflow early, skipping remaining steps. See [Early Completion](/guide/early-completion). |

## `prev` vs `steps`

`prev` is the most recent step's output. `steps` is the full history — reach any earlier step directly without forwarding data through every intermediate step:

```typescript
const workflow = createWorkflow({ name: 'pipeline', input: schema })
  .step('fetch', async ({ input }) => {
    return { url: input.url, body: await fetchPage(input.url) }
  })
  .step('parse', async ({ prev }) => {
    return { title: extractTitle(prev.body), links: extractLinks(prev.body) }
  })
  .step('save', async ({ steps }) => {
    // Access any previous step directly — no forwarding needed
    await save(steps.fetch.url, steps.parse.title, steps.parse.links)
  })
```

The `steps` object is a **frozen, deep-cloned snapshot**. Mutating `prev` in one step never affects what later steps see through `steps`.

## Persistable values

Step inputs and outputs are persisted, so they must be plain data: objects, arrays, strings, numbers, booleans, `null`, `undefined`, and `Date`. Returning a non-serializable value (a function, a class instance, `NaN`) throws a [`SerializationError`](/guide/error-handling).

## The builder is immutable

Each `.step()` returns a **new** workflow instance, so you can branch safely:

```typescript
const base = createWorkflow({ name: 'base', input: z.object({}) })
const withLogging = base.step('log', async () => { /* … */ })
const withMetrics = base.step('metric', async () => { /* … */ })
// base, withLogging, and withMetrics are independent
```

Step names must be unique within a workflow — reusing one throws [`DuplicateStepError`](/guide/error-handling).

## Next

- [The Engine](/guide/engine) — run your workflows.
- [Retry & Timeouts](/guide/retry), [Parallel Steps](/guide/parallel), [Early Completion](/guide/early-completion).

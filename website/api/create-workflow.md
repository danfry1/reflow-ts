# `createWorkflow(config)`

Creates a new, immutable workflow builder. To write the workflow as a function instead, pass it as a second argument: see [`createWorkflow(config, handler)`](#createworkflow-config-handler).

```typescript
import { createWorkflow } from 'reflow-ts'
import { z } from 'zod'

const workflow = createWorkflow({
  name: 'process-content',
  input: z.object({ url: z.string() }),
})
```

| Parameter | Type | Description |
|---|---|---|
| `config.name` | `string` | Unique workflow name. Captured as a string-literal type. |
| `config.input` | `StandardSchemaV1` | Any [Standard Schema](https://github.com/standard-schema/standard-schema)-compatible schema used to validate input. |
| `config.label` | `(input) => string` | Optional. Names each run for people, from its typed input — an order number, a user id — wherever runs are listed, such as the [dashboard](/guide/dashboard#naming-runs). Without it a run is shown by its idempotency key, or else its id. |
| `config.concurrency` | `number` | Optional. The most runs of this workflow running at once, across every process sharing the storage; the rest wait as `pending`. See [limiting a workflow](/guide/concurrency#limiting-one-workflow). |

Returns a [`Workflow`](/api/workflow) with `.step()`, `.parallel()`, `.onFailure()`, and `.parseInput()`.

The builder is **immutable** — every method returns a new `Workflow`, so a base workflow can be branched without the branches affecting each other. See [Workflows](/guide/workflows).

## `createWorkflow(config, handler)` <Badge type="warning" text="beta" />

Creates a workflow written as a function. See [Workflows as Functions](/guide/functions).

```typescript
const workflow = createWorkflow({
  name: 'process-content',
  input: z.object({ url: z.string() }),
  events: { approved: z.object({ by: z.string() }) },
}, async ({ input, step }) => {
  const page = await step.run('scrape', async () => scrape(input.url))
  await step.waitForEvent('approved')
  return await step.run('publish', async () => publish(page))
})
```

It takes the same `name`, `input`, `label`, and `concurrency` as above, and:

| Parameter | Type | Description |
|---|---|---|
| `config.events` | `Record<string, StandardSchemaV1>` | Optional. The events `step.waitForEvent()` may wait for, each with the schema `sendEvent` validates its payload against. |
| `config.onFailure` | `({ error, stepName, input }) => Promise<void>` | Optional. Called once a run has failed. `stepName` is empty when the function's own code threw. |
| `config.maxOperations` | `number` | Optional. The most operations one run may start (default 10,000). A run replays every earlier operation each time it resumes; past the limit it fails with `ConfigError`. |
| `handler` | `({ input, step, runId }) => Promise<T>` | The workflow. What it returns is the run's output. |

`step` has these operations. Each takes a name unique within the run:

| Operation | Resolves to | Description |
|---|---|---|
| `step.run(name, handler \| config)` | The handler's result | A durable step. `config` takes `handler`, `retry` (`{ maxAttempts, backoff?, initialDelay? }`), `timeout`, and `cache` (with `key: () => value`). Rejects with `StepFailedError` once every attempt has failed; any other error fails the run. |
| `step.sleep(name, duration)` | `undefined` | Durably pause for milliseconds or a duration such as `'7d'`. |
| `step.sleepUntil(name, time)` | `undefined` | Durably pause until a `Date` or epoch time. |
| `step.waitForEvent(name, { event?, timeout? })` | The payload, or `null` on timeout | Wait for a declared event — `event`, when the wait has a name of its own, or else `name`. Several may be pending at once. |

Returns a `FunctionWorkflow` (`kind: 'function'`), which has `.parseInput()` and no builder methods. `InferOutput<typeof workflow>` is what its function returns, and `InferEvents<typeof workflow>` its events' payload types.

## `workflow.parseInput(input)`

Validates an unknown value against the workflow's input schema and returns the typed, parsed value. Throws [`ValidationError`](/api/errors) if validation fails. The engine calls this for you on `enqueue()`; it's exposed for validating input at your own boundaries.

```typescript
const parsed = workflow.parseInput(req.body) // typed as the schema output, or throws
```

Async schema validation is not supported — a schema that returns a `Promise` throws a `TypeError`.

# `createEngine(config)`

Creates an engine that executes workflows against a storage backend.

```typescript
import { createEngine } from 'reflow-ts'

const engine = createEngine({
  storage,
  workflows: [orderWorkflow, emailWorkflow],
})
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `storage` | `StorageAdapter` | required | Backend for runs and step results |
| `workflows` | `readonly AnyWorkflow[]` | required | Workflows to register, chained or [written as a function](/api/create-workflow#createworkflow-config-handler) |
| `hooks` | `EngineHooks` | — | Lifecycle hooks (may be sync or `async`) |
| `concurrency` | `number` | `1` | Maximum runs executing at once |
| `runLeaseDurationMs` | `number` | `30000` | How long a claimed run stays `running` before another engine may reclaim it |
| `heartbeatIntervalMs` | `number` | `lease / 3` | How often the active worker renews its lease |

Returns an [`Engine`](/api/engine). Registering the same workflow name twice throws [`DuplicateWorkflowError`](/api/errors). Throws [`ConfigError`](/api/errors) when:

- `concurrency` is not a positive integer, `runLeaseDurationMs` or `heartbeatIntervalMs` is not a positive number, or `heartbeatIntervalMs` is not smaller than `runLeaseDurationMs`;
- a workflow sets `concurrency` but the storage does not set `supportsConcurrencyLimits`;
- a chained workflow [caches](/guide/caching) a step but the storage does not implement the cache methods.

## `EngineHooks`

All hooks are optional and receive the matching [`EngineEvent`](/api/events), `type` included. They may be synchronous or `async` (awaited). A throwing or rejecting hook never affects engine state: its error goes to `onError` as a [`HookError`](/api/errors).

| Hook | Payload |
|---|---|
| `onRunStart` | `{ runId, workflow }` |
| `onStepStart` | `{ runId, workflow, stepName }` |
| `onStepSkipped` | `{ runId, workflow, stepName }` |
| `onStepComplete` | `{ runId, workflow, stepName, output, attempts, cached? }` — `cached` is `true` when the output was reused from a [cached](/guide/caching) result |
| `onRunComplete` | `{ runId, workflow, output }` |
| `onRunFailed` | `{ runId, workflow, stepName, error }` |
| `onError` | `(error: Error)` — background failures: scheduled enqueues, poll and claim cycles, and a `HookError` when a hook, stream delivery, or `onFailure` handler throws |

See [Hooks](/guide/hooks) for behavior and [Events & Streams](/api/events) for the event shapes.

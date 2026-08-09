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
| `workflows` | `Workflow[]` | required | Workflows to register |
| `hooks` | `EngineHooks` | — | Lifecycle hooks (may be sync or `async`) |
| `concurrency` | `number` | `1` | Runs processed in parallel per tick |
| `runLeaseDurationMs` | `number` | `30000` | How long a claimed run stays `running` before another engine may reclaim it |
| `heartbeatIntervalMs` | `number` | `lease / 3` | How often the active worker renews its lease |

Returns an [`Engine`](/api/engine). Registering the same workflow name twice throws [`DuplicateWorkflowError`](/api/errors); invalid numeric config throws [`ConfigError`](/api/errors) (`concurrency` must be a positive integer, `heartbeatIntervalMs` must be smaller than `runLeaseDurationMs`).

## `EngineHooks`

All hooks are optional and receive an [`EngineEvent`](/api/events)-shaped payload. They may be synchronous or `async` (awaited). A throwing or rejecting hook never affects engine state.

| Hook | Payload |
|---|---|
| `onRunStart` | `{ runId, workflow }` |
| `onStepStart` | `{ runId, workflow, stepName }` |
| `onStepSkipped` | `{ runId, workflow, stepName }` |
| `onStepComplete` | `{ runId, workflow, stepName, output, attempts }` |
| `onRunComplete` | `{ runId, workflow, output }` |
| `onRunFailed` | `{ runId, workflow, stepName, error }` |
| `onError` | `(error: Error)` — background failures (scheduled enqueues, poll cycles) |

See [Hooks](/guide/hooks) for behavior and [Events & Streams](/api/events) for the event shapes.

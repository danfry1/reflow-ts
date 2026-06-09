# Events & Streams

Types behind [Hooks](/guide/hooks) and [`engine.stream()`](/guide/streaming).

## `EngineEvent`

A discriminated union on `type`. Every variant carries `runId` and `workflow`.

```typescript
type EngineEvent =
  | { type: 'runStart';     runId: string; workflow: string }
  | { type: 'stepStart';    runId: string; workflow: string; stepName: string }
  | { type: 'stepComplete'; runId: string; workflow: string; stepName: string; output: PersistedValue; attempts: number }
  | { type: 'runComplete';  runId: string; workflow: string; output: PersistedValue }
  | { type: 'runFailed';    runId: string; workflow: string; stepName: string; error: Error }
```

| Variant | Fires |
|---|---|
| `runStart` | A run begins executing (also on crash-recovery resume) |
| `stepStart` | Before each step (or parallel branch) runs |
| `stepComplete` | After a step's result is persisted |
| `runComplete` | A run finishes successfully; `output` is the final result |
| `runFailed` | A run fails; `stepName` is the offending step, `error` the cause |

Checking `event.type` narrows the rest of the fields:

```typescript
for await (const event of engine.stream()) {
  if (event.type === 'runComplete') {
    event.output // narrowed — available here
  }
}
```

## `EngineEventOf<T>`

Helper to extract a single variant by its `type`:

```typescript
type Completed = EngineEventOf<'runComplete'>
// { type: 'runComplete'; runId: string; workflow: string; output: PersistedValue }
```

## `StreamOptions`

```typescript
interface StreamOptions {
  bufferSize?: number // default Infinity; 0 = rendezvous; n = up to n buffered before the engine pauses
}
```

## `ResultStream`

```typescript
interface ResultStream<E extends EngineEvent = EngineEvent>
  extends AsyncIterableIterator<E> {
  [Symbol.asyncDispose](): Promise<void>
}
```

An `AsyncIterableIterator` (works with `for await`) that is also `AsyncDisposable` (works with `await using`). Disposing — by breaking out of the loop, `await using` scope exit, or `engine.stop()` — unsubscribes the stream from the engine. Event payloads are defensive clones, isolated from engine state.

## `EngineHooks`

The hook callback shapes, mirroring the events above. See [createEngine](/api/create-engine#enginehooks).

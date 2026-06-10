# Streaming Results

[Hooks](/guide/hooks) are push-based callbacks. When you'd rather **pull** results — to apply backpressure, rate-limit, or feed a producer/consumer loop — use `engine.stream()`. It returns a pull-based async iterable of execution events.

```typescript
const engine = createEngine({ storage, workflows: [importWorkflow] })
await engine.start()

for await (const event of engine.stream()) {
  if (event.type === 'runComplete') {
    await pipeline.push(event.output) // process each result as it lands
  }
}
```

Each call to `engine.stream()` returns an **independent** stream.

## Events

The yielded [`EngineEvent`](/api/events) is a discriminated union on `type`, so TypeScript narrows `event.output`, `event.error`, etc. once you check `event.type`:

```typescript
for await (const event of engine.stream()) {
  switch (event.type) {
    case 'stepComplete': /* event.stepName, event.output, event.attempts */ break
    case 'runComplete':  /* event.output */ break
    case 'runFailed':    /* event.stepName, event.error */ break
  }
}
```

The variants are `runStart`, `stepStart`, `stepComplete`, `runComplete`, and `runFailed` — all carrying `runId` and `workflow`. Across a multi-workflow engine, one stream sees events from every run.

## Backpressure

By default the stream buffers without bound and never slows the engine. Pass `bufferSize` to pace the engine against a slow consumer — the engine pauses once the buffer fills and resumes as you pull:

```typescript
// The engine won't start the next unit of work until you consume the last one.
for await (const event of engine.stream({ bufferSize: 1 })) {
  await slowlyHandle(event)
}
```

- `bufferSize: Infinity` (default) — never blocks the engine.
- `bufferSize: n` — at most `n` events buffered before the engine pauses.
- `bufferSize: 0` — strict rendezvous: every event waits for a pending pull.

::: warning Consume or dispose finite-buffer streams
A finite-buffer stream that you create but never consume **and** never dispose will pause the engine. Iterate it, dispose it, or stick with the default unbounded buffer.
:::

## Cleanup is automatic

Breaking out of the loop unsubscribes the stream. So does `await using`:

```typescript
await using stream = engine.stream()
for await (const event of stream) {
  if (event.type === 'runComplete') break // disposed on scope exit
}
```

`engine.stop()` ends every open stream, so consumer loops terminate cleanly on shutdown. Cancelling a run or losing a lease also releases a producer paused on backpressure — a stalled consumer can't wedge the engine.

## Isolation

Event payloads handed to the stream are **defensive clones** — mutating `event.output` in a consumer never affects engine state, the persisted result, or any other stream or hook.

# Concurrency

By default the engine processes one run at a time. Raise `concurrency` to process multiple runs in parallel per tick:

```typescript
const engine = createEngine({
  storage,
  workflows: [orderWorkflow],
  concurrency: 5, // process up to 5 runs in parallel per tick (default: 1)
})
```

With `concurrency: 5`, each [tick](/guide/engine#polling-vs-manual-ticks) claims up to 5 pending (or reclaimable) runs and executes them concurrently.

## What runs in parallel

- **Different runs** run in parallel, up to `concurrency`.
- **Steps within a single run** still execute **sequentially** — `prev` flows from one step to the next. To parallelize work *inside* a run, use [`.parallel()`](/guide/parallel).

Each claimed run is leased and heartbeated independently, so one slow run doesn't endanger the others' leases.

## Picking a value

`concurrency` bounds how many runs a single engine instance works on at once. Size it to your workload — I/O-bound steps (API calls, DB writes) tolerate higher concurrency than CPU-bound ones. Because claims are leased, you can also run **multiple engine instances** against the same storage; each claims its own runs and they share the work safely.

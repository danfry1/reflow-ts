# Concurrency

By default the engine processes one run at a time. Raise `concurrency` to execute several runs at once:

```typescript
const engine = createEngine({
  storage,
  workflows: [orderWorkflow],
  concurrency: 5, // execute up to 5 runs at once (default: 1)
})
```

With `concurrency: 5`, the engine keeps up to 5 pending (or reclaimable) runs executing at once. Slots are independent: when a run finishes, its slot is refilled straight away, so one slow run never holds the others idle. A manual [`tick()`](/guide/engine#polling-vs-manual-ticks) fills whichever slots are free.

## What runs in parallel

- **Different runs** run in parallel, up to `concurrency`.
- **Steps within a single run** still execute **sequentially** — `prev` flows from one step to the next. To parallelize work *inside* a run, use [`.parallel()`](/guide/parallel).

Each claimed run is leased and heartbeated independently, so one slow run doesn't endanger the others' leases.

## Limiting one workflow

Some work must not run too many at once whatever else is going on — calls to an API with a rate limit, a job that locks a shared resource. Give that workflow its own `concurrency`:

```typescript
const syncCrm = createWorkflow({
  name: 'sync-crm',
  input: z.object({ accountId: z.string() }),
  concurrency: 2, // at most 2 runs of sync-crm at once
})
```

The limit holds across every engine sharing the storage, not just one process: storage counts the workflow's running runs in the same transaction that claims the next one, so two workers cannot both take the last place. Runs past the limit wait as `pending` and start as places free up, in the order they were enqueued. Other workflows are not held back.

Only runs actually running count. A run that is sleeping, waiting for an event, or waiting out a retry backoff holds no place. A run whose process died holds its place until its lease expires — `runLeaseDurationMs`, 30 seconds by default — and is then reclaimed into a free place like any other.

Every process sharing the storage must use the same `runLeaseDurationMs`. Each judges whether another's runs are still alive by its own lease length, so one with a shorter lease would count live runs as dead — exceeding the limit, and reclaiming runs that are still running. The built-in adapters enforce the limit; an engine whose workflows set one refuses a [custom adapter](/guide/storage#custom-adapters) that does not.

## Picking a value

`concurrency` bounds how many runs a single engine instance works on at once. Size it to your workload — I/O-bound steps (API calls, DB writes) tolerate higher concurrency than CPU-bound ones. Because claims are leased, you can also run **multiple engine instances** against the same storage; each claims its own runs and they share the work safely.

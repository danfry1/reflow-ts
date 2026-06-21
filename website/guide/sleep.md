# Durable Sleep

`.sleep(name, duration)` durably pauses a workflow between steps. Unlike `await new Promise(r => setTimeout(r, ms))`, the wait survives a process exit, deploy, or crash: the run is persisted as `sleeping` and its lease released, so the process can shut down entirely during the delay. Once the time elapses, any engine instance reclaims the run and continues from the next step.

```typescript
const workflow = createWorkflow({ name: 'trial', input: z.object({ userId: z.string() }) })
  .step('start-trial', async ({ input }) => {
    await provisionTrial(input.userId)
    return { startedAt: Date.now() }
  })
  .sleep('trial-period', '14d')
  .step('charge-or-expire', async ({ input }) => {
    await convertOrExpire(input.userId)
  })
```

The engine does not hold a timer or keep the process alive for 14 days — it persists a wake time and moves on. When `start()` is polling (or on the next `tick()`) after the wake time, the run resumes.

## Duration format

`duration` is either a number of milliseconds or a string with a single unit suffix:

| Example | Meaning |
|---|---|
| `5000` | 5000 ms |
| `'500ms'` | 500 milliseconds |
| `'30s'` | 30 seconds |
| `'15m'` | 15 minutes |
| `'24h'` | 24 hours |
| `'7d'` | 7 days |

An invalid duration throws [`ConfigError`](/api/errors) when the workflow is defined.

## Behaviour

- **`prev` passes through.** A sleep produces no output; the next step receives the previous step's output as `prev`, unchanged.
- **The name must be unique** within the workflow (like a step), and the sleep appears in [`getRunStatus()`](/api/engine) as a step — `sleeping` while waiting, then `completed` once it elapses.
- **Wake granularity** is the engine poll interval (default 1s). A run sleeping until time *T* resumes on the first `tick()` at or after *T*, not at *T* exactly.
- **Crash-safe.** The wake time is persisted before the run is suspended, so a restart does not restart the timer or skip the wait.
- **Cancellable.** [`cancel()`](/api/engine) works on a sleeping run; it never resumes.

## Resuming on another machine

Because the wait lives in storage rather than in process memory, a completely separate engine instance can resume the run:

```typescript
// Process A enqueues and runs up to the sleep, then exits.
await engineA.tick() // → run is now 'sleeping'

// Hours later, process B (sharing the same database) resumes it automatically.
await engineB.start() // polls, reclaims the woken run, finishes it
```

## Zero-length sleep

`.sleep('noop', 0)` (or any already-elapsed duration) completes in the same tick without suspending — useful when a duration is computed and may be zero.

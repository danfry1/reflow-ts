# Inspecting Runs

Every run records enough to answer the questions you ask when something is slow or broken — what it returned, why it failed, what it is waiting on, how long each step took — without wiring up hooks first.

## A run's record

```typescript
const info = await engine.getRunStatus(runId)
const run = info?.run
```

| Field | Meaning |
|---|---|
| `status` | `pending`, `running`, `sleeping`, `waiting`, `completed`, `failed`, or `cancelled` |
| `createdAt` | When it was enqueued |
| `startedAt` | When it was first claimed — kept across crash recovery, so it is when work actually began. `null` until then |
| `finishedAt` | When it reached `completed`, `failed`, or `cancelled`; `null` while it can still progress |
| `output` | A completed run's final output: the last step's output, or the value passed to [`complete()`](/guide/early-completion). `undefined` otherwise |
| `error` | A failed run's `{ stepName, message, code }`, or `null` |
| `wakeAt` | When a sleeping run wakes, or a waiting run times out; `null` otherwise |
| `waitingFor` | The event a waiting run is waiting for; `null` otherwise |

`error.code` is the error's string `code` when it has one — a Reflow code such as `'STEP_TIMEOUT'` or `'WAIT_TIMEOUT'`, or a system code such as `'ECONNREFUSED'` — so failures can be grouped without parsing messages:

```typescript
const failed = await engine.listRuns({ status: 'failed' })
const timeouts = failed.filter((run) => run.error?.code === 'STEP_TIMEOUT')
```

## Steps

`info.steps` holds one `StepResult` per step that has run, in order. Each records its `status`, `output`, `error`, `attempts`, and `startedAt`; with `updatedAt` that gives how long a finished step took:

```typescript
for (const step of info?.steps ?? []) {
  const ms = step.startedAt === null ? null : step.updatedAt - step.startedAt
  console.log(step.name, step.status, ms === null ? '' : `${ms}ms`, `attempts: ${step.attempts}`)
}
```

A step's duration includes its retries and their backoff. A [sleep](/guide/sleep) or [wait](/guide/wait-for-event) keeps the time it began even when it finishes on a later claim, so its duration is the whole pause.

`history` breaks a step's duration down: one entry per attempt at its handler, oldest first, each with `startedAt`, `finishedAt`, and the `error` it failed with (`null` for the one that succeeded). It carries across executions — a durable backoff, or a crash after one — so the gap between one attempt finishing and the next starting is the backoff it waited out, plus any time before an engine picked the run up again:

```typescript
const [first, second] = step.history
if (first && second) {
  console.log(`attempt 1 failed with "${first.error}", retried ${second.startedAt - first.finishedAt}ms later`)
}
```

Sleeps, waits, and skipped steps have an empty history, as do steps recorded before 0.8. A history keeps the latest 100 attempts.

Attempts are saved when a step suspends or finishes. Retries that run in process — zero-delay retries, [parallel](/guide/parallel) branches, and an engine driven only by `tick()` — are not saved one by one, so if the process dies mid-step they are lost, as is the step's attempt count, and the step starts over.

## Counting

`countRuns()` returns a count for every status, zero or not — cheap enough to poll for a health check or a status line:

```typescript
const { failed, waiting } = await engine.countRuns()
const forOneWorkflow = await engine.countRuns({ workflow: 'order-fulfillment' })
```

## Retention

Runs, their steps, and their buffered events stay in storage until you delete them. `prune()` deletes finished runs that finished longer ago than a cutoff:

```typescript
await engine.prune({ olderThan: '30d' })

// Keep failures around longer than successes
await engine.prune({ olderThan: '7d', statuses: ['completed', 'cancelled'] })
await engine.prune({ olderThan: '90d', statuses: ['failed'] })
```

Runs that can still progress — pending, running, sleeping, waiting — are never deleted, whatever their age. `prune()` also deletes [cached step results](/guide/caching) that have expired. `prune()` works in batches of 500, each its own short transaction, so on SQLite it does not stall workers sharing the database.

To prune on a cadence, give it a workflow and a [schedule](/guide/scheduling), so exactly one instance in a fleet runs it:

```typescript
const pruneOldRuns = createWorkflow({ name: 'prune-old-runs', input: z.object({}) })
  .step('prune', async () => ({ deleted: await engine.prune({ olderThan: '30d' }) }))

await engine.schedule('prune-old-runs', {}, { cron: '0 3 * * *' })
```

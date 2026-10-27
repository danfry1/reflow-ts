# Workflows as Functions <Badge type="warning" text="beta" />

::: warning Beta
Workflows written as a function are new in 0.11. They are tested as thoroughly as chained workflows and safe to run, but their API may still change in a minor release while it settles. Chained workflows are unaffected.
:::

A workflow can also be written as one async function. Its durable operations are calls on `step`, and everything between them is ordinary TypeScript: `if` for a branch, `for` for a loop, `Promise.all` to run steps together, `try`/`catch` to handle a failure.

```typescript
import { createWorkflow, StepFailedError } from 'reflow-ts'
import { z } from 'zod'

const fulfilOrder = createWorkflow({
  name: 'fulfil-order',
  input: z.object({ orderId: z.string(), items: z.array(z.string()) }),
  events: { shipped: z.object({ tracking: z.string() }) },
}, async ({ input, step }) => {
  const payment = await step.run('charge', async ({ idempotencyKey }) => {
    const charge = await stripe.paymentIntents.create({ amount: 5000 }, { idempotencyKey })
    return { paymentIntent: charge.id }
  })

  // One print order per item, however many there are, all at once
  const prints = await Promise.all(input.items.map((item) =>
    step.run(`print-${item}`, async () => ({ printId: await gelato.order(input.orderId, item) }))))

  const shipped = await step.waitForEvent('shipped', { timeout: '10d' })
  if (!shipped) {
    await step.run('chase', () => support.chase(input.orderId))
    return { status: 'chased' }
  }

  try {
    await step.run('invoice', () => accounting.invoice(input.orderId))
  } catch (error) {
    if (!(error instanceof StepFailedError)) throw error
    await step.run('refund', () => stripe.refunds.create({ payment_intent: payment.paymentIntent }))
  }

  return { status: 'shipped', tracking: shipped.tracking, prints: prints.length }
})
```

It is registered, enqueued, and inspected exactly like a chained workflow, and runs on the same engine and storage:

```typescript
const engine = createEngine({ storage, workflows: [fulfilOrder] })
await engine.enqueue('fulfil-order', { orderId: 'o_1', items: ['mug', 'print'] })
```

What the function returns is the run's output, and it is typed: `InferOutput<typeof fulfilOrder>` is what the function can return, and [`testEngine`](/guide/testing) returns it as `output`.

## Which to use

Both are durable in the same way and share retries, timeouts, caching, events, hooks, the dashboard, and storage.

| | Chained steps | A function |
|---|---|---|
| Steps known | Before any run starts, so the dashboard shows steps a run has not reached | As runs reach them |
| Branching | `when` on a step | `if` |
| A number of steps decided at run time | Not possible | `Promise.all(items.map(...))`, or a loop |
| Handling a failure and carrying on | `onFailure`, after the run has failed | `try`/`catch` around the step |
| A wait that times out | Fails the run | Resolves to `null` |
| Earlier results | `prev` and `steps`, typed by the chain | Ordinary variables |
| Run output | Untyped | Typed from what the function returns |

Chained steps suit a fixed pipeline. A function suits anything with branches, a variable number of steps, or failures to recover from.

## How a run resumes

Every operation has a name, and its result is saved under that name the moment it finishes. When a run resumes — after a sleep, a wait, a retry's backoff, or a crash — the engine calls the function again **from the top**. Each operation that already finished returns its saved result without running again, and the function carries on from the first one that has not.

This has one consequence for how the function is written: **code between steps runs again every time the run resumes.** Anything with an effect, or a result that can differ from one call to the next — a request, a database write, `Date.now()`, `Math.random()` — belongs inside `step.run()`, where it runs once and its result is saved.

```typescript
// ✗ A new id every time the run resumes
const id = crypto.randomUUID()

// ✓ Made once, then replayed
const id = await step.run('make-id', async () => crypto.randomUUID())
```

Replaying is quick — a few thousand finished steps take tens of milliseconds — but it happens on every resume, so a run's cost grows with how many operations it has done. A run that loops for ever, or resumes thousands of times, is better split into runs that each do a bounded piece of the work. A run may start at most `maxOperations` operations (10,000 unless the workflow's config says otherwise), and fails with `ConfigError` past it.

Because results are matched by **name** rather than position, a function that takes a different branch when it resumes does not receive another step's result. It reaches a name with no saved result and runs that step.

Saved results are handed back **in the order the operations first finished**, so a `Promise.race` between operations picks the same winner on every replay as it did the first time — as long as the race is made as the operations are started, with no `await` of anything else in between. (On replay, results come back at once: an `await` between starting two operations and racing them lets both settle first, and the race then takes the first in the array rather than the first to finish.) A race against a promise that is not an operation, such as a plain `setTimeout`, can also go either way when the run resumes.

### Names

- Every operation in a run needs a name of its own. Reusing one fails the run with `DuplicateStepError`.
- In a loop, build the name from the item: `` step.run(`print-${item.id}`, ...) ``. Use something stable, such as an id, not the loop index, if the list can change between executions.
- A name keeps its kind. Each saved row records whether it was a step, a sleep, or a wait, so a deploy that reuses a name for another kind of operation fails the run with `ConfigError` rather than handing it the other's result. Give a new operation a new name. (Rows saved before 0.11, or by a custom storage adapter that does not keep a row's `kind`, are not checked.)

## `step.run(name, handler | config)`

Runs `handler` as a durable step and resolves to what it returns. The handler receives `{ signal, runId, attempt, idempotencyKey }`, as a chained step does. The workflow's input and earlier results are already in scope, so it receives no `input`, `prev`, or `steps`.

```typescript
const summary = await step.run('summarize', {
  retry: { maxAttempts: 3, backoff: 'exponential', initialDelay: '1s' },
  timeout: '30s',
  cache: { ttl: '24h', key: () => input.url },
  handler: async ({ signal }) => llm.summarize(page.html, { signal }),
})
```

| Option | Description |
|---|---|
| `retry.maxAttempts` | Attempts in all, at least 1. Without `retry`, a step gets one. |
| `retry.backoff` | `'linear'` (default) or `'exponential'`. |
| `retry.initialDelay` | The first wait between attempts: milliseconds, or a duration such as `'30s'`. Default one second. |
| `timeout` | Give up on an attempt after this long: milliseconds, or a duration. |
| `cache` | Reuse the result from an earlier run: a duration, or `{ ttl, key?, version? }`. See [Caching Steps](/guide/caching). |

Durations are written the same way everywhere in this API — `timeout`, `initialDelay`, `sleep`, a wait's `timeout` — as milliseconds or a string such as `'500ms'`, `'30s'`, `'24h'`, `'7d'`.

Without a `key`, a cached step is keyed on the workflow input, every result handed to the function before the step was called, and every failure or timed-out wait it had seen by then: everything its code could depend on. When steps run concurrently, which of their results came first can differ between runs, and so can that key — a miss, never a wrong hit. Give a cached step that runs beside others its own `key`.

A step's result is anything that can be saved: plain objects — interfaces included, recursive ones and fields typed `unknown` too, so what an SDK returns is fine as it is — arrays, strings, numbers, booleans, `null`, `undefined`, and `Date`. Functions, `Map`s, `Set`s, promises, `bigint`s, and symbols cannot be, anywhere inside the result, and returning one is a type error at the handler; so is a function workflow returning one. (The chained `.step()` is stricter: a value typed as an interface needs copying, as `{ ...charge }`.)

### When a step fails

When every attempt has failed, `step.run` rejects with a `StepFailedError`, and its `cause` is what the handler last threw. If the function lets it escape, the run fails at that step, reporting the handler's error, just as a chained workflow does. If it catches the error, the run carries on:

```typescript
try {
  await step.run('reserve-stock', () => warehouse.reserve(input.items))
} catch (error) {
  if (!(error instanceof StepFailedError)) throw error
  await step.run('backorder', () => warehouse.backorder(input.items))
}
```

A failure that was caught is saved, and replays as the same rejection when the run resumes, so the function takes the same branch again. The step is not retried behind its back. On replay `cause` is rebuilt as an `Error` with the saved message, `name`, and `code`, so branch on those — `error.cause.code === 'card_declined'` — rather than on the class of the `cause`, which only the first execution has.

Resuming a failed run with [`engine.resume()`](/api/engine) runs the step whose failure ended it again. Failures the function caught are kept and replay as before, so the resumed run takes the same branches. (A custom storage adapter without `supportsKeepFailed` discards them too, and they run again.)

### What the function can catch

`step.run` rejects with the step's own failure, and nothing else the function should handle. Anything else an operation runs into is not the step's failure and **fails the run, however the function catches it**:

- storage failing to save a result, or the engine losing its connection to storage;
- a step output, or the function's return value, that cannot be saved;
- a misconfigured call: an empty or reused name, a bad duration, an undeclared event, or a `step` operation started from inside another step's handler.

That way a `catch` meant for a declined card cannot mistake a database outage for one and take the wrong branch, with the charge made and not recorded.

::: warning Rethrow what you don't recognise
When a run is cancelled, its lease is lost, or it is suspended to sleep or wait, the operations still pending reject so the function stops. `isRunControlError(error)` is true for these. A `catch` should rethrow them rather than report them, and a `finally` block runs each time the run suspends, so keep side effects out of it:

```typescript
try {
  await step.run('charge', () => payments.charge(input.orderId))
} catch (error) {
  if (isRunControlError(error)) throw error
  await step.run('alert', () => alerts.send(String(error)))
}
```

Swallowing one cannot keep a stopped run going: every later operation rejects too, and what the function returns is ignored.
:::

## `step.sleep(name, duration)` and `step.sleepUntil(name, time)`

Durably pause, for a duration (`5000`, `'30s'`, `'7d'`) or until a `Date` or epoch time. The wake time is saved when the sleep is first reached, so resuming a run never restarts the clock. See [Durable Sleep](/guide/sleep).

## `step.waitForEvent(name, options?)`

Waits for `engine.sendEvent(runId, name, payload)` and resolves to the payload. Events are declared on the workflow with the schema each payload must match. `sendEvent` checks the payload against it and refuses names that are not declared:

```typescript
createWorkflow({
  name: 'approval',
  input: z.object({ docId: z.string() }),
  events: { approved: z.object({ by: z.string() }) },
}, async ({ step }) => {
  const approval = await step.waitForEvent('approved') // typed { by: string }
  await step.waitForEvent('rejected')                   // ✗ not declared: a type error
})
```

With a `timeout`, it resolves to `null` if no event arrives in time, and the function decides what that means. Without one, it waits as long as it takes. (So declare an event whose payload could be `null` as an object instead, or a timeout and a payload of `null` look alike.)

Events sent before the run reaches the wait are kept, in the order they were sent, and each wait takes the oldest. To wait for the same event more than once, give each wait a name of its own and name the event:

```typescript
for (const round of [1, 2]) {
  const approval = await step.waitForEvent(`approval-${round}`, { event: 'approved' })
}
```

Several waits may be pending at once, beside each other or beside other steps: `Promise.all([step.waitForEvent('paid'), step.waitForEvent('shipped')])` suspends the run until both have arrived. An event sent through the engine that is running the wait wakes it at once; one sent through another engine, while the run is busy with other steps, is noticed within a second, and one sent while the run is suspended wakes it as it would any wait. (A custom storage adapter without `supportsEventNames` allows one pending wait at a time.) See [Waiting for Events](/guide/wait-for-event).

## Running steps together

Operations started together run concurrently:

```typescript
const [user, plan] = await Promise.all([
  step.run('load-user', () => db.users.get(input.userId)),
  step.run('load-plan', () => billing.plan(input.planId)),
])
```

The run suspends only once **every** operation in flight is blocked — waiting on a sleep, an event, or a retry's backoff — and nothing has started or finished for a moment. Until then a blocked operation waits in the process, so a short sleep beside a long-running step wakes on time, and one step's backoff never cuts a sibling off. A branch that awaits something other than a step for more than that moment, beside a long sleep, may be suspended with the run; it carries on, from its last finished step, when the run resumes.

An operation the function never waits for — a `void step.sleep(...)` — does not hold the run: it is not counted when deciding whether to suspend, and if it is still going when the function returns, it is recorded as skipped. Await every operation whose result matters.

When the function fails, steps still running are aborted through their `signal`. They are not recorded as failed, so resuming the run runs them again.

## Failure handling

`onFailure` goes in the config. It runs once the run has failed, with `{ error, stepName, input }`. `stepName` is empty when the error came from the function's own code rather than a step; the dashboard shows such a run as failed "in the workflow function".

```typescript
createWorkflow({
  name: 'import',
  input: z.object({ fileId: z.string() }),
  onFailure: async ({ error, stepName }) => alerts.send(`import failed at ${stepName}: ${error.message}`),
}, async ({ input, step }) => { /* ... */ })
```

Compensation that needs earlier results is usually simpler as a `try`/`catch` in the function, where those results are ordinary variables.

## Changing a function's code

The rules from [Changing Workflows](/guide/changing-workflows) apply, with names in place of positions:

| Change | What a run already part-way through does |
|---|---|
| Add a step anywhere | Runs it when the function reaches it, including before steps the run has already finished. **Safe** if the new step depends on nothing that has already happened. |
| Remove a step | Never reaches it, so its saved result is ignored. **Safe.** |
| Reorder steps | Finished steps replay wherever they are now; unfinished ones run in the new order. **Safe** unless a step's effect must come before another's. |
| **Rename a step** | Runs it again under the new name, as for a chained step. |
| **Change what a step returns** | Code after it receives the old shape from runs that finished it before the deploy. |
| **Remove a wait's timeout** | A run whose wait already timed out still gets `null` there, whatever the types now say. |

For anything riskier, register the new code under a new workflow name and keep the old one until its runs finish.

## Coming from, or moving to, other tools

The operations map closely onto other durable-execution systems:

| Reflow | Inngest | Cloudflare Workflows | Temporal |
|---|---|---|---|
| `step.run(name, fn)` | `step.run(id, fn)` | `step.do(name, fn)` | an activity |
| `step.sleep(name, '1d')` | `step.sleep(id, '1d')` | `step.sleep(name, '1 day')` | `sleep('1 day')` |
| `step.sleepUntil(name, date)` | `step.sleepUntil(id, date)` | `step.sleepUntil(name, date)` | `sleep(until - now)` |
| `step.waitForEvent(name, { event, timeout })` | `step.waitForEvent(id, { event, timeout })` | `step.waitForEvent(name, { type, timeout })` | a signal with `condition(..., timeout)` |
| `null` on timeout | `null` on timeout | throws | `false` from `condition` |
| `StepFailedError`, caught with `try`/`catch` | `StepError` | the thrown error | `ActivityFailure` |

Some defaults differ, and are worth setting explicitly in code you may move:

| | Reflow | Inngest | Cloudflare Workflows |
|---|---|---|---|
| Attempts per step | 1, unless `retry` says more | 5 (4 retries) | 6 (5 retries), exponential from 10s |
| A wait without a timeout | Waits until the event arrives | Needs a timeout | Times out after 24 hours, and fails the instance |
| Step results | Saved with `Date` and `undefined` kept | JSON: a `Date` comes back as a string | Serializable values only |
| Step options | `{ retry, timeout, handler }` | Set per function | `step.do(name, config, fn)` |

Reflow runs in your process against your SQLite or Postgres database, with no service to deploy. Moving a function workflow to Inngest or Cloudflare Workflows later, should you outgrow it, is mostly a matter of swapping the calls above and setting those defaults; Temporal splits steps into separately deployed activities, so moving there is a larger change.

# Waiting for Events

`.waitForEvent(name, options?)` durably pauses a workflow until an external signal arrives — a webhook callback, a human approval, a third-party job finishing, a payment confirmation. The run is persisted as `waiting` and its lease released, so the process can shut down entirely during the wait. When the event is delivered with `engine.sendEvent(runId, name, payload)`, any engine instance reclaims the run and continues; the payload becomes the next step's `prev`.

```typescript
const checkout = createWorkflow({ name: 'checkout', input: z.object({ orderId: z.string() }) })
  .step('create-payment-intent', async ({ input }) => ({ intentId: await stripe.createIntent(input.orderId) }))
  .waitForEvent('payment.succeeded', { schema: z.object({ chargeId: z.string() }), timeoutMs: 30 * 60 * 1000 })
  .step('fulfil', async ({ prev, input }) => ship(input.orderId, prev.chargeId))

// ...later, in your Stripe webhook handler:
await engine.sendEvent(runId, 'payment.succeeded', { chargeId: event.data.object.id })
```

## Delivering events

`engine.sendEvent(runId, eventName, payload)` records the event:

- **Order-independent.** An event delivered *before* the run reaches the wait is buffered and consumed when it gets there — you don't have to race the workflow.
- **Validated.** If the wait declares a `schema`, the payload is validated on delivery; an invalid payload throws [`ValidationError`](/api/errors) and the run keeps waiting.
- **Typed.** The `schema` infers the type of `prev` (and `steps[name]`) for the steps that follow.
- Returns `false` if the run does not exist or has already finished (nothing is buffered then). Throws [`ConfigError`](/api/errors) if the workflow cannot wait for that event: it has no `.waitForEvent()` of that name, or, for a [workflow written as a function](/guide/functions#step-waitforevent-name-options), declares no such key in `events`.

## Timeouts

Pass `timeoutMs` — a positive number of milliseconds — to bound the wait (anything else throws `ConfigError` when the workflow is defined). If no event arrives in time, the run fails at the wait step with [`WaitTimeoutError`](/api/errors), reaching `onFailure` / `onRunFailed` like any other failure:

```typescript
.waitForEvent('approved', { timeoutMs: 24 * 60 * 60 * 1000 }) // give up after a day
```

Without `timeoutMs`, the run waits indefinitely (until the event arrives or the run is [cancelled](/guide/cancellation)).

## Behaviour

- **Crash-safe.** The wait lives in storage, not process memory, so a restart (or a different machine) resumes it — and an event delivered while no engine is running is still there when one starts.
- **`prev` is the payload.** The validated event payload becomes the next step's `prev` and is available as `steps[name]`.
- **Observable as a step.** The wait appears in [`getRunStatus()`](/api/engine) as a step — `waiting` until the event (or timeout), then `completed` (or `failed`).
- **Wake granularity** for timeouts follows [sleep](/guide/sleep#behaviour): on time for a short timeout on the instance that began the wait, otherwise at the next poll after it. A delivered event resumes the run at once on the engine that received `sendEvent()`.
- **One event at a time.** A chained workflow waits for one named event at a time; an event sent for a name the run will only reach later is simply buffered until then.

## Sleep vs. wait

Use [`.sleep()`](/guide/sleep) when you know *how long* to pause; use `.waitForEvent()` when you're waiting for *something to happen* and don't know when. Both are durable and release the lease.

## In a workflow written as a function

[`step.waitForEvent(name, { timeout })`](/guide/functions#step-waitforevent-name-options) does the same job with two differences: on timeout it resolves to `null` instead of failing the run, and `timeout` also takes a duration string such as `'24h'`. Events are declared up front in the workflow's `events`, and several waits can be pending at once.

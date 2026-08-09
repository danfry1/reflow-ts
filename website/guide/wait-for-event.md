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
- Returns `false` if the run does not exist; throws [`ConfigError`](/api/errors) if the workflow has no `waitForEvent` step with that name.

## Timeouts

Pass `timeoutMs` to bound the wait. If no event arrives in time, the run fails at the wait step with [`WaitTimeoutError`](/api/errors), reaching `onFailure` / `onRunFailed` like any other failure:

```typescript
.waitForEvent('approved', { timeoutMs: 24 * 60 * 60 * 1000 }) // give up after a day
```

Without `timeoutMs`, the run waits indefinitely (until the event arrives or the run is [cancelled](/guide/cancellation)).

## Behaviour

- **Crash-safe.** The wait lives in storage, not process memory, so a restart (or a different machine) resumes it — and an event delivered while no engine is running is still there when one starts.
- **`prev` is the payload.** The validated event payload becomes the next step's `prev` and is available as `steps[name]`.
- **Observable as a step.** The wait appears in [`getRunStatus()`](/api/engine) as a step — `waiting` until the event (or timeout), then `completed` (or `failed`).
- **Wake granularity** for timeouts is the engine poll interval.
- **One event at a time.** A sequential workflow waits for one named event at a time; an event sent for a name the run will only reach later is simply buffered until then.

## Sleep vs. wait

Use [`.sleep()`](/guide/sleep) when you know *how long* to pause; use `.waitForEvent()` when you're waiting for *something to happen* and don't know when. Both are durable and release the lease.

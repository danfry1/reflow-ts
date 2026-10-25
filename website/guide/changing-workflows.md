# Changing Workflows

A run can outlive the code that started it: it sleeps for a day, waits for an approval, or is mid-way through when you deploy. When it carries on, it runs under the workflow definition the new code registers, not the one it started under. This page is what happens then, and how to change a workflow safely.

## How a run carries on under new code

A run's saved results are keyed by **step name**. When an engine picks a run up, it walks the workflow's steps from the top, as the current code defines them: a step whose name has a saved result is skipped and its result reused; the first step without one runs, and the run continues from there.

So what matters is which names have results, not where the steps sit or what their code does.

| Change | What a run already part-way through does |
|---|---|
| Add a step after the point the run has reached | Runs it, in order. **Safe.** |
| Change a step's code, keeping its name | Uses the new code for steps it has not reached yet; steps it finished keep their saved results. **Safe.** |
| Remove a step, sleep, or wait | Skips it. **Safe**, if nothing later reads its result. |
| Change the retry or timeout settings of a step | Uses the new settings from its next attempt. **Safe.** |
| **Rename a step** | Finds no result under the new name and **runs the step again**. Renaming `charge` charges the card a second time. |
| **Add a step before the point the run has reached** | Runs the new step **late** — after the work it was meant to come before. |
| **Change what a step returns** | Later steps receive the **old shape** from runs that finished it before the deploy, whatever their types say. |
| **Change the input schema** | Runs keep the input they were enqueued with; it is not checked against the new schema. |
| **Remove a workflow from the engine** | Its runs are never claimed again. They stay `pending`, `sleeping`, or `waiting` until it is registered once more. |

## Changing a workflow safely

**Small changes in place.** Adding steps at the end, fixing a step's code, tuning retries, and removing steps nothing depends on are all safe to deploy with runs in flight.

**Anything else: a new version beside the old.** Give the changed workflow a new name, and keep the old one registered until its runs have finished:

```typescript
// The old definition, unchanged, until its runs drain
const orderV1 = createWorkflow({ name: 'order', input: OrderInput })
  .step('charge', charge)
  .step('ship', ship)

// The new one, under a new name
const order = createWorkflow({ name: 'order-v2', input: OrderInput })
  .step('validate', validate)
  .step('take-payment', charge)
  .step('ship', ship)

const engine = createEngine({ storage, workflows: [orderV1, order] })

// New runs go to the new version
await engine.enqueue('order-v2', input)
```

Runs started under `order` finish under the code they started with; new runs never see it. Once `await engine.countRuns({ workflow: 'order' })` shows nothing pending, running, sleeping, or waiting, delete `orderV1`. The dashboard lists both versions side by side while they overlap.

**Renaming a step without a new version** is safe only once no run is part-way through the workflow — for example, after pausing new enqueues and letting every run finish.

## Before you deploy

- Does this rename a step, add one before others, or change what a step returns? Then deploy it as a new version.
- Does a step have an effect that must not repeat? Pass its [`idempotencyKey`](/guide/crash-recovery#what-runs-once-and-what-may-run-again) to the API it calls, so a repeat — for any reason — is caught where it matters.
- Are you removing a workflow? Check its runs have finished first.

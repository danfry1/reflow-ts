---
layout: home
title: Reflow — Durable workflows for TypeScript
titleTemplate: false
---

<div class="landing">

<section class="home-intro">

<h1>Steps that survive a crash.</h1>

<p class="home-lede">Reflow runs multi-step workflows in your TypeScript app. Each step's result is saved to SQLite the moment it finishes, so a run that dies half-way — a deploy, an out-of-memory kill, a closed laptop — carries on from the next step instead of starting over. There is nothing to deploy beside your app.</p>

```sh
npm install reflow-ts
```

<p class="home-links"><a href="/reflow-ts/guide/quick-start">Quick start</a><a href="/reflow-ts/guide/">How it works</a><a href="https://github.com/danfry1/reflow-ts">GitHub</a></p>

</section>

<CrashTest />

<section class="home-section">

<h2>What you write</h2>

<p>A workflow is a sequence of steps. Each one returns what the next needs; the engine saves it before moving on.</p>

```typescript
import { createWorkflow, createEngine } from 'reflow-ts'
import { SQLiteStorage } from 'reflow-ts/sqlite-node'
import { z } from 'zod'

const order = createWorkflow({
  name: 'order',
  input: z.object({ orderId: z.string(), amount: z.number() }),
  label: (input) => input.orderId,
})
  .step('charge', async ({ input }) => {
    const charge = await stripe.charges.create({ amount: input.amount })
    return { chargeId: charge.id }
  })
  .step('ship', {
    retry: { maxAttempts: 5, backoff: 'exponential' },
    handler: async ({ prev }) => ({ tracking: await warehouse.ship(prev.chargeId) }),
  })
  .step('notify', async ({ input, prev }) => {
    await email.send(input.orderId, `On its way: ${prev.tracking}`)
  })

const engine = createEngine({ storage: new SQLiteStorage('./app.db'), workflows: [order] })
await engine.start()
await engine.enqueue('order', { orderId: 'ORD-1001', amount: 5000 })
```

<p>If the process dies after <code>charge</code>, the customer has been charged once and stays charged once: the next engine to pick the run up starts at <code>ship</code>. If the warehouse is down, <code>ship</code> is retried with growing waits, and a restart in the middle of those waits does not reset the count.</p>

</section>

<section class="home-section home-dashboard">

<h2>See every run</h2>

<p>The dashboard shows each workflow, its runs, and for each run what every step did — including each failed attempt and how long it waited before the next. Point it at a database file, or mount it in your own app behind your own auth.</p>

```sh
npx reflow-ts dashboard ./app.db
```

<figure class="home-shot">
  <img class="shot-light" src="/dashboard-light.jpg" alt="The Reflow dashboard: workflows in a sidebar, a charge-invoice workflow's runs in a list, and one run open showing its capture step failing after four attempts, with the wait before each retry" width="2000" height="1194" loading="lazy" />
  <img class="shot-dark" src="/dashboard-dark.jpg" alt="The Reflow dashboard: workflows in a sidebar, a charge-invoice workflow's runs in a list, and one run open showing its capture step failing after four attempts, with the wait before each retry" width="2000" height="1194" loading="lazy" />
</figure>

</section>

<section class="home-section">

<h2>What else it does</h2>

<dl class="home-list">
  <div><dt><a href="/reflow-ts/guide/sleep">Sleep for days</a></dt><dd>A sleeping run holds no worker and survives restarts; it wakes on time.</dd></div>
  <div><dt><a href="/reflow-ts/guide/wait-for-event">Wait for an event</a></dt><dd>Pause until an approval, a webhook, or a reply arrives, with a timeout.</dd></div>
  <div><dt><a href="/reflow-ts/guide/retry">Retry with backoff</a></dt><dd>Per step, linear or exponential, with per-attempt timeouts.</dd></div>
  <div><dt><a href="/reflow-ts/guide/parallel">Run steps in parallel</a></dt><dd>Branches run together; the first real failure cancels the rest.</dd></div>
  <div><dt><a href="/reflow-ts/guide/scheduling">Schedule runs</a></dt><dd>On an interval or a cron expression, fired once across all your processes.</dd></div>
  <div><dt><a href="/reflow-ts/guide/failure-handling">Compensate on failure</a></dt><dd>Undo what a failed run already did, with <code>onFailure</code>.</dd></div>
  <div><dt><a href="/reflow-ts/guide/workflows">Typed end to end</a></dt><dd>Inputs come from your schema; each step's <code>prev</code> is the previous step's output.</dd></div>
  <div><dt><a href="/reflow-ts/guide/concurrency">Several processes, one file</a></dt><dd>Workers share a SQLite database; each run is claimed by exactly one.</dd></div>
</dl>

</section>

<section class="home-section">

<h2>When to reach for it</h2>

|  | Reflow | Temporal | Inngest |
|---|---|---|---|
| What you run | Your app and a SQLite file | A cluster, a database, and workers | A hosted service and an SDK |
| Type safety | End to end, inferred | Partial | Partial |
| Good for | Apps, CLIs, and agents on one machine or a few | Large distributed systems | Serverless functions |

<p>Reflow is for apps that need multi-step work to be reliable without running workflow infrastructure. It is not built for spreading one workload across many machines, for sub-second dispatch at very high volume, or for teams already on Temporal.</p>

<p class="home-links"><a href="/reflow-ts/guide/">Read the guide</a></p>

</section>

</div>

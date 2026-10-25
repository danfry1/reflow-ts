---
layout: home
title: Reflow — Durable workflows for TypeScript
titleTemplate: false
---

<div class="landing">

<section class="home-intro">

<h1>Steps that survive a crash.</h1>

<p class="home-lede">Reflow runs multi-step workflows in your TypeScript app and saves each step's result the moment it finishes. Kill the process half-way and the run carries on from the next step — nothing to deploy beside your app.</p>

<div class="home-start">

```sh
npm install reflow-ts
```

<p class="home-links"><a href="/guide/quick-start">Quick start</a><a href="/guide/">How it works</a><a href="https://github.com/danfry1/reflow-ts">GitHub</a></p>

</div>

<p class="home-facts"><span>Node 18 or later, and Bun</span><span>SQLite in a file next to your app, or PostgreSQL</span><span>Zod, Valibot, ArkType, or any Standard Schema</span><span>MIT licensed</span></p>

</section>

<section class="home-play">

<h2>Crash it</h2>

<p>The real Reflow engine, running in your browser.</p>

<Playground />

</section>

<section class="home-section">

<h2>What you write</h2>

<p>A workflow is a sequence of steps. Each returns what the next needs, and the engine saves it before moving on — that is the whole trick.</p>

<div class="home-code">

```typescript
import { createWorkflow, createEngine } from 'reflow-ts'
import { SQLiteStorage } from 'reflow-ts/sqlite-node-builtin'
import { z } from 'zod'

const order = createWorkflow({
  name: 'order',
  input: z.object({ orderId: z.string(), amount: z.number() }),
  label: (input) => input.orderId,
})
  .step('charge', async ({ input, idempotencyKey }) => {
    const charge = await stripe.paymentIntents.create({ amount: input.amount, currency: 'usd' }, { idempotencyKey })
    return { chargeId: charge.id }
  })
  .step('ship', {
    retry: { maxAttempts: 5, backoff: 'exponential' },
    handler: async ({ prev }) => ({ tracking: await warehouse.ship(prev.chargeId) }),
  })
  .sleep('wait-a-day', '1d')
  .step('ask-for-review', async ({ input, prev }) => {
    await email.send(input.orderId, `How was it? ${prev.tracking}`)
  })

const engine = createEngine({ storage: new SQLiteStorage('./app.db'), workflows: [order] })
await engine.start()
await engine.enqueue('order', { orderId: 'ORD-1001', amount: 5000 })
```

<ol class="home-notes">
  <li><strong>charge</strong> is saved before <code>ship</code> starts, so a crash after it never charges the card again. A crash <em>during</em> it runs it again, with the same <code>idempotencyKey</code>, so Stripe returns the first charge instead of making a second.</li>
  <li><strong>ship</strong> retries with growing waits. The attempt count is saved too, so a restart mid-wait does not start the count over.</li>
  <li><strong>wait-a-day</strong> holds no worker for the day. The run sleeps in the database and wakes on time, whichever process is running then.</li>
  <li><strong>label</strong> names each run in the dashboard.</li>
</ol>

</div>

</section>

<section class="home-section home-dashboard">

<h2>On your own runs</h2>

<p>The dashboard above is the one that ships in the package. Point it at a database file, or mount it in your app behind your own auth — see <a href="/guide/dashboard">the dashboard guide</a>.</p>

```sh
npx reflow-ts dashboard ./app.db
```

</section>

<section class="home-section">

<h2>What else it does</h2>

<dl class="home-list">
  <div><dt><a href="/guide/sleep">Sleep for days</a></dt><dd>A sleeping run holds no worker and survives restarts; it wakes on time.</dd></div>
  <div><dt><a href="/guide/wait-for-event">Wait for an event</a></dt><dd>Pause until an approval, a webhook, or a reply arrives, with a timeout.</dd></div>
  <div><dt><a href="/guide/retry">Retry with backoff</a></dt><dd>Per step, linear or exponential, with per-attempt timeouts.</dd></div>
  <div><dt><a href="/guide/parallel">Run steps in parallel</a></dt><dd>Branches run together; the first real failure cancels the rest.</dd></div>
  <div><dt><a href="/guide/scheduling">Schedule runs</a></dt><dd>On an interval or a cron expression, fired once across all your processes.</dd></div>
  <div><dt><a href="/guide/failure-handling">Compensate on failure</a></dt><dd>Undo what a failed run already did, with <code>onFailure</code>.</dd></div>
  <div><dt><a href="/guide/workflows">Typed end to end</a></dt><dd>Inputs come from your schema; each step's <code>prev</code> is the previous step's output.</dd></div>
  <div><dt><a href="/guide/storage#postgresstorage">From one file to many machines</a></dt><dd>Workers share a SQLite file, or a Postgres database across machines; each run is claimed by exactly one.</dd></div>
  <div><dt><a href="/guide/concurrency#limiting-one-workflow">Limit one workflow</a></dt><dd>At most so many runs of a workflow at once, across every process — for rate-limited APIs.</dd></div>
</dl>

</section>

<section class="home-section">

<h2>When to reach for it</h2>

|  | Reflow | Temporal | Inngest |
|---|---|---|---|
| What you run | Your app, and a SQLite file or your Postgres | A cluster, a database, and workers | A hosted service and an SDK |
| Type safety | End to end, inferred | Partial | Partial |
| Good for | Apps, CLIs, and agents, from one process to many machines | Very large, multi-language systems | Serverless functions |

<p>Reflow is for apps that need multi-step work to be reliable without running workflow infrastructure. Start on SQLite and move to Postgres when workers span machines, without changing a workflow. It is not built for workflows written in several languages, for sub-second dispatch at very high volume, or for teams already on Temporal.</p>

<p class="home-links"><a href="/guide/">Read the guide</a></p>

</section>

</div>

---
layout: home
title: Reflow — Durable Workflows for TypeScript
titleTemplate: false
hero:
  name: Reflow
  text: Durable workflow execution for TypeScript.
  tagline: Multi-step workflows with full type safety, automatic retries, and crash recovery — powered by SQLite, no external services required.
  image:
    src: /favicon.svg
    alt: Reflow
  actions:
    - theme: brand
      text: Get Started
      link: /guide/
    - theme: alt
      text: Quick Start
      link: /guide/quick-start
    - theme: alt
      text: GitHub
      link: https://github.com/danfry1/reflow-ts
features:
  - title: No infrastructure
    details: Just a SQLite file. No Temporal cluster, no cloud service, no message broker. Add it to a single-process app, a CLI, or an AI agent.
  - title: Crash recovery built in
    details: Each step is checkpointed. If the process dies after step 2 of 5, another engine reclaims the stale run after its lease expires and resumes at step 3 — completed steps never re-run.
  - title: End-to-end type safety
    details: Workflow names are literal types, input is inferred from your schema, and each step's prev is typed as the previous step's output. Mistakes are compile errors.
  - title: Retries, timeouts, and sagas
    details: Per-step retry with linear or exponential backoff, per-attempt timeouts, early completion, parallel groups, and onFailure compensation — all first-class.
---

## Durable in five lines

```typescript
import { createWorkflow, createEngine } from 'reflow-ts'
import { SQLiteStorage } from 'reflow-ts/sqlite-node'
import { z } from 'zod'

const orderWorkflow = createWorkflow({
  name: 'order-fulfillment',
  input: z.object({ orderId: z.string(), amount: z.number() }),
})
  .step('charge', async ({ input }) => {
    const charge = await stripe.charges.create({ amount: input.amount })
    return { chargeId: charge.id }
  })
  .step('fulfill', async ({ prev }) => {
    const shipment = await warehouse.ship(prev.chargeId)
    return { trackingNumber: shipment.tracking }
  })
  .step('notify', async ({ prev, input }) => {
    await email.send(input.orderId, `Shipped! Track: ${prev.trackingNumber}`)
  })

const engine = createEngine({ storage: new SQLiteStorage('./workflows.db'), workflows: [orderWorkflow] })
await engine.start()

await engine.enqueue('order-fulfillment', { orderId: 'ORD_123', amount: 5000 })
```

If the process crashes after `charge` but before `fulfill`, the customer is charged exactly once and the run resumes at `fulfill` on restart — no double charges, no lost shipments, no manual checkpoint code.

## When to reach for Reflow

|  | Reflow | Temporal | Inngest |
|---|---|---|---|
| Infrastructure | None (SQLite file) | Server + DB | Cloud service |
| Type safety | Full end-to-end | Partial | Partial |
| Setup | `bun add reflow-ts` | Cluster deployment | Account + SDK |
| Best for | Single-process apps, CLIs, AI agents | Large distributed systems | Serverless |

Reflow is for **solo devs and small teams** who need reliable multi-step workflows without running a workflow cluster. It is **not** for distributed execution across many machines, sub-second dispatch latency, or shops already on Temporal.

[Read the guide →](/guide/)

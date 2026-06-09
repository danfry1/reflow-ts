# Quick Start

This walks through defining a workflow, wiring up an engine, and enqueuing a run.

## 1. Define a workflow

A workflow is a named sequence of steps with a validated input schema. Each step returns a value that flows into the next as `prev`.

```typescript
import { createWorkflow } from 'reflow-ts'
import { z } from 'zod'

const pipeline = createWorkflow({
  name: 'process-content',
  input: z.object({ url: z.string() }),
})
  .step('scrape', async ({ input }) => {
    const page = await fetchPage(input.url)
    return { content: page.text }
  })
  .step('summarize', {
    retry: { maxAttempts: 3, backoff: 'exponential' },
    handler: async ({ prev }) => {
      const summary = await llm(prev.content)
      return { summary }
    },
  })
  .step('store', async ({ input, prev }) => {
    await db.insert({ url: input.url, summary: prev.summary })
  })
```

## 2. Create an engine

The engine connects workflows to storage and runs them.

```typescript
import { createEngine } from 'reflow-ts'
import { SQLiteStorage } from 'reflow-ts/sqlite-node'

const storage = new SQLiteStorage('./reflow.db')
const engine = createEngine({ storage, workflows: [pipeline] })

await engine.start() // initializes storage and starts polling
```

## 3. Enqueue work

```typescript
const run = await engine.enqueue('process-content', {
  url: 'https://example.com/article',
})

console.log(run.id) // a unique id you can use to check status later
```

`enqueue()` is fully type-safe — it only accepts registered workflow names with their matching input shape.

## 4. Check status

```typescript
const info = await engine.getRunStatus(run.id)
if (info) {
  info.run.status // 'pending' | 'running' | 'completed' | 'failed' | 'cancelled'
  info.steps      // each step's output, error, and attempt count
}
```

## 5. Shut down cleanly

```typescript
await engine.stop() // stops polling, clears schedules, waits for in-flight work
```

## Without a polling loop

For CLI tools and tests, drive the engine manually with `tick()` instead of `start()` — it claims and runs one batch of pending work. Call `storage.initialize()` first:

```typescript
await storage.initialize()
await engine.enqueue('process-content', { url: '…' })
await engine.tick()
```

Or use the dedicated [test helper](/guide/testing), which runs a workflow to completion synchronously and returns typed results.

## Where to go next

- [Workflows](/guide/workflows) — `prev`, the typed `steps` context, and the immutable builder.
- [Retry & Timeouts](/guide/retry) — make individual steps resilient.
- [Crash Recovery](/guide/crash-recovery) — what makes runs durable.

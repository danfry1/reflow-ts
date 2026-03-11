/**
 * Example 3: AI Agent — Multi-step Content Pipeline
 *
 * Scenario: You're building an AI-powered content pipeline that:
 *   1. Scrapes a URL for content
 *   2. Calls an LLM to summarize it (slow, expensive — $0.05 per call)
 *   3. Calls an LLM to extract structured data (another $0.05)
 *   4. Stores the results in your database
 *
 * The LLM calls take 10-30 seconds each and cost money. If step 4
 * fails (DB is down), you don't want to re-run the LLM calls.
 * That's $0.10 wasted and another minute of waiting.
 *
 * Run: npx tsx examples/3-ai-pipeline.ts
 */
import { createWorkflow, createEngine } from '../src/index'
import { SQLiteStorage } from '../src/storage/sqlite-node'
import { z } from 'zod'

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms))
}
let llmCalls = 0
let totalCost = 0

// =====================================================
// ❌ WITHOUT REFLOW — Expensive retries
// =====================================================
//
// async function processUrl(url: string) {
//   const content = await scrape(url)                    // 2s
//   const summary = await llm.summarize(content)         // 15s, $0.05
//   const entities = await llm.extractEntities(content)  // 15s, $0.05
//   await db.store({ url, summary, entities })           // 💥 DB timeout
// }
//
// // The DB was down for 30 seconds. You retry:
// // - scrape: runs again (wasted 2s)
// // - summarize: runs again (wasted 15s + $0.05)
// // - extractEntities: runs again (wasted 15s + $0.05)
// // - store: works this time
// //
// // You just burned an extra $0.10 and 32 seconds for a DB blip.
// //
// // Now imagine this in a batch of 100 URLs. A single DB timeout
// // means re-running LLM calls that already succeeded = $10 wasted.
// //
// // "Just cache the results" — now you're building your own
// // checkpoint system. That's what Reflow already is.

// =====================================================
// ✅ WITH REFLOW — Each LLM call is persisted
// =====================================================

const contentPipeline = createWorkflow({
  name: 'process-content',
  input: z.object({
    url: z.string(),
    maxTokens: z.number().default(1000),
  }),
})
  .step('scrape', async ({ input }) => {
    console.log(`  Scraping ${input.url}...`)
    await sleep(500)

    const content = `This is a fascinating article about TypeScript workflows.
      It covers durable execution patterns, SQLite-based persistence,
      and how small teams can build reliable systems without infrastructure.
      The key insight is that most apps don't need Temporal — they need
      something simpler that just works.`

    console.log(`  ✓ Scraped ${content.length} chars`)
    return { content }
    // Persisted. If we crash after this, we won't re-scrape.
  })
  .step('summarize', {
    // LLM calls are flaky — retry, but with backoff to avoid rate limits
    retry: { maxAttempts: 3, backoff: 'exponential', initialDelayMs: 2000 },
    handler: async ({ prev, input }) => {
      llmCalls++
      totalCost += 0.05
      console.log(`  Calling LLM to summarize (call #${llmCalls}, $${totalCost.toFixed(2)} total)...`)
      await sleep(2000) // Simulating a slow LLM call

      const summary = `A guide to building durable TypeScript workflows with SQLite, ` +
        `targeting small teams who want reliability without infrastructure overhead.`

      console.log(`  ✓ Summary generated (${summary.length} chars)`)
      return { content: prev.content, summary }
      // This $0.05 result is now persisted. Even if the next step fails,
      // we'll never re-run this LLM call.
    },
  })
  .step('extract-entities', {
    retry: { maxAttempts: 3, backoff: 'exponential', initialDelayMs: 2000 },
    handler: async ({ prev }) => {
      llmCalls++
      totalCost += 0.05
      console.log(`  Calling LLM to extract entities (call #${llmCalls}, $${totalCost.toFixed(2)} total)...`)
      await sleep(2000)

      const entities = {
        topics: ['TypeScript', 'durable workflows', 'SQLite'],
        technologies: ['Reflow', 'Temporal', 'SQLite'],
        audience: 'small engineering teams',
        sentiment: 'positive' as const,
      }

      console.log(`  ✓ Extracted ${entities.topics.length} topics, ${entities.technologies.length} technologies`)
      return { summary: prev.summary, entities }
      // Another $0.05 saved. Both LLM results are now durable.
    },
  })
  .step('store-results', {
    retry: { maxAttempts: 3, backoff: 'linear', initialDelayMs: 1000 },
    handler: async ({ prev, input }) => {
      console.log('  Storing results in database...')
      await sleep(300)

      const record = {
        url: input.url,
        summary: prev.summary,
        entities: prev.entities,
        processedAt: new Date().toISOString(),
      }

      console.log('  ✓ Stored! Final record:')
      console.log(`    URL:       ${record.url}`)
      console.log(`    Summary:   ${record.summary.slice(0, 80)}...`)
      console.log(`    Topics:    ${record.entities.topics.join(', ')}`)
      console.log(`    Sentiment: ${record.entities.sentiment}`)
      return record
      // If THIS step had failed (DB down), Reflow would retry it
      // WITHOUT re-running the two LLM calls above. That's $0.10 saved.
    },
  })

// =====================================================
// Run a batch — each URL is an independent workflow run
// =====================================================

async function main() {
  const storage = new SQLiteStorage('/tmp/reflow-ai-example.db')
  await storage.initialize()
  const engine = createEngine({ storage, workflows: [contentPipeline] })

  const urls = [
    'https://example.com/article-1',
    'https://example.com/article-2',
    'https://example.com/article-3',
  ]

  console.log(`Processing ${urls.length} URLs through AI pipeline...\n`)

  // Enqueue all URLs — they'll be processed one at a time
  for (const url of urls) {
    await engine.enqueue('process-content', { url, maxTokens: 1000 })
  }

  // Process all runs
  for (let i = 0; i < urls.length; i++) {
    console.log(`--- Processing URL ${i + 1}/${urls.length} ---`)
    await engine.tick()
    console.log()
  }

  console.log(`Done! ${llmCalls} LLM calls, $${totalCost.toFixed(2)} total cost`)
  console.log(`(Without Reflow, a single retry would have doubled that cost)`)

  storage.close()
}

main()

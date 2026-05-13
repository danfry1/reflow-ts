/**
 * Example 4: Parallel Enrichment — Fan-out / Fan-in
 *
 * Scenario: You've just received a new user signup. Before letting them in,
 * you want to enrich their profile from three independent services:
 *   - A geo-IP service to determine their region
 *   - A risk/fraud API to score their signup
 *   - A CRM lookup to see if they already exist as a contact
 *
 * Each call is ~1 second. Done sequentially, that's 3+ seconds of latency.
 * Done in parallel, it's just one. They share no data — perfect for `.parallel()`.
 *
 * Run: npx tsx examples/4-parallel-enrichment.ts
 */
import { createWorkflow, createEngine } from '../src/index'
import { SQLiteStorage } from '../src/storage/sqlite-node'
import { z } from 'zod'

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms))
}

// =====================================================
// ❌ WITHOUT PARALLEL — Sequential calls accumulate latency
// =====================================================
//
// async function enrich(userId: string, email: string, ip: string) {
//   const region = await geoLookup(ip)       // 1s
//   const risk = await scoreSignup(userId)   // 1s
//   const crm = await findContact(email)     // 1s
//   // 3s of wall-clock time for work that didn't need to be sequential.
// }

// =====================================================
// ✅ WITH .parallel() — All three run concurrently
// =====================================================

const enrichmentWorkflow = createWorkflow({
  name: 'enrich-signup',
  input: z.object({
    userId: z.string(),
    email: z.string(),
    ip: z.string(),
  }),
})
  .step('record-signup', async ({ input }) => {
    console.log(`  Recording signup for ${input.email}...`)
    await sleep(200)
    return { signupId: `signup_${input.userId}_${Date.now()}` }
  })
  .parallel({
    geo: async ({ input }) => {
      console.log(`  [geo] Looking up region for ${input.ip}...`)
      await sleep(1000)
      console.log(`  [geo] ✓ Done`)
      return { region: 'EU-West', country: 'NL' }
    },
    risk: {
      // Risk scoring is the flakiest of the three — retry on transient errors.
      retry: { maxAttempts: 3, backoff: 'exponential', initialDelayMs: 200 },
      timeoutMs: 3000,
      handler: async ({ input }) => {
        console.log(`  [risk] Scoring ${input.userId}...`)
        await sleep(1000)
        console.log(`  [risk] ✓ Done`)
        return { score: 0.12, tier: 'low' as const }
      },
    },
    crm: async ({ input }) => {
      console.log(`  [crm] Checking for existing contact ${input.email}...`)
      await sleep(1000)
      console.log(`  [crm] ✓ Done`)
      return { exists: false, contactId: null }
    },
  })
  .step('finalize', async ({ prev, input, steps }) => {
    // `prev` is the merged record of all three branch outputs — fully typed.
    console.log(`  Finalizing profile for ${input.email}:`)
    console.log(`    region:  ${prev.geo.region} (${prev.geo.country})`)
    console.log(`    risk:    ${prev.risk.score} (${prev.risk.tier})`)
    console.log(`    crm:     ${prev.crm.exists ? prev.crm.contactId : 'new contact'}`)
    console.log(`    signup:  ${steps['record-signup'].signupId}`)
    return { ready: true }
  })

async function main() {
  const storage = new SQLiteStorage('/tmp/reflow-parallel-example.db')
  await storage.initialize()
  const engine = createEngine({ storage, workflows: [enrichmentWorkflow] })

  const started = Date.now()
  console.log('Enrich signup for alice@example.com...\n')

  await engine.enqueue('enrich-signup', {
    userId: 'u_alice',
    email: 'alice@example.com',
    ip: '203.0.113.42',
  })
  await engine.tick()

  const elapsed = Date.now() - started
  console.log(`\nDone in ${elapsed}ms (≈ 1.2s).`)
  console.log('Sequential would have taken ≈ 3.2s.')

  storage.close()
}

main()

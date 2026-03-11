/**
 * Example 1: Solo SaaS — User Signup Pipeline
 *
 * Scenario: A user clicks "Sign Up" on your SaaS app. You need to:
 *   1. Create their account in your database
 *   2. Create a Stripe customer and subscribe them
 *   3. Provision their resources (create a workspace, seed data)
 *   4. Send a welcome email
 *
 * This takes 5-10 seconds total. You can't block the HTTP request.
 * And if step 3 fails, you've already charged them — you need to know
 * what succeeded and what didn't, and pick up where you left off.
 *
 * Run: npx tsx examples/1-saas-signup.ts
 */
import { createServer } from 'node:http'
import { createWorkflow, createEngine } from '../src/index'
import { SQLiteStorage } from '../src/storage/sqlite-node'
import { z } from 'zod'

// Simulated services (imagine these are real API calls)
const db = {
  createUser: async (email: string) => {
    await sleep(500) // slow DB call
    return { userId: `usr_${Date.now()}` }
  },
}
const stripe = {
  createCustomer: async (email: string, plan: string) => {
    await sleep(800) // Stripe API is slow
    return { customerId: `cus_${Date.now()}` }
  },
}
const provisioner = {
  createWorkspace: async (userId: string) => {
    await sleep(2000) // provisioning takes a while
    return { workspaceId: `ws_${Date.now()}` }
  },
}
const email = {
  sendWelcome: async (to: string) => {
    await sleep(300)
  },
}
function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms))
}

// =====================================================
// ❌ WITHOUT REFLOW — What most people do
// =====================================================
//
// app.post('/signup', async (req, res) => {
//   const { email, plan } = req.body
//
//   try {
//     const user = await db.createUser(email)
//     const customer = await stripe.createCustomer(email, plan)
//     const workspace = await provisioner.createWorkspace(user.userId)  // 💥 what if this crashes?
//     await email.sendWelcome(email)
//
//     res.json({ userId: user.userId })
//   } catch (err) {
//     // Problems:
//     // 1. If provisioning fails, the user exists in DB and Stripe but has no workspace
//     // 2. You don't know WHICH step failed without extra logging
//     // 3. There's no way to resume — you'd have to re-run everything or build
//     //    manual state tracking with flags like `stripe_done`, `workspace_done`
//     // 4. The user is staring at a loading spinner for 3+ seconds
//     res.status(500).json({ error: 'Signup failed' })
//   }
// })
//
// To make this reliable, you'd need to:
//   - Add a `signup_state` column tracking which steps completed
//   - Build a retry loop that checks state and skips completed steps
//   - Handle partial failures and compensation
//   - Build a background job system to not block the request
//
// That's ~200 lines of state management code. Or...

// =====================================================
// ✅ WITH REFLOW
// =====================================================

const signupWorkflow = createWorkflow({
  name: 'user-signup',
  input: z.object({
    email: z.string(),
    plan: z.enum(['free', 'pro', 'enterprise']),
  }),
})
  .step('create-account', async ({ input }) => {
    const user = await db.createUser(input.email)
    console.log(`  ✓ Account created: ${user.userId}`)
    return { userId: user.userId }
    // If the process crashes here, this step is marked "completed" in SQLite.
    // On restart, it won't run again.
  })
  .step('setup-billing', async ({ prev, input }) => {
    const customer = await stripe.createCustomer(input.email, input.plan)
    console.log(`  ✓ Stripe customer: ${customer.customerId}`)
    return { userId: prev.userId, customerId: customer.customerId }
    // prev.userId comes from the previous step — fully typed.
  })
  .step('provision-workspace', async ({ prev }) => {
    const workspace = await provisioner.createWorkspace(prev.userId)
    console.log(`  ✓ Workspace ready: ${workspace.workspaceId}`)
    return { userId: prev.userId, workspaceId: workspace.workspaceId }
    // This is the slow step. If it fails, Reflow knows steps 1-2 succeeded.
    // On retry, it skips account creation and billing — no double charges.
  })
  .step('send-welcome', async ({ input }) => {
    await email.sendWelcome(input.email)
    console.log(`  ✓ Welcome email sent to ${input.email}`)
  })
  .onFailure(async ({ error, stepName, input }) => {
    console.error(`  ✗ Signup failed at "${stepName}" for ${input.email}: ${error.message}`)
    // You know exactly which step failed and can compensate
  })

// Wire it up
const storage = new SQLiteStorage('/tmp/reflow-saas-example.db')
const engine = createEngine({ storage, workflows: [signupWorkflow] })

const server = createServer(async (req, res) => {
  if (req.method === 'POST' && req.url === '/signup') {
    const body = await readBody(req)
    const input = JSON.parse(body)

    // enqueue() returns immediately — user isn't waiting
    const run = await engine.enqueue('user-signup', input)

    // User sees this instantly, while the workflow runs in the background
    res.writeHead(202, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({
      message: 'Signup started! You\'ll receive a welcome email shortly.',
      runId: run.id,
    }))
    return
  }

  res.writeHead(404).end('Not found')
})

async function readBody(req: import('node:http').IncomingMessage): Promise<string> {
  let data = ''
  for await (const chunk of req) data += chunk
  return data
}

async function main() {
  await engine.start(500)

  server.listen(3000, () => {
    console.log('SaaS app running on http://localhost:3000\n')
    console.log('Try it:')
    console.log('  curl -X POST http://localhost:3000/signup \\')
    console.log("    -H 'Content-Type: application/json' \\")
    console.log("    -d '{\"email\":\"alice@example.com\",\"plan\":\"pro\"}'")
    console.log('\nWatch the steps execute in the background...\n')
  })

  process.on('SIGINT', () => {
    engine.stop()
    storage.close()
    server.close()
    process.exit(0)
  })
}

main()

# What is Reflow

Reflow is durable workflow execution for TypeScript. You define a multi-step operation — a signup, an import, an AI pipeline — as a chain of steps, and Reflow makes each step durable: persisted, retryable, and resumable after a crash. It runs on a SQLite file next to your app, or on your PostgreSQL database — there is no workflow service to deploy.

## The problem

You have a multi-step operation. You write it as a normal async function:

```typescript
app.post('/signup', async (req, res) => {
  await createAccount(req.body)     // ✅ done
  await chargeStripe(req.body)      // ✅ done
  // 💥 process crashes, deploy happens, laptop sleeps
  await sendWelcomeEmail(req.body)  // ❌ never runs
})
```

Now the user is charged but never got their welcome email — and you don't know which steps completed. Re-run everything and they get double-charged.

The usual fix is to hand-roll checkpoint logic: state columns, retry loops, deduplication. That's a few hundred lines of infrastructure that's hard to test and easy to get wrong.

**Reflow makes each step durable instead.** If the process crashes after step 2 of 5, a new engine instance reclaims the stale run once its lease expires and picks up at step 3. Active workers heartbeat their lease while they run, completed steps are never re-executed, and each step's output is persisted in SQLite or Postgres.

## Who is this for

**Solo devs and small teams** who need reliable multi-step workflows but don't want to run Temporal clusters or pay for cloud workflow services.

- **AI pipelines** — LLM calls cost money; don't re-run a \$0.05 call because the next step failed.
- **SaaS apps** — background jobs that must complete: signup flows, billing, provisioning.
- **CLI tools** — long-running imports or migrations that should resume after interruption.

**Don't use Reflow when:**

- You need workflows written in several languages, or event histories in the millions per run.
- You need sub-second pickup of work enqueued by a different process (it waits for the next poll).
- You're already running Temporal or similar.

## How it compares

|  | Reflow | Temporal | Inngest |
|---|---|---|---|
| What you run | Your app, and a SQLite file or your Postgres | Temporal Server, a database, and workers | Inngest's server (hosted or self-hosted) and an SDK |
| Setup | `npm install reflow-ts` | Cluster deployment | Account or self-hosted server, plus the SDK |
| Best for | Apps, CLIs, and AI agents, from one process to many machines | Very large, multi-language systems | Serverless functions |

## Next steps

- [Installation](/guide/install) — add Reflow and pick a storage adapter.
- [Quick Start](/guide/quick-start) — define and run your first workflow.
- [Workflows](/guide/workflows) — the building blocks: steps, `prev`, `steps`, and the immutable builder.

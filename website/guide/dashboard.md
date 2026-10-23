# Dashboard

A web view of your runs: what is running, what is waiting and on what, what failed and why, and how each run spent its time. It runs inside your own app, against the storage your engine already uses, with no extra services.

<img class="dashboard-shot shot-light" src="/dashboard-light.jpg" alt="The Reflow dashboard: workflows in a sidebar, a charge-invoice workflow's runs in a list, and one run open showing its capture step failing after four attempts, with the wait before each retry" width="2000" height="1194" />
<img class="dashboard-shot shot-dark" src="/dashboard-dark.jpg" alt="The Reflow dashboard: workflows in a sidebar, a charge-invoice workflow's runs in a list, and one run open showing its capture step failing after four attempts, with the wait before each retry" width="2000" height="1194" />

## From the command line

Point it at a SQLite database to look around:

```bash
npx reflow-ts dashboard ./reflow.db
# http://127.0.0.1:4400/  (read-only)
```

It serves on `127.0.0.1` by default and cannot change anything: with no engine attached there is nothing to cancel or resume with. It opens the database read-only, so the file is left exactly as it was — safe for a live database or a backup. It answers only to requests addressed to the host and port it is bound on, so a website you visit cannot reach it by re-pointing its own domain at your machine. Pass `--port` to pick a port, and `--host` to bind elsewhere — anyone who can reach that address can read every run's input and output.

A database from an older version of Reflow is refused rather than upgraded behind your back; start your app on the new version once, or pass `--migrate`. The command never creates a database at a path that does not exist.

## In your app

`createDashboard()` returns a standard Fetch API handler — `(request: Request) => Promise<Response>` — so it mounts directly wherever your server speaks Request and Response:

```typescript
import { createDashboard } from 'reflow-ts/dashboard'

const dashboard = createDashboard({
  storage,
  engine, // optional — see below
  workflows: [signup, invoice], // optional — the list you pass to createEngine
  authorize: (request) => request.headers.get('authorization') === `Bearer ${process.env.REFLOW_ADMIN_TOKEN}`,
})
```

::: code-group

```typescript [Bun]
Bun.serve({ port: 4400, fetch: dashboard })
```

```typescript [Hono]
const app = new Hono()
const reflow = createDashboard({ storage, engine, authorize, basePath: '/admin/reflow' })
app.all('/admin/reflow/*', (c) => reflow(c.req.raw))
```

```typescript [Next.js]
// app/admin/reflow/[[...path]]/route.ts
const reflow = createDashboard({ storage, engine, authorize, basePath: '/admin/reflow' })
export const GET = reflow
export const POST = reflow
```

```typescript [Express / node:http]
import { toNodeHandler } from 'reflow-ts/dashboard'

// Express strips the mount path, so leave basePath as '/'
app.use('/admin/reflow', toNodeHandler(dashboard))

// or on its own
createServer(toNodeHandler(dashboard)).listen(4400)
```

:::

| Option | Description |
|---|---|
| `storage` | The storage to read runs from — the same one your engine uses. |
| `authorize(request)` | **Required.** Return `true` to let the request through, `false` to answer 401. Called for the page and every API request; may be `async`. Check what the request carries — a session, a token — not where it comes from. |
| `allowedHosts` | The `Host` values to answer to, such as `['localhost:4400']`; anything else gets 421. **Set it whenever `authorize` checks no credentials.** Also list your public host if a proxy in front rewrites `Host`. |
| `engine` | Optional. Enables cancelling and resuming runs, and live updates. Without it the dashboard is read-only and refreshes every few seconds. |
| `workflows` | Optional. The workflows your engine runs, as passed to `createEngine`. With them the dashboard lists every workflow with its steps in order, and shows which steps a run has not reached yet. Without them it lists the workflows seen in recent runs, and only the steps that have run. |
| `basePath` | The path it is mounted at, when your server passes full paths (Bun, Hono, Next.js). Default `/`. |
| `title` | Shown in the header and the browser tab. Default `Reflow`. |

## What it shows

A sidebar lists every workflow, with how many of its runs have failed or are still in progress, so switching between them is one click. Pick one and its runs fill the middle; pick a run and its detail opens beside them, so moving to the next run — a click, or the arrow keys — never means going back. On a phone the panes show one at a time.

- **Workflows.** Each workflow you defined, with its steps in order and how many of its runs are failed, waiting, sleeping, or completed. A workflow is the recipe; every `enqueue()` starts a new, independent run of it.
- **A workflow's runs**, newest first. Each run is labelled by its input — *email: ada@example.com* — so runs of the same workflow can be told apart, with one line on where it stands (*Retrying capture in 3s, after 2 failed attempts*, *Waiting for approved, times out in 23h*) and its steps so far. Counts by status double as filters.
- **One run.** Where it stands in a sentence, then each step with its status, how long it took, and where it fell in the run's time. A retried step lists every attempt with its error and the wait before the next; a stretch where nothing ran, such as a crash before another engine picked the run up, is called out between steps. Then the run's input and output.
- **All runs**, across workflows, for triage — every failed run, say.
- **Schedules**, with when each next fires.

## Live updates and load

With `engine`, the page updates as that engine moves runs on. Changes made by other processes — workers deployed separately from the app serving the dashboard — show up within five seconds, through polling. Without `engine`, everything arrives that way.

However busy the engine, an open dashboard refreshes at most once a second, and the counts, run pages, and insights it reads are shared between viewers for a second or two, so several open tabs cost about as much as one. Against SQLite, every read it makes is an index range rather than a scan; on a database of a million runs, each answers in a few milliseconds, except listing runs of every workflow filtered by status, which takes on the order of 100ms.

## Naming runs

Ten runs of `user-signup` are easier to tell apart by who signed up than by id. Give the workflow a `label`, computed from each run's typed input:

```typescript
const signup = createWorkflow({
  name: 'user-signup',
  input: z.object({ userId: z.string(), email: z.string() }),
  label: (input) => input.userId,
})
```

Without one, a run is shown by its idempotency key if it has one, or else by its short id. The dashboard never shows input in the list unless a `label` puts it there.

A label is shown in every list, so choose something that is fine for anyone with dashboard access to see: an id or an order number rather than an email address or a name. If personal data is the useful handle, mask it — `` `${input.email.slice(0, 2)}…@${input.email.split('@')[1]}` ``. The run's full input is still shown on its own page, to anyone `authorize` lets in.

The dashboard reads labels from the definitions passed as `workflows`.

## Security

Run inputs and outputs are your application's data, so the dashboard is built to be safe to expose behind your own auth:

- `authorize` is required — there is no default that serves data to anyone. Open access is an explicit `() => true`, and must come with `allowedHosts`: a dashboard that checks no credentials is otherwise readable by any site the viewer visits, which can re-point its own domain at the dashboard's address (DNS rebinding) and so count as same-origin.
- Cancel and resume exist only when you pass `engine`, and only as `POST` requests carrying a header that a cross-site page cannot send, from the dashboard's own host. A link or form elsewhere cannot trigger them. The host is compared rather than the whole origin, so actions keep working behind a TLS-terminating proxy.
- A slow or stalled browser tab cannot make the server buffer live updates, nor slow the engine: notices it cannot take are folded into a single refresh.
- The page loads nothing from any other origin and runs under a strict content security policy with a per-response nonce; it cannot be framed. Run data is only ever rendered as text, never as markup.
- The run list never includes inputs or outputs; only opening a run fetches them.

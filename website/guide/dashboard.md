# Dashboard

A web view of your runs: what is running, what is waiting and on what, what failed and why, and how each run spent its time. It runs inside your own app, against the storage your engine already uses, with no extra services.

## From the command line

Point it at a SQLite database to look around:

```bash
npx reflow-ts dashboard ./reflow.db
# http://127.0.0.1:4400/  (read-only)
```

It serves on `127.0.0.1` by default and cannot change anything: with no engine attached there is nothing to cancel or resume with. Pass `--port` to pick a port, and `--host` to bind elsewhere — anyone who can reach that address can read every run's input and output.

A database from an older version of Reflow is refused rather than upgraded behind your back; start your app on the new version once, or pass `--migrate`. The command never creates a database at a path that does not exist.

## In your app

`createDashboard()` returns a standard Fetch API handler — `(request: Request) => Promise<Response>` — so it mounts directly wherever your server speaks Request and Response:

```typescript
import { createDashboard } from 'reflow-ts/dashboard'

const dashboard = createDashboard({
  storage,
  engine, // optional — see below
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
| `authorize(request)` | **Required.** Return `true` to let the request through, `false` to answer 401. Called for the page and every API request; may be `async`. |
| `engine` | Optional. Enables cancelling and resuming runs, and live updates. Without it the dashboard is read-only and refreshes every few seconds. |
| `basePath` | The path it is mounted at, when your server passes full paths (Bun, Hono, Next.js). Default `/`. |
| `title` | Shown in the header and the browser tab. Default `Reflow`. |

## What it shows

- **Counts by status**, which double as the filter: click *Failed* to see only failed runs. Filter by workflow name too.
- **Every run, newest first**, with one line on where it stands — *Waiting for email-verified, times out in 23h*, *Resumes in 8m*, *ship: Warehouse API 503*.
- **A run's flow**: its steps drawn on one line, each as long as the time it took. Work is solid, durable pauses — sleeps, waits, retry backoffs — are dashed, a failure ends in a stop, and each retry is a notch. Branches of a parallel group are stacked, since they ran side by side.
- **Its steps** with their status, attempts, duration, and errors, and the run's input and output.
- **Schedules**, with when each next fires.

## Security

Run inputs and outputs are your application's data, so the dashboard is built to be safe to expose behind your own auth:

- `authorize` is required — there is no default that serves data to anyone. Open access is an explicit `() => true`.
- Cancel and resume exist only when you pass `engine`, and only as `POST` requests carrying a header that a cross-site page cannot send, from the dashboard's own origin. A link or form elsewhere cannot trigger them.
- The page loads nothing from any other origin and runs under a strict content security policy with a per-response nonce; it cannot be framed. Run data is only ever rendered as text, never as markup.
- The run list never includes inputs or outputs; only opening a run fetches them.

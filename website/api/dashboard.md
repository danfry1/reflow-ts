# Dashboard

The web dashboard's API, exported from `reflow-ts/dashboard`. See [Dashboard](/guide/dashboard) for setup, mounting, and what the page shows.

## `createDashboard(options)`

```typescript
import { basicAuth, createDashboard } from 'reflow-ts/dashboard'

const dashboard = createDashboard({
  storage,
  engine,
  workflows: [orderWorkflow, emailWorkflow],
  authorize: basicAuth({ username: 'admin', password: process.env.REFLOW_DASHBOARD_PASSWORD }),
})

Bun.serve({ port: 4400, fetch: dashboard })
```

Returns a `DashboardHandler` — `(request: Request) => Promise<Response>`, a standard Fetch API handler — so it mounts as-is in Bun.serve, Deno, Hono, or a Next.js route handler. Use [`toNodeHandler`](#tonodehandler-handler) for `node:http` and Express.

| Option | Type | Default | Description |
|---|---|---|---|
| `storage` | `StorageAdapter` | required | Where runs are read from — the same storage your engine uses |
| `authorize` | `(request) => boolean \| Response \| Promise<…>` | required | Decides whether a request may see the dashboard; called for every request, page and API alike. Return `false` to answer 401, or a `Response` to send it instead, such as a redirect to your login page. Check something the request carries — a session cookie, a bearer token — not where it comes from. To serve it with no check, pass `() => true` together with `allowedHosts` |
| `allowedHosts` | `readonly string[]` | — | The `Host` values the dashboard answers to, such as `['localhost:4400']`; any other host gets 421. Set it whenever `authorize` does not check credentials, against DNS rebinding. Also list the public host when a proxy in front rewrites `Host` |
| `engine` | `Pick<Engine, 'cancel' \| 'resume' \| 'stream'>` | — | With it, the dashboard can cancel and resume runs and updates live; without it, it is read-only and refreshes by polling |
| `basePath` | `string` | `'/'` | The path the dashboard is mounted at, when the handler receives full request paths (Bun.serve, Hono, Next.js). Leave it as `/` when the host strips the prefix itself, as Express's `app.use(prefix, …)` does |
| `title` | `string` | `'Reflow'` | Shown in the page title and header |
| `workflows` | `readonly DescribedWorkflow[]` | — | The workflows your engine runs — the same list passed to `createEngine`. With it, every workflow is listed with its steps in order, including steps a run has not reached; without it, only workflows seen in recent runs, and only the steps that have run |

## `DescribedWorkflow`

The part of a workflow the dashboard reads. Any workflow from `createWorkflow` satisfies it.

```typescript
interface DescribedWorkflow {
  readonly name: string
  readonly executionUnits?: readonly ExecutionUnit[]  // none for a workflow written as a function
  label?(input: PersistedValue): string
}
```

## `basicAuth(options)`

An `authorize` that asks for a username and password with the browser's own sign-in prompt — for an app with no login of its own to reuse. The browser sends the password with every request, encoded but not encrypted, so serve the dashboard over HTTPS anywhere but localhost.

| Option (`BasicAuthOptions`) | Type | Description |
|---|---|---|
| `username` | `string` | Non-empty, without `:` |
| `password` | `string \| undefined` | Read it from the environment. Typed to accept `undefined` so an unset variable fails at startup |
| `realm` | `string` | Optional. Shown by some browsers in the sign-in prompt. Defaults to `"Reflow"` |

Throws [`ConfigError`](/api/errors) if the username or password is empty or missing, so an unset environment variable fails at startup rather than locking everyone out or letting an empty password in.

## `toNodeHandler(handler)`

Adapts a Fetch API handler, such as a `DashboardHandler`, to Node's `(req, res)` signature. Streams the response as it is produced, so live updates work, and aborts the request's `signal` when the client disconnects.

```typescript
import { createServer } from 'node:http'
import { toNodeHandler } from 'reflow-ts/dashboard'

createServer(toNodeHandler(dashboard)).listen(4400)

// Express strips the mount path, so leave `basePath` as '/'
app.use('/reflow', toNodeHandler(dashboard))
```

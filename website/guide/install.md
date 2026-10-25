# Installation

Reflow is ESM-only and ships TypeScript types. **Node.js 18.18 or newer** is required (the first Node 18 release with `Symbol.asyncDispose`, used by the [result stream](/guide/streaming)).

## Install

On Node.js 22.13+ and on Bun, SQLite is built into the runtime, so Reflow is the only package to install:

::: code-group
```bash [npm]
npm install reflow-ts
```
```bash [pnpm]
pnpm add reflow-ts
```
```bash [bun]
bun add reflow-ts
```
:::

On older Node, add `better-sqlite3`; to store runs in PostgreSQL, add `pg`. Both are optional peer dependencies.

## Pick a storage adapter

Reflow's storage is pluggable. Three SQLite adapters ship in the box, one per driver, and behave the same; pick the one for your runtime:

```typescript
// Node.js 22.13+ — built-in node:sqlite, no native dependencies
import { SQLiteStorage } from 'reflow-ts/sqlite-node-builtin'
const storage = new SQLiteStorage('./reflow.db')
```

```typescript
// Bun — built-in bun:sqlite, no native dependencies
import { SQLiteStorage } from 'reflow-ts/sqlite-bun'
const storage = new SQLiteStorage('./reflow.db')
```

```typescript
// Node.js 18.18+ — better-sqlite3 (a native addon)
import { SQLiteStorage } from 'reflow-ts/sqlite-node'
const storage = new SQLiteStorage('./reflow.db')
```

When workers run on more than one machine, use [PostgreSQL](/guide/storage#postgresstorage) instead (`npm install pg`):

```typescript
import { PostgresStorage } from 'reflow-ts/postgres'
const storage = new PostgresStorage({ connectionString: process.env.DATABASE_URL })
```

::: tip node:sqlite prints an experimental warning
Node still marks `node:sqlite` experimental, so it prints an `ExperimentalWarning` once at startup. It works without a flag from Node 22.13 and 23.4; Node 22.5–22.12 need `--experimental-sqlite`. To avoid the warning, use `reflow-ts/sqlite-node` with `better-sqlite3`.
:::

::: warning better-sqlite3 13 requires Node 22
Reflow supports Node 18.18+, but `better-sqlite3` 13 raises its own floor to Node 22, so a plain `npm install better-sqlite3` fails on Node 18–21 with an engine error. Reflow's peer range is `>=9`, so pin the older major there:

```bash
npm install reflow-ts better-sqlite3@12   # Node 18-21
npm install reflow-ts better-sqlite3      # Node 22+
```
:::

See [Storage](/guide/storage) for the in-memory adapter and how to write your own.

## Pick a schema library

Reflow validates workflow input with [Standard Schema](https://github.com/standard-schema/standard-schema), so any compatible library works:

```bash
bun add zod        # or
bun add valibot    # or
bun add arktype    # or any Standard Schema-compatible library
```

Next: [Quick Start](/guide/quick-start) walks through defining and running a workflow.

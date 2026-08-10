# Installation

Reflow is ESM-only and ships TypeScript types. **Node.js 18.18 or newer** is required (the first Node 18 release with `Symbol.asyncDispose`, used by the [result stream](/guide/streaming)).

## Install

::: code-group
```bash [bun]
# Uses the built-in bun:sqlite module — no native dependencies
bun add reflow-ts
```
```bash [npm]
npm install reflow-ts better-sqlite3
```
```bash [pnpm]
pnpm add reflow-ts better-sqlite3
```
:::

## Pick a storage adapter

Reflow's storage is pluggable. Two SQLite adapters ship in the box — pick the one for your runtime:

```typescript
// Bun — zero native dependencies (built-in bun:sqlite)
import { SQLiteStorage } from 'reflow-ts/sqlite-bun'
const storage = new SQLiteStorage('./reflow.db')
```

```typescript
// Node.js — uses better-sqlite3 (a native addon), WAL mode
import { SQLiteStorage } from 'reflow-ts/sqlite-node'
const storage = new SQLiteStorage('./reflow.db')
```

On Node, `better-sqlite3` is an optional peer dependency — install it alongside Reflow. On Bun, nothing extra is needed.

::: warning better-sqlite3 13 requires Node 22
Reflow supports Node 18.18+, but `better-sqlite3` 13 raises its own floor to Node 22, so a plain `npm install better-sqlite3` fails on Node 18–21 with an engine error.

Reflow's peer range is `>=9`, so pin the older major there:

```bash
npm install reflow-ts better-sqlite3@12   # Node 18-21
npm install reflow-ts better-sqlite3      # Node 22+
```

Only the `reflow-ts/sqlite-node` adapter is affected. Bun's adapter and the `node:sqlite` adapter have no native dependency — though the latter needs Node 22.5+ for its own reasons.
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

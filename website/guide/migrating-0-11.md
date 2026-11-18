# Migrating to 0.11

This page covers upgrading from 0.9 or 0.10. 0.10 added [cached steps](/guide/caching); 0.11 added [workflows written as functions](/guide/functions) (beta). Chained workflows are unchanged.

**Workflow code needs no changes.** The built-in storage adapters migrate their tables on `initialize()`, which `engine.start()` calls. Some types changed, which may need a line of code; see below.

## Databases migrate themselves

| Release | SQLite schema | Postgres schema | Adds |
|---|---|---|---|
| 0.10 | 5 | 2 | A table for cached steps |
| 0.11 | 6 | 3 | A nullable `kind` column on step rows |

Coming from 0.9, both migrations apply on the first `initialize()`. Each runs once and keeps every existing row.

- **Postgres roles.** A role that may only read and write rows cannot run the migration. Run `initialize()` once as the schema's owner — at deploy time, say — before the app starts under its usual role.
- **Rolling deploys.** Workers still on the old version that are already running carry on against the migrated database, but refuse it once they restart. Finish the rollout rather than rolling back. Downgrading needs a database from before the upgrade.
- **The dashboard CLI.** `npx reflow-ts dashboard` opens a database read-only, so it refuses one from before the upgrade. Start your app on the new version once, or pass `--migrate`.

## Type changes

- **`AnyWorkflow` includes workflows written as functions.** Code that reads `executionUnits`, or chains steps, on an `AnyWorkflow` narrows on `workflow.kind === 'chain'` first, or uses the new `ChainWorkflow` type.
- **`ReflowErrorCode` gains `'RUN_SUSPENDED'`.** An exhaustive `switch` over the codes needs a case for it.
- **A step that resolves to `undefined` is typed `undefined`**, not `never`, in `.step()` and `.parallel()` branches alike. Code after such a step may now be asked to handle the `undefined` it was told could not happen.
- **`InferSteps` of something that is not a workflow** is `Record<string, PersistedValue>`, not `never`.

## Custom storage adapters

Everything new is optional. An adapter without it keeps working with the behaviour it had before. See [Custom adapters](/guide/storage#custom-adapters) for each method's contract.

- **0.10:** `getCachedStep`, `putCachedStep`, and `deleteExpiredCachedSteps` let workflows [cache steps](/guide/caching); an engine refuses a chained workflow that caches a step on an adapter without them. `consumeEvent` takes a waiting run's event and saves its step in one transaction, so a crash between the two can no longer lose the event.
- **0.11:** store and return `StepResult.kind`. `supportsEventNames` (with `consumeEvent`'s `eventName` and `waitRun` taking a list of names) lets a function workflow name a wait apart from its event and have several pending at once. `supportsKeepFailed` (with `requeueRun(runId, { keepFailed })`) keeps the failures a function caught when a run is resumed.

The conformance suite checks all of these:

```typescript
import { storageConformanceCases } from 'reflow-ts/conformance'
```

# Caching Steps

A step can reuse the result an earlier run already computed, instead of running again. Fetching the same profile, embedding the same document, or calling the same paid API for the same input: cache the step, and later runs get the saved result.

```typescript
const summarize = createWorkflow({ name: 'summarize', input: z.object({ url: z.string() }) })
  .step('fetch', async ({ input }) => ({ html: await fetchPage(input.url) }))
  .step('summary', {
    cache: '24h',
    handler: async ({ prev }) => ({ text: await llm.summarize(prev.html) }),
  })
```

A second run for the same page within 24 hours skips `summary`'s handler. The step is still recorded as `completed`, with the cached output, and later steps receive it as `prev` and in `steps` exactly as if it had run.

This is different from what a run does after a crash. Within one run, finished steps are never repeated anyway; see [Crash Recovery](/guide/crash-recovery). Caching is for **separate runs** that would compute the same thing.

## What counts as the same

By default a cached result is reused when the step's **workflow input and previous step's output** both match, so a step is never handed a result computed from different data. To decide for yourself, give `key`:

```typescript
.step('profile', {
  cache: { ttl: '1h', key: ({ input }) => input.userId },
  handler: async ({ input }) => await crm.getProfile(input.userId),
})
```

`key` receives `{ input, prev, steps }` and returns any plain value. Results are kept per workflow and step, so two steps with the same key never share one.

## When the code changes

Reflow cannot tell when a step's code changes, so a cached result from the old code is reused until it expires. Change `version` to start afresh:

```typescript
cache: { ttl: '7d', version: 2 }, // bumped when the prompt changed
```

## What is cached

- Only a step that **completes**. A failed step is not cached, so the next run tries it again.
- Not a step that calls `complete()`, since reusing it would not end the run.
- Sequential `.step()` only. A `.parallel()` branch with `cache` throws a [`ConfigError`](/guide/error-handling).

A step reused from the cache fires `stepStart` and `stepComplete` as usual, with `cached: true` on `stepComplete` and `attempts: 0`.

## Expiry and clean-up

`ttl` is milliseconds or a duration such as `'30m'`, `'24h'`, or `'7d'`. Shortening it applies straight away, to results cached under the longer one too. [`engine.prune()`](/guide/inspecting-runs) deletes expired results along with old runs.

If storage fails to read or write the cache, the step runs as if nothing was cached and the error goes to the [`onError`](/guide/hooks) hook, so a cache problem never fails a run.

## Storage

The built-in SQLite, Postgres, and in-memory adapters store cached results in a table of their own, which outlives the runs that produced them. A [custom adapter](/guide/storage#custom-adapters) needs `getCachedStep`, `putCachedStep`, and `deleteExpiredCachedSteps`; an engine with a cached step refuses an adapter without them.

# Parallel Steps

Run independent steps concurrently with `.parallel()`. Each branch is a named handler that runs at the same time as its siblings; the next step receives a merged record of all branch outputs as `prev`.

```typescript
const pipeline = createWorkflow({ name: 'pipeline', input: z.object({ url: z.string() }) })
  .step('fetch', async ({ input }) => ({ body: await fetchPage(input.url) }))
  .parallel({
    summary: async ({ prev }) => ({ text: await summarize(prev.body) }),
    keywords: async ({ prev }) => ({ tags: await extractKeywords(prev.body) }),
    images: async ({ prev }) => ({ urls: await extractImages(prev.body) }),
  })
  .step('save', async ({ prev }) => {
    // prev is fully typed:
    // { summary: { text }, keywords: { tags }, images: { urls } }
    await save(prev.summary.text, prev.keywords.tags, prev.images.urls)
  })
```

## Per-branch retry and timeout

Branches accept the same `{ retry, timeoutMs, handler }` config form as `.step()`, so each branch can have its own policy:

```typescript
.parallel({
  flaky: {
    retry: { maxAttempts: 3, backoff: 'exponential', initialDelayMs: 100 },
    handler: async () => await callFlakyApi(),
  },
  stable: async () => await callStableApi(),
})
```

## Semantics

- **Fail-fast.** When one branch fails (after exhausting its own retries), siblings receive `signal.abort()` and the run is marked `failed`. [`onRunFailed`](/guide/hooks) and [`onFailure`](/guide/failure-handling) report the branch that actually caused the failure, not a sibling aborted by the propagation.
- **Crash recovery is per-branch.** If the engine crashes after some branches persisted their results, recovery skips those and re-runs only the missing ones. Side effects in already-completed branches do not fire twice.
- **No `complete()` inside a branch.** Calling `complete()` from a parallel branch throws [`ParallelCompleteError`](/guide/error-handling) — [early completion](/guide/early-completion) is only meaningful in sequential context.
- **Each branch must be idempotent.** Like sequential steps, a branch may run multiple times across crash recoveries before its result is persisted.
- **`steps` is a frozen snapshot.** All sibling branches see the same `steps` view, taken before the group started; they cannot observe each other's outputs mid-flight.

## Naming

Branch names share the same namespace as `.step()` names — a duplicate anywhere in the workflow throws [`DuplicateStepError`](/guide/error-handling). In the next step's `prev` and in `steps`, each branch appears under its own key.

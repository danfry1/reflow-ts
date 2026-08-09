import { ParallelCompleteError, ReflowError, RunControlError } from '../errors'
import type { PersistedValue } from '../types'
import type { StepDefinition } from '../workflow'
import { toError } from './signals'
import { runStepHandler } from './step'
import type { ExecutionContext, UnitExecutor, UnitOutcome } from './types'

/**
 * Carries branch metadata (name, original error, attempts) out through a
 * rejected branch promise, so the group can report *which* branch failed.
 *
 * @internal Not exported from the public API.
 */
class BranchFailedError extends ReflowError {
  constructor(
    public readonly branchName: string,
    public readonly branchError: Error,
    public readonly attempts: number,
  ) {
    super(branchError.message)
    this.name = 'BranchFailedError'
  }
}

/**
 * Executes a `.parallel()` group.
 *
 * Branches run concurrently and settle together: the first genuine failure
 * aborts its siblings, but every branch is given the chance to unwind before
 * the group reports, so a failure cannot leave sibling work running detached.
 * Branches that already persisted as `completed` are replayed rather than
 * re-run, which is what makes a partially-completed group crash-safe.
 */
export const parallelExecutor: UnitExecutor<{ kind: 'parallel'; branches: readonly StepDefinition[] }> = {
  async execute(unit, ctx, prev): Promise<UnitOutcome> {
    const merged: Record<string, PersistedValue> = {}
    const pendingBranches: StepDefinition[] = []

    // Crash-recovery: reuse any branch already persisted as `completed`. Failed
    // records do not count — they retry fresh, matching sequential semantics.
    for (const branchDef of unit.branches) {
      const existing = ctx.replay.get(branchDef.name)
      if (existing?.status === 'completed') {
        merged[branchDef.name] = existing.output
        ctx.steps[branchDef.name] = structuredClone(existing.output)
      } else {
        pendingBranches.push(branchDef)
      }
    }

    if (pendingBranches.length === 0) {
      return { kind: 'advance', output: merged }
    }

    const frozenSteps = ctx.snapshotSteps()
    const runSignal = ctx.signals.run.signal

    // Branches share a group-scoped abort so one failure can cancel its
    // siblings without aborting the run itself.
    const groupAbort = new AbortController()
    // Track which branch was the *original* cause of the abort. Siblings that
    // fail because they observed the abort are downstream effects, not the
    // cause — distinguishing them keeps `onRunFailed` accurate.
    let causeBranch: BranchFailedError | null = null

    const onRunAbort = () => {
      if (!groupAbort.signal.aborted) {
        groupAbort.abort(runSignal.reason)
      }
    }

    if (runSignal.aborted) {
      groupAbort.abort(runSignal.reason)
    } else {
      runSignal.addEventListener('abort', onRunAbort, { once: true })
    }

    try {
      for (const branchDef of pendingBranches) {
        await ctx.emit({
          type: 'stepStart',
          runId: ctx.run.id,
          workflow: ctx.run.workflow,
          stepName: branchDef.name,
        })
      }

      const settled = await Promise.allSettled(
        pendingBranches.map(async (branchDef) => {
          // `complete()` has no meaning inside a concurrent branch — there is no
          // well-defined "rest of the workflow" to skip from here.
          const guardedDef: StepDefinition = {
            ...branchDef,
            handler: (handlerCtx) => branchDef.handler({
              ...handlerCtx,
              complete: (): never => {
                throw new ParallelCompleteError(branchDef.name)
              },
            }),
          }

          const outcome = await runStepHandler(
            guardedDef,
            { input: ctx.run.input, runSignal: groupAbort.signal },
            prev,
            frozenSteps,
          )

          // `early-complete` is unreachable: the guard above throws
          // ParallelCompleteError (a plain error) before EarlyCompleteError can
          // be raised, so a branch only ever completes or fails.
          if (outcome.kind === 'failed') {
            const failure = new BranchFailedError(branchDef.name, outcome.error, outcome.attempts)
            if (!groupAbort.signal.aborted) {
              causeBranch = failure
              groupAbort.abort(outcome.error)
            }
            throw failure
          }

          return { name: branchDef.name, output: outcome.output, attempts: outcome.attempts }
        }),
      )

      // If the run itself was aborted by a control-flow signal while branches
      // ran, surface that instead of a branch failure — a branch that failed
      // because it observed the run-level abort is a downstream effect, and
      // reporting it would mark a stopped, reclaimable run as failed.
      if (runSignal.aborted && runSignal.reason instanceof RunControlError) {
        return { kind: 'halt' }
      }

      const firstFailure = causeBranch ?? findFirstFailure(settled, pendingBranches)
      if (firstFailure === 'halt') {
        return { kind: 'halt' }
      }

      if (firstFailure) {
        // Persist the failed branch. If the lease was lost mid-write we still
        // report the failure — the caller's run-status update fails the same
        // lease check and no-ops.
        await ctx.trySaveStep({
          name: firstFailure.branchName,
          status: 'failed',
          output: null,
          error: firstFailure.branchError.message,
          attempts: firstFailure.attempts,
        })

        return { kind: 'failed', stepName: firstFailure.branchName, error: firstFailure.branchError }
      }

      for (const result of settled) {
        if (result.status !== 'fulfilled') continue
        const branch = result.value

        await ctx.saveStep({
          name: branch.name,
          status: 'completed',
          output: branch.output,
          attempts: branch.attempts,
        })

        await ctx.emit({
          type: 'stepComplete',
          runId: ctx.run.id,
          workflow: ctx.run.workflow,
          stepName: branch.name,
          output: branch.output,
          attempts: branch.attempts,
        })

        ctx.steps[branch.name] = structuredClone(branch.output)
        merged[branch.name] = branch.output
      }

      return { kind: 'advance', output: merged }
    } catch (error) {
      // `Promise.allSettled` absorbs per-branch failures, so this only fires for
      // a LeaseExpiredError thrown while persisting the success path.
      const err = toError(error)

      if (err instanceof RunControlError) {
        return { kind: 'halt' }
      }

      return { kind: 'failed', stepName: pendingBranches[0].name, error: err }
    } finally {
      runSignal.removeEventListener('abort', onRunAbort)
    }
  },
}

/**
 * Pick the failure to report when no branch recorded itself as the cause —
 * for example when the group was aborted from outside the branch loop.
 * Returns `'halt'` if the rejection was a control-flow signal.
 */
function findFirstFailure(
  settled: readonly PromiseSettledResult<{ name: string; output: PersistedValue; attempts: number }>[],
  pendingBranches: readonly StepDefinition[],
): BranchFailedError | 'halt' | null {
  for (const result of settled) {
    if (result.status !== 'rejected') continue

    const err = toError(result.reason)
    if (err instanceof RunControlError) {
      return 'halt'
    }
    if (err instanceof BranchFailedError) {
      return err
    }

    // Defensive: attribute an unrecognised rejection to the first pending branch.
    return new BranchFailedError(pendingBranches[0].name, err, 1)
  }

  return null
}

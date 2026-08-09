import { ConfigError } from './errors'

/**
 * The first occurrence strictly after `now`, given a firing that was due at
 * `dueAt` and repeats every `intervalMs`.
 *
 * Missed occurrences are **skipped, not backfilled**: a process down for three
 * hours with an hourly schedule fires once when it returns and then resumes the
 * normal cadence, rather than enqueuing three runs at once. Backfilling a queue
 * of stale work is almost never what a recurring job wants, and it turns an
 * outage into a thundering herd.
 *
 * Computed arithmetically rather than by stepping the interval, so a long
 * outage on a short interval costs one operation instead of millions.
 */
export function nextOccurrence(dueAt: number, intervalMs: number, now: number): number {
  // A non-positive or non-finite interval would yield NaN, and `NaN <= now` is
  // false forever — the schedule would stop firing and never say why. Only a
  // corrupted row can reach this (`schedule()` validates the interval), so
  // failing loudly is better than writing a value that silently kills it.
  if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
    throw new ConfigError(
      `Schedule interval must be a positive, finite number of milliseconds, got ${intervalMs}`,
    )
  }

  if (dueAt > now) {
    return dueAt
  }

  const missed = Math.floor((now - dueAt) / intervalMs) + 1
  return dueAt + missed * intervalMs
}

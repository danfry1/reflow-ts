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
  if (dueAt > now) {
    return dueAt
  }

  const missed = Math.floor((now - dueAt) / intervalMs) + 1
  return dueAt + missed * intervalMs
}

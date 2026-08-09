import { parseCron, nextCronOccurrence } from './cron'
import { ConfigError } from './errors'
import type { ScheduleRecurrence } from './types'

/**
 * The first occurrence strictly after `now`, given a firing that was due at
 * `dueAt` under `recurrence`.
 *
 * Missed occurrences are **skipped, not backfilled**: a process down for three
 * hours with an hourly schedule fires once when it returns and then resumes the
 * normal cadence, rather than enqueuing three runs at once. Backfilling a queue
 * of stale work is almost never what a recurring job wants, and it turns an
 * outage into a thundering herd.
 */
export function nextOccurrence(
  dueAt: number,
  recurrence: ScheduleRecurrence,
  now: number,
): number {
  if (dueAt > now) {
    return dueAt
  }

  if (recurrence.kind === 'cron') {
    // Anchored on `now` rather than on `dueAt`, which is what skips the missed
    // occurrences: asking from `dueAt` would walk them one at a time.
    return nextCronOccurrence(parseCron(recurrence.expression), now)
  }

  const { intervalMs } = recurrence

  // A non-positive or non-finite interval would yield NaN, and `NaN <= now` is
  // false forever — the schedule would stop firing and never say why. Only a
  // corrupted row can reach this (`schedule()` validates on registration), so
  // failing loudly is better than writing a value that silently kills it.
  if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
    throw new ConfigError(
      `Schedule interval must be a positive, finite number of milliseconds, got ${intervalMs}`,
    )
  }

  // Computed arithmetically rather than by stepping the interval, so a long
  // outage on a short interval costs one operation instead of millions.
  const missed = Math.floor((now - dueAt) / intervalMs) + 1
  return dueAt + missed * intervalMs
}

/** Whether two recurrences describe the same cadence, for cadence-preserving upserts. */
export function sameRecurrence(left: ScheduleRecurrence, right: ScheduleRecurrence): boolean {
  if (left.kind === 'cron' && right.kind === 'cron') {
    return left.expression === right.expression
  }
  if (left.kind === 'interval' && right.kind === 'interval') {
    return left.intervalMs === right.intervalMs
  }
  return false
}

/** The first occurrence of a newly registered schedule, measured from `now`. */
export function firstOccurrence(recurrence: ScheduleRecurrence, now: number): number {
  return recurrence.kind === 'cron'
    ? nextCronOccurrence(parseCron(recurrence.expression), now)
    : now + recurrence.intervalMs
}

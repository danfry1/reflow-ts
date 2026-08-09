import { ConfigError } from './errors'

const UNIT_MS = {
  ms: 1,
  s: 1000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
} as const satisfies Record<string, number>

/** A supported duration suffix. Derived from {@link UNIT_MS} — the units are declared once. */
export type DurationUnit = keyof typeof UNIT_MS

// Built from the unit table so the accepted suffixes cannot drift from the ones
// that have a conversion. Longest-first, so `ms` is matched before `m`.
const UNIT_ALTERNATION = Object.keys(UNIT_MS)
  .sort((left, right) => right.length - left.length)
  .join('|')

const DURATION_PATTERN = new RegExp(`^(\\d+(?:\\.\\d+)?)\\s*(${UNIT_ALTERNATION})$`)

function isDurationUnit(value: string): value is DurationUnit {
  return value in UNIT_MS
}

/**
 * Normalize a duration to milliseconds.
 *
 * Accepts a non-negative number of milliseconds, or a string with a single unit
 * suffix: `ms`, `s`, `m`, `h`, or `d` (e.g. `'500ms'`, `'30s'`, `'24h'`, `'7d'`).
 *
 * @throws {ConfigError} if the value is negative, non-finite, or an unparseable string.
 */
export function parseDuration(value: number | string): number {
  if (typeof value === 'number') {
    if (!Number.isFinite(value) || value < 0) {
      throw new ConfigError(`Invalid duration: ${value}ms must be a non-negative, finite number`)
    }
    return value
  }

  const match = DURATION_PATTERN.exec(value.trim())
  const rawAmount = match?.[1]
  const rawUnit = match?.[2]

  // The pattern guarantees both groups when it matches; narrowing them here
  // proves it to the compiler instead of asserting it.
  if (rawAmount === undefined || rawUnit === undefined || !isDurationUnit(rawUnit)) {
    throw new ConfigError(
      `Invalid duration string "${value}". Use a number of milliseconds or a value with a unit suffix, e.g. "500ms", "30s", "24h", "7d".`,
    )
  }

  return Number(rawAmount) * UNIT_MS[rawUnit]
}

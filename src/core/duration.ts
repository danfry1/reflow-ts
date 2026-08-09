import { ConfigError } from './errors'

const UNIT_MS: Record<string, number> = {
  ms: 1,
  s: 1000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
}

const DURATION_PATTERN = /^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)$/

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
  if (!match) {
    throw new ConfigError(
      `Invalid duration string "${value}". Use a number of milliseconds or a value with a unit suffix, e.g. "500ms", "30s", "24h", "7d".`,
    )
  }

  const amount = Number(match[1])
  const unit = match[2]
  return amount * UNIT_MS[unit]
}

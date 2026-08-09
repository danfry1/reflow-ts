import { ConfigError } from './errors'

/**
 * A minimal, dependency-free cron parser and occurrence calculator.
 *
 * Supports the standard five-field form — `minute hour day-of-month month
 * day-of-week` — with wildcards, single values, `a-b` ranges, a `/step`
 * suffix on either, comma-separated lists, three-letter month and day names,
 * and the common `@hourly` / `@daily` / `@weekly` / `@monthly` / `@yearly`
 * aliases.
 *
 * **All times are UTC.** Reflow does not interpret cron expressions in a local
 * time zone, because doing that correctly means deciding what a schedule means
 * during a DST gap (a wall-clock time that does not occur) and a DST overlap (one
 * that occurs twice) — and guessing produces a scheduler that silently skips or
 * doubles a run twice a year. UTC has no such ambiguity.
 */

/** A parsed cron expression. Field sets hold every value that matches. */
export interface CronExpression {
  /** The expression as written, for error messages and round-tripping. */
  readonly source: string
  readonly minutes: ReadonlySet<number>
  readonly hours: ReadonlySet<number>
  readonly daysOfMonth: ReadonlySet<number>
  readonly months: ReadonlySet<number>
  readonly daysOfWeek: ReadonlySet<number>
  /**
   * Whether the day-of-month and day-of-week fields were narrowed from `*`.
   *
   * Cron's oldest quirk: when *both* are restricted the fields are OR'd, not
   * AND'd, so `0 0 13 * 5` means "the 13th, and also every Friday". When only
   * one is restricted the other is ignored.
   */
  readonly dayOfMonthRestricted: boolean
  readonly dayOfWeekRestricted: boolean
}

interface FieldSpec {
  readonly name: string
  readonly min: number
  readonly max: number
  readonly names?: Readonly<Record<string, number>>
}

const MONTH_NAMES = {
  jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6,
  jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12,
} as const

const DAY_NAMES = {
  sun: 0, mon: 1, tue: 2, wed: 3, thu: 4, fri: 5, sat: 6,
} as const

const FIELDS: readonly FieldSpec[] = [
  { name: 'minute', min: 0, max: 59 },
  { name: 'hour', min: 0, max: 23 },
  { name: 'day-of-month', min: 1, max: 31 },
  { name: 'month', min: 1, max: 12, names: MONTH_NAMES },
  { name: 'day-of-week', min: 0, max: 7, names: DAY_NAMES },
]

const ALIASES: Readonly<Record<string, string>> = {
  '@yearly': '0 0 1 1 *',
  '@annually': '0 0 1 1 *',
  '@monthly': '0 0 1 * *',
  '@weekly': '0 0 * * 0',
  '@daily': '0 0 * * *',
  '@midnight': '0 0 * * *',
  '@hourly': '0 * * * *',
}

/** How far ahead {@link nextCronOccurrence} searches before declaring an expression unsatisfiable. */
const SEARCH_LIMIT_DAYS = 4 * 366

/**
 * Parse a cron expression.
 *
 * @throws {ConfigError} if the expression is malformed or a field is out of range.
 */
export function parseCron(source: string): CronExpression {
  const trimmed = source.trim()
  if (trimmed.length === 0) {
    throw new ConfigError('Cron expression must not be empty')
  }

  const normalized = ALIASES[trimmed.toLowerCase()] ?? trimmed
  const parts = normalized.split(/\s+/)

  if (parts.length !== 5) {
    throw new ConfigError(
      `Cron expression "${source}" must have 5 fields (minute hour day-of-month month day-of-week), got ${parts.length}`,
    )
  }

  const sets = parts.map((part, index) => {
    const spec = FIELDS[index]
    if (!spec) {
      throw new ConfigError(`Cron expression "${source}" has too many fields`)
    }
    return parseField(part, spec, source)
  })

  const [minutes, hours, daysOfMonth, months, dayOfWeekRaw] = sets
  if (!minutes || !hours || !daysOfMonth || !months || !dayOfWeekRaw) {
    throw new ConfigError(`Cron expression "${source}" could not be parsed`)
  }

  // Both 0 and 7 mean Sunday; normalise so matching only has to check one.
  const daysOfWeek = new Set([...dayOfWeekRaw].map((day) => (day === 7 ? 0 : day)))

  return {
    source: trimmed,
    minutes,
    hours,
    daysOfMonth,
    months,
    daysOfWeek,
    dayOfMonthRestricted: parts[2] !== '*',
    dayOfWeekRestricted: parts[4] !== '*',
  }
}

function parseField(field: string, spec: FieldSpec, source: string): Set<number> {
  const values = new Set<number>()

  for (const term of field.split(',')) {
    if (term.length === 0) {
      throw new ConfigError(`Cron expression "${source}" has an empty ${spec.name} term`)
    }

    const [rangePart, stepPart, ...excess] = term.split('/')
    if (excess.length > 0 || rangePart === undefined) {
      throw new ConfigError(`Cron ${spec.name} "${term}" in "${source}" has more than one step`)
    }

    let step = 1
    if (stepPart !== undefined) {
      step = Number(stepPart)
      if (!Number.isInteger(step) || step < 1) {
        throw new ConfigError(`Cron ${spec.name} step "${stepPart}" in "${source}" must be a positive integer`)
      }
    }

    let start: number
    let end: number

    if (rangePart === '*') {
      start = spec.min
      end = spec.max
    } else {
      const bounds = rangePart.split('-')
      if (bounds.length > 2) {
        throw new ConfigError(`Cron ${spec.name} "${term}" in "${source}" is not a valid range`)
      }
      const [lowRaw, highRaw] = bounds
      start = parseValue(lowRaw, spec, source)
      // `5/15` means "from 5 to the field maximum, every 15" — a bare value with
      // a step is an open-ended range, not a single value.
      end = highRaw !== undefined ? parseValue(highRaw, spec, source) : (stepPart !== undefined ? spec.max : start)
    }

    if (start > end) {
      throw new ConfigError(`Cron ${spec.name} range "${term}" in "${source}" starts after it ends`)
    }

    for (let value = start; value <= end; value += step) {
      values.add(value)
    }
  }

  if (values.size === 0) {
    throw new ConfigError(`Cron ${spec.name} "${field}" in "${source}" matches nothing`)
  }

  return values
}

function parseValue(raw: string | undefined, spec: FieldSpec, source: string): number {
  if (raw === undefined || raw.length === 0) {
    throw new ConfigError(`Cron ${spec.name} in "${source}" has an empty value`)
  }

  const named = spec.names?.[raw.toLowerCase()]
  const value = named ?? Number(raw)

  if (!Number.isInteger(value)) {
    throw new ConfigError(`Cron ${spec.name} "${raw}" in "${source}" is not a valid value`)
  }
  if (value < spec.min || value > spec.max) {
    throw new ConfigError(
      `Cron ${spec.name} "${raw}" in "${source}" is out of range (${spec.min}-${spec.max})`,
    )
  }

  return value
}

/** Whether a given UTC date satisfies the expression's day fields. */
function matchesDay(cron: CronExpression, date: Date): boolean {
  if (!cron.months.has(date.getUTCMonth() + 1)) {
    return false
  }

  const dayOfMonthMatches = cron.daysOfMonth.has(date.getUTCDate())
  const dayOfWeekMatches = cron.daysOfWeek.has(date.getUTCDay())

  // The OR quirk: with both fields restricted, either one matching is enough.
  if (cron.dayOfMonthRestricted && cron.dayOfWeekRestricted) {
    return dayOfMonthMatches || dayOfWeekMatches
  }
  if (cron.dayOfMonthRestricted) {
    return dayOfMonthMatches
  }
  if (cron.dayOfWeekRestricted) {
    return dayOfWeekMatches
  }
  return true
}

/**
 * The first time strictly after `after` (epoch ms, UTC) that matches `cron`.
 *
 * Searches by whole days rather than by minute, so a sparse expression like
 * `0 0 1 1 *` costs a few hundred iterations rather than half a million.
 *
 * @throws {ConfigError} if nothing matches within four years, which means the
 * expression is unsatisfiable — `0 0 30 2 *` asks for the 30th of February.
 */
export function nextCronOccurrence(cron: CronExpression, after: number): number {
  // Start at the next whole minute; cron has no sub-minute resolution, and
  // starting at `after` itself would re-fire the occurrence just handled.
  const cursor = new Date(after)
  cursor.setUTCSeconds(0, 0)
  cursor.setUTCMinutes(cursor.getUTCMinutes() + 1)

  // A wall-clock deadline rather than an iteration counter: every branch below
  // moves the cursor strictly forward, so the loop provably terminates.
  const deadline = after + SEARCH_LIMIT_DAYS * 86_400_000

  const nextDay = () => {
    cursor.setUTCDate(cursor.getUTCDate() + 1)
    cursor.setUTCHours(0, 0, 0, 0)
  }

  while (cursor.getTime() <= deadline) {
    if (!matchesDay(cron, cursor)) {
      nextDay()
      continue
    }

    const hour = nextInSet(cron.hours, cursor.getUTCHours())
    if (hour === null) {
      nextDay()
      continue
    }
    if (hour !== cursor.getUTCHours()) {
      // Moved to a later hour, so the minute search restarts from the top of it.
      cursor.setUTCHours(hour, 0, 0, 0)
    }

    const minute = nextInSet(cron.minutes, cursor.getUTCMinutes())
    if (minute === null) {
      // Rolls into the next hour, and past midnight into the next day if needed.
      cursor.setUTCHours(cursor.getUTCHours() + 1, 0, 0, 0)
      continue
    }

    cursor.setUTCMinutes(minute, 0, 0)
    return cursor.getTime()
  }

  throw new ConfigError(
    `Cron expression "${cron.source}" has no occurrence within four years — it cannot be satisfied`,
  )
}

/** The smallest member of `set` that is >= `from`, or null if there is none. */
function nextInSet(set: ReadonlySet<number>, from: number): number | null {
  let best: number | null = null
  for (const value of set) {
    if (value >= from && (best === null || value < best)) {
      best = value
    }
  }
  return best
}

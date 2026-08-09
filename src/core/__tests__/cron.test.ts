import { describe, it, expect } from 'vitest'
import { parseCron, nextCronOccurrence } from '../cron'
import { ConfigError } from '../errors'

/** Next occurrence after an ISO instant, as an ISO string — readable expectations. */
function next(expression: string, after: string): string {
  return new Date(nextCronOccurrence(parseCron(expression), Date.parse(after))).toISOString()
}

describe('parseCron', () => {
  it('expands a wildcard to the whole field', () => {
    const cron = parseCron('* * * * *')

    expect(cron.minutes.size).toBe(60)
    expect(cron.hours.size).toBe(24)
    expect(cron.daysOfMonth.size).toBe(31)
    expect(cron.months.size).toBe(12)
  })

  it('parses single values, lists, ranges, and steps', () => {
    expect([...parseCron('5 * * * *').minutes]).toStrictEqual([5])
    expect([...parseCron('1,3,5 * * * *').minutes]).toStrictEqual([1, 3, 5])
    expect([...parseCron('10-13 * * * *').minutes]).toStrictEqual([10, 11, 12, 13])
    expect([...parseCron('*/15 * * * *').minutes]).toStrictEqual([0, 15, 30, 45])
    expect([...parseCron('0-30/10 * * * *').minutes]).toStrictEqual([0, 10, 20, 30])
  })

  it('treats a bare value with a step as an open-ended range', () => {
    // `5/15` is "from 5 onward, every 15" — not the single value 5.
    expect([...parseCron('5/15 * * * *').minutes]).toStrictEqual([5, 20, 35, 50])
  })

  it('accepts month and day names, case-insensitively', () => {
    expect([...parseCron('0 0 * JAN *').months]).toStrictEqual([1])
    expect([...parseCron('0 0 * * mon').daysOfWeek]).toStrictEqual([1])
    expect([...parseCron('0 0 * mar-may *').months]).toStrictEqual([3, 4, 5])
  })

  it('treats day-of-week 7 and 0 as the same Sunday', () => {
    expect([...parseCron('0 0 * * 7').daysOfWeek]).toStrictEqual([0])
    expect([...parseCron('0 0 * * 0').daysOfWeek]).toStrictEqual([0])
  })

  it('expands the named aliases', () => {
    expect(next('@daily', '2026-03-10T12:00:00Z')).toBe('2026-03-11T00:00:00.000Z')
    expect(next('@hourly', '2026-03-10T12:30:00Z')).toBe('2026-03-10T13:00:00.000Z')
    expect(next('@weekly', '2026-03-10T12:00:00Z')).toBe('2026-03-15T00:00:00.000Z')
    expect(next('@monthly', '2026-03-10T12:00:00Z')).toBe('2026-04-01T00:00:00.000Z')
    expect(next('@yearly', '2026-03-10T12:00:00Z')).toBe('2027-01-01T00:00:00.000Z')
  })

  it('records which day fields were narrowed', () => {
    const both = parseCron('0 0 13 * 5')
    expect(both.dayOfMonthRestricted).toBe(true)
    expect(both.dayOfWeekRestricted).toBe(true)

    const neither = parseCron('0 0 * * *')
    expect(neither.dayOfMonthRestricted).toBe(false)
    expect(neither.dayOfWeekRestricted).toBe(false)
  })

  it.each([
    ['', 'empty'],
    ['* * * *', 'four fields'],
    ['* * * * * *', 'six fields'],
    ['60 * * * *', 'minute out of range'],
    ['* 24 * * *', 'hour out of range'],
    ['* * 0 * *', 'day-of-month below range'],
    ['* * 32 * *', 'day-of-month above range'],
    ['* * * 13 *', 'month out of range'],
    ['* * * * 8', 'day-of-week out of range'],
    ['30-10 * * * *', 'range runs backwards'],
    ['*/0 * * * *', 'zero step'],
    ['*/-1 * * * *', 'negative step'],
    ['1//2 * * * *', 'double step'],
    ['nope * * * *', 'not a number'],
    ['1,,2 * * * *', 'empty list term'],
  ])('rejects %s (%s)', (expression) => {
    expect(() => parseCron(expression)).toThrow(ConfigError)
  })
})

describe('nextCronOccurrence', () => {
  it('returns the next matching minute, never the current one', () => {
    // Exactly on an occurrence must advance, or a schedule would re-fire the
    // occurrence it just handled.
    expect(next('*/15 * * * *', '2026-03-10T12:00:00Z')).toBe('2026-03-10T12:15:00.000Z')
    expect(next('*/15 * * * *', '2026-03-10T12:00:30Z')).toBe('2026-03-10T12:15:00.000Z')
    expect(next('*/15 * * * *', '2026-03-10T12:14:59Z')).toBe('2026-03-10T12:15:00.000Z')
  })

  it('rolls forward across hour, day, month, and year boundaries', () => {
    expect(next('0 * * * *', '2026-03-10T12:30:00Z')).toBe('2026-03-10T13:00:00.000Z')
    expect(next('0 9 * * *', '2026-03-10T12:00:00Z')).toBe('2026-03-11T09:00:00.000Z')
    expect(next('0 0 1 * *', '2026-03-10T12:00:00Z')).toBe('2026-04-01T00:00:00.000Z')
    expect(next('0 0 1 1 *', '2026-03-10T12:00:00Z')).toBe('2027-01-01T00:00:00.000Z')
  })

  it('handles weekday schedules', () => {
    // 2026-03-10 is a Tuesday.
    expect(next('0 9 * * 1-5', '2026-03-10T10:00:00Z')).toBe('2026-03-11T09:00:00.000Z')
    // Friday 13:00 → the next weekday occurrence is Monday.
    expect(next('0 9 * * 1-5', '2026-03-13T13:00:00Z')).toBe('2026-03-16T09:00:00.000Z')
  })

  it('ORs day-of-month with day-of-week when both are restricted', () => {
    // Cron's oldest quirk. `0 0 13 * 5` means the 13th *or* any Friday.
    // From Mon 2026-03-09: Friday the 13th is both, so it is the next either way —
    // the discriminating case is the Friday before the 13th of a later month.
    expect(next('0 0 13 * 5', '2026-03-09T00:00:00Z')).toBe('2026-03-13T00:00:00.000Z')
    // April 2026: the 3rd is a Friday, well before the 13th.
    expect(next('0 0 13 * 5', '2026-04-01T00:00:00Z')).toBe('2026-04-03T00:00:00.000Z')
  })

  it('ignores day-of-week when only day-of-month is restricted', () => {
    // The 13th regardless of weekday. 2026-05-13 is a Wednesday.
    expect(next('0 0 13 * *', '2026-05-01T00:00:00Z')).toBe('2026-05-13T00:00:00.000Z')
  })

  it('skips months that do not have the requested day', () => {
    // The 31st: from January, the next is March — February never has one.
    expect(next('0 0 31 * *', '2026-01-31T12:00:00Z')).toBe('2026-03-31T00:00:00.000Z')
  })

  it('handles 29 February only in leap years', () => {
    // 2027 and 2028: the next 29 Feb after 2026 is in 2028.
    expect(next('0 0 29 2 *', '2026-03-01T00:00:00Z')).toBe('2028-02-29T00:00:00.000Z')
  })

  it('rejects an expression that can never match', () => {
    // 30 February. Better to fail loudly than to search forever.
    expect(() => next('0 0 30 2 *', '2026-01-01T00:00:00Z')).toThrow(ConfigError)
    expect(() => next('0 0 30 2 *', '2026-01-01T00:00:00Z')).toThrow(/cannot be satisfied/)
  })

  it('is stable when applied repeatedly', () => {
    // Feeding each result back in must walk the schedule forward one step at a
    // time — the property the engine relies on when advancing a claimed row.
    const cron = parseCron('0 9 * * 1-5')
    let at = Date.parse('2026-03-09T00:00:00Z')
    const seen: string[] = []

    for (let i = 0; i < 6; i++) {
      at = nextCronOccurrence(cron, at)
      seen.push(new Date(at).toISOString())
    }

    expect(seen).toStrictEqual([
      '2026-03-09T09:00:00.000Z',
      '2026-03-10T09:00:00.000Z',
      '2026-03-11T09:00:00.000Z',
      '2026-03-12T09:00:00.000Z',
      '2026-03-13T09:00:00.000Z',
      '2026-03-16T09:00:00.000Z',
    ])
  })

  it('always returns a time strictly after the input', () => {
    const cron = parseCron('*/7 */3 * * *')
    for (const iso of [
      '2026-01-01T00:00:00Z', '2026-06-15T23:59:59Z',
      '2026-12-31T23:58:00Z', '2026-02-28T12:34:56Z',
    ]) {
      const at = Date.parse(iso)
      expect(nextCronOccurrence(cron, at)).toBeGreaterThan(at)
    }
  })

  it('lands on a zeroed second and millisecond', () => {
    const at = nextCronOccurrence(parseCron('*/5 * * * *'), Date.parse('2026-03-10T12:01:37.482Z'))
    const date = new Date(at)

    expect(date.getUTCSeconds()).toBe(0)
    expect(date.getUTCMilliseconds()).toBe(0)
  })
})

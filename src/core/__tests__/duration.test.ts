import { describe, it, expect } from 'vitest'
import { parseDuration } from '../duration'
import { ConfigError } from '../errors'

describe('parseDuration', () => {
  it('passes through non-negative millisecond numbers', () => {
    expect(parseDuration(0)).toBe(0)
    expect(parseDuration(1500)).toBe(1500)
  })

  it('parses unit-suffixed strings', () => {
    expect(parseDuration('500ms')).toBe(500)
    expect(parseDuration('30s')).toBe(30_000)
    expect(parseDuration('5m')).toBe(300_000)
    expect(parseDuration('24h')).toBe(86_400_000)
    expect(parseDuration('7d')).toBe(604_800_000)
  })

  it('tolerates whitespace and decimals', () => {
    expect(parseDuration(' 2h ')).toBe(7_200_000)
    expect(parseDuration('1.5s')).toBe(1500)
  })

  it('rejects negative or non-finite numbers', () => {
    expect(() => parseDuration(-1)).toThrow(ConfigError)
    expect(() => parseDuration(Number.POSITIVE_INFINITY)).toThrow(ConfigError)
    expect(() => parseDuration(Number.NaN)).toThrow(ConfigError)
  })

  it('rejects unparseable strings', () => {
    expect(() => parseDuration('soon')).toThrow(ConfigError)
    expect(() => parseDuration('10')).toThrow(ConfigError)
    expect(() => parseDuration('10 weeks')).toThrow(ConfigError)
    expect(() => parseDuration('')).toThrow(ConfigError)
  })
})

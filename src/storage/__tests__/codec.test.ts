import { describe, it, expect } from 'vitest'
import {
  serializePersistedValue,
  deserializePersistedValue,
  clonePersistedValue,
  persistedValuesEqual,
} from '../codec'

describe('codec', () => {
  describe('serializePersistedValue / deserializePersistedValue', () => {
    it('roundtrips primitives', () => {
      expect(deserializePersistedValue(serializePersistedValue('hello', 'test'))).toBe('hello')
      expect(deserializePersistedValue(serializePersistedValue(42, 'test'))).toBe(42)
      expect(deserializePersistedValue(serializePersistedValue(true, 'test'))).toBe(true)
      expect(deserializePersistedValue(serializePersistedValue(false, 'test'))).toBe(false)
      expect(deserializePersistedValue(serializePersistedValue(null, 'test'))).toBe(null)
    })

    it('roundtrips undefined through a sentinel', () => {
      expect(deserializePersistedValue(serializePersistedValue(undefined, 'test'))).toBeUndefined()
    })

    it('deserializes null input (SQL NULL) as null', () => {
      expect(deserializePersistedValue(null)).toBe(null)
    })

    it('roundtrips nested objects and arrays', () => {
      const value = { a: [1, 'two', null, { nested: true }], b: undefined }
      const result = deserializePersistedValue(serializePersistedValue(value, 'test'))
      expect(result).toEqual(value)
    })

    it('rejects NaN', () => {
      expect(() => serializePersistedValue(NaN, 'Input')).toThrow(/NaN or Infinity/)
    })

    it('rejects Infinity', () => {
      expect(() => serializePersistedValue(Infinity, 'Input')).toThrow(/NaN or Infinity/)
    })

    it('rejects negative Infinity', () => {
      expect(() => serializePersistedValue(-Infinity, 'Input')).toThrow(/NaN or Infinity/)
    })

    it('rejects the reserved undefined sentinel key', () => {
      expect(() =>
        serializePersistedValue({ __reflowUndefined__: true }, 'Input'),
      ).toThrow(/reserved key/)
    })

    it('rejects invalid Date values on serialization', () => {
      expect(() => serializePersistedValue(new Date('not-a-date'), 'Input')).toThrow(/invalid Date/)
    })

    it('rejects invalid Date strings on deserialization', () => {
      const corrupted = JSON.stringify({ __reflowDate__: 'not-a-date' })
      expect(() => deserializePersistedValue(corrupted)).toThrow(/invalid Date/)
    })

    it('rejects the reserved Date sentinel key', () => {
      expect(() =>
        serializePersistedValue({ __reflowDate__: '2025-01-01' }, 'Input'),
      ).toThrow(/reserved key/)
    })

    it('rejects non-JSON-compatible values', () => {
      const fn = () => {}
      expect(() => serializePersistedValue(fn as any, 'Input')).toThrow(/JSON-compatible/)
    })

    it('rejects non-plain objects (class instances)', () => {
      expect(() => serializePersistedValue(new Map() as any, 'Input')).toThrow(/JSON-compatible/)
    })

    it('roundtrips Date objects', () => {
      const date = new Date('2025-06-15T12:00:00.000Z')
      const result = deserializePersistedValue(serializePersistedValue(date, 'test'))
      expect(result).toBeInstanceOf(Date)
      expect((result as Date).toISOString()).toBe('2025-06-15T12:00:00.000Z')
    })

    it('roundtrips Date inside nested objects', () => {
      const value = { createdAt: new Date('2025-01-01'), tags: ['a'] }
      const result = deserializePersistedValue(serializePersistedValue(value, 'test'))
      expect((result as Record<string, unknown>).createdAt).toBeInstanceOf(Date)
      expect((result as Record<string, unknown>).tags).toEqual(['a'])
    })

    it('roundtrips Date inside arrays', () => {
      const value = [new Date('2025-01-01'), new Date('2025-06-01')]
      const result = deserializePersistedValue(serializePersistedValue(value, 'test'))
      expect((result as unknown[])[0]).toBeInstanceOf(Date)
      expect((result as unknown[])[1]).toBeInstanceOf(Date)
    })

    it('roundtrips empty string', () => {
      expect(deserializePersistedValue(serializePersistedValue('', 'test'))).toBe('')
    })

    it('roundtrips zero', () => {
      expect(deserializePersistedValue(serializePersistedValue(0, 'test'))).toBe(0)
    })
  })

  describe('clonePersistedValue', () => {
    it('creates a deep clone', () => {
      const original = { a: [1, 2], b: { c: 3 } }
      const clone = clonePersistedValue(original, 'test')
      expect(clone).toEqual(original)
      expect(clone).not.toBe(original)
    })
  })

  describe('persistedValuesEqual', () => {
    it('returns true for structurally equal values', () => {
      expect(persistedValuesEqual({ a: 1 }, { a: 1 })).toBe(true)
    })

    it('returns false for different values', () => {
      expect(persistedValuesEqual({ a: 1 }, { a: 2 })).toBe(false)
    })

    it('returns true for identical references', () => {
      const obj = { a: 1 }
      expect(persistedValuesEqual(obj, obj)).toBe(true)
    })

    it('is key-order independent for objects', () => {
      const left = { b: 2, a: 1 }
      const right = { a: 1, b: 2 }
      expect(persistedValuesEqual(left, right)).toBe(true)
    })

    it('compares arrays element-by-element', () => {
      expect(persistedValuesEqual([1, 2, 3], [1, 2, 3])).toBe(true)
      expect(persistedValuesEqual([1, 2], [1, 2, 3])).toBe(false)
      expect(persistedValuesEqual([1, 2, 3], [1, 2])).toBe(false)
    })

    it('returns false for mismatched types', () => {
      expect(persistedValuesEqual(1, '1')).toBe(false)
      expect(persistedValuesEqual(null, undefined)).toBe(false)
      expect(persistedValuesEqual([1], { 0: 1 })).toBe(false)
    })

    it('handles null and undefined correctly', () => {
      expect(persistedValuesEqual(null, null)).toBe(true)
      expect(persistedValuesEqual(undefined, undefined)).toBe(true)
    })

    it('compares nested structures deeply', () => {
      expect(persistedValuesEqual({ a: { b: [1] } }, { a: { b: [1] } })).toBe(true)
      expect(persistedValuesEqual({ a: { b: [1] } }, { a: { b: [2] } })).toBe(false)
    })

    it('returns false for objects with different key counts', () => {
      expect(persistedValuesEqual({ a: 1 }, { a: 1, b: 2 })).toBe(false)
    })

    it('compares primitive values correctly', () => {
      expect(persistedValuesEqual(42, 42)).toBe(true)
      expect(persistedValuesEqual('hello', 'hello')).toBe(true)
      expect(persistedValuesEqual(true, false)).toBe(false)
    })
  })

  describe('isPlainObject edge cases', () => {
    it('handles Object.create(null) objects', () => {
      const nullProto = Object.create(null)
      nullProto.key = 'value'
      const result = deserializePersistedValue(serializePersistedValue(nullProto, 'test'))
      expect(result).toEqual({ key: 'value' })
    })
  })
})

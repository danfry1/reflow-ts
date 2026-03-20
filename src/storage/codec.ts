import { SerializationError } from '../core/errors'
import type { PersistedObject, PersistedValue } from '../core/types'

const UNDEFINED_SENTINEL_KEY = '__reflowUndefined__'
const DATE_SENTINEL_KEY = '__reflowDate__'

export function serializePersistedValue(value: PersistedValue, context: string): string {
  return JSON.stringify(encodePersistedValue(value, context, '$'))
}

export function deserializePersistedValue(serialized: string | null): PersistedValue {
  if (serialized === null) {
    return null
  }

  return decodePersistedValue(JSON.parse(serialized))
}

export function clonePersistedValue(value: PersistedValue, context: string): PersistedValue {
  return deserializePersistedValue(serializePersistedValue(value, context))
}

export function persistedValuesEqual(left: PersistedValue, right: PersistedValue): boolean {
  if (left === right) return true
  if (left === null || right === null || left === undefined || right === undefined) return left === right
  if (typeof left !== typeof right) return false
  if (typeof left !== 'object') return left === right
  if (left instanceof Date || right instanceof Date) {
    return left instanceof Date && right instanceof Date && left.getTime() === right.getTime()
  }
  if (Array.isArray(left) !== Array.isArray(right)) return false
  if (Array.isArray(left) && Array.isArray(right)) {
    if (left.length !== right.length) return false
    return left.every((entry, index) => persistedValuesEqual(entry, right[index]))
  }
  const leftObj = left as Record<string, PersistedValue>
  const rightObj = right as Record<string, PersistedValue>
  const leftKeys = Object.keys(leftObj)
  const rightKeys = Object.keys(rightObj)
  if (leftKeys.length !== rightKeys.length) return false
  return leftKeys.every((key) => persistedValuesEqual(leftObj[key], rightObj[key]))
}

function encodePersistedValue(value: PersistedValue, context: string, path: string): unknown {
  if (value === undefined) {
    return { [UNDEFINED_SENTINEL_KEY]: true }
  }

  if (
    value === null
    || typeof value === 'string'
    || typeof value === 'boolean'
  ) {
    return value
  }

  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new SerializationError(`${context} must not contain NaN or Infinity at ${path}`, path)
    }
    return value
  }

  if (value instanceof Date) {
    if (!Number.isFinite(value.getTime())) {
      throw new SerializationError(`${context} must not contain an invalid Date at ${path}`, path)
    }
    return { [DATE_SENTINEL_KEY]: value.toISOString() }
  }

  if (Array.isArray(value)) {
    return value.map((entry, index) => encodePersistedValue(entry, context, `${path}[${index}]`))
  }

  if (isPlainObject(value)) {
    if (Object.prototype.hasOwnProperty.call(value, UNDEFINED_SENTINEL_KEY)) {
      throw new SerializationError(
        `${context} must not use the reserved key "${UNDEFINED_SENTINEL_KEY}" at ${path}`,
        path,
      )
    }

    if (Object.prototype.hasOwnProperty.call(value, DATE_SENTINEL_KEY)) {
      throw new SerializationError(
        `${context} must not use the reserved key "${DATE_SENTINEL_KEY}" at ${path}`,
        path,
      )
    }

    const encoded: Record<string, unknown> = {}
    for (const [key, entry] of Object.entries(value)) {
      encoded[key] = encodePersistedValue(entry, context, `${path}.${key}`)
    }
    return encoded
  }

  throw new SerializationError(
    `${context} must be JSON-compatible data (plain objects, arrays, primitives, or undefined) at ${path}`,
    path,
  )
}

function decodePersistedValue(value: unknown): PersistedValue {
  if (
    value === null
    || value === undefined
    || typeof value === 'string'
    || typeof value === 'number'
    || typeof value === 'boolean'
  ) {
    return value
  }

  if (Array.isArray(value)) {
    return value.map((entry) => decodePersistedValue(entry))
  }

  if (isPlainObject(value)) {
    if (
      Object.keys(value).length === 1
      && value[UNDEFINED_SENTINEL_KEY] === true
    ) {
      return undefined
    }

    if (
      Object.keys(value).length === 1
      && typeof value[DATE_SENTINEL_KEY] === 'string'
    ) {
      const date = new Date(value[DATE_SENTINEL_KEY] as string)
      if (!Number.isFinite(date.getTime())) {
        throw new SerializationError(
          `Stored value contains an invalid Date string: "${value[DATE_SENTINEL_KEY]}"`,
          '$',
        )
      }
      return date
    }

    const decoded: PersistedObject = {}
    for (const [key, entry] of Object.entries(value)) {
      decoded[key] = decodePersistedValue(entry)
    }
    return decoded
  }

  throw new SerializationError('Stored value is not a valid Reflow payload', '$')
}

function isPlainObject(value: unknown): value is Record<string, PersistedValue> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return false
  }

  const prototype = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}

import { describe, it, expect } from 'vitest'
import { InternalError } from '../../core/errors'
import { emptyRunCounts, parseRunError, parseRunStatus, parseStepStatus } from '../records'

describe('stored record parsing', () => {
  it('accepts every known run and step status', () => {
    expect(parseRunStatus('waiting')).toBe('waiting')
    expect(parseStepStatus('completed-early')).toBe('completed-early')
  })

  it('refuses a status this version does not know', () => {
    expect(() => parseRunStatus('paused')).toThrow(InternalError)
    expect(() => parseStepStatus('paused')).toThrow(InternalError)
  })

  it('parses a stored run error', () => {
    expect(parseRunError('{"stepName":"a","message":"boom","code":null}')).toStrictEqual({
      stepName: 'a',
      message: 'boom',
      code: null,
    })
  })

  it('refuses a stored run error missing a field', () => {
    expect(() => parseRunError('{"stepName":"a","message":"boom"}')).toThrow(InternalError)
    expect(() => parseRunError('"boom"')).toThrow(InternalError)
  })

  it('counts zero for every status', () => {
    expect(emptyRunCounts()).toStrictEqual({
      pending: 0, running: 0, sleeping: 0, waiting: 0, completed: 0, failed: 0, cancelled: 0,
    })
  })
})

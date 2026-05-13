import { describe, it, expect } from 'vitest'
import { z } from 'zod'
import { createWorkflow } from '../workflow'
import type { ExecutionUnit, StepDefinition } from '../workflow'

function getParallelBranches(wf: { executionUnits: readonly ExecutionUnit[] }, index: number): readonly StepDefinition[] {
  const unit = wf.executionUnits[index]
  if (unit.kind !== 'parallel') throw new Error(`Expected parallel at index ${index}`)
  return unit.branches
}

describe('.parallel()', () => {
  describe('builder basics', () => {
    it('adds a parallel execution unit', () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({ x: z.number() }),
      })
        .step('fetch', async ({ input }) => ({ data: input.x }))
        .parallel({
          a: async ({ prev }) => ({ fromA: prev.data + 1 }),
          b: async ({ prev }) => ({ fromB: prev.data + 2 }),
        })

      expect(wf.executionUnits).toHaveLength(2)
      const branches = getParallelBranches(wf, 1)
      expect(branches).toHaveLength(2)
      expect(branches.map((b) => b.name)).toEqual(['a', 'b'])
    })

    it('is immutable — returns a new workflow instance', () => {
      const base = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('a', async () => ({ x: 1 }))

      const withParallel = base.parallel({
        b: async () => ({ y: 1 }),
        c: async () => ({ z: 1 }),
      })

      expect(base.executionUnits).toHaveLength(1)
      expect(withParallel.executionUnits).toHaveLength(2)
    })

    it('allows chaining .step() after .parallel()', () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({ x: z.number() }),
      })
        .step('fetch', async ({ input }) => ({ data: input.x }))
        .parallel({
          a: async () => ({ fromA: 1 }),
          b: async () => ({ fromB: 2 }),
        })
        .step('merge', async ({ prev }) => ({ sum: prev.a.fromA + prev.b.fromB }))

      expect(wf.executionUnits).toHaveLength(3)
      expect(wf.executionUnits[2].kind).toBe('step')
    })

    it('allows chaining .parallel() after .parallel()', () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      })
        .parallel({
          a: async () => ({ x: 1 }),
          b: async () => ({ y: 2 }),
        })
        .parallel({
          c: async () => ({ z: 3 }),
          d: async () => ({ w: 4 }),
        })

      expect(wf.executionUnits).toHaveLength(2)
      expect(wf.executionUnits[0].kind).toBe('parallel')
      expect(wf.executionUnits[1].kind).toBe('parallel')
    })

    it('preserves onFailure through parallel chaining', () => {
      const handler = async () => {}
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      })
        .onFailure(handler)
        .parallel({
          a: async () => ({ x: 1 }),
        })

      expect(wf.failureHandler).toBe(handler)
    })
  })

  describe('branch configuration', () => {
    it('accepts bare handler functions', () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).parallel({
        a: async () => ({ x: 1 }),
      })

      const branches = getParallelBranches(wf, 0)
      expect(branches[0].retry).toBeUndefined()
      expect(branches[0].timeoutMs).toBeUndefined()
    })

    it('accepts config objects with retry and timeout', () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).parallel({
        a: {
          retry: { maxAttempts: 3, backoff: 'exponential' as const },
          timeoutMs: 5000,
          handler: async () => ({ x: 1 }),
        },
      })

      const branches = getParallelBranches(wf, 0)
      expect(branches[0].retry).toEqual({
        maxAttempts: 3,
        backoff: 'exponential',
      })
      expect(branches[0].timeoutMs).toBe(5000)
    })

    it('accepts a mix of bare handlers and config objects', () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).parallel({
        a: async () => ({ x: 1 }),
        b: {
          retry: { maxAttempts: 2, backoff: 'linear' as const },
          handler: async () => ({ y: 2 }),
        },
      })

      const branches = getParallelBranches(wf, 0)
      expect(branches[0].retry).toBeUndefined()
      expect(branches[1].retry).toEqual({
        maxAttempts: 2,
        backoff: 'linear',
      })
    })
  })

  describe('validation', () => {
    it('rejects empty parallel object', () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      })

      expect(() => wf.parallel({})).toThrow('parallel() requires at least one branch')
    })

    it('rejects branch name that conflicts with existing step', () => {
      const wf = createWorkflow({
        name: 'dup-test',
        input: z.object({}),
      }).step('fetch', async () => ({ x: 1 }))

      expect(() =>
        wf.parallel({
          fetch: async () => ({ y: 2 }),
        }),
      ).toThrow('Step "fetch" is already defined in workflow "dup-test"')
    })

    it('rejects branch name that conflicts with a branch in a prior parallel group', () => {
      const wf = createWorkflow({
        name: 'dup-test',
        input: z.object({}),
      }).parallel({
        analyze: async () => ({ x: 1 }),
      })

      expect(() =>
        wf.parallel({
          analyze: async () => ({ y: 2 }),
        }),
      ).toThrow('Step "analyze" is already defined in workflow "dup-test"')
    })

    it('rejects step name that conflicts with a branch in a prior parallel group', () => {
      const wf = createWorkflow({
        name: 'dup-test',
        input: z.object({}),
      }).parallel({
        analyze: async () => ({ x: 1 }),
      })

      expect(() => wf.step('analyze', async () => ({}))).toThrow(
        'Step "analyze" is already defined in workflow "dup-test"',
      )
    })
  })
})

import { describe, it, expect } from 'vitest'
import { z } from 'zod'
import { createWorkflow } from '../workflow'

describe('createWorkflow', () => {
  describe('builder basics', () => {
    it('creates a workflow with a name and input schema', () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({ x: z.number() }),
      })

      expect(wf.name).toBe('test')
      expect(wf.steps).toHaveLength(0)
      expect(wf.failureHandler).toBeUndefined()
    })

    it('preserves the input schema for validation', () => {
      const schema = z.object({ email: z.email() })
      const wf = createWorkflow({ name: 'test', input: schema })

      expect(wf.inputSchema).toBe(schema)
    })
  })

  describe('step chaining', () => {
    it('adds steps in order', () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({ x: z.number() }),
      })
        .step('first', async ({ input }) => ({ a: input.x + 1 }))
        .step('second', async ({ prev }) => ({ b: prev.a * 2 }))

      expect(wf.steps).toHaveLength(2)
      expect(wf.steps[0].name).toBe('first')
      expect(wf.steps[1].name).toBe('second')
    })

    it('is immutable — each .step() returns a new workflow instance', () => {
      const base = createWorkflow({
        name: 'test',
        input: z.object({}),
      })

      const withA = base.step('a', async () => ({ x: 1 }))
      const withB = base.step('b', async () => ({ y: 2 }))

      expect(base.steps).toHaveLength(0)
      expect(withA.steps).toHaveLength(1)
      expect(withA.steps[0].name).toBe('a')
      expect(withB.steps).toHaveLength(1)
      expect(withB.steps[0].name).toBe('b')
    })

    it('supports long chains (5+ steps)', () => {
      const wf = createWorkflow({
        name: 'pipeline',
        input: z.object({ x: z.number() }),
      })
        .step('s1', async ({ input }) => ({ v: input.x }))
        .step('s2', async ({ prev }) => ({ v: prev.v + 1 }))
        .step('s3', async ({ prev }) => ({ v: prev.v + 1 }))
        .step('s4', async ({ prev }) => ({ v: prev.v + 1 }))
        .step('s5', async ({ prev }) => ({ v: prev.v + 1 }))

      expect(wf.steps).toHaveLength(5)
      expect(wf.steps.map((s) => s.name)).toEqual(['s1', 's2', 's3', 's4', 's5'])
    })

    it('preserves the workflow name through chaining', () => {
      const wf = createWorkflow({
        name: 'my-workflow',
        input: z.object({}),
      })
        .step('a', async () => ({}))
        .step('b', async () => ({}))

      expect(wf.name).toBe('my-workflow')
    })

    it('rejects duplicate step names in the same workflow', () => {
      const wf = createWorkflow({
        name: 'duplicate-step',
        input: z.object({}),
      }).step('repeat', async () => ({ ok: true }))

      expect(() => wf.step('repeat', async () => ({ still: true }))).toThrow(
        'Step "repeat" is already defined in workflow "duplicate-step"',
      )
    })
  })

  describe('step configuration', () => {
    it('accepts a handler function directly', () => {
      const handler = async () => ({ x: 1 })
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('simple', handler)

      expect(wf.steps[0].retry).toBeUndefined()
    })

    it('accepts a config object with handler and retry', () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({ x: z.number() }),
      }).step('retryable', {
        retry: { maxAttempts: 3, backoff: 'exponential' },
        handler: async ({ input }) => ({ result: input.x }),
      })

      expect(wf.steps[0].name).toBe('retryable')
      expect(wf.steps[0].retry).toEqual({
        maxAttempts: 3,
        backoff: 'exponential',
      })
    })

    it('supports retry config with initialDelayMs', () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('delayed', {
        retry: { maxAttempts: 5, backoff: 'linear', initialDelayMs: 500 },
        handler: async () => ({}),
      })

      expect(wf.steps[0].retry).toEqual({
        maxAttempts: 5,
        backoff: 'linear',
        initialDelayMs: 500,
      })
    })
  })

  describe('input validation', () => {
    it('parseInput returns the parsed value for valid input', () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({ x: z.number() }),
      })

      const result = wf.parseInput({ x: 42 })
      expect(result).toEqual({ x: 42 })
    })

    it('parseInput throws for invalid input', () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({ x: z.number() }),
      })

      expect(() => wf.parseInput({ x: 'not a number' })).toThrow()
    })

    it('parseInput applies default values from the schema', () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({ x: z.number().default(10) }),
      })

      const result = wf.parseInput({})
      expect(result).toEqual({ x: 10 })
    })

    it('parseInput works with complex nested schemas', () => {
      const wf = createWorkflow({
        name: 'test',
        input: z.object({
          user: z.object({ name: z.string(), age: z.number().min(0) }),
          tags: z.array(z.string()),
        }),
      })

      const input = { user: { name: 'Alice', age: 30 }, tags: ['admin'] }
      expect(wf.parseInput(input)).toEqual(input)
    })
  })

  describe('failure handler', () => {
    it('attaches an onFailure handler', () => {
      const failHandler = async () => {}
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      })
        .step('a', async () => ({ x: 1 }))
        .onFailure(failHandler)

      expect(wf.failureHandler).toBe(failHandler)
    })

    it('onFailure is immutable — returns a new workflow instance', () => {
      const base = createWorkflow({
        name: 'test',
        input: z.object({}),
      }).step('a', async () => ({ x: 1 }))

      const handler1 = async () => {}
      const handler2 = async () => {}

      const wf1 = base.onFailure(handler1)
      const wf2 = base.onFailure(handler2)

      expect(base.failureHandler).toBeUndefined()
      expect(wf1.failureHandler).toBe(handler1)
      expect(wf2.failureHandler).toBe(handler2)
    })

    it('onFailure handler is preserved through subsequent step calls', () => {
      const failHandler = async () => {}
      const wf = createWorkflow({
        name: 'test',
        input: z.object({}),
      })
        .step('a', async () => ({ x: 1 }))
        .onFailure(failHandler)
        .step('b', async ({ prev }) => ({ y: prev.x + 1 }))

      expect(wf.failureHandler).toBe(failHandler)
      expect(wf.steps).toHaveLength(2)
    })
  })
})

import { describe, it, expect } from 'vitest'
import { z } from 'zod'
import { createWorkflow } from '../core/workflow'
import { testEngine } from '../test/index'

describe('testEngine', () => {
  describe('successful workflows', () => {
    it('runs a workflow and returns step-by-step results', async () => {
      const wf = createWorkflow({
        name: 'math',
        input: z.object({ x: z.number() }),
      })
        .step('double', async ({ input }) => ({ doubled: input.x * 2 }))
        .step('add-ten', async ({ prev }) => ({ result: prev.doubled + 10 }))

      const engine = testEngine({ workflows: [wf] })
      const result = await engine.run('math', { x: 5 })

      expect(result.status).toBe('completed')
      expect(result.steps.double.status).toBe('completed')
      expect(result.steps.double.output).toEqual({ doubled: 10 })
      expect(result.steps['add-ten'].output).toEqual({ result: 20 })
    })

    it('handles a single-step workflow', async () => {
      const wf = createWorkflow({
        name: 'one',
        input: z.object({ msg: z.string() }),
      }).step('echo', async ({ input }) => ({ echoed: input.msg }))

      const engine = testEngine({ workflows: [wf] })
      const result = await engine.run('one', { msg: 'hello' })

      expect(result.status).toBe('completed')
      expect(result.steps.echo.output).toEqual({ echoed: 'hello' })
    })
  })

  describe('failed workflows', () => {
    it('captures the failure status and error message', async () => {
      const wf = createWorkflow({
        name: 'fail',
        input: z.object({}),
      }).step('boom', async () => {
        throw new Error('kaboom')
      })

      const engine = testEngine({ workflows: [wf] })
      const result = await engine.run('fail', {})

      expect(result.status).toBe('failed')
      expect(result.steps.boom.status).toBe('failed')
      expect(result.steps.boom.error).toContain('kaboom')
    })

    it('marks only the failing step as failed', async () => {
      const wf = createWorkflow({
        name: 'partial-fail',
        input: z.object({}),
      })
        .step('ok', async () => ({ x: 1 }))
        .step('bad', async () => {
          throw new Error('fail here')
        })

      const engine = testEngine({ workflows: [wf] })
      const result = await engine.run('partial-fail', {})

      expect(result.status).toBe('failed')
      expect(result.steps.ok.status).toBe('completed')
      expect(result.steps.ok.output).toEqual({ x: 1 })
      expect(result.steps.bad.status).toBe('failed')
    })
  })

  describe('retry behavior', () => {
    it('retries through the test engine', async () => {
      let attempts = 0
      const wf = createWorkflow({
        name: 'retry',
        input: z.object({}),
      }).step('flaky', {
        retry: { maxAttempts: 3, backoff: 'linear', initialDelayMs: 0 },
        handler: async () => {
          attempts++
          if (attempts < 3) throw new Error('not yet')
          return { success: true }
        },
      })

      const engine = testEngine({ workflows: [wf] })
      const result = await engine.run('retry', {})

      expect(result.status).toBe('completed')
      expect(result.steps.flaky.output).toEqual({ success: true })
      expect(attempts).toBe(3)
    })
  })

  describe('input validation', () => {
    it('rejects invalid input', async () => {
      const wf = createWorkflow({
        name: 'typed',
        input: z.object({ name: z.string() }),
      }).step('greet', async ({ input }) => ({ msg: `Hi ${input.name}` }))

      const engine = testEngine({ workflows: [wf] })

      await expect(engine.run('typed', { name: 123 } as any)).rejects.toThrow()
    })
  })

  describe('multiple workflows', () => {
    it('runs the correct workflow when multiple are registered', async () => {
      const wfA = createWorkflow({
        name: 'alpha',
        input: z.object({ a: z.string() }),
      }).step('go', async ({ input }) => ({ result: `alpha-${input.a}` }))

      const wfB = createWorkflow({
        name: 'beta',
        input: z.object({ b: z.number() }),
      }).step('go', async ({ input }) => ({ result: `beta-${input.b}` }))

      const engine = testEngine({ workflows: [wfA, wfB] })

      const resultA = await engine.run('alpha', { a: 'hello' })
      const resultB = await engine.run('beta', { b: 42 })

      expect(resultA.steps.go.output).toEqual({ result: 'alpha-hello' })
      expect(resultB.steps.go.output).toEqual({ result: 'beta-42' })
    })
  })
})

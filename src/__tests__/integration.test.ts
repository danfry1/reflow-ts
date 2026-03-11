import { describe, it, expect, vi } from 'vitest'
import { z } from 'zod'
import { createWorkflow, createEngine } from '../index'
import { MemoryStorage } from '../storage/memory'

describe('integration: real-world scenarios', () => {
  describe('e-commerce order fulfillment', () => {
    it('charges, fulfills, and notifies in sequence', async () => {
      const log: string[] = []

      const wf = createWorkflow({
        name: 'order',
        input: z.object({ orderId: z.string(), amount: z.number() }),
      })
        .step('charge', async ({ input }) => {
          log.push(`charging ${input.amount}`)
          return { chargeId: `ch_${input.orderId}` }
        })
        .step('fulfill', async ({ prev }) => {
          log.push(`fulfilling ${prev.chargeId}`)
          return { trackingNumber: 'TRK_001' }
        })
        .step('notify', async ({ prev, input }) => {
          log.push(`notifying ${input.orderId}: ${prev.trackingNumber}`)
        })

      const storage = new MemoryStorage()
      const engine = createEngine({ storage, workflows: [wf] })

      await engine.enqueue('order', { orderId: 'ORD_1', amount: 5000 })
      await engine.tick()

      expect(log).toEqual([
        'charging 5000',
        'fulfilling ch_ORD_1',
        'notifying ORD_1: TRK_001',
      ])
    })

    it('compensates on fulfillment failure (saga pattern)', async () => {
      const compensated = vi.fn()

      const wf = createWorkflow({
        name: 'compensate',
        input: z.object({ orderId: z.string() }),
      })
        .step('charge', async () => ({ chargeId: 'ch_1' }))
        .step('fulfill', async () => {
          throw new Error('warehouse down')
        })
        .onFailure(async ({ error, stepName, input }) => {
          compensated({ error: error.message, stepName, input })
        })

      const storage = new MemoryStorage()
      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('compensate', { orderId: 'ORD_FAIL' })
      await engine.tick()

      expect(compensated).toHaveBeenCalledWith({
        error: 'warehouse down',
        stepName: 'fulfill',
        input: { orderId: 'ORD_FAIL' },
      })
    })
  })

  describe('crash recovery', () => {
    it('resumes from the last completed step after a process restart', async () => {
      const log: string[] = []

      const wf = createWorkflow({
        name: 'resumable',
        input: z.object({ id: z.string() }),
      })
        .step('step-a', async () => {
          log.push('a')
          return { a: true }
        })
        .step('step-b', async () => {
          log.push('b')
          return { b: true }
        })
        .step('step-c', async () => {
          log.push('c')
          return { c: true }
        })

      const storage = new MemoryStorage()

      // First engine: enqueue and simulate partial execution
      const engine1 = createEngine({ storage, workflows: [wf] })
      const run = await engine1.enqueue('resumable', { id: 'test' })

      // Simulate: step-a completed before crash
      await storage.saveStepResult({
        id: 'existing_step',
        runId: run.id,
        name: 'step-a',
        status: 'completed',
        output: { a: true },
        error: null,
        attempts: 1,
        createdAt: Date.now(),
        updatedAt: Date.now(),
      })

      // Second engine: simulates process restart
      const engine2 = createEngine({ storage, workflows: [wf] })
      await engine2.tick()

      // step-a should NOT have re-executed
      expect(log).toEqual(['b', 'c'])
    })
  })

  describe('retry with backoff', () => {
    it('succeeds after transient failures', async () => {
      let callCount = 0

      const wf = createWorkflow({
        name: 'retry',
        input: z.object({}),
      }).step('flaky', {
        retry: { maxAttempts: 3, backoff: 'linear', initialDelayMs: 0 },
        handler: async () => {
          callCount++
          if (callCount < 3) throw new Error(`fail ${callCount}`)
          return { success: true }
        },
      })

      const storage = new MemoryStorage()
      const engine = createEngine({ storage, workflows: [wf] })
      await engine.enqueue('retry', {})
      await engine.tick()

      expect(callCount).toBe(3)
    })
  })

  describe('multi-workflow engine', () => {
    it('routes runs to the correct workflow', async () => {
      const results: string[] = []

      const emailWf = createWorkflow({
        name: 'send-email',
        input: z.object({ to: z.string() }),
      }).step('send', async ({ input }) => {
        results.push(`email:${input.to}`)
        return {}
      })

      const smsWf = createWorkflow({
        name: 'send-sms',
        input: z.object({ phone: z.string() }),
      }).step('send', async ({ input }) => {
        results.push(`sms:${input.phone}`)
        return {}
      })

      const storage = new MemoryStorage()
      const engine = createEngine({ storage, workflows: [emailWf, smsWf] })

      await engine.enqueue('send-email', { to: 'alice@example.com' })
      await engine.enqueue('send-sms', { phone: '+1234567890' })
      await engine.tick()
      await engine.tick()

      expect(results).toContain('email:alice@example.com')
      expect(results).toContain('sms:+1234567890')
    })
  })

  describe('queue processing', () => {
    it('processes a backlog of runs one at a time', async () => {
      const processed: number[] = []

      const wf = createWorkflow({
        name: 'queue',
        input: z.object({ n: z.number() }),
      }).step('process', async ({ input }) => {
        processed.push(input.n)
        return {}
      })

      const storage = new MemoryStorage()
      const engine = createEngine({ storage, workflows: [wf] })

      // Enqueue 5 runs
      for (let i = 1; i <= 5; i++) {
        await engine.enqueue('queue', { n: i })
      }

      // Process all
      for (let i = 0; i < 5; i++) {
        await engine.tick()
      }

      expect(processed).toEqual([1, 2, 3, 4, 5])
    })
  })

  describe('AI pipeline crash recovery', () => {
    it('never re-runs expensive LLM calls after a crash', async () => {
      const scrapeCall = vi.fn()
      const summarizeCall = vi.fn()
      const extractCall = vi.fn()
      const storeCall = vi.fn()

      const pipeline = createWorkflow({
        name: 'process-content',
        input: z.object({ url: z.string() }),
      })
        .step('scrape', async ({ input }) => {
          scrapeCall()
          return { content: `page content from ${input.url}` }
        })
        .step('summarize', async ({ prev }) => {
          summarizeCall()
          // Simulates an expensive LLM call ($0.12, 8 seconds)
          return { summary: `summary of: ${prev.content}` }
        })
        .step('extract', async ({ prev }) => {
          extractCall()
          // Simulates another expensive LLM call ($0.08, 6 seconds)
          return { summary: prev.summary, entities: ['TypeScript', 'SQLite'] }
        })
        .step('store', async ({ input, prev }) => {
          storeCall()
          return { url: input.url, summary: prev.summary, entities: prev.entities }
        })

      const storage = new MemoryStorage()

      // === First engine run: complete scrape + summarize + extract, then "crash" ===
      const engine1 = createEngine({ storage, workflows: [pipeline] })
      const run = await engine1.enqueue('process-content', { url: 'https://example.com/article' })

      // Simulate: scrape, summarize, and extract all completed before crash
      const now = Date.now()
      await storage.saveStepResult({
        id: 'step_scrape',
        runId: run.id,
        name: 'scrape',
        status: 'completed',
        output: { content: 'page content from https://example.com/article' },
        error: null,
        attempts: 1,
        createdAt: now,
        updatedAt: now,
      })
      await storage.saveStepResult({
        id: 'step_summarize',
        runId: run.id,
        name: 'summarize',
        status: 'completed',
        output: { summary: 'summary of: page content from https://example.com/article' },
        error: null,
        attempts: 1,
        createdAt: now,
        updatedAt: now,
      })
      await storage.saveStepResult({
        id: 'step_extract',
        runId: run.id,
        name: 'extract',
        status: 'completed',
        output: {
          summary: 'summary of: page content from https://example.com/article',
          entities: ['TypeScript', 'SQLite'],
        },
        error: null,
        attempts: 1,
        createdAt: now,
        updatedAt: now,
      })

      // === Process crashed here — store never ran ===

      // === Second engine: simulates process restart ===
      const engine2 = createEngine({ storage, workflows: [pipeline] })
      await engine2.tick()

      // The expensive steps must NOT re-execute
      expect(scrapeCall).not.toHaveBeenCalled()
      expect(summarizeCall).not.toHaveBeenCalled()
      expect(extractCall).not.toHaveBeenCalled()

      // Only store should have executed
      expect(storeCall).toHaveBeenCalledOnce()

      // Verify the final result used the cached outputs
      const status = await engine2.getRunStatus(run.id)
      expect(status?.run.status).toBe('completed')

      const steps = status?.steps ?? []
      expect(steps).toHaveLength(4)

      const storeStep = steps.find((s) => s.name === 'store')
      expect(storeStep?.status).toBe('completed')
      expect(storeStep?.output).toEqual({
        url: 'https://example.com/article',
        summary: 'summary of: page content from https://example.com/article',
        entities: ['TypeScript', 'SQLite'],
      })
    })

    it('re-runs a failed step but skips completed ones on retry', async () => {
      let storeAttempts = 0

      const pipeline = createWorkflow({
        name: 'flaky-store',
        input: z.object({ url: z.string() }),
      })
        .step('scrape', async ({ input }) => {
          return { content: `scraped ${input.url}` }
        })
        .step('summarize', async ({ prev }) => {
          return { summary: `summary: ${prev.content}` }
        })
        .step('store', {
          retry: { maxAttempts: 3, backoff: 'linear', initialDelayMs: 0 },
          handler: async ({ prev }) => {
            storeAttempts++
            if (storeAttempts < 3) throw new Error('DB timeout')
            return { stored: true, summary: prev.summary }
          },
        })

      const storage = new MemoryStorage()
      const engine = createEngine({ storage, workflows: [pipeline] })
      const run = await engine.enqueue('flaky-store', { url: 'https://example.com' })
      await engine.tick()

      // Store should have retried until it succeeded
      expect(storeAttempts).toBe(3)

      const status = await engine.getRunStatus(run.id)
      expect(status?.run.status).toBe('completed')

      // Verify the store step recorded 3 attempts
      const storeStep = status?.steps.find((s) => s.name === 'store')
      expect(storeStep?.attempts).toBe(3)
      expect(storeStep?.output).toEqual({
        stored: true,
        summary: 'summary: scraped https://example.com',
      })
    })
  })
})

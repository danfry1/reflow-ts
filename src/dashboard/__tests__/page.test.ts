import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Window } from 'happy-dom'
import { z } from 'zod'
import { createEngine, createWorkflow } from '../../index'
import { MemoryStorage } from '../../storage/memory'
import { createDashboard, type DashboardOptions } from '../index'

// The page's script runs in happy-dom against a real dashboard handler, so
// these follow what a person sees: what the page fetches, renders, and does
// when clicked.

const ORIGIN = 'http://dash.test'

const order = createWorkflow({ name: 'order', input: z.object({ id: z.string(), fail: z.string().optional() }), label: (input) => `Order ${input.id}` })
  .step('reserve', async () => ({}))
  .step('charge', {
    retry: { maxAttempts: 2, backoff: 'linear', initialDelayMs: 0 },
    handler: async ({ input }) => {
      if (input.fail) throw new Error(input.fail)
      return {}
    },
  })
  .step('ship', async () => ({}))

const approval = createWorkflow({ name: 'approval', input: z.object({}) })
  .step('submit', async () => ({}))
  .waitForEvent('approved')
  .step('pay', async () => ({}))

let storage: MemoryStorage
let window: Window | null = null
// Requests the page has made and not yet had answered.
let inFlight = 0

beforeEach(async () => {
  storage = new MemoryStorage()
  await storage.initialize()
})

afterEach(async () => {
  await window?.happyDOM.close()
  window = null
})

async function seed(): Promise<void> {
  const engine = createEngine({ storage, workflows: [order, approval] })
  await engine.enqueue('order', { id: '1001' })
  await engine.enqueue('order', { id: '1002', fail: 'Card declined' })
  await engine.enqueue('approval', {})
  for (let index = 0; index < 3; index++) await engine.tick()
}

/** Open the dashboard at a hash route and wait for its first render. */
async function open(hash = '#/', options: Partial<DashboardOptions> = {}): Promise<Window> {
  const handler = createDashboard({ storage, authorize: () => true, workflows: [order, approval], ...options })
  const page = new Window({ url: `${ORIGIN}/${hash}`, settings: { enableJavaScriptEvaluation: true } })
  window = page
  // The page's requests go straight to the handler, as they would over HTTP.
  Object.defineProperty(page, 'fetch', {
    value: async (input: unknown, init?: RequestInit): Promise<Response> => {
      inFlight++
      try {
        return await handler(new Request(new URL(String(input), page.location.href), init))
      } finally {
        inFlight--
      }
    },
  })
  const html = await (await handler(new Request(`${ORIGIN}/`))).text()
  // The dashboard's own page, loaded into a test window: write() is what makes
  // happy-dom run its inline script, which setting innerHTML would not.
  page.document.write(html)
  await settle()
  return page
}

function doc(): Window['document'] {
  if (!window) throw new Error('No page is open')
  return window.document
}

function texts(selector: string): string[] {
  return [...doc().querySelectorAll(selector)].map((el) => (el.textContent ?? '').replace(/\s+/g, ' ').trim())
}

/**
 * Wait until the page has no request outstanding and has rendered what came
 * back. The page keeps intervals running (polling, ticking clocks), so
 * waiting for every timer would never end.
 */
async function settle(): Promise<void> {
  for (let quiet = 0; quiet < 3;) {
    await new Promise((resolve) => setTimeout(resolve, 5))
    quiet = inFlight === 0 ? quiet + 1 : 0
  }
}

function click(selector: string, text?: string): void {
  const target = [...doc().querySelectorAll(selector)].find((el) => text === undefined || (el.textContent ?? '').includes(text))
  if (!target || !('click' in target) || typeof target.click !== 'function') {
    throw new Error(`Nothing to click matching ${selector}${text ? ` with "${text}"` : ''}`)
  }
  target.click()
}

describe('dashboard page', () => {
  it('opens on every workflow with its steps in order and how its runs stand', async () => {
    await seed()
    await open()

    expect(texts('.wf-name')).toStrictEqual(['order', 'approval'])
    expect(texts('.wfs li:first-child .recipe li')).toStrictEqual(['reserve', 'charge', 'ship'])
    expect(texts('.wfs li:first-child .count')).toStrictEqual(['1 failed', '1 completed'])
    expect(texts('#nav .nav-item .nm')).toStrictEqual(['Overview', 'order', 'approval', 'All runs', 'Schedules'])
  })

  it("lists only the chosen workflow's runs, by their labels, beside its insights", async () => {
    await seed()
    await open('#/workflows/order')

    expect(texts('.pane-list .row .name').sort()).toStrictEqual(['Order 1001', 'Order 1002'])
    expect(doc().querySelector('#nav [aria-current="page"]')?.textContent).toContain('order')
    expect(texts('.stage-name')).toStrictEqual(['reserve', 'charge', 'ship'])
    expect(texts('.failure .what')).toStrictEqual(['Card declined'])
  })

  it('opens a run beside the list, with each attempt and the reason it failed', async () => {
    await seed()
    await open('#/workflows/order')
    click('.pane-list .row', 'Order 1002')
    await settle()

    expect(doc().querySelector('.detail-title')?.textContent).toBe('Order 1002')
    expect(doc().querySelector('.state')?.textContent).toBe('Failed at charge after 2 attempts: Card declined')
    expect(texts('.steps tr.step .name')).toStrictEqual(['reserve', 'charge', 'ship'])
    expect(texts('.steps tr.step .status')).toStrictEqual(['Completed', 'Failed after 2 attempts', 'Did not run'])
    const [attempt, outcome] = texts('.steps tr.attempt td')
    expect(attempt).toBe('Attempt 1')
    expect(outcome).toMatch(/^Failed after \d+ms: Card declined Next attempt \d+ms later\.$/)
    // The list stays beside the run, with the open one marked.
    expect(doc().querySelector('.pane-list .row[aria-current="true"] .name')?.textContent).toBe('Order 1002')
  })

  it("shows a waiting run's next steps as not reached yet", async () => {
    await seed()
    await open('#/workflows/approval')
    click('.pane-list .row')
    await settle()

    expect(doc().querySelector('.state')?.textContent).toBe('Waiting for approved: No timeout')
    expect(texts('.steps tr.step .status')).toStrictEqual(['Completed', 'Waiting for approved', 'Not reached yet'])
  })

  it('filters runs by status, and by the step a group of failures stopped at', async () => {
    await seed()
    await open('#/workflows/order')

    click('.filters button', 'Failed')
    await settle()
    expect(texts('.pane-list .row .name')).toStrictEqual(['Order 1002'])
    expect(doc().querySelector('.filters button[aria-pressed="true"]')?.textContent).toContain('Failed')

    click('.filters button', 'All')
    await settle()
    click('.failure')
    await settle()
    expect(doc().querySelector('.scope')?.textContent).toContain('Failed at charge')
    expect(texts('.pane-list .row .name')).toStrictEqual(['Order 1002'])
  })

  it('finds a run by its label', async () => {
    await seed()
    await open('#/workflows/order')
    const search = doc().querySelector('.pane-list .search')
    if (!search || !('value' in search)) throw new Error('No search box')

    search.value = '1001'
    search.dispatchEvent(new (window as Window).Event('input'))
    await vi.waitFor(() => {
      expect(texts('.pane-list .row .name')).toStrictEqual(['Order 1001'])
    })
  })

  it('moves between runs with the arrow keys and closes one with Escape', async () => {
    await seed()
    await open('#/workflows/order')
    const press = (key: string) => doc().dispatchEvent(new (window as Window).KeyboardEvent('keydown', { key }))

    press('ArrowDown')
    await settle()
    const first = texts('.pane-list .row .name')[0]
    expect(doc().querySelector('.detail-title')?.textContent).toBe(first)

    press('ArrowDown')
    await settle()
    expect(doc().querySelector('.detail-title')?.textContent).toBe(texts('.pane-list .row .name')[1])

    press('Escape')
    await settle()
    expect(window?.location.hash).toBe('#/workflows/order')
    expect(doc().querySelector('.stage-name')).not.toBeNull()
  })

  it('renders run data as text, never as markup', async () => {
    const hostile = createWorkflow({ name: '<img src=x onerror=alert(1)>', input: z.object({ note: z.string() }), label: (input) => input.note })
      .step('<b>step</b>', async () => {
        throw new Error('<script>alert(2)</script>')
      })
    const engine = createEngine({ storage, workflows: [hostile] })
    await engine.enqueue('<img src=x onerror=alert(1)>', { note: '<svg onload=alert(3)>' })
    await engine.tick()
    await open(`#/workflows/${encodeURIComponent('<img src=x onerror=alert(1)>')}`, { workflows: [hostile] })
    click('.pane-list .row')
    await settle()

    expect(doc().querySelector('main img, main svg, main b, main script')).toBeNull()
    expect(doc().querySelector('.detail-title')?.textContent).toBe('<svg onload=alert(3)>')
    expect(doc().querySelector('.state')?.textContent).toContain('<script>alert(2)</script>')
  })

  it('says each workflow has no runs yet, before any is enqueued', async () => {
    await open()

    expect(texts('.wf .counts')).toStrictEqual(['No runs yet', 'No runs yet'])
  })

  it('says how to begin when there are neither runs nor definitions', async () => {
    await open('#/', { workflows: undefined })

    expect(doc().querySelector('.empty strong')?.textContent).toBe('No runs yet')
  })

  it('lists workflows seen in recent runs, and says so, without definitions', async () => {
    await seed()
    await open('#/', { workflows: undefined })

    expect(texts('.wf-name').sort()).toStrictEqual(['approval', 'order'])
    expect(doc().querySelector('.page .note')?.textContent).toContain('Pass workflows to createDashboard()')
  })
})

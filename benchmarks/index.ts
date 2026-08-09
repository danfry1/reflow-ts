/**
 * Reflow throughput benchmarks.
 *
 * Run with `bun run bench` (or `bun benchmarks/index.ts`). Measures engine +
 * storage overhead for enqueue and end-to-end execution across the in-memory
 * and Bun SQLite adapters. Handlers are trivial and the input schema is a no-op
 * passthrough, so the numbers reflect Reflow's own cost rather than a workload
 * or a validation library.
 *
 * Numbers are machine- and runtime-dependent; the header prints the context.
 */
import { existsSync, rmSync } from 'node:fs'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import { createEngine, createWorkflow } from '../src/index'
import type { StorageAdapter } from '../src/index'
import { MemoryStorage } from '../src/storage/memory'
import { SQLiteStorage } from '../src/storage/sqlite-bun'

declare const Bun: { version: string } | undefined

/** A Standard Schema that validates nothing, to keep schema cost out of the measurement. */
const passthrough: StandardSchemaV1<{ i: number }> = {
  '~standard': {
    version: 1,
    vendor: 'reflow-bench',
    validate: (value) => ({ value: value as { i: number } }),
  },
}

function buildWorkflow(steps: number) {
  let wf = createWorkflow({ name: 'bench', input: passthrough }).step('s0', async ({ input }) => ({ n: input.i }))
  for (let s = 1; s < steps; s++) {
    wf = wf.step(`s${s}`, async ({ prev }) => ({ n: prev.n + 1 }))
  }
  return wf
}

interface Result {
  storage: string
  steps: number
  enqueuePerSec: number
  runsPerSec: number
  stepsPerSec: number
}

/**
 * Best of several timed passes, plus how far the worst pass fell behind.
 *
 * A single pass is not reportable: a GC pause, a thermal dip, or anything else
 * sharing the machine only ever makes a pass slower, and one bad pass would be
 * published as though it were the library's throughput. Taking the best
 * approaches the machine's actual ceiling, and printing the spread means a
 * noisy run announces itself instead of quietly producing a wrong number.
 */
function bestOf(passes: readonly Result[]): Result & { spread: number } {
  const best = (pick: (r: Result) => number) => Math.max(...passes.map(pick))
  const worstRuns = Math.min(...passes.map((pass) => pass.runsPerSec))
  const bestRuns = best((pass) => pass.runsPerSec)

  const first = passes[0]
  if (!first) {
    throw new Error('bestOf requires at least one pass')
  }

  return {
    storage: first.storage,
    steps: first.steps,
    enqueuePerSec: best((pass) => pass.enqueuePerSec),
    runsPerSec: bestRuns,
    stepsPerSec: best((pass) => pass.stepsPerSec),
    spread: bestRuns > 0 ? 1 - worstRuns / bestRuns : 0,
  }
}

async function measure(
  storageLabel: string,
  makeStorage: () => StorageAdapter,
  steps: number,
  runs: number,
  concurrency: number,
): Promise<Result> {
  const storage = makeStorage()
  await storage.initialize()

  let completed = 0
  const engine = createEngine({
    storage,
    workflows: [buildWorkflow(steps)],
    concurrency,
    hooks: { onRunComplete: () => { completed += 1 } },
  })

  const enqueueStart = performance.now()
  for (let i = 0; i < runs; i++) {
    await engine.enqueue('bench', { i })
  }
  const enqueueMs = performance.now() - enqueueStart

  const drainStart = performance.now()
  // ceil(runs / concurrency) ticks should drain it; bound generously so a stuck
  // run can never spin the loop forever.
  let ticks = 0
  while (completed < runs) {
    if (ticks++ > runs + 10) {
      throw new Error(`drain stalled at ${completed}/${runs} after ${ticks} ticks`)
    }
    await engine.tick()
  }
  const drainMs = performance.now() - drainStart

  storage.close()

  return {
    storage: storageLabel,
    steps,
    enqueuePerSec: (runs / enqueueMs) * 1000,
    runsPerSec: (runs / drainMs) * 1000,
    stepsPerSec: ((runs * steps) / drainMs) * 1000,
  }
}

function sqliteStorage(path: string): () => StorageAdapter {
  return () => {
    for (const suffix of ['', '-wal', '-shm']) {
      if (existsSync(`${path}${suffix}`)) rmSync(`${path}${suffix}`)
    }
    return new SQLiteStorage(path)
  }
}

function fmt(n: number): string {
  return Math.round(n).toLocaleString('en-US')
}

async function main(): Promise<void> {
  const RUNS = 2000
  const CONCURRENCY = 25
  const REPEATS = 3
  const dbPath = '/tmp/reflow-bench.db'
  const memory = (): StorageAdapter => new MemoryStorage()

  // Warm up the JIT and disk so the timed passes are representative.
  await measure('warmup', memory, 5, 500, CONCURRENCY)
  await measure('warmup', sqliteStorage(dbPath), 5, 500, CONCURRENCY)

  const results: Array<Result & { spread: number }> = []
  for (const steps of [1, 5]) {
    for (const [label, makeStorage] of [
      ['memory', memory],
      ['sqlite-bun', sqliteStorage(dbPath)],
    ] as const) {
      const passes: Result[] = []
      for (let pass = 0; pass < REPEATS; pass++) {
        passes.push(await measure(label, makeStorage, steps, RUNS, CONCURRENCY))
      }
      results.push(bestOf(passes))
    }
  }

  for (const suffix of ['', '-wal', '-shm']) {
    if (existsSync(`${dbPath}${suffix}`)) rmSync(`${dbPath}${suffix}`)
  }

  const runtime = typeof Bun !== 'undefined' ? `Bun ${Bun.version}` : `Node ${process.version}`
  console.log(
    `\nReflow benchmarks — ${runtime}, ${RUNS.toLocaleString('en-US')} runs/scenario, ` +
      `concurrency ${CONCURRENCY}, best of ${REPEATS}\n`,
  )
  console.log('| Storage     | Steps | Enqueue/s | Runs/s  | Steps/s | Spread |')
  console.log('|-------------|-------|-----------|---------|---------|--------|')
  for (const r of results) {
    console.log(
      `| ${r.storage.padEnd(11)} | ${String(r.steps).padEnd(5)} | ${fmt(r.enqueuePerSec).padStart(9)} ` +
        `| ${fmt(r.runsPerSec).padStart(7)} | ${fmt(r.stepsPerSec).padStart(7)} ` +
        `| ${`${Math.round(r.spread * 100)}%`.padStart(6)} |`,
    )
  }

  const noisiest = Math.max(...results.map((r) => r.spread))
  if (noisiest > 0.25) {
    console.log(
      `\n! Spread reached ${Math.round(noisiest * 100)}% between passes — this machine is too ` +
        'busy for these numbers to mean much. Close other work and re-run before quoting them.',
    )
  }
  console.log('')
}

await main()

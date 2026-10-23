<script setup>
// One order, run two ways side by side: as a plain async function, and as a
// Reflow workflow on the real engine over in-memory storage. Crash the server
// part way through and it restarts on its own: the plain function starts over
// and charges the card again; Reflow skips the steps it saved and carries on.
import { computed, onMounted, onUnmounted, reactive, ref } from 'vue'
import { z } from 'zod'
import { createEngine, createWorkflow } from '../../../../src/index'
import { MemoryStorage } from '../../../../src/storage/memory'
import { createDashboard } from '../../../../src/dashboard/index'

const STEPS = [
  { name: 'charge', label: 'Charge card', ms: 1500 },
  { name: 'ship', label: 'Ship order', ms: 3200 },
  { name: 'send-receipt', label: 'Send receipt', ms: 1300 },
]
const LEASE_MS = 1200
const RESTART_MS = 900

// idle -> running -> crashed -> restarting -> resumed -> done
const phase = ref('idle')
const now = ref(Date.now())
let crashedOnce = false

// Each lane: what every step is doing, and how many times the card was charged.
function lane() {
  return reactive({ steps: STEPS.map(() => ({ state: 'idle', startedAt: 0 })), charges: 0, done: false })
}
const plain = lane()
const reflow = lane()

function work(ms, signal) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, ms)
    signal.addEventListener('abort', () => {
      clearTimeout(timer)
      reject(signal.reason)
    }, { once: true })
  })
}

// --- The plain function: nothing is saved, so a restart begins at the top.
let plainAbort = null
async function runPlain(again) {
  plainAbort = new AbortController()
  const { signal } = plainAbort
  for (let i = 0; i < STEPS.length; i++) plain.steps[i] = { state: 'idle', startedAt: 0 }
  try {
    for (let i = 0; i < STEPS.length; i++) {
      plain.steps[i] = { state: again ? 'again' : 'running', startedAt: Date.now() }
      await work(STEPS[i].ms, signal)
      if (STEPS[i].name === 'charge') plain.charges++
      plain.steps[i] = { state: again ? 'redone' : 'done', startedAt: 0 }
    }
    plain.done = true
  } catch {
    // Killed with the server; whatever was running is lost.
  }
}

// --- The Reflow workflow, on the real engine.
const order = createWorkflow({ name: 'order', input: z.object({ orderId: z.string() }), label: (input) => input.orderId })
  .step('charge', async ({ signal }) => {
    await work(STEPS[0].ms, signal)
    reflow.charges++
    return { charged: true }
  })
  .step('ship', async ({ signal }) => {
    await work(STEPS[1].ms, signal)
    return { shipped: true }
  })
  .step('send-receipt', async ({ signal }) => {
    await work(STEPS[2].ms, signal)
    return { sent: true }
  })

const storage = new MemoryStorage()
let engine = null
let runId = null
// The run a crash cut off, until the new server picks it up.
let resumingId = null
const indexOf = (name) => STEPS.findIndex((step) => step.name === name)

async function startEngine() {
  engine = createEngine({
    storage,
    workflows: [order],
    runLeaseDurationMs: LEASE_MS,
    heartbeatIntervalMs: 300,
    hooks: {
      onRunStart: async (event) => {
        if (event.runId !== resumingId) return
        resumingId = null
        // What the database says is already done is skipped, not run again.
        const saved = (await storage.getStepResults(event.runId)).filter((s) => s.status === 'completed')
        for (const step of saved) reflow.steps[indexOf(step.name)] = { state: 'kept', startedAt: 0 }
        phase.value = 'resumed'
      },
      onStepStart: (event) => {
        // The step the crash cut off is where the new server picks up.
        const index = indexOf(event.stepName)
        reflow.steps[index] = { state: reflow.steps[index].state === 'lost' ? 'resumed' : 'running', startedAt: Date.now() }
      },
      onStepComplete: (event) => {
        const index = indexOf(event.stepName)
        reflow.steps[index] = { state: reflow.steps[index].state === 'resumed' ? 'picked' : 'saved', startedAt: 0 }
      },
      onRunComplete: () => {
        reflow.done = true
        if (plain.done || !crashedOnce) phase.value = 'done'
      },
    },
  })
  await engine.start(100)
}

let orders = 0
async function start() {
  orders++
  crashedOnce = false
  for (const l of [plain, reflow]) {
    l.charges = 0
    l.done = false
    for (let i = 0; i < STEPS.length; i++) l.steps[i] = { state: 'idle', startedAt: 0 }
  }
  phase.value = 'running'
  if (!engine) await startEngine()
  const run = await engine.enqueue('order', { orderId: `ORD-${1000 + orders}` })
  runId = run.id
  runPlain(false)
}

async function crash() {
  if (phase.value !== 'running') return
  crashedOnce = true
  phase.value = 'crashed'
  plainAbort?.abort()
  const dying = engine
  engine = null
  // Whatever was mid-step dies with the server, in both.
  for (const l of [plain, reflow]) {
    for (const step of l.steps) {
      if (step.state === 'running' || step.state === 'again' || step.state === 'resumed') step.state = 'lost'
    }
  }
  await dying?.stop()
  // The plain function remembered nothing: everything it did is gone with it.
  for (const step of plain.steps) if (step.state === 'done') step.state = 'lost'
  setTimeout(async () => {
    phase.value = 'restarting'
    runPlain(true)
    resumingId = runId
    await startEngine()
  }, RESTART_MS)
}

// Finish when both lanes have.
const finished = computed(() => plain.done && reflow.done)
function tick() {
  now.value = Date.now()
  if (finished.value && phase.value !== 'done' && phase.value !== 'idle') phase.value = 'done'
}
// Started on mount, not in setup, so the server render leaves no timer behind.
let ticker = null

function progress(step, index) {
  if (step.state !== 'running' && step.state !== 'again' && step.state !== 'resumed') return step.state === 'idle' ? 0 : 1
  return Math.min(1, (now.value - step.startedAt) / STEPS[index].ms)
}

const WORD = {
  idle: '',
  running: 'running',
  again: 'running again',
  done: 'done',
  redone: 'done again',
  resumed: 'picked up here',
  picked: 'picked up here',
  saved: 'saved',
  kept: 'already saved',
  lost: 'lost',
}

// The same, short enough for a phone's narrow columns.
const SHORT = {
  idle: '', running: 'running', again: 'again', done: 'done', redone: 'again',
  resumed: 'resumed', picked: 'resumed', saved: 'saved', kept: 'skipped', lost: 'lost',
}

const canCrash = computed(() => phase.value === 'running' && reflow.steps[0].state === 'saved' && plain.steps[0].state === 'done')

const banner = computed(() => {
  switch (phase.value) {
    case 'idle': return 'One order, run two ways. Start it, then crash the server while it ships.'
    case 'running': return canCrash.value ? 'The card is charged. Crash the server now.' : 'Charging the card…'
    case 'crashed': return 'The server crashed.'
    case 'restarting': return 'The server is back. Watch where each one starts.'
    case 'resumed': return 'Reflow picked up where it left off. The plain function started over.'
    default: return crashedOnce
      ? `Plain code charged the card ${plain.charges === 2 ? 'twice' : `${plain.charges} times`}. Reflow charged it once and finished from where it stopped.`
      : 'Both finished. Now run it again and crash the server while it ships.'
  }
})

// --- The real dashboard, in a frame, reading the same storage.
//
// Its requests are answered by createDashboard()'s handler in this page, as a
// server would answer them; its live updates are nudged by a timer, since
// there is no server to stream them.
const frame = ref(null)
const control = {
  cancel: (id) => (engine ? engine.cancel(id) : storage.cancelRun(id)),
  resume: (id) => (engine ? engine.resume(id) : storage.requeueRun(id)),
}
async function mountDashboard() {
  const win = frame.value && frame.value.contentWindow
  if (!win) return
  // Written from this page, the frame's document takes this page's URL, which
  // ends in a slash as the dashboard expects; its API sits beneath it.
  const path = window.location.pathname.replace(/[^/]*$/, '')
  const dashboard = createDashboard({ storage, engine: control, workflows: [order], authorize: () => true, basePath: path })
  win.fetch = (input, init) => dashboard(new Request(new URL(String(input), win.location.href), init))
  win.EventSource = class {
    constructor() {
      win.setTimeout(() => this.onopen && this.onopen(), 0)
      this.timer = win.setInterval(() => this.onmessage && this.onmessage({ data: '{}' }), 600)
    }
    close() { win.clearInterval(this.timer) }
  }
  const html = await (await dashboard(new Request(new URL(path, window.location.origin)))).text()
  // write() is what makes the frame run the dashboard page's own script; the
  // page is the dashboard's, rendered by its handler a moment ago.
  win.document.open()
  win.document.write(html)
  win.document.close()
}

onMounted(async () => {
  ticker = setInterval(tick, 100)
  await storage.initialize()
  await mountDashboard()
})
onUnmounted(async () => {
  clearInterval(ticker)
  plainAbort?.abort()
  if (engine) await engine.stop()
  engine = null
})
</script>

<template>
  <section class="pg" :data-phase="phase" aria-label="One order, run as plain code and as a Reflow workflow">
    <p class="pg-banner" aria-live="polite">{{ banner }}</p>

    <div class="pg-grid" role="table" aria-label="The order's steps in each version">
      <div class="pg-head" role="row">
        <span role="columnheader" />
        <span v-for="step in STEPS" :key="step.name" role="columnheader" class="pg-step-head">{{ step.label }}</span>
        <span role="columnheader" class="pg-charged-head">Card charged</span>
      </div>

      <div v-for="(l, key) in { plain, reflow }" :key="key" class="pg-lane" :class="key" role="row">
        <span class="pg-lane-name" role="rowheader">
          <template v-if="key === 'plain'">Plain code</template>
          <template v-else>With Reflow</template>
          <span class="pg-lane-note">{{ key === 'plain' ? 'remembers nothing' : 'saves each step' }}</span>
        </span>
        <span v-for="(step, index) in l.steps" :key="index" class="pg-cell" :data-state="step.state" role="cell">
          <span class="pg-fill" :style="{ '--p': progress(step, index) }" />
          <span class="pg-word"><span class="long">{{ WORD[step.state] }}</span><span class="short">{{ SHORT[step.state] }}</span></span>
        </span>
        <span class="pg-charged" :class="{ twice: l.charges > 1 }" role="cell">{{ l.charges }}×</span>
      </div>

      <div v-if="phase === 'crashed' || phase === 'restarting'" class="pg-crash" aria-hidden="true">
        {{ phase === 'crashed' ? 'Server crashed' : 'Server restarting…' }}
      </div>
    </div>

    <div class="pg-actions">
      <button v-if="phase === 'idle' || phase === 'done'" class="pg-go" @click="start">{{ phase === 'done' ? 'Run it again' : 'Start the order' }}</button>
      <button v-else-if="phase === 'running'" class="pg-crash-btn" :class="{ urge: canCrash }" :disabled="!canCrash" @click="crash">Crash the server</button>
    </div>
  </section>

  <section class="pg-dash" aria-labelledby="pg-dash-title">
    <h2 id="pg-dash-title">The same run, in the dashboard</h2>
    <p>Reflow's real dashboard, reading the Reflow run above. Open the order workflow and pick a run to see each step, when it ran, and what it returned.</p>
    <iframe ref="frame" class="pg-frame" title="The Reflow dashboard, showing the run from the demo above" />
  </section>
</template>

<style scoped>
.pg {
  --pg-live: #2563eb;
  --pg-good: #1e7a52;
  --pg-bad: #c0392f;
  --pg-held: #8f6100;
  position: relative;
  padding: 22px 24px 24px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 12px;
  background: var(--vp-c-bg-soft);
}
.dark .pg { --pg-live: #6e9bff; --pg-good: #4cc08a; --pg-bad: #f06a5f; --pg-held: #e8a317; }

.pg-banner { margin: 0 0 20px; min-height: 2.6em; font: 700 1.35rem/1.3 var(--rf-sign); color: var(--vp-c-text-1); }
.pg[data-phase='crashed'] .pg-banner { color: var(--pg-bad); }

.pg-grid { position: relative; display: grid; gap: 10px; }
.pg-head, .pg-lane { display: grid; grid-template-columns: 9.5em repeat(3, minmax(0, 1fr)) 5.5em; gap: 10px; align-items: center; }
.pg-step-head, .pg-charged-head { font-size: 0.86rem; font-weight: 600; color: var(--vp-c-text-2); }
.pg-charged-head { text-align: center; }
.pg-lane-name { font: 700 1.05rem var(--rf-sign); line-height: 1.2; }
.pg-lane-note { display: block; font: 400 0.8rem var(--vp-font-family-base); color: var(--vp-c-text-2); }

/* A step: a block that fills as it runs, and keeps or loses its fill. */
.pg-cell { position: relative; height: 54px; border-radius: 8px; background: var(--vp-c-bg); box-shadow: inset 0 0 0 1px var(--vp-c-divider); overflow: hidden; }
.pg-fill { position: absolute; inset: 0; transform-origin: left; transform: scaleX(var(--p)); background: var(--pg-live); opacity: 0.22; transition: background-color 0.2s; }
.pg-word { position: relative; display: flex; align-items: center; height: 100%; padding: 0 12px; font-weight: 700; font-size: 0.95rem; }
.pg-cell[data-state='running'] .pg-word { color: var(--pg-live); }
.pg-cell[data-state='again'] .pg-fill, .pg-cell[data-state='redone'] .pg-fill { background: var(--pg-held); }
.pg-cell[data-state='again'] .pg-word, .pg-cell[data-state='redone'] .pg-word { color: var(--pg-held); }
.pg-cell[data-state='resumed'] { box-shadow: inset 0 0 0 2px var(--pg-live); }
.pg-cell[data-state='resumed'] .pg-word { color: var(--pg-live); }
.pg-cell[data-state='done'] .pg-fill { background: var(--vp-c-text-3); }
.pg-cell[data-state='done'] .pg-word { color: var(--vp-c-text-2); }
.pg-cell[data-state='saved'] .pg-fill, .pg-cell[data-state='kept'] .pg-fill, .pg-cell[data-state='picked'] .pg-fill { background: var(--pg-good); opacity: 0.28; }
.pg-cell[data-state='saved'] .pg-word, .pg-cell[data-state='kept'] .pg-word, .pg-cell[data-state='picked'] .pg-word { color: var(--pg-good); }
.pg-cell[data-state='picked'] { box-shadow: inset 0 0 0 2px var(--pg-live); }
.pg-cell[data-state='saved']::after, .pg-cell[data-state='kept']::after, .pg-cell[data-state='picked']::after { content: '✓'; position: absolute; right: 12px; top: 50%; transform: translateY(-50%); color: var(--pg-good); font-weight: 700; }
.pg-cell[data-state='kept'] { box-shadow: inset 0 0 0 2px var(--pg-good); }
.pg-cell[data-state='lost'] { box-shadow: inset 0 0 0 2px var(--pg-bad); background: repeating-linear-gradient(135deg, color-mix(in srgb, var(--pg-bad) 12%, transparent) 0 6px, transparent 6px 12px); }
.pg-cell[data-state='lost'] .pg-fill { opacity: 0; }
.pg-cell[data-state='lost'] .pg-word { color: var(--pg-bad); }

.pg-word .short { display: none; }
.pg-charged { text-align: center; font: 700 1.7rem var(--rf-sign); color: var(--vp-c-text-1); }
.pg-charged.twice { color: var(--pg-bad); }

.pg-crash { position: absolute; inset: 0; display: grid; place-items: center; border-radius: 8px; background: color-mix(in srgb, var(--vp-c-bg) 55%, transparent); font: 700 1.6rem var(--rf-sign); color: var(--pg-bad); pointer-events: none; animation: pg-flash 0.5s ease-out; }
.pg[data-phase='restarting'] .pg-crash { color: var(--vp-c-text-1); }
@keyframes pg-flash { from { background: color-mix(in srgb, var(--pg-bad) 35%, transparent); } }

.pg-actions { display: flex; justify-content: center; min-height: 50px; margin-top: 22px; }
.pg-go, .pg-crash-btn { font: inherit; font-size: 1.05rem; font-weight: 700; padding: 13px 26px; border-radius: 8px; border: 0; cursor: pointer; }
.pg-go { background: var(--vp-c-text-1); color: var(--vp-c-bg); }
.pg-crash-btn { background: var(--pg-bad); color: #fff; }
.pg-crash-btn:disabled { opacity: 0.35; cursor: default; }
.pg-crash-btn.urge { animation: pg-urge 1s ease-in-out infinite; }
.pg-go:focus-visible, .pg-crash-btn:focus-visible { outline: 2px solid var(--vp-c-brand-1); outline-offset: 3px; }
@keyframes pg-urge { 50% { box-shadow: 0 0 0 8px color-mix(in srgb, var(--pg-bad) 25%, transparent); } }

/* Wider than the text column, so the dashboard has room for its three panes. */
.pg-dash { margin: 72px calc(50% - min(640px, 50vw - 24px)) 0; }
.pg-dash h2 { max-width: 980px; margin: 0 auto 8px; padding: 0 24px; border: 0; font-family: var(--rf-sign); font-size: 2rem; font-weight: 700; }
.pg-dash p { max-width: 980px; margin: 0 auto 20px; padding: 0 24px; color: var(--vp-c-text-2); }
.pg-frame { display: block; width: 100%; height: 720px; border: 1px solid var(--vp-c-divider); border-radius: 10px; background: var(--vp-c-bg-alt); }

@media (max-width: 720px) {
  .pg { padding: 18px 14px 20px; }
  .pg-banner { font-size: 1.15rem; }
  /* The lane's name takes its own line, so the steps get the full width. */
  .pg-head, .pg-lane { grid-template-columns: repeat(3, minmax(0, 1fr)) 2.6em; gap: 6px; }
  .pg-head > :first-child { display: none; }
  .pg-lane-name { grid-column: 1 / -1; margin-top: 4px; font-size: 0.95rem; }
  .pg-lane-note { display: inline; margin-left: 0.5em; }
  .pg-step-head { font-size: 0.74rem; }
  .pg-charged-head { font-size: 0; }
  .pg-charged-head::after { content: 'Paid'; font-size: 0.74rem; }
  .pg-cell { height: 48px; }
  .pg-word { padding: 0 6px; font-size: 0.8rem; line-height: 1.15; }
  .pg-word .long { display: none; }
  .pg-word .short { display: inline; }
  .pg-cell[data-state='saved']::after, .pg-cell[data-state='kept']::after, .pg-cell[data-state='picked']::after { display: none; }
  .pg-charged { font-size: 1.3rem; }
  .pg-frame { height: 640px; }
}
@media (prefers-reduced-motion: reduce) {
  .pg-crash-btn.urge, .pg-crash { animation: none; }
}
</style>

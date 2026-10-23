<script setup>
// Reflow itself, running in the page: a real workflow on the real engine over
// in-memory storage. The middle of the demo is the conversation between the
// process and the database — every line is a storage call the engine really
// made, which is where durability comes from: claim a run, read what is
// already saved, save each step's result, keep renewing the claim. Killing
// the process stops the engine mid-step, as a crash would; a new process
// waits for the dead one's claim to lapse, reads what was saved, and carries
// on from the first step not saved.
import { computed, nextTick, onMounted, onUnmounted, reactive, ref } from 'vue'
import { z } from 'zod'
import { createEngine, createWorkflow } from '../../../../src/index'
import { MemoryStorage } from '../../../../src/storage/memory'
import { createDashboard } from '../../../../src/dashboard/index'

const LEASE_MS = 2500
const HEARTBEAT_MS = 700
const DURATION = { charge: 1800, ship: 3000, 'send-receipt': 1300 }
const STEPS = ['charge', 'ship', 'send-receipt']

const world = reactive({
  stage: 'intro', // intro, running, killed, recovering, done, free
  process: 0,
  alive: false,
  starting: false,
  warehouseDown: false,
  charges: 0,
  orders: 0,
  rerunsAvoided: 0,
  focus: null,
})
const runs = ref([])
const saved = ref(new Map())
const working = reactive(new Map()) // run id -> { step, startedAt }
const lost = reactive(new Map()) // run id -> step it was cut off in
const now = ref(Date.now())

// --- The conversation: what the process asks of the database, and back.
const messages = ref([])
const logEl = ref(null)
let messageId = 0
function push(message) {
  messages.value = [...messages.value, { id: ++messageId, at: Date.now(), count: 1, ...message }].slice(-40)
  nextTick(() => {
    const log = logEl.value
    if (log) log.scrollTop = log.scrollHeight
  })
}
function say(from, text, detail = '', tone = '') {
  push({ from, text, detail, tone })
}

const base = new MemoryStorage()
const labels = new Map()
const label = (runId) => labels.get(runId) ?? 'the order'

// The storage the engine uses: the real one, with each call it makes told
// as a line of the conversation. The page and the dashboard read `base`
// directly, so only the engine's own calls appear.
const engineStorage = new Proxy(base, {
  get(target, prop) {
    const value = target[prop]
    if (typeof value !== 'function') return value
    return async (...args) => {
      const result = await value.apply(target, args)
      observe(prop, args, result)
      return result
    }
  },
})
let claimWaitTold = false
function observe(method, args, result) {
  const p = `Process ${world.process}`
  if (method === 'createRun') {
    labels.set(args[0].id, args[0].input.orderId)
    say('app', `Save ${args[0].input.orderId} as a new run`, 'It waits in the database until a process claims it.')
  } else if (method === 'claimNextRun' && result) {
    const expired = lost.has(result.id)
    say(p, `Claim ${label(result.id)}`, expired
      ? `Process ${world.process - 1}'s claim has run out, so this process can take the run over.`
      : 'The run is this process\'s alone while it keeps renewing the claim.')
  } else if (method === 'claimNextRun' && !result && world.alive && lost.size && !claimWaitTold) {
    claimWaitTold = true
    say(p, 'Ask for work', `${[...lost.keys()].map(label).join(', ')} is still claimed by process ${world.process - 1}. A dead process looks like a slow one, so this one waits for the claim to run out.`, 'held')
  } else if (method === 'getStepResults') {
    const done = result.filter((s) => s.status === 'completed').map((s) => s.name)
    const next = STEPS.find((name) => !done.includes(name))
    say('db', done.length ? `Saved so far: ${done.join(', ')}` : 'Saved so far: nothing', done.length
      ? `So the process skips ${done.join(' and ')} and starts at ${next}.`
      : `So the process starts at ${next}.`, done.length ? 'good' : '')
  } else if (method === 'saveStepResult' && result) {
    const step = args[0]
    if (step.status === 'completed') {
      say(p, `Save ${step.name}'s result`, `${summary(step)} — kept even if the process dies now.`, 'good')
    } else if (step.status === 'retrying') {
      say(p, `Save ${step.name}: attempt ${step.attempts} failed`, `${step.error}. The attempt count is saved too, so a restart will not reset it.`, 'bad')
    }
  } else if (method === 'sleepRun' && result) {
    say(p, `Let go of ${label(args[0])} until ${clock(args[2])}`, 'The retry waits in the database; no process has to hold it.', 'held')
  } else if (method === 'heartbeatRun' && result) {
    const last = messages.value[messages.value.length - 1]
    if (last && last.kind === 'beat' && last.runId === args[0]) {
      last.count++
      messages.value = [...messages.value]
    } else {
      push({ from: p, text: `Renew claim on ${label(args[0])}`, detail: 'Still alive, still working on it.', tone: 'quiet', kind: 'beat', runId: args[0] })
    }
  } else if (method === 'completeRun' && result) {
    say(p, `Mark ${label(args[0])} finished`, '', 'good')
  }
}
function clock(ts) {
  return `+${Math.max(0, (ts - Date.now()) / 1000).toFixed(1)}s`
}
function summary(step) {
  const out = step.output
  if (out && typeof out === 'object') {
    const [key, value] = Object.entries(out)[0] || []
    return key ? `${key}: ${JSON.stringify(value)}` : '{}'
  }
  return JSON.stringify(out)
}

// A step's work, abandoned the moment the process is killed.
function work(ms, signal) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, ms)
    signal.addEventListener('abort', () => {
      clearTimeout(timer)
      reject(signal.reason)
    }, { once: true })
  })
}
const token = () => Math.random().toString(36).slice(2, 7)

const order = createWorkflow({
  name: 'order',
  input: z.object({ orderId: z.string() }),
  label: (input) => input.orderId,
})
  .step('charge', async ({ signal }) => {
    await work(DURATION.charge, signal)
    world.charges++
    return { chargeId: `ch_${token()}` }
  })
  .step('ship', {
    retry: { maxAttempts: 5, backoff: 'exponential', initialDelayMs: 2000 },
    handler: async ({ signal }) => {
      await work(DURATION.ship, signal)
      if (world.warehouseDown) throw new Error('Warehouse is down')
      return { tracking: `TRK-${token().toUpperCase()}` }
    },
  })
  .step('send-receipt', async ({ signal }) => {
    await work(DURATION['send-receipt'], signal)
    return { emailed: true }
  })

let engine = null

async function startProcess() {
  if (world.alive || world.starting) return
  world.starting = true
  const n = world.process + 1
  const process = createEngine({
    storage: engineStorage,
    workflows: [order],
    concurrency: 4,
    runLeaseDurationMs: LEASE_MS,
    heartbeatIntervalMs: HEARTBEAT_MS,
    hooks: {
      onStepStart: (event) => { working.set(event.runId, { step: event.stepName, startedAt: Date.now() }) },
      onRunStart: async (event) => {
        await refresh()
        const done = new Set((saved.value.get(event.runId) || []).filter((s) => s.status === 'completed').map((s) => s.name))
        const step = STEPS.find((name) => !done.has(name))
        if (step) working.set(event.runId, { step, startedAt: Date.now() })
        lost.delete(event.runId)
      },
      // Read the saved result at once, so the process's view never shows a
      // step running while the one before it looks unfinished.
      onStepComplete: async (event) => {
        await refresh()
        working.delete(event.runId)
      },
      onRunComplete: (event) => {
        working.delete(event.runId)
        if (event.runId === world.focus && (world.stage === 'running' || world.stage === 'recovering')) {
          world.stage = 'done'
        }
      },
      onRunFailed: (event) => { working.delete(event.runId) },
    },
  })
  world.process = n
  claimWaitTold = false
  await process.start(200)
  engine = process
  world.alive = true
  world.starting = false
  say('event', `Process ${n} started`, n > 1 ? 'It knows nothing yet — everything it needs is in the database.' : '', 'event')
  if (world.stage === 'killed') world.stage = 'recovering'
}

async function kill() {
  if (!engine) return
  const process = engine
  const n = world.process
  engine = null
  world.alive = false
  let rerun = 0
  for (const [runId, current] of working) {
    lost.set(runId, current.step)
    rerun += (saved.value.get(runId) || []).filter((s) => s.status === 'completed').length
  }
  working.clear()
  world.rerunsAvoided += rerun
  await process.stop()
  const cut = [...lost.entries()].map(([runId, step]) => `${step} for ${label(runId)}`).join(', ')
  say('event', `Process ${n} killed`, cut
    ? `It was part way through ${cut}; that work was never saved and is lost. Its claim is no longer renewed.`
    : 'Nothing was mid-step. Its claims are no longer renewed.', 'dead')
  if (world.stage !== 'intro') world.stage = 'killed'
}

async function startOrder() {
  world.orders++
  const orderId = `ORD-${1000 + world.orders}`
  const writer = engine ?? createEngine({ storage: engineStorage, workflows: [order] })
  const run = await writer.enqueue('order', { orderId })
  if (world.stage === 'intro') world.stage = 'running'
  if (world.stage === 'done') world.stage = 'free'
  world.focus = run.id
  await refresh()
}

function toggleWarehouse() {
  world.warehouseDown = !world.warehouseDown
  if (world.stage === 'done') world.stage = 'free'
  say('event', world.warehouseDown ? 'The warehouse went down' : 'The warehouse is back', world.warehouseDown
    ? 'ship will fail and be retried, waiting longer each time.'
    : 'Retrying orders will ship at their next attempt.', 'event')
}

async function refresh() {
  const listed = await base.listRuns({ limit: 6 })
  runs.value = listed
  saved.value = await base.getStepResultsForRuns(listed.map((r) => r.id))
}

// --- What the view shows

// One short line on what is happening, and the one button that matters now.
const caption = computed(() => {
  const focusStep = working.get(world.focus)?.step
  switch (world.stage) {
    case 'intro': return 'A process runs this order workflow. Watch what it tells the database as it goes.'
    case 'running': return focusStep === 'ship'
      ? 'charge is saved. Kill the process now, while ship is running.'
      : 'The process is working through the steps, saving each result.'
    case 'killed': return 'The process is gone. What did the database keep? Start a new process to find out.'
    case 'recovering': return 'A new process, knowing nothing, reads the database and carries on.'
    case 'done': return world.process > 1
      ? 'Finished. Only the step that was cut off ran again; the card was charged once.'
      : 'Finished, each step saved as it went. Start another order and kill the process while it ships.'
    default: return world.warehouseDown
      ? 'The warehouse is down: ship fails and retries. Kill the process during a wait — the count survives.'
      : 'Try more: start orders, kill the process at any point, or break the warehouse.'
  }
})

const primary = computed(() => {
  if (world.stage === 'intro') return { label: 'Start an order', act: startOrder }
  if (!world.alive) return { label: 'Start a new process', act: startProcess, busy: world.starting }
  if (world.stage === 'running' || world.stage === 'recovering') {
    return { label: 'Kill the process', act: kill, danger: true, urge: world.stage === 'running' && working.get(world.focus)?.step === 'ship' }
  }
  return { label: 'Start another order', act: startOrder }
})

const stages = [
  { label: 'Start an order', done: () => world.stage !== 'intro' },
  { label: 'Kill the process', done: () => world.process > 1 || world.stage === 'killed' },
  { label: 'Start a new one', done: () => world.process > 1 },
  { label: 'See what it repeats', done: () => (world.stage === 'done' || world.stage === 'free') && world.process > 1 },
]

const focusRun = computed(() => runs.value.find((r) => r.id === world.focus) || runs.value[0] || null)

// The process's view of the order it is on: each step done, running, cut off, or ahead.
const processView = computed(() => {
  const run = focusRun.value
  if (!run) return []
  const steps = saved.value.get(run.id) || []
  return STEPS.map((name) => {
    const step = steps.find((s) => s.name === name)
    if (step && step.status === 'completed') return { name, state: 'saved', word: 'saved' }
    if (lost.get(run.id) === name) return { name, state: 'lost', word: 'cut off' }
    if (working.get(run.id)?.step === name) return { name, state: 'running', word: 'running' }
    if (step && step.status === 'retrying') return { name, state: 'retrying', word: `retry ${step.attempts + 1} at ${clock(run.wakeAt || Date.now())}` }
    return { name, state: 'ahead', word: '' }
  })
})

function progress(runId) {
  const current = working.get(runId)
  if (!current) return 0
  return Math.min(1, (now.value - current.startedAt) / (DURATION[current.step] || 1000))
}

const claimLeft = computed(() => {
  if (!world.alive || lost.size === 0) return 0
  const waiting = runs.value.filter((r) => lost.has(r.id) && r.status === 'running')
  if (waiting.length === 0) return 0
  return Math.max(0, Math.min(...waiting.map((r) => r.updatedAt + LEASE_MS - now.value)))
})

const dbRuns = computed(() => runs.value.slice(0, 4).map((run) => ({
  run,
  steps: (saved.value.get(run.id) || []).filter((s) => s.status === 'completed' || s.status === 'retrying'),
  owner: run.status === 'running'
    ? (lost.has(run.id) ? `claimed by process ${world.alive ? world.process - 1 : world.process}, which is dead` : `claimed by process ${world.process}`)
    : run.status === 'completed' ? 'finished'
      : run.status === 'sleeping' ? 'waiting to retry, unclaimed'
        : run.status === 'pending' ? 'waiting for a process' : run.status,
})))

function fromLabel(from) {
  if (from === 'app') return 'Your app'
  if (from === 'db') return 'Database'
  return from
}

// --- The real dashboard, in a frame, reading the same storage.
//
// Its requests are answered by createDashboard()'s handler in this page, as a
// server would answer them; its live updates are nudged by a timer, since
// there is no server to stream them.
const frame = ref(null)
const control = {
  cancel: (runId) => (engine ? engine.cancel(runId) : base.cancelRun(runId)),
  resume: (runId) => (engine ? engine.resume(runId) : base.requeueRun(runId)),
}

async function mountDashboard() {
  const win = frame.value && frame.value.contentWindow
  if (!win) return
  // Written from this page, the frame's document takes this page's URL, which
  // ends in a slash as the dashboard expects; its API sits beneath it.
  const path = window.location.pathname.replace(/[^/]*$/, '')
  const dashboard = createDashboard({ storage: base, engine: control, workflows: [order], authorize: () => true, basePath: path })
  win.fetch = (input, init) => dashboard(new Request(new URL(String(input), win.location.href), init))
  win.EventSource = class {
    constructor() {
      win.setTimeout(() => this.onopen && this.onopen(), 0)
      this.timer = win.setInterval(() => this.onmessage && this.onmessage({ data: '{}' }), 600)
    }
    close() { win.clearInterval(this.timer) }
  }
  const html = await (await dashboard(new Request(new URL(path, window.location.origin)))).text()
  win.document.open()
  win.document.write(html)
  win.document.close()
}

let poller = null
let ticker = null
onMounted(async () => {
  await base.initialize()
  await mountDashboard()
  await startProcess()
  poller = setInterval(refresh, 150)
  ticker = setInterval(() => { now.value = Date.now() }, 100)
})

onUnmounted(async () => {
  clearInterval(poller)
  clearInterval(ticker)
  if (engine) await engine.stop()
  engine = null
})
</script>

<template>
  <section class="pg" aria-label="Reflow running in this page">
    <div class="pg-top">
      <ol class="pg-story" aria-label="Steps of the demo">
        <li v-for="(stage, index) in stages" :key="stage.label" :class="{ done: stage.done() }">
          <span class="pg-story-n">{{ index + 1 }}</span>{{ stage.label }}
        </li>
      </ol>
      <div class="pg-say">
        <p aria-live="polite">{{ caption }}</p>
        <button class="pg-primary" :class="{ danger: primary.danger, urge: primary.urge }" :disabled="primary.busy" @click="primary.act">{{ primary.label }}</button>
      </div>
    </div>

    <div class="pg-world">
      <div class="pg-col pg-process" :data-alive="world.alive">
        <h3 class="pg-col-title">
          <span class="pg-lamp" :class="world.alive ? 's-live' : 's-dead'" aria-hidden="true" />
          <template v-if="world.alive">Process {{ world.process }}</template>
          <template v-else>Process {{ world.process }} <span class="pg-dead">killed</span></template>
        </h3>
        <p class="pg-col-sub">Runs the steps. Remembers nothing on its own.</p>
        <template v-if="focusRun">
          <p class="pg-on">{{ focusRun.input.orderId }}</p>
          <ol class="pg-steps">
            <li v-for="step in processView" :key="step.name" :data-state="step.state">
              <span class="pg-mark" aria-hidden="true" />
              <span class="pg-step-name">{{ step.name }}</span>
              <span class="pg-step-word">{{ step.word }}</span>
              <span v-if="step.state === 'running'" class="pg-bar" :style="{ '--p': progress(focusRun.id) }" />
            </li>
          </ol>
          <p v-if="world.alive && claimLeft > 0" class="pg-wait">Waiting {{ (claimLeft / 1000).toFixed(1) }}s for the dead process's claim to run out</p>
        </template>
        <p v-else class="pg-idle">{{ world.alive ? 'Waiting for work' : 'Dead' }}</p>
      </div>

      <div class="pg-col pg-talk">
        <h3 class="pg-col-title">What they say to each other</h3>
        <p class="pg-col-sub">Every line is a call the engine really made to the database.</p>
        <ol ref="logEl" class="pg-log" aria-label="Messages between the process and the database">
          <li v-for="m in messages" :key="m.id" :class="[m.tone, 'from-' + (m.from === 'db' ? 'db' : m.from === 'event' ? 'event' : m.from === 'app' ? 'app' : 'process')]">
            <span class="who">{{ m.from === 'event' ? '' : fromLabel(m.from) }}<span v-if="m.from !== 'event'" class="arrow" aria-hidden="true">{{ m.from === 'db' ? ' ←' : ' →' }}</span></span>
            <span class="what">{{ m.text }}<span v-if="m.count > 1" class="count"> ×{{ m.count }}</span></span>
            <span v-if="m.detail" class="why">{{ m.detail }}</span>
          </li>
          <li v-if="messages.length === 0" class="quiet"><span class="what">Nothing yet.</span></li>
        </ol>
      </div>

      <div class="pg-col pg-db">
        <h3 class="pg-col-title">The database</h3>
        <p class="pg-col-sub">Outlives any process. Holds every result.</p>
        <ul class="pg-rows">
          <li v-for="entry in dbRuns" :key="entry.run.id" class="pg-row" :class="{ focus: entry.run.id === world.focus }">
            <span class="pg-row-head"><strong>{{ entry.run.input.orderId }}</strong> <span class="pg-row-owner">{{ entry.owner }}</span></span>
            <span v-for="step in entry.steps" :key="step.name" class="pg-saved" :data-state="step.status">
              <span class="name">{{ step.name }}</span>
              <span class="value">{{ step.status === 'retrying' ? `attempt ${step.attempts} failed` : summary(step) }}</span>
            </span>
            <span v-if="entry.steps.length === 0" class="pg-saved none">no results yet</span>
          </li>
          <li v-if="dbRuns.length === 0" class="pg-idle">Empty</li>
        </ul>
      </div>
    </div>

    <div class="pg-foot">
      <dl class="pg-tally">
        <div><dt>cards charged</dt><dd>{{ world.charges }}</dd></div>
        <div><dt>orders</dt><dd>{{ world.orders }}</dd></div>
        <div><dt>steps a plain function would have run again</dt><dd>{{ world.rerunsAvoided }}</dd></div>
      </dl>
      <div v-if="world.stage === 'done' || world.stage === 'free'" class="pg-more">
        <button class="pg-btn" @click="startOrder">Start another order</button>
        <button v-if="world.alive" class="pg-btn danger" @click="kill">Kill the process</button>
        <button v-else class="pg-btn" @click="startProcess">Start a new process</button>
        <label class="pg-toggle"><input type="checkbox" :checked="world.warehouseDown" @change="toggleWarehouse" /> Break the warehouse</label>
      </div>
    </div>
  </section>

  <section class="pg-dash" aria-labelledby="pg-dash-title">
    <h2 id="pg-dash-title">The same runs, in the dashboard</h2>
    <p>This is Reflow's real dashboard — the handler you would mount in your app — reading the database above. Open the order workflow and pick a run to see each step, its timing, and any retries.</p>
    <iframe ref="frame" class="pg-frame" title="The Reflow dashboard, showing the runs from the demo above" />
  </section>
</template>

<style scoped>
.pg {
  --pg-live: #2563eb;
  --pg-good: #1e7a52;
  --pg-bad: #c0392f;
  --pg-held: #8f6100;
  --pg-faint: #677086;
  border: 1px solid var(--vp-c-divider);
  border-radius: 10px;
  background: var(--vp-c-bg-soft);
  overflow: hidden;
}
.dark .pg { --pg-live: #6e9bff; --pg-good: #4cc08a; --pg-bad: #f06a5f; --pg-held: #e8a317; --pg-faint: #8590a8; }

.pg-top { border-bottom: 1px solid var(--vp-c-divider); background: var(--vp-c-bg); }
.pg-story { display: flex; flex-wrap: wrap; gap: 6px 26px; margin: 0; padding: 14px 20px 0; list-style: none; font-size: 0.9rem; color: var(--vp-c-text-2); }
.pg-story li { display: flex; align-items: center; gap: 8px; margin: 0; }
.pg-story-n { display: inline-grid; place-items: center; width: 22px; height: 22px; border-radius: 50%; font: 700 0.8rem var(--rf-sign); box-shadow: inset 0 0 0 1.5px var(--pg-faint); }
.pg-story li.done { color: var(--vp-c-text-1); }
.pg-story li.done .pg-story-n { background: var(--pg-good); box-shadow: none; color: #fff; }
.pg-say { display: flex; align-items: center; gap: 12px 24px; padding: 14px 20px 16px; }
.pg-say p { flex: 1; margin: 0; font: 700 1.25rem/1.35 var(--rf-sign); color: var(--vp-c-text-1); }
.pg-primary { flex: none; font: inherit; font-weight: 700; padding: 11px 20px; border-radius: 6px; border: 1px solid transparent; background: var(--vp-c-text-1); color: var(--vp-c-bg); cursor: pointer; }
.pg-primary.danger { background: var(--pg-bad); color: #fff; }
.pg-primary.urge { animation: pg-urge 1.1s ease-in-out infinite; }
.pg-primary:disabled { opacity: 0.5; cursor: default; }
.pg-primary:focus-visible, .pg-btn:focus-visible { outline: 2px solid var(--vp-c-brand-1); outline-offset: 2px; }
@keyframes pg-urge { 50% { box-shadow: 0 0 0 6px color-mix(in srgb, var(--pg-bad) 25%, transparent); } }

.pg-world { display: grid; grid-template-columns: minmax(0, 0.8fr) minmax(0, 1.6fr) minmax(0, 1fr); }
.pg-col { padding: 16px 18px; border-left: 1px solid var(--vp-c-divider); min-width: 0; }
.pg-col:first-child { border-left: 0; }
.pg-col-title { display: flex; align-items: center; gap: 8px; margin: 0; padding: 0; border: 0; font: 700 1.1rem var(--rf-sign); letter-spacing: 0; }
.pg-col-sub { margin: 2px 0 14px; font-size: 0.84rem; color: var(--vp-c-text-2); }
.pg-dead { color: var(--pg-bad); }
.pg-process[data-alive='false'] { background: color-mix(in srgb, var(--pg-bad) 6%, transparent); }

.pg-lamp { flex: none; width: 10px; height: 10px; border-radius: 50%; }
.pg-lamp.s-live { background: var(--pg-live); animation: pg-pulse 1s ease-in-out infinite; }
.pg-lamp.s-dead { background: var(--pg-bad); }
@keyframes pg-pulse { 50% { opacity: 0.45; } }

.pg-on { margin: 0 0 6px; font: 700 1rem var(--rf-sign); }
.pg-steps { margin: 0; padding: 0; list-style: none; }
.pg-steps li { display: grid; grid-template-columns: 16px minmax(0, 1fr) auto; grid-template-areas: "mark name word" ". bar bar"; gap: 4px 8px; align-items: center; margin: 0; padding: 7px 0; border-top: 1px solid var(--vp-c-divider); font-size: 0.92rem; }
.pg-mark { grid-area: mark; width: 10px; height: 10px; border-radius: 50%; box-shadow: inset 0 0 0 2px var(--pg-faint); }
.pg-step-name { grid-area: name; font-family: var(--vp-font-family-mono); font-size: 0.86rem; }
.pg-step-word { grid-area: word; font-size: 0.84rem; color: var(--vp-c-text-2); }
.pg-steps [data-state='saved'] .pg-mark { background: var(--pg-good); box-shadow: none; }
.pg-steps [data-state='saved'] .pg-step-word { color: var(--pg-good); }
.pg-steps [data-state='running'] .pg-mark { background: var(--pg-live); box-shadow: none; }
.pg-steps [data-state='running'] .pg-step-word { color: var(--pg-live); }
.pg-steps [data-state='lost'] .pg-mark { background: var(--pg-bad); box-shadow: none; }
.pg-steps [data-state='lost'] .pg-step-word { color: var(--pg-bad); font-weight: 600; }
.pg-steps [data-state='retrying'] .pg-mark { box-shadow: inset 0 0 0 2px var(--pg-held); }
.pg-steps [data-state='retrying'] .pg-step-word { color: var(--pg-held); }
.pg-steps [data-state='ahead'] .pg-step-name { color: var(--vp-c-text-2); }
.pg-bar { grid-area: bar; display: block; height: 5px; border-radius: 3px; background: var(--vp-c-divider); overflow: hidden; position: relative; }
.pg-bar::after { content: ''; position: absolute; inset: 0; transform-origin: left; transform: scaleX(var(--p)); background: var(--pg-live); }
.pg-wait { margin: 10px 0 0; font-size: 0.86rem; color: var(--pg-held); }
.pg-idle { margin: 0; font-size: 0.92rem; color: var(--vp-c-text-2); }

/* The conversation. */
.pg-talk { background: var(--vp-c-bg); }
.pg-log { margin: 0; padding: 0; list-style: none; height: 330px; overflow-y: auto; scroll-behavior: smooth; }
.pg-log li { display: grid; grid-template-columns: 7.6em minmax(0, 1fr); gap: 1px 10px; margin: 0; padding: 7px 0; border-top: 1px solid var(--vp-c-divider); font-size: 0.9rem; animation: pg-in 0.35s ease-out; }
.pg-log li:first-child { border-top: 0; }
.pg-log .who { font-size: 0.8rem; font-weight: 600; color: var(--vp-c-text-2); white-space: nowrap; }
.pg-log .from-db .who { color: var(--pg-good); }
.pg-log .what { font-weight: 600; color: var(--vp-c-text-1); }
.pg-log .count { font-weight: 400; color: var(--vp-c-text-2); }
.pg-log .why { grid-column: 2; font-size: 0.84rem; color: var(--vp-c-text-2); line-height: 1.45; }
.pg-log .good .what { color: var(--pg-good); }
.pg-log .bad .what { color: var(--pg-bad); }
.pg-log .held .what { color: var(--pg-held); }
.pg-log .quiet .what { font-weight: 400; color: var(--vp-c-text-2); }
.pg-log .from-event { grid-template-columns: minmax(0, 1fr); padding: 9px 10px; margin: 4px 0; border: 0; border-radius: 6px; background: var(--vp-c-bg-soft); }
.pg-log .from-event .who { display: none; }
.pg-log .from-event .why { grid-column: 1; }
.pg-log .dead { background: color-mix(in srgb, var(--pg-bad) 10%, transparent); }
.pg-log .dead .what { color: var(--pg-bad); }
@keyframes pg-in { from { opacity: 0; transform: translateY(4px); } }

.pg-rows { margin: 0; padding: 0; list-style: none; display: flex; flex-direction: column; gap: 8px; }
.pg-row { display: flex; flex-direction: column; gap: 4px; margin: 0; padding: 8px 10px; border-radius: 6px; background: var(--vp-c-bg); font-size: 0.86rem; }
.pg-row.focus { box-shadow: inset 0 0 0 1px var(--vp-c-text-3); }
.pg-row-owner { display: block; font-size: 0.8rem; color: var(--vp-c-text-2); }
.pg-saved { display: grid; grid-template-columns: auto minmax(0, 1fr); gap: 8px; align-items: baseline; font-family: var(--vp-font-family-mono); font-size: 0.78rem; }
.pg-saved .name { color: var(--pg-good); }
.pg-saved[data-state='retrying'] .name, .pg-saved[data-state='retrying'] .value { color: var(--pg-held); }
.pg-saved .value { color: var(--vp-c-text-2); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.pg-saved.none { display: block; font-family: var(--vp-font-family-base); color: var(--vp-c-text-2); }

.pg-foot { display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 12px 24px; padding: 14px 20px; border-top: 1px solid var(--vp-c-divider); background: var(--vp-c-bg); }
.pg-tally { display: flex; flex-wrap: wrap; gap: 8px 32px; margin: 0; }
.pg-tally div { margin: 0; display: flex; align-items: baseline; gap: 8px; }
.pg-tally dt { font-size: 0.86rem; color: var(--vp-c-text-2); }
.pg-tally dd { margin: 0; order: -1; font: 700 1.6rem var(--rf-sign); }
.pg-more { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 12px; }
.pg-btn { font: inherit; font-size: 0.9rem; font-weight: 600; padding: 7px 13px; border-radius: 6px; border: 1px solid var(--vp-c-divider); background: var(--vp-c-bg-soft); color: var(--vp-c-text-1); cursor: pointer; }
.pg-btn:hover { border-color: var(--vp-c-text-2); }
.pg-btn.danger { color: var(--pg-bad); border-color: currentColor; background: transparent; }
.pg-toggle { display: flex; align-items: center; gap: 7px; font-size: 0.9rem; color: var(--vp-c-text-2); cursor: pointer; }

/* Wider than the text column, so the dashboard has room for its three panes. */
.pg-dash { margin: 72px calc(50% - min(640px, 50vw - 24px)) 0; }
.pg-dash h2 { max-width: 980px; margin: 0 auto 8px; padding: 0 24px; border: 0; font-family: var(--rf-sign); font-size: 2rem; font-weight: 700; }
.pg-dash p { max-width: 980px; margin: 0 auto 20px; padding: 0 24px; color: var(--vp-c-text-2); }
.pg-frame { display: block; width: 100%; height: 720px; border: 1px solid var(--vp-c-divider); border-radius: 10px; background: var(--vp-c-bg-alt); }

@media (max-width: 900px) {
  .pg-world { grid-template-columns: minmax(0, 1fr); }
  .pg-col { border-left: 0; border-top: 1px solid var(--vp-c-divider); }
  .pg-col:first-child { border-top: 0; }
  .pg-story { display: grid; grid-template-columns: 1fr 1fr; gap: 8px 12px; font-size: 0.86rem; }
  .pg-say { flex-direction: column; align-items: stretch; }
  .pg-say p { font-size: 1.1rem; }
  .pg-log { height: 280px; }
  .pg-frame { height: 640px; }
}
@media (prefers-reduced-motion: reduce) {
  .pg-lamp.s-live, .pg-primary.urge, .pg-log li { animation: none; }
  .pg-log { scroll-behavior: auto; }
}
</style>

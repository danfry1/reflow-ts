<script setup>
// Reflow itself, running in the page: a real workflow on the real engine over
// in-memory storage, drawn as the three things that matter — the workflow's
// code, the process running it, and the database holding what each step
// returned. Killing the process stops the engine mid-step, as a crash would
// leave things; a new process finds the run in the database once the dead
// one's claim expires, and carries on from the first step not saved.
import { computed, onMounted, onUnmounted, reactive, ref } from 'vue'
import { z } from 'zod'
import { createEngine, createWorkflow } from '../../../../src/index'
import { MemoryStorage } from '../../../../src/storage/memory'
import { createDashboard } from '../../../../src/dashboard/index'

const LEASE_MS = 2500
const HEARTBEAT_MS = 400
const DURATION = { charge: 1600, ship: 2800, 'send-receipt': 1200 }
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
  focus: null, // the run the story follows
  message: '',
  tone: '',
})
const runs = ref([])
const saved = ref(new Map())
// What the live process is doing: run id -> { step, startedAt }.
const working = reactive(new Map())
// What the killed process was doing when it died: run id -> step name.
const lost = reactive(new Map())
const now = ref(Date.now())

function tell(message, tone = '') {
  world.message = message
  world.tone = tone
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

const storage = new MemoryStorage()
let engine = null

function nextUnsaved(runId) {
  const done = new Set((saved.value.get(runId) || []).filter((s) => s.status === 'completed').map((s) => s.name))
  return STEPS.find((name) => !done.has(name)) ?? null
}

async function startProcess() {
  if (world.alive || world.starting) return
  world.starting = true
  const n = world.process + 1
  const process = createEngine({
    storage,
    workflows: [order],
    concurrency: 4,
    runLeaseDurationMs: LEASE_MS,
    heartbeatIntervalMs: HEARTBEAT_MS,
    hooks: {
      onStepStart: (event) => { working.set(event.runId, { step: event.stepName, startedAt: Date.now() }) },
      onRunStart: async (event) => {
        await refresh()
        const step = nextUnsaved(event.runId)
        if (step) working.set(event.runId, { step, startedAt: Date.now() })
        if (lost.has(event.runId)) {
          lost.delete(event.runId)
          if (event.runId === world.focus && world.stage === 'recovering') {
            tell(`Process ${n} found ${labelOf(event.runId)} in the database and carried on from ${step}. charge is already saved, so it does not run again.`, 'good')
          }
        }
      },
      onStepComplete: (event) => { working.delete(event.runId) },
      onRunComplete: (event) => {
        working.delete(event.runId)
        if (event.runId === world.focus && (world.stage === 'running' || world.stage === 'recovering')) {
          world.stage = 'done'
          tell(world.rerunsAvoided
            ? `${labelOf(event.runId)} finished. The process died half-way, yet only the step it was cut off in ran again — every finished step ran once, and the card was charged once. That is durable execution.`
            : `${labelOf(event.runId)} finished: three steps, each saved as it finished. Now start another order and kill the process while it ships.`, 'good')
        }
      },
      onRunFailed: (event) => { working.delete(event.runId) },
    },
  })
  await process.start(150)
  engine = process
  world.process = n
  world.alive = true
  world.starting = false
  if (world.stage === 'killed') {
    world.stage = 'recovering'
    tell(lost.size
      ? `Process ${n} is up. In the database it finds ${[...lost.keys()].map(labelOf).join(', ')}, still claimed by process ${n - 1}. A crashed process looks like a slow one, so it waits for that claim to expire — ${LEASE_MS / 1000}s after process ${n - 1}'s last heartbeat.`
      : `Process ${n} is up.`)
  }
}

async function kill() {
  if (!engine) return
  const process = engine
  const n = world.process
  engine = null
  world.alive = false
  // What dies with the process: the steps it was part way through.
  let rerun = 0
  for (const [runId, current] of working) {
    lost.set(runId, current.step)
    rerun += (saved.value.get(runId) || []).filter((s) => s.status === 'completed').length
  }
  working.clear()
  world.rerunsAvoided += rerun
  await process.stop()
  const focusLost = lost.get(world.focus)
  if (world.stage === 'running' || world.stage === 'done' || world.stage === 'free') {
    world.stage = 'killed'
  }
  if (focusLost) {
    tell(`Process ${n} is dead, in the middle of ${focusLost}. Whatever ${focusLost} had done is gone — it was never saved. ${focusLost === 'charge' ? 'Nothing was saved yet, so nothing is repeated either.' : "But charge's result is safe in the database. A plain async function would now start over from the top, and charge the card a second time."}`, 'bad')
  } else {
    tell(`Process ${n} is dead. Anything it was part way through is lost; everything it finished is in the database.`, 'bad')
  }
}

async function startOrder() {
  world.orders++
  const orderId = `ORD-${1000 + world.orders}`
  if (!engine) {
    tell(`${orderId} is saved to the database, but no process is running to work on it. Start one.`)
  }
  const writer = engine ?? createEngine({ storage, workflows: [order] })
  const run = await writer.enqueue('order', { orderId })
  if (world.stage === 'intro') {
    world.stage = 'running'
    world.focus = run.id
    tell(`${orderId} is in the database, and process ${world.process} has picked it up. Watch charge: when it finishes, its result is saved.`)
  } else if (world.stage === 'done' || world.stage === 'free') {
    world.stage = 'free'
    world.focus = run.id
  }
  await refresh()
}

function toggleWarehouse() {
  world.warehouseDown = !world.warehouseDown
  if (world.stage === 'done') world.stage = 'free'
  tell(world.warehouseDown
    ? 'The warehouse is down: ship will fail and retry, waiting longer each time. Each failed attempt is saved — kill the process during a wait, and the next one keeps the count.'
    : 'The warehouse is back. Orders waiting to retry ship will get through at their next attempt.')
}

async function refresh() {
  const listed = await storage.listRuns({ limit: 6 })
  runs.value = listed
  saved.value = await storage.getStepResultsForRuns(listed.map((r) => r.id))
}

function labelOf(runId) {
  return runs.value.find((r) => r.id === runId)?.input.orderId ?? 'the order'
}

// The story's next move, as the one button that matters now.
const primary = computed(() => {
  if (world.stage === 'intro') return { label: 'Start an order', act: startOrder }
  if (!world.alive) return { label: 'Start a new process', act: startProcess, busy: world.starting }
  if (world.stage === 'running') {
    const shipping = working.get(world.focus)?.step === 'ship'
    return { label: 'Kill the process', act: kill, danger: true, urge: shipping }
  }
  if (world.stage === 'recovering') return { label: 'Kill the process', act: kill, danger: true }
  return { label: 'Start another order', act: startOrder }
})

// The guidance that changes as the story moves on.
const nudge = computed(() => {
  if (world.stage === 'running' && working.get(world.focus)?.step === 'ship') {
    return 'charge is saved. ship is running now — kill the process before it finishes.'
  }
  return ''
})

const stages = [
  { key: 'start', label: 'Start an order', done: () => world.stage !== 'intro' },
  { key: 'kill', label: 'Kill the process', done: () => world.process > 1 || world.stage === 'killed' },
  { key: 'restart', label: 'Start a new one', done: () => world.process > 1 },
  { key: 'finish', label: 'See it finish', done: () => (world.stage === 'done' || world.stage === 'free') && world.process > 1 },
]

// The run the code and the boxes follow: the story's, or the newest.
const focusRun = computed(() => runs.value.find((r) => r.id === world.focus) || runs.value[0] || null)

function codeState(name) {
  const run = focusRun.value
  if (!run) return ''
  const step = (saved.value.get(run.id) || []).find((s) => s.name === name)
  if (step && step.status === 'completed') return 'saved'
  if (lost.get(run.id) === name) return 'lost'
  if (working.get(run.id)?.step === name) return 'running'
  if (step && step.status === 'retrying') return 'retrying'
  return ''
}

const claimLeft = computed(() => {
  if (!world.alive || lost.size === 0) return 0
  const waiting = runs.value.filter((r) => lost.has(r.id) && r.status === 'running')
  if (waiting.length === 0) return 0
  return Math.max(0, Math.min(...waiting.map((r) => r.updatedAt + LEASE_MS - now.value)))
})

function progress(runId) {
  const current = working.get(runId)
  if (!current) return 0
  return Math.min(1, (now.value - current.startedAt) / (DURATION[current.step] || 1000))
}

function summary(step) {
  if (step.status === 'retrying') return `attempt ${step.history.length} failed: ${step.error}`
  const out = step.output
  if (out && typeof out === 'object') {
    const [key, value] = Object.entries(out)[0] || []
    return key ? `${key}: ${JSON.stringify(value)}` : '{}'
  }
  return JSON.stringify(out)
}

const dbRuns = computed(() => runs.value.slice(0, 4).map((run) => ({
  run,
  steps: (saved.value.get(run.id) || []).filter((s) => s.status === 'completed' || s.status === 'retrying'),
})))

// --- The real dashboard, in a frame, reading the same storage.
//
// Its requests are answered by createDashboard()'s handler in this page, as a
// server would answer them; its live updates are nudged by a timer, since
// there is no server to stream them.
const frame = ref(null)
const control = {
  cancel: (runId) => (engine ? engine.cancel(runId) : storage.cancelRun(runId)),
  resume: (runId) => (engine ? engine.resume(runId) : storage.requeueRun(runId)),
}

async function mountDashboard() {
  const win = frame.value && frame.value.contentWindow
  if (!win) return
  // Written from this page, the frame's document takes this page's URL, which
  // ends in a slash as the dashboard expects; its API sits beneath it.
  const base = window.location.pathname.replace(/[^/]*$/, '')
  const dashboard = createDashboard({ storage, engine: control, workflows: [order], authorize: () => true, basePath: base })
  win.fetch = (input, init) => dashboard(new Request(new URL(String(input), win.location.href), init))
  win.EventSource = class {
    constructor() {
      win.setTimeout(() => this.onopen && this.onopen(), 0)
      this.timer = win.setInterval(() => this.onmessage && this.onmessage({ data: '{}' }), 600)
    }
    close() { win.clearInterval(this.timer) }
  }
  const html = await (await dashboard(new Request(new URL(base, window.location.origin)))).text()
  win.document.open()
  win.document.write(html)
  win.document.close()
}

let poller = null
let clock = null
onMounted(async () => {
  await storage.initialize()
  await mountDashboard()
  await startProcess()
  tell('On the left is a workflow with three steps. A process, in the middle, runs it; as each step finishes, its result is saved to the database, on the right. Start an order to watch.')
  poller = setInterval(refresh, 150)
  clock = setInterval(() => { now.value = Date.now() }, 100)
})

onUnmounted(async () => {
  clearInterval(poller)
  clearInterval(clock)
  if (engine) await engine.stop()
  engine = null
})
</script>

<template>
  <section class="pg" aria-label="Reflow running in this page">
    <ol class="pg-story" aria-label="Steps of the demo">
      <li v-for="(stage, index) in stages" :key="stage.key" :class="{ done: stage.done() }">
        <span class="pg-story-n">{{ index + 1 }}</span>{{ stage.label }}
      </li>
    </ol>

    <div class="pg-say" :class="world.tone">
      <p aria-live="polite">{{ world.message }}<strong v-if="nudge">{{ ' ' + nudge }}</strong></p>
      <button
        class="pg-primary"
        :class="{ danger: primary.danger, urge: primary.urge }"
        :disabled="primary.busy"
        @click="primary.act"
      >{{ primary.label }}</button>
    </div>

    <div class="pg-world">
      <div class="pg-col">
        <h3 class="pg-col-title">The workflow</h3>
        <pre class="pg-code"><code><span class="ln">createWorkflow('order')</span><span v-for="name in STEPS" :key="name" class="ln step" :data-state="codeState(name)">  .step('{{ name }}', …)<span class="mark" /></span></code></pre>
        <p class="pg-col-note">Each step returns a result. Reflow saves it before starting the next.</p>
      </div>

      <div class="pg-col pg-process" :data-alive="world.alive">
        <h3 class="pg-col-title">
          <span class="pg-lamp" :class="world.alive ? 's-live' : 's-dead'" aria-hidden="true" />
          <template v-if="world.alive">Process {{ world.process }}</template>
          <template v-else-if="world.starting">Starting…</template>
          <template v-else>Process {{ world.process }}, killed</template>
        </h3>
        <ul class="pg-jobs">
          <li v-for="[runId, job] in working" :key="runId" class="pg-job">
            <span class="pg-job-what"><strong>{{ labelOf(runId) }}</strong> running {{ job.step }}</span>
            <span class="pg-bar" :style="{ '--p': progress(runId) }" />
          </li>
          <li v-for="[runId, step] in lost" :key="'lost-' + runId" class="pg-job lost">
            <span class="pg-job-what"><strong>{{ labelOf(runId) }}</strong>: {{ step }} was cut off, never saved</span>
            <span v-if="world.alive && claimLeft > 0" class="pg-job-wait">Claimed by the dead process for {{ (claimLeft / 1000).toFixed(1) }}s more</span>
          </li>
          <li v-if="working.size === 0 && lost.size === 0" class="pg-idle">{{ world.alive ? 'Waiting for work' : 'Nothing running' }}</li>
        </ul>
        <p class="pg-col-note">{{ world.alive ? 'Runs the steps. Holds nothing that has to survive a crash.' : 'Gone, with everything it had not saved.' }}</p>
      </div>

      <div class="pg-col pg-db">
        <h3 class="pg-col-title">The database</h3>
        <ul class="pg-rows">
          <li v-for="entry in dbRuns" :key="entry.run.id" class="pg-row" :class="{ focus: entry.run.id === world.focus }">
            <span class="pg-row-head"><strong>{{ entry.run.input.orderId }}</strong> <span class="pg-row-status">{{ entry.run.status === 'completed' ? 'finished' : entry.run.status === 'sleeping' ? 'waiting to retry' : entry.run.status === 'running' ? 'in progress' : entry.run.status }}</span></span>
            <span v-for="step in entry.steps" :key="step.name" class="pg-saved" :data-state="step.status">
              <span class="tick" aria-hidden="true">{{ step.status === 'completed' ? '✓' : '↻' }}</span>
              <span class="name">{{ step.name }}</span>
              <span class="value">{{ summary(step) }}</span>
            </span>
            <span v-if="entry.steps.length === 0" class="pg-saved none">nothing saved yet</span>
          </li>
          <li v-if="dbRuns.length === 0" class="pg-idle">Empty</li>
        </ul>
        <p class="pg-col-note">Survives the process. This is what a new one reads.</p>
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

/* The four moves of the story, ticked off as they happen. */
.pg-story { display: flex; flex-wrap: wrap; gap: 6px 26px; margin: 0; padding: 14px 20px; list-style: none; border-bottom: 1px solid var(--vp-c-divider); background: var(--vp-c-bg); font-size: 0.92rem; color: var(--vp-c-text-2); }
.pg-story li { display: flex; align-items: center; gap: 8px; margin: 0; }
.pg-story-n { display: inline-grid; place-items: center; width: 22px; height: 22px; border-radius: 50%; font: 700 0.8rem var(--rf-sign); box-shadow: inset 0 0 0 1.5px var(--pg-faint); color: var(--vp-c-text-2); }
.pg-story li.done { color: var(--vp-c-text-1); }
.pg-story li.done .pg-story-n { background: var(--pg-good); box-shadow: none; color: #fff; }

.pg-say { display: flex; align-items: center; gap: 16px 24px; padding: 18px 20px; border-bottom: 1px solid var(--vp-c-divider); }
.pg-say p { flex: 1; margin: 0; font-size: 1.05rem; line-height: 1.55; color: var(--vp-c-text-1); }
.pg-say.bad p { color: var(--pg-bad); }
.pg-say.good p { color: var(--pg-good); }
.pg-say strong { color: var(--vp-c-text-1); }
.pg-primary { flex: none; font: inherit; font-weight: 700; padding: 11px 20px; border-radius: 6px; border: 1px solid transparent; background: var(--vp-c-text-1); color: var(--vp-c-bg); cursor: pointer; }
.pg-primary.danger { background: var(--pg-bad); color: #fff; }
.pg-primary.urge { animation: pg-urge 1.1s ease-in-out infinite; }
.pg-primary:disabled { opacity: 0.5; cursor: default; }
.pg-primary:focus-visible, .pg-btn:focus-visible { outline: 2px solid var(--vp-c-brand-1); outline-offset: 2px; }
@keyframes pg-urge { 50% { box-shadow: 0 0 0 6px color-mix(in srgb, var(--pg-bad) 25%, transparent); } }

.pg-world { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) minmax(0, 1.15fr); }
.pg-col { padding: 16px 20px 14px; border-left: 1px solid var(--vp-c-divider); min-width: 0; display: flex; flex-direction: column; }
.pg-col:first-child { border-left: 0; }
.pg-col-title { display: flex; align-items: center; gap: 8px; margin: 0 0 12px; padding: 0; border: 0; font: 700 1.1rem var(--rf-sign); letter-spacing: 0; }
.pg-col-note { margin: auto 0 0; padding-top: 12px; font-size: 0.84rem; color: var(--vp-c-text-2); }

.pg-code { margin: 0; padding: 12px 14px; border-radius: 6px; background: var(--vp-c-bg); font: 0.86rem/1.9 var(--vp-font-family-mono); overflow-x: auto; }
.pg-code .ln { display: block; position: relative; padding-right: 22px; border-radius: 3px; }
.pg-code .step[data-state='running'] { background: color-mix(in srgb, var(--pg-live) 14%, transparent); }
.pg-code .step[data-state='lost'] { background: color-mix(in srgb, var(--pg-bad) 14%, transparent); }
.pg-code .step[data-state='retrying'] { background: color-mix(in srgb, var(--pg-held) 16%, transparent); }
.pg-code .mark { position: absolute; right: 4px; top: 0; font-family: var(--vp-font-family-base); }
.pg-code .step[data-state='saved'] .mark::after { content: '✓'; color: var(--pg-good); }
.pg-code .step[data-state='running'] .mark::after { content: '◀'; color: var(--pg-live); }
.pg-code .step[data-state='lost'] .mark::after { content: '✕'; color: var(--pg-bad); }

.pg-lamp { flex: none; width: 10px; height: 10px; border-radius: 50%; }
.pg-lamp.s-live { background: var(--pg-live); animation: pg-pulse 1s ease-in-out infinite; }
.pg-lamp.s-dead { background: var(--pg-bad); }
@keyframes pg-pulse { 50% { opacity: 0.45; } }
.pg-process[data-alive='false'] { background: color-mix(in srgb, var(--pg-bad) 5%, transparent); }

.pg-jobs, .pg-rows { margin: 0; padding: 0; list-style: none; display: flex; flex-direction: column; gap: 10px; }
.pg-jobs li, .pg-rows li { margin: 0; }
.pg-job { display: flex; flex-direction: column; gap: 6px; font-size: 0.92rem; }
.pg-bar { display: block; height: 6px; border-radius: 3px; background: var(--vp-c-divider); overflow: hidden; position: relative; }
.pg-bar::after { content: ''; position: absolute; inset: 0; transform-origin: left; transform: scaleX(var(--p)); background: var(--pg-live); }
.pg-job.lost .pg-job-what { color: var(--pg-bad); }
.pg-job-wait { font-size: 0.84rem; color: var(--pg-held); }
.pg-idle { font-size: 0.92rem; color: var(--vp-c-text-2); }

.pg-row { display: flex; flex-direction: column; gap: 4px; padding: 8px 10px; border-radius: 6px; background: var(--vp-c-bg); font-size: 0.88rem; }
.pg-row.focus { box-shadow: inset 0 0 0 1px var(--vp-c-text-3); }
.pg-row-status { margin-left: 6px; color: var(--vp-c-text-2); }
.pg-saved { display: grid; grid-template-columns: 1.1em auto minmax(0, 1fr); gap: 8px; align-items: baseline; font-family: var(--vp-font-family-mono); font-size: 0.8rem; }
.pg-saved .tick { color: var(--pg-good); font-family: var(--vp-font-family-base); }
.pg-saved[data-state='retrying'] .tick, .pg-saved[data-state='retrying'] .value { color: var(--pg-held); }
.pg-saved .name { color: var(--vp-c-text-1); }
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

@media (max-width: 860px) {
  .pg-world { grid-template-columns: minmax(0, 1fr); }
  .pg-col { border-left: 0; border-top: 1px solid var(--vp-c-divider); }
  .pg-col:first-child { border-top: 0; }
  .pg-say { flex-direction: column; align-items: stretch; }
  .pg-frame { height: 640px; }
}
@media (prefers-reduced-motion: reduce) {
  .pg-lamp.s-live, .pg-primary.urge { animation: none; }
}
</style>

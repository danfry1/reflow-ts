<script setup>
// Reflow itself, running in the page: real workflows on a real engine over
// in-memory storage. "Kill the process" stops the engine mid-step the way a
// crash would leave things — runs stay claimed in storage, half done — and a
// new process finds them once their leases run out and carries on.
import { computed, onMounted, onUnmounted, reactive, ref } from 'vue'
import { z } from 'zod'
import { createEngine, createWorkflow } from '../../../../src/index'
import { MemoryStorage } from '../../../../src/storage/memory'

const LEASE_MS = 2500
const HEARTBEAT_MS = 400
const POLL_MS = 200

const world = reactive({
  process: 0,
  alive: false,
  starting: false,
  warehouseDown: false,
  charges: 0,
  orders: 0,
  kills: 0,
  recoveries: 0,
  savedSteps: 0,
  savedCharges: 0,
})
const runs = ref([])
const steps = ref(new Map())
const selectedId = ref(null)
const log = ref([])
let logId = 0

function say(text, tone = '') {
  log.value = [{ id: ++logId, at: Date.now(), text, tone }, ...log.value].slice(0, 7)
}

// A step's work, abandoned at once if the process is killed.
function work(ms, signal) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, ms)
    signal.addEventListener('abort', () => {
      clearTimeout(timer)
      reject(signal.reason)
    }, { once: true })
  })
}

const order = createWorkflow({
  name: 'order',
  input: z.object({ orderId: z.string() }),
  label: (input) => input.orderId,
})
  .step('charge', async ({ signal }) => {
    await work(1400, signal)
    world.charges++
    return { charged: true }
  })
  .step('ship', {
    retry: { maxAttempts: 5, backoff: 'exponential', initialDelayMs: 1500 },
    handler: async ({ signal }) => {
      await work(1200, signal)
      if (world.warehouseDown) throw new Error('Warehouse API unavailable')
      return { shipped: true }
    },
  })
  .sleep('wait-a-day', 5000)
  .step('ask-for-review', async ({ signal }) => {
    await work(800, signal)
    return { emailed: true }
  })

const refund = createWorkflow({
  name: 'refund',
  input: z.object({ orderId: z.string() }),
  label: (input) => `Refund ${input.orderId}`,
})
  .step('check-order', async ({ signal }) => {
    await work(900, signal)
    return { ok: true }
  })
  .waitForEvent('approved')
  .step('refund-card', async ({ signal }) => {
    await work(1100, signal)
    return { refunded: true }
  })

const OUTLINES = {
  order: [
    { name: 'charge', note: 'Charge the card' },
    { name: 'ship', note: 'Call the warehouse' },
    { name: 'wait-a-day', note: 'Sleep, standing in for a day', pause: true },
    { name: 'ask-for-review', note: 'Email the customer' },
  ],
  refund: [
    { name: 'check-order', note: 'Look the order up' },
    { name: 'approved', note: 'Wait for a person to approve', pause: true },
    { name: 'refund-card', note: 'Refund the card' },
  ],
}

const storage = new MemoryStorage()
let engine = null
// Runs a killed process left mid-step, and which process will pick each up.
const orphaned = new Map()
let refresh = null

function labelOf(run) {
  return run.workflow === 'order' ? run.input.orderId : `Refund ${run.input.orderId}`
}
function nextStep(runId, workflow) {
  const done = new Set((steps.value.get(runId) || []).filter((s) => s.status === 'completed').map((s) => s.name))
  const outline = OUTLINES[workflow] || []
  return outline.find((unit) => !done.has(unit.name))?.name ?? 'the end'
}

async function startProcess() {
  if (world.alive || world.starting) return
  world.starting = true
  const n = world.process + 1
  const process = createEngine({
    storage,
    workflows: [order, refund],
    concurrency: 4,
    runLeaseDurationMs: LEASE_MS,
    heartbeatIntervalMs: HEARTBEAT_MS,
    hooks: {
      onRunStart: (event) => {
        const left = orphaned.get(event.runId)
        if (left) {
          orphaned.delete(event.runId)
          world.recoveries++
          say(`Process ${n} picked up ${left.label} and carried on at ${nextStep(event.runId, event.workflow)}. ${left.done ? `${left.done} finished ${left.done === 1 ? 'step' : 'steps'} not run again.` : ''}`, 'good')
        }
      },
      onRunComplete: (event) => {
        const run = runs.value.find((r) => r.id === event.runId)
        if (run) say(`${labelOf(run)} finished.`, 'good')
      },
      onRunFailed: (event) => {
        const run = runs.value.find((r) => r.id === event.runId)
        if (run) say(`${labelOf(run)} failed at ${event.stepName}: ${event.error.message}`, 'bad')
      },
    },
  })
  await process.start(POLL_MS)
  engine = process
  world.process = n
  world.alive = true
  world.starting = false
  if (n > 1) {
    say(orphaned.size
      ? `Process ${n} started. It will pick up the abandoned runs once process ${n - 1}'s leases run out, ${LEASE_MS / 1000}s after its last heartbeat.`
      : `Process ${n} started.`)
  }
}

async function kill() {
  if (!engine) return
  const process = engine
  const n = world.process
  engine = null
  world.alive = false
  world.kills++
  // What the crash interrupts: runs mid-step, and what each had saved.
  const running = await storage.listRuns({ status: 'running', limit: 50 })
  let redo = 0
  let recharge = 0
  for (const run of running) {
    const saved = (await storage.getStepResults(run.id)).filter((s) => s.status === 'completed')
    orphaned.set(run.id, { label: labelOf(run), done: saved.length })
    redo += saved.length
    if (saved.some((s) => s.name === 'charge')) recharge++
  }
  world.savedSteps += redo
  world.savedCharges += recharge
  await process.stop()
  if (running.length === 0) {
    say(`Process ${n} was killed while nothing was running. Sleeping and waiting runs are safe in storage.`, 'bad')
    return
  }
  say(`Process ${n} was killed in the middle of ${running.length} ${running.length === 1 ? 'run' : 'runs'}. Their finished steps are saved; the steps they were on are lost.${redo ? ` A plain async function would start them over: ${redo} ${redo === 1 ? 'step' : 'steps'} again${recharge ? `, ${recharge} ${recharge === 1 ? 'card' : 'cards'} charged twice` : ''}.` : ''}`, 'bad')
}

async function startOrder() {
  world.orders++
  const orderId = `ORD-${1000 + world.orders}`
  // Enqueuing is a write to storage, so it works even with no process running.
  const run = engine
    ? await engine.enqueue('order', { orderId })
    : await enqueueWithoutProcess('order', { orderId })
  selectedId.value = run.id
  if (!engine) say(`${orderId} is queued in storage. It starts when a process is running.`)
}

async function startRefund() {
  const last = runs.value.find((r) => r.workflow === 'order')
  const orderId = last ? last.input.orderId : `ORD-${1000 + world.orders}`
  const run = engine
    ? await engine.enqueue('refund', { orderId })
    : await enqueueWithoutProcess('refund', { orderId })
  selectedId.value = run.id
}

// A process that is starting holds the engine; until then, enqueue through
// a short-lived one that never starts, which only writes the run.
async function enqueueWithoutProcess(name, input) {
  const writer = createEngine({ storage, workflows: [order, refund] })
  return writer.enqueue(name, input)
}

async function approve(run) {
  // An event is stored durably; a waiting run wakes on it whenever a process runs.
  if (engine) await engine.sendEvent(run.id, 'approved', { by: 'you' })
  else await storage.deliverEvent(run.id, 'approved', { by: 'you' })
  say(engine
    ? `Approved ${labelOf(run)}.`
    : `Approved ${labelOf(run)}. The approval is stored; the run carries on when a process is running.`)
}

function toggleWarehouse() {
  world.warehouseDown = !world.warehouseDown
  say(world.warehouseDown
    ? 'The warehouse API is down. Orders will retry ship, waiting longer each time.'
    : 'The warehouse API is back. Retrying orders will ship at their next attempt.')
}

async function poll() {
  const listed = await storage.listRuns({ limit: 12 })
  const stepMap = await storage.getStepResultsForRuns(listed.map((r) => r.id))
  runs.value = listed
  steps.value = stepMap
  if (!selectedId.value && listed[0]) selectedId.value = listed[0].id
}

onMounted(async () => {
  await storage.initialize()
  await startProcess()
  say('Process 1 started. Two orders are on their way.')
  await startOrder()
  setTimeout(() => { if (world.orders < 2) startOrder() }, 900)
  refresh = setInterval(poll, POLL_MS)
})

onUnmounted(async () => {
  clearInterval(refresh)
  if (engine) await engine.stop()
  engine = null
})

// --- What the view shows

const STATUS_WORD = {
  pending: 'Queued', running: 'Running', sleeping: 'Sleeping', waiting: 'Waiting',
  completed: 'Completed', failed: 'Failed', cancelled: 'Cancelled',
}

function duration(ms) {
  if (ms < 1000) return `${Math.max(0, Math.round(ms))}ms`
  return `${(ms / 1000).toFixed(1)}s`
}

const now = ref(Date.now())
let clock = null
onMounted(() => { clock = setInterval(() => { now.value = Date.now() }, 200) })
onUnmounted(() => clearInterval(clock))

function stepsOf(run) {
  return steps.value.get(run.id) || []
}

// Running in storage but with no process alive to run it: what a crash leaves.
function stranded(run) {
  return run.status === 'running' && (!world.alive || orphaned.has(run.id))
}

function sentence(run) {
  const list = stepsOf(run)
  const retrying = list.find((s) => s.status === 'retrying')
  if (stranded(run)) {
    const left = Math.max(0, run.updatedAt + LEASE_MS - now.value)
    return world.alive
      ? left > 0 ? `Abandoned at ${nextStep(run.id, run.workflow)}; free to pick up in ${duration(left)}` : `Abandoned at ${nextStep(run.id, run.workflow)}; being picked up`
      : `Stopped at ${nextStep(run.id, run.workflow)} when its process died`
  }
  if (run.status === 'running') return `Running ${nextStep(run.id, run.workflow)}`
  if (run.status === 'sleeping' && retrying) return `Retrying ship in ${duration(Math.max(0, (run.wakeAt || 0) - now.value))}, after ${retrying.attempts} failed ${retrying.attempts === 1 ? 'attempt' : 'attempts'}`
  if (run.status === 'sleeping') return `Sleeping; wakes in ${duration(Math.max(0, (run.wakeAt || 0) - now.value))}`
  if (run.status === 'waiting') return 'Waiting for approval'
  if (run.status === 'pending') return 'Queued'
  if (run.status === 'failed' && run.error) return `Failed at ${run.error.stepName}`
  if (run.status === 'completed' && run.startedAt && run.finishedAt) return `Done in ${duration(run.finishedAt - run.startedAt)}`
  return STATUS_WORD[run.status] || run.status
}

function lampOf(run) {
  if (stranded(run)) return 'stranded'
  return run.status
}

const selected = computed(() => runs.value.find((r) => r.id === selectedId.value) || null)

// The selected run's steps in order, with what each did.
const detail = computed(() => {
  const run = selected.value
  if (!run) return []
  const saved = new Map(stepsOf(run).map((s) => [s.name, s]))
  const outline = OUTLINES[run.workflow] || []
  const current = nextStep(run.id, run.workflow)
  return outline.map((unit) => {
    const step = saved.get(unit.name)
    let state = 'pending'
    let word = 'Not reached yet'
    if (step && step.status === 'completed') {
      state = 'completed'
      word = unit.pause ? 'Resumed' : step.attempts > 1 ? `Done on attempt ${step.attempts}` : 'Done'
    } else if (step && step.status === 'retrying') {
      state = 'retrying'
      word = `Attempt ${step.attempts} failed; next in ${duration(Math.max(0, (run.wakeAt || 0) - now.value))}`
    } else if (step && step.status === 'failed') {
      state = 'failed'
      word = `Failed after ${step.attempts} ${step.attempts === 1 ? 'attempt' : 'attempts'}`
    } else if (step && (step.status === 'sleeping' || step.status === 'waiting')) {
      state = step.status
      word = step.status === 'waiting' ? 'Waiting for approval' : `Sleeping; wakes in ${duration(Math.max(0, (run.wakeAt || 0) - now.value))}`
    } else if (unit.name === current && run.status === 'running') {
      state = stranded(run) ? 'stranded' : 'running'
      word = stranded(run) ? 'Lost when the process died; will run again' : 'Running'
    }
    const attempts = step && step.history && step.history.length > 1 ? step.history : []
    return { ...unit, state, word, attempts }
  })
})

const hint = computed(() => {
  const busy = runs.value.some((r) => r.status === 'running' && !stranded(r))
  if (!world.alive && world.kills > 0) return 'The runs are frozen where they were, with every finished step saved. Start a new process.'
  if (world.kills === 0 && busy) return 'Kill the process while an order is part way through.'
  if (world.kills === 0) return 'Start an order, then kill the process while it runs.'
  if (world.recoveries > 0 && !world.warehouseDown) return 'Nothing ran twice. Now break the warehouse, start an order, and kill the process while it waits to retry.'
  if (world.warehouseDown) return 'Kill the process during a backoff: the new one keeps the attempt count. Fix the warehouse to let orders through.'
  return 'Request a refund and approve it — even while no process is running.'
})
</script>

<template>
  <section class="pg" aria-label="Reflow running in this page">
    <div class="pg-bar">
      <span class="pg-process" :data-alive="world.alive">
        <span class="pg-lamp" :class="world.alive ? 's-running' : 's-failed'" aria-hidden="true" />
        <template v-if="world.alive">Process {{ world.process }} running</template>
        <template v-else-if="world.starting">Starting a process…</template>
        <template v-else>No process running</template>
      </span>
      <div class="pg-actions">
        <button class="pg-btn" @click="startOrder">Start an order</button>
        <button class="pg-btn" @click="startRefund">Request a refund</button>
        <button v-if="world.alive" class="pg-btn pg-kill" @click="kill">Kill the process</button>
        <button v-else class="pg-btn pg-go" :disabled="world.starting" @click="startProcess">Start a new process</button>
        <label class="pg-toggle">
          <input type="checkbox" :checked="world.warehouseDown" @change="toggleWarehouse" />
          Break the warehouse
        </label>
      </div>
    </div>
    <p class="pg-hint" aria-live="polite">{{ hint }}</p>

    <div class="pg-body">
      <ul class="pg-runs" aria-label="Runs">
        <li v-for="run in runs" :key="run.id">
          <button class="pg-run" :aria-current="String(run.id === selectedId)" @click="selectedId = run.id">
            <span class="pg-lamp" :class="'s-' + lampOf(run)" aria-hidden="true" />
            <span class="pg-run-name">{{ labelOf(run) }}</span>
            <span class="pg-run-why">{{ sentence(run) }}</span>
          </button>
        </li>
        <li v-if="runs.length === 0" class="pg-empty">Starting…</li>
      </ul>

      <div v-if="selected" class="pg-detail">
        <p class="pg-kicker">{{ selected.workflow }} run</p>
        <h3 class="pg-title">{{ labelOf(selected) }}</h3>
        <ol class="pg-steps">
          <li v-for="step in detail" :key="step.name" class="pg-step" :data-state="step.state">
            <span class="pg-lamp" :class="'s-' + step.state" aria-hidden="true" />
            <span class="pg-step-main">
              <span class="pg-step-name">{{ step.name }}</span>
              <span class="pg-step-note">{{ step.note }}</span>
            </span>
            <span class="pg-step-word">{{ step.word }}</span>
            <ul v-if="step.attempts.length" class="pg-attempts">
              <li v-for="(attempt, index) in step.attempts" :key="index">
                Attempt {{ index + 1 }}: {{ attempt.error ? 'failed, ' + attempt.error : 'succeeded' }}<template v-if="step.attempts[index + 1]">; next {{ duration(step.attempts[index + 1].startedAt - attempt.finishedAt) }} later</template>
              </li>
            </ul>
          </li>
        </ol>
        <button v-if="selected.status === 'waiting'" class="pg-btn pg-go pg-approve" @click="approve(selected)">Approve</button>
      </div>
    </div>

    <div class="pg-foot">
      <ol class="pg-log" aria-label="What happened">
        <li v-for="entry in log" :key="entry.id" :class="entry.tone">{{ entry.text }}</li>
      </ol>
      <dl class="pg-tally">
        <div><dt>Cards charged</dt><dd>{{ world.charges }} <span>for {{ runs.filter((r) => r.workflow === 'order' && stepsOf(r).some((s) => s.name === 'charge' && s.status === 'completed')).length }} charged orders</span></dd></div>
        <div><dt>Steps a restart would have re-run</dt><dd>{{ world.savedSteps }} <span>without Reflow, {{ world.savedCharges }} of them charges</span></dd></div>
      </dl>
    </div>
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

.pg-bar { display: flex; flex-wrap: wrap; align-items: center; gap: 12px 20px; padding: 14px 18px; border-bottom: 1px solid var(--vp-c-divider); background: var(--vp-c-bg); }
.pg-process { display: flex; align-items: center; gap: 9px; font-family: var(--rf-sign); font-weight: 700; font-size: 1.05rem; min-width: 12em; }
.pg-actions { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 10px; }
.pg-btn { font: inherit; font-size: 0.92rem; font-weight: 600; padding: 7px 13px; border-radius: 6px; border: 1px solid var(--vp-c-divider); background: var(--vp-c-bg-soft); color: var(--vp-c-text-1); cursor: pointer; }
.pg-btn:hover:not(:disabled) { border-color: var(--vp-c-text-2); }
.pg-btn:disabled { opacity: 0.5; cursor: default; }
.pg-btn:focus-visible, .pg-run:focus-visible, .pg-toggle input:focus-visible { outline: 2px solid var(--vp-c-brand-1); outline-offset: 2px; }
.pg-kill { color: var(--pg-bad); border-color: currentColor; background: transparent; }
.pg-go { background: var(--vp-c-text-1); color: var(--vp-c-bg); border-color: transparent; }
.pg-toggle { display: flex; align-items: center; gap: 7px; font-size: 0.92rem; color: var(--vp-c-text-2); cursor: pointer; }
.pg-hint { margin: 0; padding: 10px 18px; font-size: 0.95rem; color: var(--vp-c-text-1); border-bottom: 1px solid var(--vp-c-divider); background: color-mix(in srgb, var(--pg-live) 6%, transparent); }

.pg-lamp { flex: none; width: 10px; height: 10px; border-radius: 50%; box-shadow: inset 0 0 0 2px var(--pg-faint); }
.pg-lamp.s-completed { background: var(--pg-good); box-shadow: none; }
.pg-lamp.s-running { background: var(--pg-live); box-shadow: none; animation: pg-pulse 1s ease-in-out infinite; }
.pg-lamp.s-failed { background: var(--pg-bad); box-shadow: none; }
.pg-lamp.s-sleeping, .pg-lamp.s-waiting, .pg-lamp.s-retrying { box-shadow: inset 0 0 0 2px var(--pg-held); }
.pg-lamp.s-stranded { box-shadow: inset 0 0 0 2px var(--pg-bad); }
@keyframes pg-pulse { 50% { opacity: 0.45; } }

.pg-body { display: grid; grid-template-columns: minmax(0, 5fr) minmax(0, 7fr); min-height: 330px; }
.pg-runs { margin: 0; padding: 6px 0; list-style: none; border-right: 1px solid var(--vp-c-divider); max-height: 420px; overflow-y: auto; }
.pg-runs li { margin: 0; }
.pg-run { display: grid; grid-template-columns: 10px minmax(0, 1fr); grid-template-areas: "lamp name" ". why"; gap: 1px 10px; align-items: center; width: 100%; padding: 8px 16px; border: 0; background: none; text-align: left; cursor: pointer; color: inherit; font: inherit; }
.pg-run:hover { background: var(--vp-c-bg); }
.pg-run[aria-current='true'] { background: var(--vp-c-bg); box-shadow: inset 3px 0 0 var(--vp-c-text-1); }
.pg-run .pg-lamp { grid-area: lamp; }
.pg-run-name { grid-area: name; font-weight: 600; }
.pg-run-why { grid-area: why; font-size: 0.86rem; color: var(--vp-c-text-2); }
.pg-empty { padding: 12px 16px; color: var(--vp-c-text-2); }

.pg-detail { padding: 16px 20px 18px; min-width: 0; }
.pg-kicker { margin: 0; font-size: 0.85rem; color: var(--vp-c-text-2); }
.pg-title { margin: 0 0 10px; padding: 0; border: 0; font-family: var(--rf-sign); font-size: 1.5rem; font-weight: 700; }
.pg-steps { margin: 0; padding: 0; list-style: none; }
.pg-step { display: grid; grid-template-columns: 10px minmax(0, 1fr) auto; gap: 2px 12px; align-items: center; margin: 0; padding: 9px 0; border-top: 1px solid var(--vp-c-divider); }
.pg-step-main { display: flex; flex-direction: column; min-width: 0; line-height: 1.3; }
.pg-step-name { font-weight: 600; }
.pg-step-note { font-size: 0.82rem; color: var(--vp-c-text-2); }
.pg-step-word { font-size: 0.88rem; color: var(--vp-c-text-2); text-align: right; }
.pg-step[data-state='completed'] .pg-step-word { color: var(--pg-good); }
.pg-step[data-state='running'] .pg-step-word { color: var(--pg-live); }
.pg-step[data-state='failed'] .pg-step-word, .pg-step[data-state='stranded'] .pg-step-word { color: var(--pg-bad); }
.pg-step[data-state='retrying'] .pg-step-word, .pg-step[data-state='sleeping'] .pg-step-word, .pg-step[data-state='waiting'] .pg-step-word { color: var(--pg-held); }
.pg-step[data-state='pending'] .pg-step-name { color: var(--vp-c-text-2); }
.pg-attempts { grid-column: 2 / 4; margin: 4px 0 0; padding: 0; list-style: none; font-size: 0.84rem; color: var(--vp-c-text-2); }
.pg-attempts li { margin: 0; }
.pg-approve { margin-top: 12px; }

.pg-foot { display: grid; grid-template-columns: minmax(0, 7fr) minmax(0, 4fr); gap: 0; border-top: 1px solid var(--vp-c-divider); background: var(--vp-c-bg); }
.pg-log { margin: 0; padding: 10px 18px 12px; list-style: none; font-size: 0.9rem; color: var(--vp-c-text-2); min-height: 9.5em; }
.pg-log li { margin: 0 0 4px; }
.pg-log li:first-child { color: var(--vp-c-text-1); }
.pg-log li.good:first-child { color: var(--pg-good); }
.pg-log li.bad:first-child { color: var(--pg-bad); }
.pg-tally { margin: 0; padding: 12px 18px; border-left: 1px solid var(--vp-c-divider); display: flex; flex-direction: column; gap: 10px; }
.pg-tally div { margin: 0; }
.pg-tally dt { font-size: 0.82rem; color: var(--vp-c-text-2); }
.pg-tally dd { margin: 0; font-family: var(--rf-sign); font-size: 1.8rem; font-weight: 700; line-height: 1.1; }
.pg-tally dd span { font-family: var(--vp-font-family-base); font-size: 0.85rem; font-weight: 400; color: var(--vp-c-text-2); }

@media (max-width: 760px) {
  .pg-body, .pg-foot { grid-template-columns: minmax(0, 1fr); }
  .pg-actions { display: grid; grid-template-columns: 1fr 1fr; width: 100%; }
  .pg-actions .pg-btn { width: 100%; }
  .pg-toggle { grid-column: 1 / 3; }
  .pg-step { grid-template-columns: 10px minmax(0, 1fr); }
  .pg-step-word { grid-column: 2; text-align: left; }
  .pg-runs { border-right: 0; border-bottom: 1px solid var(--vp-c-divider); max-height: 230px; }
  .pg-tally { border-left: 0; border-top: 1px solid var(--vp-c-divider); }
}
@media (prefers-reduced-motion: reduce) {
  .pg-lamp.s-running { animation: none; }
}
</style>

<script setup>
import { reactive, ref, computed, onMounted, onUnmounted } from 'vue'

const root = ref(null)

// Each step's `cost` is the API / compute spend it incurs. `charge` marks a
// real side effect (charging a card) whose re-run means a duplicate charge.
const STEPS = [
  { key: 'charge', label: 'Charge customer', sub: '$50.00 on Stripe', cost: 0.3, ms: 1100, charge: true },
  { key: 'summarize', label: 'Summarize order', sub: 'LLM call', cost: 0.04, ms: 1500 },
  { key: 'enrich', label: 'Risk-check items', sub: 'LLM call', cost: 0.06, ms: 1400 },
  { key: 'email', label: 'Email the receipt', sub: 'Transactional email', cost: 0.0, ms: 800 },
]

const phase = ref('idle') // idle | running | crashed | recovering | done
const crashAt = ref(-1)

function freshLane() {
  return reactive({
    status: STEPS.map(() => 'pending'), // pending | running | done | crashed | cached
    spent: 0,
    charges: 0,
  })
}
const plain = freshLane()
const reflow = freshLane()

let token = 0
let releaseStep = null
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

function resetLane(lane) {
  for (let i = 0; i < STEPS.length; i++) lane.status[i] = 'pending'
  lane.spent = 0
  lane.charges = 0
}

function reset() {
  releaseStep = null
  resetLane(plain)
  resetLane(reflow)
  crashAt.value = -1
  phase.value = 'idle'
}

// Wait out a step, but let "Kill process" cut it short.
function stepWait(ms) {
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      releaseStep = null
      resolve(false)
    }, ms)
    releaseStep = () => {
      clearTimeout(timer)
      releaseStep = null
      resolve(true)
    }
  })
}

function killNow() {
  if (phase.value === 'running' && releaseStep) releaseStep()
}

async function run() {
  reset()
  const mine = ++token
  phase.value = 'running'

  for (let i = 0; i < STEPS.length; i++) {
    plain.status[i] = 'running'
    reflow.status[i] = 'running'
    const killed = await stepWait(STEPS[i].ms)
    if (mine !== token) return

    const last = i === STEPS.length - 1
    if (killed || last) {
      plain.status[i] = 'crashed'
      reflow.status[i] = 'crashed'
      crashAt.value = i
      phase.value = 'crashed'
      await sleep(1000)
      if (mine !== token) return
      await recover(mine)
      return
    }

    plain.status[i] = 'done'
    reflow.status[i] = 'done'
    plain.spent += STEPS[i].cost
    reflow.spent += STEPS[i].cost
    if (STEPS[i].charge) {
      plain.charges++
      reflow.charges++
    }
    await sleep(240)
    if (mine !== token) return
  }
}

async function recover(mine) {
  phase.value = 'recovering'
  await sleep(450)
  if (mine !== token) return
  await Promise.all([recoverReflow(mine), recoverPlain(mine)])
  if (mine !== token) return
  phase.value = 'done'
}

// Reflow: completed steps are checkpointed — skipped, never re-spent.
async function recoverReflow(mine) {
  for (let i = 0; i < crashAt.value; i++) {
    reflow.status[i] = 'cached'
    await sleep(160)
    if (mine !== token) return
  }
  for (let i = crashAt.value; i < STEPS.length; i++) {
    reflow.status[i] = 'running'
    await sleep(STEPS[i].ms * 0.7)
    if (mine !== token) return
    reflow.status[i] = 'done'
    reflow.spent += STEPS[i].cost
    if (STEPS[i].charge) reflow.charges++
    await sleep(180)
    if (mine !== token) return
  }
}

// Plain async function: no memory of what finished — re-runs everything.
async function recoverPlain(mine) {
  for (let i = 0; i < STEPS.length; i++) plain.status[i] = 'pending'
  for (let i = 0; i < STEPS.length; i++) {
    plain.status[i] = 'running'
    await sleep(STEPS[i].ms * 0.7)
    if (mine !== token) return
    plain.status[i] = 'done'
    plain.spent += STEPS[i].cost
    if (STEPS[i].charge) plain.charges++
    await sleep(180)
    if (mine !== token) return
  }
}

// Auto-play once when it scrolls into view so the value prop is obvious
// without requiring a click — but it stays fully interactive (kill / replay).
let observer = null
onMounted(() => {
  if (typeof IntersectionObserver === 'undefined' || !root.value) return
  observer = new IntersectionObserver(
    (entries) => {
      for (const e of entries) {
        if (e.isIntersecting && phase.value === 'idle') {
          run()
          observer?.disconnect()
        }
      }
    },
    { threshold: 0.35 },
  )
  observer.observe(root.value)
})

onUnmounted(() => {
  token++
  observer?.disconnect()
})

const money = (n) => '$' + n.toFixed(2)

const lanes = [
  { key: 'plain', state: plain },
  { key: 'reflow', state: reflow },
]

const wasted = computed(() => Math.max(0, plain.spent - reflow.spent))
const dupes = computed(() => Math.max(0, plain.charges - reflow.charges))
const crashedKey = computed(() => (crashAt.value >= 0 ? STEPS[crashAt.value].label : ''))

const statusLabel = {
  pending: 'not started',
  running: 'running',
  done: 'done',
  crashed: 'process killed',
  cached: 'already done',
}
</script>

<template>
  <section ref="root" class="ct" aria-labelledby="ct-title">
    <h2 id="ct-title" class="ct-title">One crash, two bills</h2>
    <p class="ct-lede">
      The same four-step order pipeline, written as a plain <code>async</code> function and as a Reflow
      workflow. Kill the process while it runs and watch what each does when it starts again.
    </p>

    <div class="ct-controls">
      <button
        v-if="phase === 'idle' || phase === 'done'"
        class="ct-btn ct-btn-go"
        @click="run"
      >
        {{ phase === 'done' ? 'Run it again' : 'Run the pipeline' }}
      </button>
      <button
        v-else
        class="ct-btn ct-btn-kill"
        :disabled="phase !== 'running'"
        @click="killNow"
      >
        Kill the process
      </button>

      <span class="ct-phase" :data-phase="phase" aria-live="polite">
        <template v-if="phase === 'idle'">Press run, then kill it whenever you like.</template>
        <template v-else-if="phase === 'running'">Running. Kill it whenever you like.</template>
        <template v-else-if="phase === 'crashed'">Killed during “{{ crashedKey }}”.</template>
        <template v-else-if="phase === 'recovering'">Starting again…</template>
        <template v-else>Both finished.</template>
      </span>
    </div>

    <div class="ct-lanes">
      <div v-for="lane in lanes" :key="lane.key" class="ct-lane" :class="'ct-lane-' + lane.key">
        <div class="ct-lane-head">
          <span class="ct-lane-name">
            <template v-if="lane.key === 'plain'">Plain <code>async</code> function</template>
            <template v-else>Reflow workflow</template>
          </span>
          <span class="ct-spent">{{ money(lane.state.spent) }} spent</span>
        </div>
        <ul class="ct-steps">
          <li v-for="(s, i) in STEPS" :key="s.key" class="ct-step" :data-state="lane.state.status[i]">
            <span class="ct-lamp" aria-hidden="true" />
            <span class="ct-step-main">
              <span class="ct-step-label">{{ s.label }}</span>
              <span class="ct-step-sub">{{ s.sub }}</span>
            </span>
            <span class="ct-step-status">{{ statusLabel[lane.state.status[i]] }}</span>
          </li>
        </ul>
        <p class="ct-lane-foot" :class="{ 'is-bad': phase === 'done' && lane.key === 'plain', 'is-good': phase === 'done' && lane.key === 'reflow' }">
          <template v-if="phase === 'done' && lane.key === 'plain'">
            Started over and ran every step again: {{ money(wasted) }} spent twice<template v-if="dupes > 0">, and the customer charged twice</template>.
          </template>
          <template v-else-if="phase === 'done'">
            Carried on from the step that was killed. Nothing ran twice; the customer was charged once.
          </template>
          <template v-else>&nbsp;</template>
        </p>
      </div>
    </div>

    <transition name="ct-fade">
      <p v-if="phase === 'done'" class="ct-score">
        <span class="ct-score-num">{{ money(wasted) }}</span>
        wasted by one crash<template v-if="dupes > 0">, plus a duplicate $50.00 charge</template>. With Reflow, nothing.
      </p>
    </transition>
  </section>
</template>

<style scoped>
.ct {
  --ct-live: var(--vp-c-brand-2);
  --ct-good: #1e7a52;
  --ct-bad: #c0392f;
  --ct-held: #8f6100;
  margin: 80px 0 0;
}
.dark .ct { --ct-good: #4cc08a; --ct-bad: #f06a5f; --ct-held: #e8a317; }

.ct-title {
  margin: 0 0 12px;
  padding: 0;
  border: 0;
  font-family: var(--rf-sign);
  font-size: 2rem;
  font-weight: 700;
  letter-spacing: -0.005em;
  line-height: 1.1;
}
.ct-lede { max-width: 680px; margin: 0; color: var(--vp-c-text-2); line-height: 1.65; }

.ct-controls { display: flex; align-items: center; flex-wrap: wrap; gap: 12px 18px; margin: 24px 0 18px; }
.ct-btn {
  font: inherit;
  font-weight: 600;
  padding: 10px 18px;
  border-radius: 6px;
  border: 1px solid transparent;
  cursor: pointer;
}
.ct-btn-go { background: var(--vp-c-text-1); color: var(--vp-c-bg); }
.ct-btn-go:hover { opacity: 0.88; }
.ct-btn-kill { background: transparent; color: var(--ct-bad); border-color: currentColor; }
.ct-btn-kill:hover:not(:disabled) { background: color-mix(in srgb, var(--ct-bad) 8%, transparent); }
.ct-btn-kill:disabled { opacity: 0.45; cursor: default; }
.ct-btn:focus-visible { outline: 2px solid var(--vp-c-brand-1); outline-offset: 2px; }
.ct-phase { font-size: 0.95rem; color: var(--vp-c-text-2); }
.ct-phase[data-phase='crashed'] { color: var(--ct-bad); }

.ct-lanes { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
@media (max-width: 720px) { .ct-lanes { grid-template-columns: 1fr; } }

.ct-lane { border: 1px solid var(--vp-c-divider); border-radius: 8px; background: var(--vp-c-bg-soft); }
.ct-lane-head {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  padding: 14px 16px 12px;
  border-bottom: 1px solid var(--vp-c-divider);
}
.ct-lane-name { font-family: var(--rf-sign); font-weight: 700; font-size: 1.15rem; }
.ct-lane-name :deep(code) { font-size: 0.85em; }
.ct-spent { font-family: var(--rf-sign); font-variant-numeric: tabular-nums; color: var(--vp-c-text-2); }

.ct-steps { list-style: none; margin: 0; padding: 0; }
.ct-step {
  display: grid;
  grid-template-columns: 10px minmax(0, 1fr) auto;
  align-items: center;
  gap: 12px;
  margin: 0;
  padding: 11px 16px;
  border-bottom: 1px solid var(--vp-c-divider);
  transition: background-color 0.25s ease;
}
.ct-step-main { display: flex; flex-direction: column; min-width: 0; line-height: 1.3; }
.ct-step-label { font-weight: 600; }
.ct-step-sub { font-size: 0.85rem; color: var(--vp-c-text-2); }
.ct-step-status { font-size: 0.9rem; color: var(--vp-c-text-2); white-space: nowrap; }

/* Lamps, as in the dashboard: a ring will move on, a filled lamp has settled or is live. */
.ct-lamp { width: 10px; height: 10px; border-radius: 50%; box-shadow: inset 0 0 0 2px var(--vp-c-text-3); }
.ct-step[data-state='running'] { background: color-mix(in srgb, var(--ct-live) 7%, transparent); }
.ct-step[data-state='running'] .ct-lamp { background: var(--ct-live); box-shadow: none; animation: ct-pulse 1s ease-in-out infinite; }
.ct-step[data-state='running'] .ct-step-status { color: var(--ct-live); }
.ct-step[data-state='done'] .ct-lamp { background: var(--ct-good); box-shadow: none; }
.ct-step[data-state='done'] .ct-step-status { color: var(--ct-good); }
.ct-step[data-state='crashed'] { background: color-mix(in srgb, var(--ct-bad) 9%, transparent); }
.ct-step[data-state='crashed'] .ct-lamp { background: var(--ct-bad); box-shadow: none; }
.ct-step[data-state='crashed'] .ct-step-status { color: var(--ct-bad); font-weight: 600; }
.ct-step[data-state='cached'] .ct-lamp { box-shadow: inset 0 0 0 2px var(--ct-good); }
.ct-step[data-state='cached'] .ct-step-label, .ct-step[data-state='cached'] .ct-step-sub { opacity: 0.6; }
@keyframes ct-pulse { 50% { opacity: 0.45; } }
@media (max-width: 480px) {
  .ct-step { grid-template-columns: 10px minmax(0, 1fr); row-gap: 2px; }
  .ct-step-status { grid-column: 2; }
}

.ct-lane-foot { margin: 0; padding: 12px 16px 14px; min-height: 3.2em; font-size: 0.92rem; line-height: 1.45; color: var(--vp-c-text-2); }
.ct-lane-foot.is-bad { color: var(--ct-bad); }
.ct-lane-foot.is-good { color: var(--ct-good); }

.ct-score { margin: 22px 0 0; max-width: 680px; font-size: 1.1rem; line-height: 1.5; color: var(--vp-c-text-1); }
.ct-score-num { margin-right: 8px; font-family: var(--rf-sign); font-size: 2.4rem; font-weight: 700; line-height: 1; font-variant-numeric: tabular-nums; }

.ct-fade-enter-active { transition: opacity 0.4s ease; }
.ct-fade-enter-from { opacity: 0; }

@media (prefers-reduced-motion: reduce) {
  .ct-step[data-state='running'] .ct-lamp { animation: none; }
  .ct-step { transition: none; }
}
</style>

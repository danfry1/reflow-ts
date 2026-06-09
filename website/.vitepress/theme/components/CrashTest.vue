<script setup>
import { reactive, ref, computed, onMounted, onUnmounted } from 'vue'

const root = ref(null)

// Each step's `cost` is the API / compute spend it incurs. `charge` marks a
// real side effect (charging a card) whose re-run means a duplicate charge.
const STEPS = [
  { key: 'charge', label: 'Charge customer', sub: 'Stripe · $50.00', cost: 0.3, ms: 1100, charge: true, glyph: '$' },
  { key: 'summarize', label: 'Summarize order', sub: 'LLM call', cost: 0.04, ms: 1500, glyph: '≈' },
  { key: 'enrich', label: 'Risk-check items', sub: 'LLM call', cost: 0.06, ms: 1400, glyph: '≈' },
  { key: 'email', label: 'Email the receipt', sub: 'transactional', cost: 0.0, ms: 800, glyph: '✉' },
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

const wasted = computed(() => Math.max(0, plain.spent - reflow.spent))
const dupes = computed(() => Math.max(0, plain.charges - reflow.charges))
const crashedKey = computed(() => (crashAt.value >= 0 ? STEPS[crashAt.value].label : ''))

const statusLabel = {
  pending: 'pending',
  running: 'running…',
  done: 'done',
  crashed: 'process killed',
  cached: 'skipped · checkpoint',
}
</script>

<template>
  <section ref="root" class="ct" aria-labelledby="ct-title">
    <div class="ct-head">
      <p class="ct-eyebrow">Why Reflow</p>
      <h2 id="ct-title" class="ct-title">One crash, two very different bills.</h2>
      <p class="ct-lede">
        The same four-step order pipeline, written as a plain <code>async</code> function and as a
        Reflow workflow. Hit <strong>Kill the process</strong> mid-run — a deploy, an OOM, a laptop
        asleep — and watch what each one does when it comes back.
      </p>
    </div>

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
        💥 Kill the process
      </button>

      <span class="ct-phase" :data-phase="phase" aria-live="polite">
        <template v-if="phase === 'idle'">idle — press run</template>
        <template v-else-if="phase === 'running'">running — kill it whenever you like</template>
        <template v-else-if="phase === 'crashed'">💥 killed during “{{ crashedKey }}”</template>
        <template v-else-if="phase === 'recovering'">restarting…</template>
        <template v-else>recovered</template>
      </span>
    </div>

    <div class="ct-lanes">
      <!-- Plain async -->
      <div class="ct-lane ct-lane-plain">
        <div class="ct-lane-head">
          <span class="ct-lane-name">Plain <code>async</code> function</span>
          <span class="ct-spent">{{ money(plain.spent) }} spent</span>
        </div>
        <ul class="ct-steps">
          <li
            v-for="(s, i) in STEPS"
            :key="s.key"
            class="ct-step"
            :data-state="plain.status[i]"
          >
            <span class="ct-glyph">{{ s.glyph }}</span>
            <span class="ct-step-main">
              <span class="ct-step-label">{{ s.label }}</span>
              <span class="ct-step-sub">{{ s.sub }}</span>
            </span>
            <span class="ct-step-status">{{ statusLabel[plain.status[i]] }}</span>
          </li>
        </ul>
        <div class="ct-lane-foot" :class="{ 'is-bad': phase === 'done' }">
          <template v-if="phase === 'done'">
            re-ran every step · <strong>{{ money(wasted) }} re-spent</strong>
            <template v-if="dupes > 0"> · <span class="ct-warn">⚠ customer charged twice</span></template>
          </template>
          <template v-else>&nbsp;</template>
        </div>
      </div>

      <!-- Reflow -->
      <div class="ct-lane ct-lane-reflow">
        <div class="ct-lane-head">
          <span class="ct-lane-name">Reflow workflow</span>
          <span class="ct-spent">{{ money(reflow.spent) }} spent</span>
        </div>
        <ul class="ct-steps">
          <li
            v-for="(s, i) in STEPS"
            :key="s.key"
            class="ct-step"
            :data-state="reflow.status[i]"
          >
            <span class="ct-glyph">{{ s.glyph }}</span>
            <span class="ct-step-main">
              <span class="ct-step-label">{{ s.label }}</span>
              <span class="ct-step-sub">{{ s.sub }}</span>
            </span>
            <span class="ct-step-status">{{ statusLabel[reflow.status[i]] }}</span>
          </li>
        </ul>
        <div class="ct-lane-foot" :class="{ 'is-good': phase === 'done' }">
          <template v-if="phase === 'done'">
            resumed from checkpoint · <strong>$0.00 re-spent</strong> · charged once
          </template>
          <template v-else>&nbsp;</template>
        </div>
      </div>
    </div>

    <transition name="ct-fade">
      <div v-if="phase === 'done'" class="ct-score">
        <span class="ct-score-num">{{ money(wasted) }}</span>
        <span class="ct-score-text">
          wasted on one crash<template v-if="dupes > 0"> — plus a duplicate <strong>$50.00</strong> charge</template>.
          Reflow re-spent nothing and never charged twice.
        </span>
      </div>
    </transition>
  </section>
</template>

<style scoped>
.ct {
  --ct-accent: #e46d2f;
  --ct-good: #2f9e6f;
  --ct-bad: #d2553a;
  margin: 5rem 0 1rem;
}

.ct-head { max-width: 46rem; }
.ct-eyebrow {
  font-family: var(--vp-font-family-mono, ui-monospace, monospace);
  font-size: 0.78rem;
  letter-spacing: 0.18em;
  text-transform: uppercase;
  color: var(--ct-accent);
  margin: 0 0 0.6rem;
}
.ct-title {
  font-size: clamp(1.7rem, 4vw, 2.4rem);
  line-height: 1.1;
  letter-spacing: -0.02em;
  font-weight: 800;
  margin: 0 0 0.75rem;
  border: 0;
  padding: 0;
}
.ct-lede { color: var(--vp-c-text-2); line-height: 1.6; margin: 0; }
.ct-lede code {
  font-size: 0.9em;
  background: var(--vp-c-bg-soft);
  padding: 0.1em 0.35em;
  border-radius: 5px;
}

.ct-controls {
  display: flex;
  align-items: center;
  gap: 1rem;
  flex-wrap: wrap;
  margin: 1.75rem 0 1.25rem;
}
.ct-btn {
  font: inherit;
  font-weight: 650;
  border: 0;
  border-radius: 999px;
  padding: 0.7rem 1.4rem;
  cursor: pointer;
  transition: transform 0.12s ease, box-shadow 0.2s ease, opacity 0.2s ease;
}
.ct-btn:active { transform: translateY(1px); }
.ct-btn-go {
  background: var(--ct-accent);
  color: #fff;
  box-shadow: 0 6px 22px -8px var(--ct-accent);
}
.ct-btn-go:hover { box-shadow: 0 10px 28px -8px var(--ct-accent); }
.ct-btn-kill {
  background: color-mix(in srgb, var(--ct-bad) 16%, transparent);
  color: var(--ct-bad);
  box-shadow: inset 0 0 0 1.5px color-mix(in srgb, var(--ct-bad) 45%, transparent);
  animation: ct-breathe 1.6s ease-in-out infinite;
}
.ct-btn-kill:disabled { opacity: 0.45; cursor: default; animation: none; }
@keyframes ct-breathe {
  0%, 100% { box-shadow: inset 0 0 0 1.5px color-mix(in srgb, var(--ct-bad) 45%, transparent); }
  50% { box-shadow: inset 0 0 0 1.5px var(--ct-bad), 0 0 18px -4px color-mix(in srgb, var(--ct-bad) 60%, transparent); }
}
.ct-phase {
  font-family: var(--vp-font-family-mono, ui-monospace, monospace);
  font-size: 0.82rem;
  color: var(--vp-c-text-2);
}
.ct-phase[data-phase='crashed'] { color: var(--ct-bad); }
.ct-phase[data-phase='done'] { color: var(--ct-good); }

.ct-lanes {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 1.1rem;
}
@media (max-width: 720px) { .ct-lanes { grid-template-columns: 1fr; } }

.ct-lane {
  border: 1px solid var(--vp-c-border);
  border-radius: 16px;
  padding: 1.1rem 1.1rem 0.9rem;
  background: var(--vp-c-bg-soft);
}
.ct-lane-reflow { box-shadow: inset 0 0 0 1px color-mix(in srgb, var(--ct-accent) 22%, transparent); }
.ct-lane-head {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  margin-bottom: 0.9rem;
}
.ct-lane-name { font-weight: 700; font-size: 0.98rem; }
.ct-lane-name code { font-size: 0.85em; }
.ct-spent {
  font-family: var(--vp-font-family-mono, ui-monospace, monospace);
  font-size: 0.82rem;
  color: var(--vp-c-text-2);
  font-variant-numeric: tabular-nums;
}

.ct-steps { list-style: none; padding: 0; margin: 0; display: flex; flex-direction: column; gap: 0.5rem; }
.ct-step {
  display: grid;
  grid-template-columns: auto 1fr auto;
  align-items: center;
  gap: 0.75rem;
  padding: 0.6rem 0.75rem;
  border-radius: 11px;
  background: var(--vp-c-bg);
  border: 1px solid transparent;
  transition: background 0.3s ease, border-color 0.3s ease, opacity 0.3s ease, transform 0.3s ease;
}
.ct-glyph {
  width: 1.7rem; height: 1.7rem;
  display: grid; place-items: center;
  border-radius: 8px;
  font-family: var(--vp-font-family-mono, ui-monospace, monospace);
  font-size: 0.9rem;
  background: var(--vp-c-bg-soft);
  color: var(--vp-c-text-2);
  transition: background 0.3s ease, color 0.3s ease;
}
.ct-step-main { display: flex; flex-direction: column; line-height: 1.25; min-width: 0; }
.ct-step-label { font-size: 0.9rem; font-weight: 600; }
.ct-step-sub { font-size: 0.74rem; color: var(--vp-c-text-3); }
.ct-step-status {
  font-family: var(--vp-font-family-mono, ui-monospace, monospace);
  font-size: 0.72rem;
  color: var(--vp-c-text-3);
  white-space: nowrap;
  font-variant-numeric: tabular-nums;
}

/* states */
.ct-step[data-state='running'] {
  border-color: color-mix(in srgb, var(--ct-accent) 40%, transparent);
  background: color-mix(in srgb, var(--ct-accent) 7%, var(--vp-c-bg));
}
.ct-step[data-state='running'] .ct-glyph { background: var(--ct-accent); color: #fff; animation: ct-pulse 1s ease-in-out infinite; }
.ct-step[data-state='running'] .ct-step-status { color: var(--ct-accent); }
@keyframes ct-pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.55; } }

.ct-step[data-state='done'] { border-color: color-mix(in srgb, var(--ct-good) 30%, transparent); }
.ct-step[data-state='done'] .ct-glyph { background: color-mix(in srgb, var(--ct-good) 18%, transparent); color: var(--ct-good); }
.ct-step[data-state='done'] .ct-step-status { color: var(--ct-good); }
.ct-step[data-state='done'] .ct-step-status::before { content: '✓ '; }

.ct-step[data-state='crashed'] {
  border-color: var(--ct-bad);
  background: color-mix(in srgb, var(--ct-bad) 10%, var(--vp-c-bg));
  animation: ct-shake 0.4s ease;
}
.ct-step[data-state='crashed'] .ct-glyph { background: var(--ct-bad); color: #fff; }
.ct-step[data-state='crashed'] .ct-step-status { color: var(--ct-bad); }
@keyframes ct-shake {
  0%, 100% { transform: translateX(0); }
  25% { transform: translateX(-4px); }
  75% { transform: translateX(4px); }
}

.ct-step[data-state='cached'] {
  border-style: dashed;
  border-color: color-mix(in srgb, var(--ct-accent) 45%, transparent);
  opacity: 0.62;
}
.ct-step[data-state='cached'] .ct-glyph { background: transparent; box-shadow: inset 0 0 0 1px var(--vp-c-border); }
.ct-step[data-state='cached'] .ct-step-status { color: var(--ct-accent); }

.ct-lane-foot {
  margin-top: 0.9rem;
  padding-top: 0.75rem;
  border-top: 1px dashed var(--vp-c-border);
  font-family: var(--vp-font-family-mono, ui-monospace, monospace);
  font-size: 0.78rem;
  min-height: 1.2rem;
  color: var(--vp-c-text-2);
}
.ct-lane-foot.is-bad { color: var(--ct-bad); }
.ct-lane-foot.is-good { color: var(--ct-good); }
.ct-warn { color: var(--ct-bad); font-weight: 700; }

.ct-score {
  margin-top: 1.1rem;
  display: flex;
  align-items: baseline;
  gap: 1rem;
  flex-wrap: wrap;
  padding: 1.1rem 1.3rem;
  border-radius: 14px;
  background: linear-gradient(100deg, color-mix(in srgb, var(--ct-accent) 14%, transparent), color-mix(in srgb, var(--ct-good) 10%, transparent));
  border: 1px solid color-mix(in srgb, var(--ct-accent) 30%, transparent);
}
.ct-score-num {
  font-family: var(--vp-font-family-mono, ui-monospace, monospace);
  font-size: clamp(2rem, 6vw, 3rem);
  font-weight: 800;
  color: var(--ct-accent);
  letter-spacing: -0.03em;
  font-variant-numeric: tabular-nums;
  line-height: 1;
}
.ct-score-text { color: var(--vp-c-text-1); line-height: 1.5; flex: 1; min-width: 14rem; }

.ct-fade-enter-active { transition: opacity 0.5s ease, transform 0.5s cubic-bezier(0.22, 1, 0.36, 1); }
.ct-fade-enter-from { opacity: 0; transform: translateY(12px) scale(0.98); }

@media (prefers-reduced-motion: reduce) {
  .ct-btn-kill, .ct-step[data-state='running'] .ct-glyph, .ct-step[data-state='crashed'] { animation: none; }
  .ct-fade-enter-active { transition: opacity 0.3s ease; }
  .ct-fade-enter-from { transform: none; }
}
</style>

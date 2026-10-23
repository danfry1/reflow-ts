/**
 * The dashboard's single page: markup, styles, and script in one document.
 *
 * Nothing is fetched from elsewhere, so it works offline and under the strict
 * content security policy the handler sends. The script builds every element
 * through the DOM rather than HTML strings, so run data — which is arbitrary
 * application data — is only ever rendered as text.
 */

/** Options for {@link renderPage}. */
export interface PageOptions {
  readonly title: string
  /** Per-response nonce permitting exactly this page's inline style and script. */
  readonly nonce: string
}

/** Render the dashboard document. */
export function renderPage({ title, nonce }: PageOptions): string {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="light dark">
<title>${escapeHtml(title)}</title>
<link rel="icon" href="data:image/svg+xml,${encodeURIComponent(MARK_SVG)}">
<style nonce="${nonce}">${STYLES}</style>
</head>
<body>
<div class="shell">
<aside class="side">
  <a class="brand" href="#/">${MARK_SVG}<span>${escapeHtml(title)}</span></a>
  <nav id="nav" aria-label="Workflows and views"></nav>
  <span class="live" id="live" hidden>Live</span>
</aside>
<main id="app" aria-live="polite"></main>
</div>
<script nonce="${nonce}">${SCRIPT}</script>
</body>
</html>`
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
}

/** A line broken mid-way that carries on past the break: a run that resumed. */
const MARK_SVG = '<svg class="mark" viewBox="0 0 32 32" aria-hidden="true"><path d="M3 22h10M19 10h10" stroke="currentColor" stroke-width="4" stroke-linecap="round"/><path d="M14.5 20.5l3-9" stroke="#E8A317" stroke-width="4" stroke-linecap="round" stroke-dasharray="0.01 6.5"/></svg>'

const STYLES = String.raw`
:root {
  --canvas: #F4F6F9;
  --surface: #FFFFFF;
  --ink: #14213D;
  --ink-soft: #4A5672;
  --ink-faint: #8A93A8;
  --rule: #DCE1EA;
  --focus: #2563EB;
  --live: #2563EB;
  --hold: #C98A0B;
  --proceed: #23825A;
  --stop: #D2463C;
  --withdrawn: #6B7488;
  --code-bg: #EAEEF4;
  /* Signage: DIN where the system has it (macOS, Windows), for names and figures. */
  --sign: "DIN Alternate", Bahnschrift, "DIN Next", "D-DIN", "Barlow Semi Condensed", "Helvetica Neue", Arial, sans-serif;
  --body: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
  --code: ui-monospace, "SF Mono", "Cascadia Code", Menlo, Consolas, monospace;
  color-scheme: light;
}
@media (prefers-color-scheme: dark) {
  :root {
    --canvas: #0E1626;
    --surface: #131D31;
    --ink: #E6EAF2;
    --ink-soft: #A9B3C7;
    --ink-faint: #6E7A93;
    --rule: #24314A;
    --focus: #7AA7FF;
    --live: #6E9BFF;
    --hold: #E8A317;
    --proceed: #4CC08A;
    --stop: #F06A5F;
    --withdrawn: #8D97AC;
    --code-bg: #0A111E;
    color-scheme: dark;
  }
}
* { box-sizing: border-box; }
html { background: var(--canvas); }
body { margin: 0; font: 15px/1.5 var(--body); color: var(--ink); background: var(--canvas); }
a { color: inherit; }
:focus-visible { outline: 2px solid var(--focus); outline-offset: 2px; }
button { font: inherit; color: inherit; }
.sign, h1, .bar, .filters, .row .when, .facts dd, .sched td { font-family: var(--sign); font-variant-numeric: tabular-nums; }

.shell { display: grid; grid-template-columns: 248px minmax(0, 1fr); min-height: 100vh; }

/* The sidebar: every workflow, always one click away. */
.side { position: sticky; top: 0; height: 100vh; overflow-y: auto; display: flex; flex-direction: column; gap: 18px; padding: 18px 12px; border-right: 1px solid var(--rule); background: var(--surface); }
.brand { display: flex; align-items: center; gap: 10px; padding: 0 8px; text-decoration: none; font-weight: 700; font-size: 19px; font-family: var(--sign); }
.mark { width: 26px; height: 26px; flex: none; }
#nav { display: flex; flex-direction: column; gap: 18px; }
.nav-group h2 { margin: 0 0 4px; padding: 0 8px; font: 600 12.5px var(--body); color: var(--ink-faint); }
.nav-group ul { list-style: none; margin: 0; padding: 0; }
.nav-item { display: flex; align-items: center; gap: 8px; padding: 6px 8px; border-radius: 5px; text-decoration: none; color: var(--ink-soft); font-size: 14.5px; }
.nav-item:hover { background: var(--canvas); color: var(--ink); }
.nav-item[aria-current="page"] { background: var(--canvas); color: var(--ink); font-weight: 600; }
.nav-item .nm { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.nav-item .tally { display: flex; gap: 10px; font: 13px var(--sign); font-variant-numeric: tabular-nums; color: var(--ink-faint); }
.nav-item .tally > span { display: flex; align-items: center; gap: 4px; }
.nav-item .tally .lamp { width: 7px; height: 7px; }
.nav-item .tally .bad { color: var(--stop); }
.nav-item .tally .busy { color: var(--hold); }
.live { margin-top: auto; display: flex; align-items: center; gap: 8px; padding: 0 8px; font-size: 13px; color: var(--ink-soft); }
.live[hidden] { display: none; }
.live::before { content: ""; width: 8px; height: 8px; border-radius: 50%; background: var(--live); }

/* Runs beside the run being looked at. */
.panes { display: grid; grid-template-columns: minmax(300px, 400px) minmax(0, 1fr); height: 100vh; }
.pane-list { border-right: 1px solid var(--rule); padding: 20px 16px 40px; min-width: 0; overflow-y: auto; }
.pane-detail { padding: 20px 28px 56px; min-width: 0; overflow-y: auto; }
.pane-list h1 { margin: 0 0 4px; font-size: 24px; }
.pane-list .intro { margin: 0 0 14px; font-size: 13.5px; }
.pane-list .filters { gap: 2px 14px; margin-bottom: 12px; }
.pane-list .filters button { font-size: 14px; }
.pane-list .filters .n { font-size: 16px; }
.pane-list .rows { border-radius: 6px; }
.pane-list .row { padding: 10px 12px; }
.pane-list .row[aria-current="true"] { background: var(--canvas); box-shadow: inset 3px 0 0 var(--ink); }
.placeholder { margin-top: 18vh; text-align: center; color: var(--ink-soft); }
.placeholder strong { display: block; margin-bottom: 4px; color: var(--ink); font: 700 18px var(--sign); }
.back { display: none; }
@media (max-width: 980px) {
  .shell { grid-template-columns: minmax(0, 1fr); }
  .side { position: static; height: auto; flex-direction: row; flex-wrap: wrap; align-items: center; border-right: 0; border-bottom: 1px solid var(--rule); }
  #nav { flex-direction: row; overflow-x: auto; gap: 12px; flex: 1 1 100%; }
  .nav-group h2 { display: none; }
  .nav-group ul { display: flex; gap: 2px; }
  .nav-item { white-space: nowrap; }
  .live { margin: 0 0 0 auto; }
  .panes { grid-template-columns: minmax(0, 1fr); height: auto; }
  .pane-list, .pane-detail { overflow-y: visible; }
  .pane-list { border-right: 0; }
  .panes.has-run .pane-list { display: none; }
  .panes:not(.has-run) .pane-detail { display: none; }
  .back { display: inline-block; margin-bottom: 10px; color: var(--ink-soft); font-size: 14px; }
}

main { min-width: 0; }
.page { padding: 28px 32px 56px; max-width: 1100px; }

/* Status lamps: a filled lamp is a settled state, a ring one that will move on. */
.lamp { display: inline-block; width: 10px; height: 10px; border-radius: 50%; background: currentColor; flex: none; }
.lamp.ring { background: transparent; box-shadow: inset 0 0 0 2px currentColor; }
.s-completed, .s-completed-early { color: var(--proceed); }
.s-running { color: var(--live); }
.s-sleeping, .s-waiting, .s-retrying { color: var(--hold); }
.s-failed { color: var(--stop); }
.s-cancelled { color: var(--withdrawn); }
.s-pending, .s-skipped, .s-all { color: var(--ink-faint); }

.filters { display: flex; flex-wrap: wrap; align-items: baseline; gap: 6px 26px; margin: 0 0 18px; padding: 0; list-style: none; }
.filters button { display: flex; align-items: center; gap: 8px; padding: 4px 0; border: 0; border-bottom: 2px solid transparent; background: none; cursor: pointer; font-family: var(--sign); font-size: 16px; color: var(--ink-soft); }
.filters .n { font-size: 22px; font-weight: 700; color: var(--ink); }
.filters .zero .n { color: var(--ink-faint); }
.filters button[aria-pressed="true"] { color: var(--ink); border-bottom-color: currentColor; }
.filters button[aria-pressed="true"] .lamp { box-shadow: 0 0 0 3px var(--canvas), 0 0 0 4px currentColor; }


.search { width: 100%; margin-bottom: 12px; padding: 8px 10px; border: 1px solid var(--rule); border-radius: 4px; background: var(--surface); color: var(--ink); font: inherit; }

.rows { list-style: none; margin: 0; padding: 0; background: var(--surface); border: 1px solid var(--rule); border-radius: 8px; overflow: hidden; }
.row { display: grid; grid-template-columns: 10px minmax(0, 1fr) auto; grid-template-areas: "lamp name when" ". why why"; gap: 1px 12px; align-items: center; padding: 9px 14px; border-top: 1px solid var(--rule); text-decoration: none; }
.row.changed { animation: changed 1.6s ease-out; }
@keyframes changed { from { background: color-mix(in srgb, var(--live) 22%, transparent); } to { background: transparent; } }
@media (prefers-reduced-motion: reduce) { .row.changed { animation: none; } }
.rows li:first-child .row { border-top: 0; }
.row:hover { background: var(--canvas); }
.row[aria-current="true"] { background: var(--canvas); box-shadow: inset 3px 0 0 var(--ink); }
.row .lamp { grid-area: lamp; }
.row .name { grid-area: name; font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.row .when { grid-area: when; text-align: right; color: var(--ink-soft); font-size: 14px; white-space: nowrap; }
.row .why { grid-area: why; font-size: 13.5px; color: var(--ink-soft); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.more { display: block; width: 100%; margin-top: 10px; padding: 9px; border: 1px solid var(--rule); border-radius: 4px; background: none; cursor: pointer; color: var(--ink-soft); }
.more:hover { color: var(--ink); border-color: var(--ink-soft); }

.empty { padding: 40px 16px; text-align: center; color: var(--ink-soft); background: var(--surface); border: 1px solid var(--rule); border-radius: 8px; }
.empty strong { display: block; color: var(--ink); font-family: var(--sign); font-size: 18px; margin-bottom: 4px; }

/* The detail sits on the canvas, not in a card: the line is the thing to look at. */
.detail-head { display: flex; align-items: flex-start; gap: 16px; margin-top: 6px; }
.runid { display: flex; flex-wrap: wrap; gap: 2px 18px; margin-top: 4px; font-size: 13px; color: var(--ink-faint); overflow-wrap: anywhere; }
.runid .input { color: var(--ink-soft); }
.actions { margin-left: auto; display: flex; gap: 8px; align-items: center; }
.btn { padding: 7px 14px; border-radius: 4px; border: 1px solid var(--rule); background: var(--surface); cursor: pointer; }
.btn:hover { border-color: var(--ink-soft); }
.btn.danger { color: var(--stop); }
.close { border: 0; background: none; cursor: pointer; color: var(--ink-soft); padding: 4px 8px; font-size: 22px; line-height: 1; }

.state { display: flex; align-items: baseline; gap: 10px; margin: 18px 0 0; font-size: 17px; }
.state .lamp { position: relative; top: 1px; }
.state .reason { color: var(--ink-soft); overflow-wrap: anywhere; }
.facts { display: flex; flex-wrap: wrap; gap: 4px 32px; margin: 14px 0 0; }
.facts div { display: flex; gap: 8px; align-items: baseline; }
.facts dt { font-size: 13px; color: var(--ink-soft); }
.facts dd { margin: 0; font-size: 16px; }

h2 { margin: 34px 0 10px; font: 700 18px var(--sign); }

/* Steps: one row each, with a bar on the run's own time axis. */
.steps { width: 100%; border-collapse: collapse; font-size: 14px; }
.steps th { text-align: left; font: 600 13px var(--body); color: var(--ink-soft); padding: 0 12px 8px 0; border-bottom: 1px solid var(--rule); }
.steps td { padding: 10px 12px 10px 0; border-top: 1px solid var(--rule); vertical-align: middle; }
.steps tbody tr:first-child td { border-top: 0; }
.steps .name { font-weight: 600; overflow-wrap: anywhere; }
.steps .name .aside { display: block; font-weight: 400; font-size: 12.5px; color: var(--ink-faint); }
.steps .status { white-space: nowrap; }
.steps .status .lamp { margin-right: 8px; }
.steps .when { width: 38%; min-width: 120px; }
.steps .took { text-align: right; white-space: nowrap; font-family: var(--sign); font-variant-numeric: tabular-nums; color: var(--ink-soft); }
.steps th.took { font-family: var(--body); }
.axis { display: flex; justify-content: space-between; font-family: var(--sign); font-weight: 400; color: var(--ink-faint); }
.track { position: relative; height: 10px; border-radius: 2px; background: var(--rule); }
.seg { position: absolute; top: 0; height: 10px; min-width: 2px; border-radius: 2px; }
.seg.ok { background: var(--proceed); }
.seg.bad { background: var(--stop); }
.seg.live { background: var(--live); }
.seg.pause { top: 3px; height: 4px; background: var(--hold); opacity: 0.75; }
.steps tr.attempt td { padding-top: 4px; padding-bottom: 4px; border-top: 0; font-size: 13.5px; color: var(--ink-soft); }
.steps tr.attempt .name { padding-left: 14px; font-weight: 400; color: var(--ink-soft); }
.steps tr.attempt .bad { color: var(--stop); }
.steps tr.attempt .after { color: var(--ink-soft); }
.steps tr.idle td { font-size: 13.5px; color: var(--hold); padding-left: 0; }
.note { color: var(--ink-soft); }
@media (max-width: 640px) {
  .steps thead { display: none; }
  .steps, .steps tbody, .steps tr, .steps td { display: block; width: auto; }
  .steps tr.step { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 6px 12px; padding: 12px 0; border-top: 1px solid var(--rule); }
  .steps tbody tr:first-child { border-top: 0; }
  .steps tr.step td { padding: 0; border: 0; }
  .steps tr.step .status { grid-column: 1 / 3; }
  .steps tr.step .when { grid-column: 1 / 3; width: auto; min-width: 0; }
  .steps tr.step .took { grid-row: 1; grid-column: 2; }
  .steps tr.attempt { padding: 2px 0 6px 14px; }
  .steps tr.attempt td { display: inline; padding: 0; }
  .steps tr.attempt .name { padding: 0 8px 0 0; }
  .steps tr.idle td { padding: 8px 0; border-top: 1px solid var(--rule); }
  .filters { gap: 4px 18px; }
  .filters button { font-size: 15px; }
  .filters .n { font-size: 18px; }
}

/* Page heads: where you are in workflows, then runs, then one run. */
.page > h1, .pane-list > h1 { margin: 0 0 6px; font-size: 30px; line-height: 1.1; font-weight: 700; overflow-wrap: anywhere; }
.kicker { margin: 0 0 2px; font-size: 14px; color: var(--ink-soft); }
.kicker a { color: var(--ink-soft); }
.kicker a:hover { color: var(--ink); }
.detail-title { margin: 0; font-size: 28px; line-height: 1.15; font-weight: 700; font-family: var(--sign); overflow-wrap: anywhere; }
.intro { margin: 4px 0 20px; max-width: 46em; color: var(--ink-soft); }

/* Workflows: each with its steps and how its runs stand. */
.wfs { list-style: none; margin: 0; padding: 0; background: var(--surface); border: 1px solid var(--rule); border-radius: 8px; overflow: hidden; }
.wfs li + li .wf { border-top: 1px solid var(--rule); }
.wf { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 8px 16px; padding: 16px 18px; text-decoration: none; }
.wf:hover { background: var(--canvas); }
.wf-name { font: 700 19px var(--sign); }
.wf-total { font-family: var(--sign); color: var(--ink-soft); text-align: right; }
.wf .recipe, .wf .counts { grid-column: 1 / 3; }
.recipe { display: flex; flex-wrap: wrap; gap: 4px 22px; margin: 0; padding: 0; list-style: none; counter-reset: step; font-size: 14px; }
.recipe li { counter-increment: step; display: flex; gap: 7px; align-items: baseline; }
.recipe li::before { content: counter(step); font: 700 13px var(--sign); color: var(--ink-faint); }
.recipe-block { margin: 14px 0 8px; }
.recipe-block .label { margin: 0 0 6px; font-size: 13px; color: var(--ink-soft); }
.recipe-block .recipe { font-size: 15px; }
.counts { display: flex; flex-wrap: wrap; gap: 4px 20px; font-size: 14px; color: var(--ink-soft); }
.counts.none { color: var(--ink-faint); }
.count { display: flex; align-items: center; gap: 7px; }
.count .n { font: 700 15px var(--sign); color: var(--ink); }
.row .name .label { display: block; font-weight: 400; font-size: 13px; color: var(--ink-faint); overflow: hidden; text-overflow: ellipsis; }
.steps tr.unreached .name, .steps tr.unreached .status { color: var(--ink-faint); }

/* A workflow's picture: where runs get to, why they fail, how busy it is. */
.pipeline { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 0; margin: 0; padding: 0; list-style: none; border: 1px solid var(--rule); border-radius: 8px; background: var(--surface); overflow: hidden; }
.stage { display: flex; flex-direction: column; gap: 6px; padding: 14px 16px 16px; border-left: 1px solid var(--rule); min-width: 0; }
.stage:first-child { border-left: 0; }
.stage .kind { font: 600 12px var(--body); color: var(--ink-faint); white-space: pre; }
.stage-name { font: 700 16px var(--sign); overflow-wrap: anywhere; }
.reach { display: block; height: 4px; border-radius: 2px; background: var(--rule); }
.reach .fill { display: block; height: 100%; border-radius: 2px; background: var(--ink-soft); }
.reached { font-size: 13px; color: var(--ink-soft); }
.reached .n { font: 700 22px var(--sign); color: var(--ink); margin-right: 2px; }
.outcomes { display: flex; flex-direction: column; gap: 3px; margin: 0; padding: 0; list-style: none; font-size: 13px; color: var(--ink-soft); }
.outcomes li, .outcomes button { display: flex; align-items: center; gap: 6px; }
.outcomes .lamp { width: 8px; height: 8px; }
.outcomes .held { color: var(--hold); }
.linkish { padding: 0; border: 0; background: none; cursor: pointer; color: var(--ink-soft); text-decoration: underline; text-underline-offset: 2px; text-decoration-color: var(--rule); }
.linkish:hover { color: var(--ink); text-decoration-color: currentColor; }
.linkish.bad { color: var(--stop); }
.failures { margin: 0; padding: 0; list-style: none; border: 1px solid var(--rule); border-radius: 8px; background: var(--surface); overflow: hidden; }
.failures li + li { border-top: 1px solid var(--rule); }
.failure { display: grid; grid-template-columns: 3.2em minmax(0, 1fr) auto; gap: 14px; align-items: baseline; width: 100%; padding: 11px 16px; border: 0; background: none; text-align: left; cursor: pointer; }
.failure:hover { background: var(--canvas); }
.failure .count { font: 700 20px var(--sign); color: var(--stop); text-align: right; }
.failure .what { display: block; font-weight: 600; overflow-wrap: anywhere; }
.failure .where { display: block; font-size: 13px; color: var(--ink-soft); }
.failure .last { font-size: 13px; color: var(--ink-faint); white-space: nowrap; }
.activity .cols { display: flex; align-items: flex-end; gap: 2px; height: 72px; padding: 0 1px; border-bottom: 1px solid var(--rule); }
.activity .col { flex: 1; height: 100%; display: flex; align-items: flex-end; }
.activity .bar { position: relative; display: block; width: 100%; min-height: 0; background: var(--proceed); border-radius: 2px 2px 0 0; opacity: 0.85; }
.activity .bar .bad { position: absolute; left: 0; right: 0; bottom: 0; background: var(--stop); }
.activity .axis { margin-top: 4px; font-size: 12px; }
.legend { display: flex; flex-wrap: wrap; gap: 4px 20px; margin: 8px 0 0; font-size: 13.5px; color: var(--ink-soft); }
.legend > span { display: flex; align-items: center; gap: 6px; }
.legend .n { font: 700 15px var(--sign); color: var(--ink); }
.scope { display: flex; gap: 10px; align-items: baseline; margin: 0 0 10px; font-size: 13.5px; color: var(--stop); }
.runid .linkish { margin-left: 4px; font-size: 13px; }


pre.json { margin: 0; padding: 14px 16px; background: var(--code-bg); border-radius: 6px; overflow: auto; max-height: 320px; font: 12.5px/1.6 var(--code); }
.pair { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 20px; }
@media (max-width: 640px) {
  .pair { grid-template-columns: minmax(0, 1fr); }
  .page, .pane-detail, .pane-list { padding: 18px 16px 40px; }
  .detail-head h1 { font-size: 26px; }
}

.sched { width: 100%; border-collapse: collapse; background: var(--surface); border: 1px solid var(--rule); border-radius: 8px; }
.sched th, .sched td { text-align: left; padding: 11px 16px; border-top: 1px solid var(--rule); }
.sched th { border-top: 0; font: 600 13px var(--body); color: var(--ink-soft); }
.sched code { font: 13px var(--code); }
.problem { margin: 0 0 18px; color: var(--stop); font-weight: 600; }
`

const SCRIPT = String.raw`
'use strict';
(function () {
  // Every request below is relative to the page, so the page must sit under a
  // trailing slash — a host that strips the mount path (Express) cannot add it.
  if (!/\/$/.test(location.pathname)) {
    location.replace(location.pathname + '/' + location.search + location.hash);
    return;
  }

  // Most urgent first: what needs a person, then what is moving, then history.
  var FILTERS = ['failed', 'running', 'waiting', 'sleeping', 'pending', 'completed', 'cancelled'];
  var STATUS_LABEL = {
    all: 'All', running: 'Running', waiting: 'Waiting', sleeping: 'Sleeping', pending: 'Queued',
    failed: 'Failed', completed: 'Completed', cancelled: 'Cancelled',
    retrying: 'Retrying', skipped: 'Skipped', 'completed-early': 'Completed early'
  };
  var TERMINAL = { completed: true, failed: true, cancelled: true };
  // A ring lamp is a state that will move on by itself; a filled one has settled or is live.
  var RING = { sleeping: true, waiting: true, retrying: true, pending: true };
  // Time with no step executing, longer than this, is called out between steps.
  var BREAK_MS = 1000;

  var app = document.getElementById('app');
  var liveBadge = document.getElementById('live');
  var state = { overview: null, workflows: null, schedules: null, insights: null, status: null, workflow: '', search: '', find: '', stepFilter: null, scope: null, runs: [], next: null, detail: null, problem: null };

  // --- DOM helpers: every piece of run data goes in as text, never markup.
  function h(tag, props, children) {
    var el = document.createElement(tag);
    if (props) {
      for (var key in props) {
        var value = props[key];
        if (value === null || value === undefined || value === false) continue;
        if (key === 'class') el.className = value;
        else if (key === 'text') el.textContent = value;
        else if (key.indexOf('on') === 0) el.addEventListener(key.slice(2), value);
        else el.setAttribute(key, value === true ? '' : String(value));
      }
    }
    (children || []).forEach(function (child) {
      if (child === null || child === undefined || child === false) return;
      el.appendChild(typeof child === 'string' ? document.createTextNode(child) : child);
    });
    return el;
  }
  function api(path, init) {
    return fetch(path, init).then(function (response) {
      return response.json().then(function (body) {
        if (!response.ok) throw new Error(body && body.error ? body.error : 'Request failed with status ' + response.status);
        return body;
      });
    });
  }

  // --- Formatting
  function duration(ms) {
    if (ms === null || ms === undefined || ms < 0) return '';
    if (ms < 1000) return Math.round(ms) + 'ms';
    var s = ms / 1000;
    if (s < 60) return (s < 10 ? s.toFixed(1) : Math.round(s)) + 's';
    var m = Math.floor(s / 60), rs = Math.round(s % 60);
    if (m < 60) return m + 'm' + (rs ? ' ' + rs + 's' : '');
    var hr = Math.floor(m / 60);
    if (hr < 48) return hr + 'h' + (m % 60 ? ' ' + (m % 60) + 'm' : '');
    return Math.floor(hr / 24) + 'd' + (hr % 24 ? ' ' + (hr % 24) + 'h' : '');
  }
  function ago(ts) {
    if (!ts) return '';
    var d = Date.now() - ts;
    if (d < 0) return 'in ' + duration(-d);
    if (d < 5000) return 'just now';
    return duration(d).split(' ')[0] + ' ago';
  }
  function absolute(ts) { return ts ? new Date(ts).toLocaleString() : ''; }
  function clock(ts) {
    if (!ts) return '';
    var d = new Date(ts);
    var sameDay = d.toDateString() === new Date().toDateString();
    return sameDay ? d.toLocaleTimeString() : d.toLocaleString();
  }
  function time(ts, relative) {
    return h('time', { datetime: ts ? new Date(ts).toISOString() : null, title: absolute(ts) }, [relative ? ago(ts) : clock(ts)]);
  }
  function shortId(id) { return id.length > 12 ? id.slice(0, 8) : id; }
  function json(value) { return value === undefined ? 'undefined' : JSON.stringify(value, null, 2); }
  function plural(n, word) { return n + ' ' + word + (n === 1 ? '' : 's'); }

  function lamp(status, title) {
    return h('span', { class: 'lamp s-' + status + (RING[status] ? ' ring' : ''), title: title || STATUS_LABEL[status], 'aria-hidden': 'true' });
  }

  // One line on why a run is where it is — the question the list answers.
  function why(run) {
    var retrying = (run.steps || []).filter(function (step) { return step.status === 'retrying'; })[0];
    if (run.status === 'failed' && run.error) return 'Failed at ' + run.error.stepName + ': ' + run.error.message;
    if (run.status === 'sleeping' && retrying && run.wakeAt) return 'Retrying ' + retrying.name + ' ' + ago(run.wakeAt) + ', after ' + plural(retrying.attempts, 'failed attempt');
    if (run.status === 'waiting') return 'Waiting for ' + (run.waitingFor || 'an event') + (run.wakeAt ? ', times out ' + ago(run.wakeAt) : '');
    if (run.status === 'sleeping' && run.wakeAt) return 'Resumes ' + ago(run.wakeAt);
    if (run.status === 'pending') return run.startedAt ? 'Queued to carry on' : 'Queued';
    if (run.status === 'running') return 'Running for ' + duration(Date.now() - run.startedAt);
    if (run.status === 'cancelled') return 'Cancelled' + (run.finishedAt ? ' ' + ago(run.finishedAt) : '');
    if (run.finishedAt && run.startedAt) return 'Took ' + duration(run.finishedAt - run.startedAt);
    return '';
  }

  // --- Steps: what ran, in order, in words.

  // Parallel branches share a start time and run side by side. A step that
  // merely began in the millisecond its predecessor ended shares the time
  // too, so overlap is what tells them apart.
  function unitsOf(steps) {
    var units = [];
    steps.forEach(function (step) {
      var last = units[units.length - 1];
      var sibling = last && step.startedAt && last.startedAt === step.startedAt
        && last.members.some(function (member) { return member.updatedAt > step.startedAt; });
      if (sibling) last.members.push(step);
      else units.push({ startedAt: step.startedAt, members: [step] });
    });
    return units;
  }

  // Sleeps and waits have no handler, so they make no attempts.
  function isPause(step) {
    return step.attempts === 0 && (step.status === 'sleeping' || step.status === 'waiting' || step.status === 'completed');
  }
  function stepStart(step) { return step.startedAt || step.createdAt; }
  function stepEnd(step) {
    return step.status === 'sleeping' || step.status === 'waiting' || step.status === 'retrying' ? Date.now() : step.updatedAt;
  }

  // The status a group of parallel branches shows: its most urgent member's.
  var URGENCY = ['failed', 'retrying', 'waiting', 'sleeping', 'running', 'skipped', 'completed-early', 'completed'];
  function unitStatus(unit) {
    var statuses = unit.members.map(function (step) { return step.status; });
    for (var i = 0; i < URGENCY.length; i++) if (statuses.indexOf(URGENCY[i]) >= 0) return URGENCY[i];
    return statuses[0];
  }

  function stepStatusText(step, run) {
    if (step.status === 'waiting') return 'Waiting for ' + (run.waitingFor || 'an event');
    if (step.status === 'sleeping') return 'Sleeping';
    if (step.status === 'retrying') return 'Retrying ' + (run.wakeAt ? ago(run.wakeAt) : 'soon');
    if (step.status === 'completed' && isPause(step)) return 'Resumed';
    if (step.status === 'failed' && step.attempts > 1) return 'Failed after ' + plural(step.attempts, 'attempt');
    if (step.status === 'completed' && step.attempts > 1) return 'Completed on attempt ' + step.attempts;
    return STATUS_LABEL[step.status] || step.status;
  }

  // A bar placed on the run's own time axis. Retries draw each attempt, with
  // the waits between them faint; a sleep or wait is one faint bar.
  function timeline(step, axis) {
    var track = h('div', { class: 'track' });
    function seg(from, to, kind) {
      var el = h('span', { class: 'seg ' + kind });
      el.style.left = axis.at(from) + '%';
      el.style.width = Math.max(0, axis.at(to) - axis.at(from)) + '%';
      track.appendChild(el);
    }
    var history = step.history || [];
    if (isPause(step)) seg(stepStart(step), stepEnd(step), 'pause');
    else if (history.length === 0) seg(stepStart(step), stepEnd(step), step.status === 'failed' ? 'bad' : 'ok');
    else {
      history.forEach(function (attempt, index) {
        var prev = history[index - 1];
        if (prev) seg(prev.finishedAt, attempt.startedAt, 'pause');
        seg(attempt.startedAt, attempt.finishedAt, attempt.error === null ? 'ok' : 'bad');
      });
      if (step.status === 'retrying') seg(history[history.length - 1].finishedAt, Date.now(), 'pause');
    }
    return track;
  }

  function stepsTable(run, steps, outline) {
    if (steps.length === 0 && run.status !== 'running' && !outline) {
      return h('p', { class: 'note', text: run.startedAt ? 'No step has finished yet.' : 'Waiting for an engine to pick it up.' });
    }
    var origin = run.startedAt || run.createdAt;
    var total = Math.max(1, (run.finishedAt || Date.now()) - origin);
    var axis = { at: function (t) { return Math.max(0, Math.min(100, 100 * (t - origin) / total)); } };

    var rows = [];
    var cursor = origin;
    unitsOf(steps).forEach(function (unit) {
      var from = stepStart(unit.members[0]);
      if (from - cursor > BREAK_MS) {
        rows.push(h('tr', { class: 'idle' }, [h('td', { colspan: '4', text: 'Nothing ran for ' + duration(from - cursor) + ', then an engine picked the run up again.' })]));
      }
      unit.members.forEach(function (step) {
        var span = stepEnd(step) - stepStart(step);
        var open = step.status === 'sleeping' || step.status === 'waiting' || step.status === 'retrying';
        rows.push(h('tr', { class: 'step' }, [
          h('td', { class: 'name' }, [
            h('span', { text: step.name }),
            unit.members.length > 1 ? h('span', { class: 'aside', text: 'in parallel' }) : null
          ]),
          h('td', { class: 'status' }, [lamp(step.status), h('span', { text: stepStatusText(step, run) })]),
          h('td', { class: 'when' }, [timeline(step, axis)]),
          h('td', { class: 'took', text: step.status === 'skipped' ? '' : duration(span) + (open ? ' so far' : '') })
        ]));
        attemptRows(step, run).forEach(function (row) { rows.push(row); });
        cursor = Math.max(cursor, stepEnd(step));
      });
    });
    if (run.status === 'running') {
      rows.push(h('tr', { class: 'step' }, [
        h('td', { class: 'name' }, [h('span', { class: 'aside', text: 'Next step' })]),
        h('td', { class: 'status' }, [lamp('running'), h('span', { text: 'Running' })]),
        h('td', { class: 'when' }, [(function () {
          var track = h('div', { class: 'track' });
          var el = h('span', { class: 'seg live' });
          el.style.left = axis.at(cursor) + '%';
          el.style.width = (100 - axis.at(cursor)) + '%';
          track.appendChild(el);
          return track;
        })()]),
        h('td', { class: 'took', text: duration(Date.now() - cursor) + ' so far' })
      ]));
    }

    // Steps the run has not reached, from the workflow's definition.
    if (outline) {
      var ran = {};
      steps.forEach(function (step) { ran[step.name] = true; });
      outline.forEach(function (unit) {
        var names = unit.kind === 'parallel' ? unit.branches : [unit.name];
        if (names.some(function (name) { return ran[name]; })) return;
        rows.push(h('tr', { class: 'step unreached' }, [
          h('td', { class: 'name' }, [h('span', { text: names.join(' + ') }), unit.kind === 'parallel' ? h('span', { class: 'aside', text: 'in parallel' }) : null]),
          h('td', { class: 'status' }, [lamp('pending'), h('span', { text: TERMINAL[run.status] ? 'Did not run' : 'Not reached yet' })]),
          h('td', { class: 'when' }, [h('div', { class: 'track' })]),
          h('td', { class: 'took' })
        ]));
      });
    }

    return h('table', { class: 'steps' }, [
      h('thead', null, [h('tr', null, [
        h('th', { text: 'Step' }),
        h('th', { text: 'Status' }),
        h('th', { class: 'when' }, [h('span', { class: 'axis' }, [h('span', { text: '0s' }), h('span', { text: duration(total) })])]),
        h('th', { class: 'took', text: 'Took' })
      ])]),
      h('tbody', null, rows)
    ]);
  }

  // A step that needed more than one try lists each, and the wait after it.
  function attemptRows(step, run) {
    var history = step.history || [];
    if (history.length < 2 && !(history.length === 1 && history[0].error !== null && step.status === 'retrying')) return [];
    var earlier = step.attempts - history.length;
    return history.map(function (attempt, index) {
      var next = history[index + 1];
      var text = attempt.error === null
        ? 'Succeeded in ' + duration(attempt.finishedAt - attempt.startedAt) + '.'
        : 'Failed after ' + duration(attempt.finishedAt - attempt.startedAt) + ': ' + attempt.error;
      var after = next
        ? ' Next attempt ' + duration(next.startedAt - attempt.finishedAt) + ' later.'
        : step.status === 'retrying' && run.wakeAt ? ' Next attempt ' + ago(run.wakeAt) + '.' : '';
      return h('tr', { class: 'attempt' }, [
        h('td', { class: 'name', text: 'Attempt ' + (earlier + index + 1) }),
        h('td', { colspan: '3', class: attempt.error === null ? '' : 'bad' }, [text, after ? h('span', { class: 'after', text: after }) : null])
      ]);
    });
  }

  // --- Routing. A run is addressed inside the list it was picked from, so
  // the list stays beside it: #/workflows/<name>/runs/<id> or #/runs/<id>.
  function decode(raw) {
    try { return decodeURIComponent(raw); } catch (error) { return raw; }
  }
  function route() {
    var hash = location.hash.replace(/^#/, '') || '/';
    if (hash === '/schedules') return { view: 'schedules' };
    var all = hash.match(/^\/runs(?:\/(.+))?$/);
    if (all) return { view: 'runs', runId: all[1] ? decode(all[1]) : null };
    var inWorkflow = hash.match(/^\/workflows\/([^/]+)(?:\/runs\/(.+))?$/);
    if (inWorkflow) return { view: 'workflow', workflow: decode(inWorkflow[1]), runId: inWorkflow[2] ? decode(inWorkflow[2]) : null };
    return { view: 'home' };
  }
  function workflowHref(name) { return '#/workflows/' + encodeURIComponent(name); }
  function runHref(r, id) {
    return r.view === 'workflow'
      ? workflowHref(r.workflow) + '/runs/' + encodeURIComponent(id)
      : '#/runs/' + encodeURIComponent(id);
  }
  function listHref(r) { return r.view === 'workflow' ? workflowHref(r.workflow) : '#/runs'; }

  // --- Data
  function query(extra) {
    var params = new URLSearchParams();
    if (state.status) params.set('status', state.status);
    if (state.workflow) params.set('workflow', state.workflow);
    for (var key in extra) params.set(key, extra[key]);
    return 'api/runs?' + params.toString();
  }
  function loadRuns() {
    // A step filter applies to what is loaded, so it loads a longer page.
    return api(query(state.stepFilter ? { limit: '200' } : {})).then(function (page) { state.runs = page.runs; state.next = page.next; });
  }
  function loadMore() {
    if (!state.next) return;
    api(query({ before: state.next.before, beforeId: state.next.beforeId })).then(function (page) {
      state.runs = state.runs.concat(page.runs);
      state.next = page.next;
      render();
    }, showProblem);
  }

  function refresh() {
    var r = route();
    var loads = [
      api('api/overview').then(function (overview) { state.overview = overview; }),
      api('api/workflows').then(function (body) { state.workflows = body; })
    ];
    if (r.view === 'schedules') {
      loads.push(api('api/schedules').then(function (body) { state.schedules = body.schedules; }));
    }
    if (r.view === 'workflow' || r.view === 'runs') {
      // Switching lists starts unfiltered; moving between runs keeps the filter.
      var scope = r.view + ':' + (r.workflow || '');
      if (state.scope !== scope) {
        state.scope = scope;
        state.status = null;
        state.stepFilter = null;
        state.find = '';
      }
      state.workflow = r.view === 'workflow' ? r.workflow : state.search || '';
      loads.push(loadRuns());
    }
    // Insights read a sample of runs, so they refresh at most every few seconds.
    if (r.view === 'workflow' && !r.runId) {
      var cached = state.insights && state.insights.name === r.workflow && Date.now() - state.insights.at < 4000;
      if (!cached) {
        loads.push(api('api/workflows/' + encodeURIComponent(r.workflow) + '/insights').then(function (body) {
          state.insights = { name: r.workflow, at: Date.now(), body: body };
        }));
      }
    }
    if (r.runId) {
      loads.push(api('api/runs/' + encodeURIComponent(r.runId)).then(
        function (detail) { state.detail = detail; },
        function () { state.detail = null; }
      ));
    } else {
      state.detail = null;
    }
    return Promise.all(loads).then(function () { state.problem = null; render(); }, showProblem);
  }

  function showProblem(error) {
    state.problem = 'Could not load the dashboard: ' + error.message;
    render();
  }

  // --- Views
  function render() {
    var r = route();
    renderNav(r);
    var problem = state.problem ? h('p', { class: 'problem', role: 'alert', text: state.problem }) : null;
    if (r.view === 'schedules') {
      app.replaceChildren(h('div', { class: 'page' }, [problem].concat(schedulesView()).filter(Boolean)));
    } else if (r.view === 'home') {
      app.replaceChildren(h('div', { class: 'page' }, [problem].concat(workflowsView()).filter(Boolean)));
    } else {
      app.replaceChildren(h('div', { class: 'panes' + (r.runId ? ' has-run' : '') }, [
        h('section', { class: 'pane-list', 'aria-label': 'Runs' }, [problem].concat(listPane(r)).filter(Boolean)),
        h('section', { class: 'pane-detail', 'aria-label': 'Run' }, detailPane(r))
      ]));
    }
  }

  // The sidebar: overview, every workflow with how its runs stand, then the cross-cutting views.
  function renderNav(r) {
    var nav = document.getElementById('nav');
    function item(href, label, current, tally) {
      return h('li', null, [h('a', { class: 'nav-item', href: href, 'aria-current': current ? 'page' : null }, [
        h('span', { class: 'nm', text: label, title: label }), tally || null
      ])]);
    }
    var workflows = state.workflows ? state.workflows.workflows : [];
    nav.replaceChildren(
      h('div', { class: 'nav-group' }, [h('ul', null, [item('#/', 'Overview', r.view === 'home')])]),
      h('div', { class: 'nav-group' }, [
        h('h2', { text: 'Workflows' }),
        h('ul', null, workflows.map(function (workflow) {
          var c = workflow.counts;
          var held = (c.running || 0) + (c.waiting || 0) + (c.sleeping || 0) + (c.pending || 0);
          var tally = h('span', { class: 'tally' }, [
            c.failed ? h('span', { class: 'bad', title: c.failed + ' failed' }, [lamp('failed'), String(c.failed)]) : null,
            held ? h('span', { class: 'busy', title: held + ' in progress' }, [lamp('sleeping'), String(held)]) : null
          ]);
          return item(workflowHref(workflow.name), workflow.name, r.view === 'workflow' && r.workflow === workflow.name, tally);
        }))
      ]),
      h('div', { class: 'nav-group' }, [h('ul', null, [
        item('#/runs', 'All runs', r.view === 'runs'),
        item('#/schedules', 'Schedules', r.view === 'schedules')
      ])])
    );
  }

  function countsLine(counts) {
    var parts = FILTERS.filter(function (status) { return counts[status]; }).map(function (status) {
      return h('span', { class: 'count' }, [lamp(status), h('span', { class: 'n', text: String(counts[status]) }), ' ' + STATUS_LABEL[status].toLowerCase()]);
    });
    return parts.length ? h('div', { class: 'counts' }, parts) : h('div', { class: 'counts none', text: 'No runs yet' });
  }

  function workflowEntry(name) {
    var list = state.workflows ? state.workflows.workflows : [];
    return list.filter(function (workflow) { return workflow.name === name; })[0] || null;
  }
  function outlineFor(name) {
    var entry = workflowEntry(name);
    return entry ? entry.steps : null;
  }

  // The recipe: a workflow's steps in the order every run takes them.
  function recipe(outline) {
    return h('ol', { class: 'recipe' }, outline.map(function (unit) {
      var text = unit.kind === 'parallel' ? unit.branches.join(' + ')
        : unit.kind === 'sleep' ? unit.name + ' (sleep ' + duration(unit.durationMs) + ')'
        : unit.kind === 'wait' ? unit.name + ' (wait for event' + (unit.timeoutMs ? ', up to ' + duration(unit.timeoutMs) : '') + ')'
        : unit.name;
      return h('li', { text: text });
    }));
  }

  function workflowsView() {
    var data = state.workflows;
    if (!data) return [];
    var intro = h('p', { class: 'intro', text: 'A workflow is a sequence of steps defined in code. Each time your app enqueues one, it starts a run that works through those steps on its own. Pick a workflow to see its runs.' });
    if (data.workflows.length === 0) {
      return [h('h1', { text: 'Overview' }), intro, h('div', { class: 'empty' }, [h('strong', { text: 'No runs yet' }), 'Workflows appear here once your app enqueues a run.'])];
    }
    var rows = data.workflows.map(function (workflow) {
      var total = FILTERS.reduce(function (sum, status) { return sum + (workflow.counts[status] || 0); }, 0);
      return h('li', null, [h('a', { class: 'wf', href: workflowHref(workflow.name) }, [
        h('span', { class: 'wf-name', text: workflow.name }),
        h('span', { class: 'wf-total', text: plural(total, 'run') }),
        workflow.steps ? recipe(workflow.steps) : null,
        countsLine(workflow.counts)
      ])]);
    });
    return [
      h('h1', { text: 'Overview' }),
      intro,
      h('ul', { class: 'wfs' }, rows),
      data.source === 'recent-runs'
        ? h('p', { class: 'note', text: 'Showing the workflows seen in the latest runs, without their step lists. Pass workflows to createDashboard() to list every workflow with its steps.' })
        : null
    ];
  }

  function statusFilters(counts) {
    var total = FILTERS.reduce(function (sum, status) { return sum + (counts[status] || 0); }, 0);
    function filter(status, n) {
      return h('li', null, [h('button', {
        class: 's-' + status + (n === 0 ? ' zero' : ''),
        'aria-pressed': String((state.status || 'all') === status),
        onclick: function () { state.status = status === 'all' ? null : status; reloadList(); }
      }, [
        status === 'all' ? null : lamp(status),
        h('span', { class: 'n', text: String(n) }),
        h('span', { text: STATUS_LABEL[status] })
      ])]);
    }
    var shown = FILTERS.filter(function (status) { return counts[status] || state.status === status; });
    return h('ul', { class: 'filters', 'aria-label': 'Filter by status' },
      [filter('all', total)].concat(shown.map(function (status) { return filter(status, counts[status] || 0); })));
  }

  var searchTimer = null;
  function listPane(r) {
    var head;
    if (r.view === 'workflow') {
      var entry = workflowEntry(r.workflow);
      head = [
        h('h1', { text: r.workflow }),
        entry ? statusFilters(entry.counts) : null
      ];
    } else {
      head = [
        h('h1', { text: 'All runs' }),
        statusFilters(state.overview ? state.overview.counts : {})
      ];
    }
    return head.concat([
      h('input', {
        class: 'search', type: 'search',
        placeholder: r.view === 'workflow' ? 'Find a run by name' : 'Filter by workflow name',
        'aria-label': r.view === 'workflow' ? 'Find a run by name' : 'Filter by workflow name',
        value: r.view === 'workflow' ? state.find : state.search || '',
        oninput: function (event) {
          var value = event.target.value.trim();
          clearTimeout(searchTimer);
          searchTimer = setTimeout(function () {
            if (r.view === 'workflow') { state.find = value; render(); }
            else { state.search = value; state.workflow = value; reloadList(); }
          }, 200);
        }
      }),
      state.stepFilter ? h('p', { class: 'scope' }, [
        'Failed at ' + state.stepFilter + ' ',
        h('button', { class: 'linkish', onclick: function () { state.stepFilter = null; state.status = null; reloadList(); }, text: 'Show all' })
      ]) : null,
      runRows(r, r.view !== 'workflow')
    ]);
  }

  // With no run picked, the workflow's picture: where runs get to, why they
  // fail, and how busy it has been.
  function detailPane(r) {
    if (r.runId) return runView(r);
    if (r.view !== 'workflow') {
      return [h('div', { class: 'placeholder' }, [h('strong', { text: 'Pick a run' }), 'Its steps, attempts, input, and output show here.'])];
    }
    var insights = state.insights && state.insights.name === r.workflow ? state.insights.body : null;
    var entry = workflowEntry(r.workflow);
    var out = [
      h('div', { class: 'detail-head' }, [h('div', null, [
        h('p', { class: 'kicker', text: 'Workflow' }),
        h('h1', { class: 'detail-title', text: r.workflow })
      ])]),
      entry ? h('p', { class: 'intro', text: 'A recipe of ' + plural(entry.steps ? entry.steps.length : 0, 'step') + '. Every run takes them in order, on its own; pick a run on the left to follow one.' }) : null
    ];
    if (!insights) return out;
    var sampleNote = insights.complete
      ? 'From all ' + plural(insights.sample, 'run') + '.'
      : 'From the latest ' + insights.sample + ' runs, since ' + clock(insights.since) + '.';
    out.push(h('h2', { text: 'Where runs get to' }), pipelineView(insights, entry), h('p', { class: 'note', text: sampleNote }));
    out.push(h('h2', { text: 'Why runs fail' }), failuresView(insights));
    out.push(h('h2', { text: 'Last hour' }), activityView(insights.activity));
    return out;
  }

  function pipelineView(insights, entry) {
    var outline = entry && entry.steps;
    return h('ol', { class: 'pipeline' }, insights.pipeline.map(function (stage, index) {
      var unit = outline ? outline[index] : null;
      var kind = unit ? (unit.kind === 'sleep' ? 'Sleep' : unit.kind === 'wait' ? 'Wait for event' : unit.kind === 'parallel' ? 'In parallel' : 'Step') : 'Step';
      var share = insights.sample ? stage.reached / insights.sample : 0;
      var bar = h('span', { class: 'reach' }, [h('span', { class: 'fill' })]);
      bar.firstChild.style.width = Math.round(share * 100) + '%';
      return h('li', { class: 'stage' }, [
        h('span', { class: 'kind', text: (index + 1) + '  ' + kind }),
        h('span', { class: 'stage-name', text: stage.name }),
        bar,
        h('span', { class: 'reached' }, [h('span', { class: 'n', text: String(stage.reached) }), ' reached']),
        h('ul', { class: 'outcomes' }, [
          stage.completed ? h('li', null, [lamp('completed'), stage.completed + ' done']) : null,
          stage.here ? h('li', { class: 'held' }, [lamp(stage.retrying ? 'retrying' : 'sleeping'), stage.here + (stage.retrying ? ' here, retrying' : ' here now')]) : null,
          stage.failed ? h('li', null, [h('button', {
            class: 'linkish bad', onclick: function () { filterFailedAt(stage.name); }
          }, [lamp('failed'), stage.failed + ' failed'])]) : null,
          stage.skipped ? h('li', null, [lamp('skipped'), stage.skipped + ' skipped']) : null
        ])
      ]);
    }));
  }

  function failuresView(insights) {
    if (insights.failures.length === 0) return h('p', { class: 'note', text: 'None of these runs failed.' });
    return h('ul', { class: 'failures' }, insights.failures.map(function (group) {
      return h('li', null, [h('button', { class: 'failure', onclick: function () { filterFailedAt(group.stepName); } }, [
        h('span', { class: 'count', text: String(group.count) }),
        h('span', { class: 'msg' }, [
          h('span', { class: 'what', text: group.message || 'Failed without a message' }),
          h('span', { class: 'where', text: 'at ' + group.stepName + (group.attempts > 1 ? ', after ' + plural(group.attempts, 'attempt') : '') + (group.code ? ' (' + group.code + ')' : '') })
        ]),
        h('span', { class: 'last' }, ['last ', agoEl(group.lastAt)])
      ])]);
    }));
  }

  // One column a minute: started runs as the column, failures in red at its foot.
  function activityView(activity) {
    var peak = Math.max.apply(null, activity.started.concat(activity.completed, activity.failed, [1]));
    var total = function (list) { return list.reduce(function (sum, n) { return sum + n; }, 0); };
    var columns = activity.started.map(function (started, minute) {
      var failed = activity.failed[minute];
      var done = activity.completed[minute];
      var col = h('span', { class: 'col', title: clock(activity.start + minute * activity.bucketMs) + ': ' + started + ' started, ' + done + ' completed, ' + failed + ' failed' });
      var bar = h('span', { class: 'bar' });
      bar.style.height = Math.round(100 * Math.max(started, done + failed) / peak) + '%';
      var bad = h('span', { class: 'bad' });
      bad.style.height = Math.max(started, done + failed) ? Math.round(100 * failed / Math.max(started, done + failed)) + '%' : '0';
      bar.appendChild(bad);
      col.appendChild(bar);
      return col;
    });
    return h('div', { class: 'activity' }, [
      h('div', { class: 'cols', role: 'img', 'aria-label': total(activity.started) + ' runs started, ' + total(activity.completed) + ' completed, and ' + total(activity.failed) + ' failed in the last hour' }, columns),
      h('div', { class: 'axis' }, [h('span', { text: '60 min ago' }), h('span', { text: 'now' })]),
      h('p', { class: 'legend' }, [
        h('span', null, [h('span', { class: 'n', text: String(total(activity.started)) }), ' started']),
        h('span', null, [lamp('completed'), h('span', { class: 'n', text: String(total(activity.completed)) }), ' completed']),
        h('span', null, [lamp('failed'), h('span', { class: 'n', text: String(total(activity.failed)) }), ' failed'])
      ])
    ]);
  }

  function filterFailedAt(stepName) {
    state.status = 'failed';
    state.stepFilter = stepName;
    reloadList();
  }

  function reloadList() {
    loadRuns().then(render, showProblem);
  }

  // Runs whose status changed since the last render flash once, so a change
  // arriving live is noticed without being hunted for.
  var seenStatus = {};
  function runRows(r, showWorkflow) {
    var runs = state.runs.filter(function (run) {
      if (state.stepFilter && !(run.error && run.error.stepName === state.stepFilter)) return false;
      if (r.view === 'workflow' && state.find) {
        var hay = (run.label || '') + ' ' + run.id;
        if (hay.toLowerCase().indexOf(state.find.toLowerCase()) < 0) return false;
      }
      return true;
    });
    if (runs.length === 0) {
      return h('div', { class: 'empty' }, [
        h('strong', { text: state.find ? 'No run matches "' + state.find + '"' : state.status ? 'No ' + STATUS_LABEL[state.status].toLowerCase() + ' runs' : 'No runs yet' }),
        state.find || state.status ? 'Clear the search or choose All.' : 'Runs appear here as soon as your app enqueues one.'
      ]);
    }
    var rows = h('ul', { class: 'rows' }, runs.map(function (run) {
      var title = showWorkflow ? run.workflow : run.label || 'Run ' + shortId(run.id);
      var changed = seenStatus[run.id] && seenStatus[run.id] !== run.status;
      seenStatus[run.id] = run.status;
      return h('li', null, [h('a', {
        class: 'row' + (changed ? ' changed' : ''), href: runHref(r, run.id), 'aria-current': String(run.id === r.runId)
      }, [
        lamp(run.status),
        h('span', { class: 'name', text: title, title: run.label || run.id }),
        h('span', { class: 'why', text: (showWorkflow && run.label ? run.label + ': ' : '') + (why(run) || STATUS_LABEL[run.status]) }),
        h('span', { class: 'when' }, [agoEl(run.createdAt)])
      ])]);
    }));
    return h('div', null, [rows, state.next ? h('button', { class: 'more', onclick: loadMore, text: 'Load older runs' }) : null]);
  }

  function runView(r) {
    var detail = state.detail;
    var back = h('a', { class: 'back', href: listHref(r), text: r.view === 'workflow' ? 'All runs of ' + r.workflow : 'All runs' });
    if (!detail) {
      return [back, h('h1', { class: 'detail-title', text: 'Run not found' }), h('p', { text: 'It may have been pruned, or the link is wrong.' })];
    }
    var run = detail.run;
    var steps = detail.steps;
    var actions = [];
    if (state.overview && state.overview.actions) {
      if (!TERMINAL[run.status]) actions.push(h('button', { class: 'btn danger', onclick: function () { act(run, 'cancel'); }, text: 'Cancel run' }));
      if (run.status === 'failed' || run.status === 'cancelled') actions.push(h('button', { class: 'btn', onclick: function () { act(run, 'resume'); }, text: 'Resume run' }));
    }
    actions.push(h('a', { class: 'close', href: listHref(r), 'aria-label': 'Close run', title: 'Close (Esc)', text: '×' }));
    var copy = h('button', { class: 'linkish copy', title: 'Copy run id', onclick: function () {
      if (navigator.clipboard) navigator.clipboard.writeText(run.id).then(function () { copy.textContent = 'Copied'; setTimeout(function () { copy.textContent = 'Copy id'; }, 1200); });
    }, text: 'Copy id' });
    return [
      back,
      h('div', { class: 'detail-head' }, [
        h('div', null, [
          h('p', { class: 'kicker' }, [h('a', { href: workflowHref(run.workflow), text: run.workflow }), ' run']),
          h('h1', { class: 'detail-title', text: detail.label || 'Run ' + shortId(run.id) }),
          h('span', { class: 'runid' }, [h('span', { text: run.id }), copy])
        ]),
        h('div', { class: 'actions' }, actions)
      ]),
      statement(run, steps),
      facts(run),
      h('h2', { text: 'Steps of this run' }),
      stepsTable(run, steps, detail.outline),
      h('div', { class: 'pair' }, [
        h('div', null, [h('h2', { text: 'Input' }), h('pre', { class: 'json', text: json(run.input) })]),
        run.status === 'completed' ? h('div', null, [h('h2', { text: 'Output' }), h('pre', { class: 'json', text: json(run.output) })]) : null
      ])
    ];
  }

  // Relative times keep counting between refreshes.
  function agoEl(ts) {
    return h('time', { 'data-ago': String(ts), datetime: new Date(ts).toISOString(), title: absolute(ts) }, [ago(ts)]);
  }
  setInterval(function () {
    document.querySelectorAll('[data-ago]').forEach(function (el) { el.textContent = ago(Number(el.getAttribute('data-ago'))); });
  }, 1000);

  // Up and down move through the runs beside the one being looked at.
  document.addEventListener('keydown', function (event) {
    if (event.target && /^(INPUT|TEXTAREA|SELECT)$/.test(event.target.tagName)) return;
    var r = route();
    if (r.view !== 'workflow' && r.view !== 'runs') return;
    if (event.key === 'Escape' && r.runId) {
      location.hash = listHref(r);
      return;
    }
    if (event.key !== 'ArrowDown' && event.key !== 'ArrowUp') return;
    var ids = state.runs.map(function (run) { return run.id; });
    if (ids.length === 0) return;
    var index = r.runId ? ids.indexOf(r.runId) : -1;
    var next = event.key === 'ArrowDown' ? Math.min(ids.length - 1, index + 1) : Math.max(0, index - 1);
    if (next === index) return;
    event.preventDefault();
    location.hash = runHref(r, ids[next]);
  });

  // Where the run stands, as one sentence beside its lamp.
  function statement(run, steps) {
    var retrying = steps.filter(function (step) { return step.status === 'retrying'; })[0];
    var failedStep = run.error ? steps.filter(function (step) { return step.name === run.error.stepName; })[0] : null;
    var head = STATUS_LABEL[run.status];
    var reason = '';
    if (run.status === 'failed' && run.error) {
      head = 'Failed at ' + run.error.stepName + (failedStep && failedStep.attempts > 1 ? ' after ' + plural(failedStep.attempts, 'attempt') : '');
      reason = run.error.message + (run.error.code ? ' (' + run.error.code + ')' : '');
    } else if (run.status === 'sleeping' && retrying && run.wakeAt) {
      head = 'Retrying ' + retrying.name + ' ' + ago(run.wakeAt);
      reason = plural(retrying.attempts, 'attempt') + ' so far' + (retrying.error ? '; last error: ' + retrying.error : '');
    } else if (run.status === 'waiting') {
      head = 'Waiting for ' + (run.waitingFor || 'an event');
      reason = run.wakeAt ? 'Times out at ' + clock(run.wakeAt) : 'No timeout';
    } else if (run.status === 'sleeping' && run.wakeAt) {
      head = 'Sleeping until ' + clock(run.wakeAt) + ' (' + ago(run.wakeAt) + ')';
    } else if (run.status === 'pending') {
      head = run.startedAt ? 'Queued to carry on' : 'Queued';
    } else if (run.status === 'running') {
      head = 'Running';
    }
    return h('p', { class: 'state' }, [lamp(run.status), h('span', null, [head + (reason ? ':' : ''), reason ? h('span', { class: 'reason', text: ' ' + reason }) : null])]);
  }

  function facts(run) {
    var items = [['Enqueued', time(run.createdAt, false)]];
    if (run.startedAt) items.push([run.finishedAt ? 'Ran' : 'Elapsed', duration((run.finishedAt || Date.now()) - run.startedAt)]);
    if (run.finishedAt) items.push(['Finished', time(run.finishedAt, false)]);
    return h('dl', { class: 'facts' }, items.map(function (item) {
      return h('div', null, [h('dt', { text: item[0] }), h('dd', null, [item[1]])]);
    }));
  }

  function act(run, action) {
    var verb = action === 'cancel' ? 'Cancel' : 'Resume';
    if (!window.confirm(verb + ' this ' + run.workflow + ' run?')) return;
    api('api/runs/' + encodeURIComponent(run.id) + '/' + action, { method: 'POST', headers: { 'x-reflow-dashboard': '1' } })
      .then(function (result) {
        if (!result.done) state.problem = verb + ' had no effect: the run has already moved on.';
        return refresh();
      }, showProblem);
  }

  function schedulesView() {
    var schedules = state.schedules || [];
    var head = [h('h1', { text: 'Schedules' }), h('p', { class: 'intro', text: 'Schedules start a new run of a workflow on a timetable.' })];
    if (schedules.length === 0) {
      return head.concat([h('div', { class: 'empty' }, [
        h('strong', { text: 'No schedules registered' }),
        'Schedules registered with engine.schedule() appear here.'
      ])]);
    }
    return head.concat([h('table', { class: 'sched' }, [
      h('thead', null, [h('tr', null, [h('th', { text: 'Workflow' }), h('th', { text: 'Repeats' }), h('th', { text: 'Next run' }), h('th', { text: 'Key' })])]),
      h('tbody', null, schedules.map(function (schedule) {
        var repeats = schedule.recurrence.kind === 'cron'
          ? h('code', { text: schedule.recurrence.expression + ' (UTC)' })
          : 'Every ' + duration(schedule.recurrence.intervalMs);
        return h('tr', null, [
          h('td', null, [h('a', { href: workflowHref(schedule.workflow), text: schedule.workflow })]),
          h('td', null, [repeats]),
          h('td', null, [time(schedule.nextRunAt, false), ' (' + ago(schedule.nextRunAt) + ')']),
          h('td', null, [h('code', { text: schedule.key })])
        ]);
      }))
    ])]);
  }

  // --- Updates: live from the engine when available, otherwise polling.
  var pending = null;
  function soon() {
    if (pending) return;
    pending = setTimeout(function () { pending = null; refresh(); }, 300);
  }

  function connect() {
    if (!state.overview || !state.overview.live || !window.EventSource) {
      setInterval(function () { if (document.visibilityState === 'visible') refresh(); }, 3000);
      return;
    }
    var source = new EventSource('api/live');
    source.onopen = function () { liveBadge.hidden = false; };
    source.onerror = function () { liveBadge.hidden = true; };
    source.onmessage = soon;
    // A safety net: if the stream goes quiet without closing — an engine that
    // stopped, a proxy holding the connection — the view still catches up.
    setInterval(function () { if (document.visibilityState === 'visible') refresh(); }, 30000);
  }

  // The current view stays up until the next one has loaded, so moving
  // between runs swaps the detail in place rather than blanking it.
  window.addEventListener('hashchange', refresh);
  refresh().then(connect);
})();
`

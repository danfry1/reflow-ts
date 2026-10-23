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
<header class="bar">
  <a class="brand" href="#/">${MARK_SVG}<span>${escapeHtml(title)}</span></a>
  <nav class="views" aria-label="Views">
    <a href="#/" data-view="runs">Runs</a>
    <a href="#/schedules" data-view="schedules">Schedules</a>
  </nav>
  <span class="live" id="live" hidden>Live</span>
</header>
<main id="app" aria-live="polite"></main>
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
  --line: #DCE1EA;
  --focus: #2563EB;
  --proceed: #23825A;
  --running: #2563EB;
  --hold: #C98A0B;
  --stop: #D2463C;
  --withdrawn: #6B7488;
  --pending: #8A93A8;
  --stop-wash: #FCEDEB;
  --hold-wash: #FBF3E1;
  --mono: ui-monospace, "SF Mono", "Cascadia Code", Menlo, Consolas, monospace;
  --sans: system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", sans-serif;
  color-scheme: light;
}
@media (prefers-color-scheme: dark) {
  :root {
    --canvas: #0E1626;
    --surface: #152036;
    --ink: #E6EAF2;
    --ink-soft: #A9B3C7;
    --ink-faint: #6E7A93;
    --line: #26334D;
    --focus: #7AA7FF;
    --proceed: #4CC08A;
    --running: #6E9BFF;
    --hold: #E8A317;
    --stop: #F06A5F;
    --withdrawn: #8D97AC;
    --pending: #6E7A93;
    --stop-wash: #3A1E24;
    --hold-wash: #33291A;
    color-scheme: dark;
  }
}
* { box-sizing: border-box; }
html { background: var(--canvas); }
body { margin: 0; font: 15px/1.5 var(--sans); color: var(--ink); background: var(--canvas); font-variant-numeric: tabular-nums; }
a { color: inherit; }
:focus-visible { outline: 2px solid var(--focus); outline-offset: 2px; border-radius: 4px; }
button { font: inherit; color: inherit; }

.bar { display: flex; align-items: center; gap: 24px; padding: 12px 24px; border-bottom: 1px solid var(--line); background: var(--surface); position: sticky; top: 0; z-index: 2; }
.brand { display: flex; align-items: center; gap: 10px; text-decoration: none; font-weight: 650; font-size: 16px; letter-spacing: -0.01em; color: var(--ink); }
.mark { width: 26px; height: 26px; flex: none; }
.views { display: flex; gap: 4px; }
.views a { text-decoration: none; padding: 6px 10px; border-radius: 6px; color: var(--ink-soft); }
.views a[aria-current="page"] { color: var(--ink); background: var(--canvas); font-weight: 600; }
.live { margin-left: auto; display: flex; align-items: center; gap: 6px; font-size: 13px; color: var(--ink-soft); }
.live[hidden] { display: none; }
.live::before { content: ""; width: 8px; height: 8px; border-radius: 50%; background: var(--proceed); }

main { padding: 20px 24px 48px; max-width: 1440px; margin: 0 auto; }

.signals { display: flex; flex-wrap: wrap; gap: 6px; margin: 0 0 16px; padding: 0; list-style: none; }
.signal { display: flex; align-items: baseline; gap: 8px; padding: 7px 12px; border: 1px solid var(--line); border-radius: 8px; background: var(--surface); cursor: pointer; }
.signal .n { font-weight: 650; font-size: 17px; }
.signal .label { color: var(--ink-soft); font-size: 13px; }
.signal[aria-pressed="true"] { border-color: currentColor; box-shadow: inset 0 0 0 1px currentColor; }
.signal[aria-pressed="true"] .label { color: inherit; }
.signal.zero .n { color: var(--ink-faint); }

.c-completed { color: var(--proceed); }
.c-running { color: var(--running); }
.c-sleeping, .c-waiting, .c-retrying { color: var(--hold); }
.c-failed { color: var(--stop); }
.c-cancelled { color: var(--withdrawn); }
.c-pending, .c-skipped { color: var(--pending); }
.c-all { color: var(--ink); }

.layout { display: grid; grid-template-columns: minmax(0, 1fr); gap: 20px; }
.layout.open { grid-template-columns: minmax(0, 5fr) minmax(0, 7fr); }
@media (max-width: 980px) { .layout.open { grid-template-columns: minmax(0, 1fr); } .layout.open .list { display: none; } }

.toolbar { display: flex; gap: 8px; margin-bottom: 10px; }
.toolbar input { flex: 1; min-width: 0; padding: 7px 10px; border: 1px solid var(--line); border-radius: 6px; background: var(--surface); color: var(--ink); font: inherit; }

.panel { background: var(--surface); border: 1px solid var(--line); border-radius: 10px; }
.rows { list-style: none; margin: 0; padding: 0; }
.row { display: grid; grid-template-columns: 14px minmax(0, 1fr) auto; gap: 4px 12px; align-items: center; padding: 10px 14px; border-top: 1px solid var(--line); text-decoration: none; }
.rows li:first-child .row { border-top: 0; }
.row:hover, .row[aria-current="true"] { background: var(--canvas); }
.row .dot { width: 10px; height: 10px; border-radius: 50%; background: currentColor; }
.row .dot.hollow { background: transparent; box-shadow: inset 0 0 0 2px currentColor; }
.row .wf { font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.row .when { color: var(--ink-soft); font-size: 13px; text-align: right; white-space: nowrap; }
.row .sub { grid-column: 2 / 4; display: flex; gap: 12px; font-size: 13px; color: var(--ink-soft); min-width: 0; }
.row .sub .id { font-family: var(--mono); font-size: 12px; color: var(--ink-faint); }
.row .sub .why { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.more { display: block; width: 100%; padding: 10px; border: 0; border-top: 1px solid var(--line); background: none; cursor: pointer; color: var(--ink-soft); border-radius: 0 0 10px 10px; }
.more:hover { background: var(--canvas); color: var(--ink); }

.empty { padding: 32px 16px; text-align: center; color: var(--ink-soft); }
.empty strong { display: block; color: var(--ink); margin-bottom: 4px; }

.detail { padding: 20px 22px 24px; }
.detail-head { display: flex; align-items: flex-start; gap: 12px; flex-wrap: wrap; }
.detail-head h1 { margin: 0; font-size: 22px; line-height: 1.25; letter-spacing: -0.015em; font-weight: 650; }
.detail-head .runid { display: block; font: 12px var(--mono); color: var(--ink-faint); margin-top: 2px; word-break: break-all; }
.badge { display: inline-flex; align-items: center; gap: 6px; padding: 3px 10px; border-radius: 999px; font-size: 13px; font-weight: 600; border: 1px solid currentColor; }
.actions { margin-left: auto; display: flex; gap: 8px; }
.btn { padding: 6px 12px; border-radius: 6px; border: 1px solid var(--line); background: var(--surface); cursor: pointer; }
.btn:hover { border-color: var(--ink-soft); }
.btn.danger { color: var(--stop); }
.close { border: 0; background: none; cursor: pointer; color: var(--ink-soft); padding: 4px 6px; font-size: 20px; line-height: 1; }

.facts { display: grid; grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); gap: 12px 20px; margin: 18px 0; }
.facts dt { font-size: 12px; color: var(--ink-soft); }
.facts dd { margin: 0; font-weight: 550; }

.notice { margin: 0 0 18px; padding: 12px 14px; border-radius: 8px; border-left: 3px solid currentColor; }
.notice p { margin: 0; color: var(--ink); }
.notice .code { font: 12px var(--mono); color: var(--ink-soft); margin-top: 4px; }
.notice.stop { background: var(--stop-wash); color: var(--stop); }
.notice.hold { background: var(--hold-wash); color: var(--hold); }

h2 { font-size: 15px; margin: 24px 0 10px; font-weight: 650; }

/* The flow lane: one run, drawn as the signal diagram of its steps. */
.lane { display: flex; align-items: center; height: 34px; gap: 3px; margin: 6px 0 4px; }
.seg { position: relative; height: 12px; border-radius: 3px; min-width: 6px; background: currentColor; }
.seg.queued { height: 2px; background: none; border-top: 2px dotted var(--line); }
.seg.hold { background: none; height: 0; border-top: 3px dashed currentColor; }
.seg.skipped { height: 4px; opacity: 0.5; }
.seg.failed::after { content: ""; position: absolute; right: -5px; top: -6px; width: 4px; height: 24px; border-radius: 2px; background: currentColor; }
.seg-group { display: flex; flex-direction: column; gap: 2px; min-width: 6px; }
.seg.thin { height: 4px; min-width: 4px; }
.seg .tick { position: absolute; top: -4px; width: 2px; height: 20px; background: var(--surface); }
.seg.live-now { animation: breathe 1.6s ease-in-out infinite; }
@keyframes breathe { 50% { opacity: 0.45; } }
@media (prefers-reduced-motion: reduce) { .seg.live-now { animation: none; } }
.lane-scale { display: flex; justify-content: space-between; font-size: 12px; color: var(--ink-faint); }

.steps { width: 100%; border-collapse: collapse; font-size: 14px; }
.steps th { text-align: left; font-weight: 500; font-size: 12px; color: var(--ink-soft); padding: 6px 8px; border-bottom: 1px solid var(--line); }
.steps td { padding: 8px; border-bottom: 1px solid var(--line); vertical-align: top; }
.steps tr:last-child td { border-bottom: 0; }
.steps .name { font-weight: 600; }
.steps .num { text-align: right; white-space: nowrap; }
.steps .err { display: block; color: var(--stop); font-size: 13px; margin-top: 2px; overflow-wrap: anywhere; }

pre.json { margin: 0; padding: 12px 14px; background: var(--canvas); border-radius: 8px; overflow: auto; max-height: 320px; font: 12.5px/1.55 var(--mono); }
.pair { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 12px; }
@media (max-width: 640px) { .pair { grid-template-columns: minmax(0, 1fr); } .bar { padding: 10px 16px; gap: 12px; } main { padding: 16px 16px 40px; } }

.sched { width: 100%; border-collapse: collapse; }
.sched th, .sched td { text-align: left; padding: 10px 14px; border-bottom: 1px solid var(--line); }
.sched th { font-weight: 500; font-size: 12px; color: var(--ink-soft); }
.sched tr:last-child td { border-bottom: 0; }
.sched code { font: 12.5px var(--mono); }
.problem { padding: 12px 14px; border-radius: 8px; background: var(--stop-wash); color: var(--stop); margin-bottom: 16px; }
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

  var STATUS_ORDER = ['running', 'waiting', 'sleeping', 'pending', 'failed', 'completed', 'cancelled'];
  var STATUS_LABEL = {
    all: 'All runs', running: 'Running', waiting: 'Waiting', sleeping: 'Sleeping', pending: 'Queued',
    failed: 'Failed', completed: 'Completed', cancelled: 'Cancelled',
    retrying: 'Retrying', skipped: 'Skipped', 'completed-early': 'Completed early'
  };
  var TERMINAL = { completed: true, failed: true, cancelled: true };
  var HOLDS = { sleeping: true, waiting: true, retrying: true };

  var app = document.getElementById('app');
  var liveBadge = document.getElementById('live');
  var state = { overview: null, status: null, workflow: '', runs: [], next: null, selected: null, detail: null, problem: null };

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
    if (m < 60) return m + 'm ' + rs + 's';
    var hr = Math.floor(m / 60);
    if (hr < 48) return hr + 'h ' + (m % 60) + 'm';
    return Math.floor(hr / 24) + 'd ' + (hr % 24) + 'h';
  }
  function ago(ts) {
    if (!ts) return '';
    var d = Date.now() - ts;
    if (d < 0) return 'in ' + duration(-d);
    if (d < 5000) return 'just now';
    return duration(d).split(' ')[0] + ' ago';
  }
  function absolute(ts) { return ts ? new Date(ts).toLocaleString() : ''; }
  function time(ts, relative) {
    return h('time', { datetime: ts ? new Date(ts).toISOString() : null, title: absolute(ts) }, [relative ? ago(ts) : absolute(ts)]);
  }
  function shortId(id) { return id.length > 12 ? id.slice(0, 8) : id; }
  function json(value) { return value === undefined ? 'undefined' : JSON.stringify(value, null, 2); }

  function runElapsed(run) {
    if (!run.startedAt) return null;
    return (run.finishedAt || Date.now()) - run.startedAt;
  }

  // One line on why a run is where it is — the question the list answers.
  function why(run) {
    if (run.status === 'failed' && run.error) return run.error.stepName + ': ' + run.error.message;
    if (run.status === 'waiting') return 'Waiting for ' + (run.waitingFor || 'an event') + (run.wakeAt ? ', times out ' + ago(run.wakeAt) : '');
    if (run.status === 'sleeping' && run.wakeAt) return 'Resumes ' + ago(run.wakeAt);
    if (run.status === 'pending') return run.startedAt ? 'Queued to resume' : 'Queued';
    if (run.status === 'running') return 'Running for ' + duration(runElapsed(run));
    if (run.status === 'cancelled') return 'Cancelled' + (run.finishedAt ? ' ' + ago(run.finishedAt) : '');
    if (run.finishedAt && run.startedAt) return 'Took ' + duration(run.finishedAt - run.startedAt);
    return '';
  }

  // --- Routing
  function route() {
    var hash = location.hash.replace(/^#/, '') || '/';
    var match = hash.match(/^\/runs\/(.+)$/);
    if (hash === '/schedules') return { view: 'schedules' };
    var runId = null;
    if (match) {
      try { runId = decodeURIComponent(match[1]); } catch (error) { runId = match[1]; }
    }
    return { view: 'runs', runId: runId };
  }

  function markView(view) {
    document.querySelectorAll('.views a').forEach(function (link) {
      if (link.getAttribute('data-view') === view) link.setAttribute('aria-current', 'page');
      else link.removeAttribute('aria-current');
    });
  }

  // --- Data
  function loadOverview() {
    return api('api/overview').then(function (overview) { state.overview = overview; });
  }
  function query(extra) {
    var params = new URLSearchParams();
    if (state.status) params.set('status', state.status);
    if (state.workflow) params.set('workflow', state.workflow);
    for (var key in extra) params.set(key, extra[key]);
    return 'api/runs?' + params.toString();
  }
  function loadRuns() {
    return api(query({})).then(function (page) { state.runs = page.runs; state.next = page.next; });
  }
  function loadMore() {
    if (!state.next) return;
    api(query({ before: state.next.before, beforeId: state.next.beforeId })).then(function (page) {
      state.runs = state.runs.concat(page.runs);
      state.next = page.next;
      render();
    }, showProblem);
  }
  function loadDetail(id) {
    return api('api/runs/' + encodeURIComponent(id)).then(
      function (detail) { state.detail = detail; },
      function () { state.detail = null; }
    );
  }

  function refresh() {
    var r = route();
    markView(r.view);
    if (r.view === 'schedules') {
      return api('api/schedules').then(function (body) { state.problem = null; renderSchedules(body.schedules); }, showProblem);
    }
    state.selected = r.runId;
    return Promise.all([loadOverview(), loadRuns(), r.runId ? loadDetail(r.runId) : null]).then(
      function () { state.problem = null; render(); },
      showProblem
    );
  }

  function showProblem(error) {
    state.problem = 'Could not load the dashboard: ' + error.message;
    render();
  }

  // --- Views
  function render() {
    var r = route();
    if (r.view !== 'runs') return;
    app.replaceChildren.apply(app, [
      state.problem ? h('p', { class: 'problem', role: 'alert', text: state.problem }) : null,
      state.overview ? signals() : null,
      h('div', { class: 'layout' + (state.selected ? ' open' : '') }, [
        h('section', { class: 'list', 'aria-label': 'Runs' }, [toolbar(), runList()]),
        state.selected ? detailPanel() : null
      ])
    ].filter(Boolean));
  }

  function signals() {
    var counts = state.overview.counts;
    var total = STATUS_ORDER.reduce(function (sum, status) { return sum + (counts[status] || 0); }, 0);
    function chip(status, n) {
      return h('li', null, [h('button', {
        class: 'signal c-' + status + (n === 0 ? ' zero' : ''),
        'aria-pressed': String((state.status || 'all') === status),
        onclick: function () { state.status = status === 'all' ? null : status; reloadList(); }
      }, [h('span', { class: 'n', text: String(n) }), h('span', { class: 'label', text: STATUS_LABEL[status] })])]);
    }
    return h('ul', { class: 'signals', 'aria-label': 'Filter by status' },
      [chip('all', total)].concat(STATUS_ORDER.map(function (status) { return chip(status, counts[status] || 0); })));
  }

  var filterTimer = null;
  function toolbar() {
    return h('div', { class: 'toolbar' }, [h('input', {
      type: 'search', placeholder: 'Filter by workflow name', 'aria-label': 'Filter by workflow name', value: state.workflow,
      oninput: function (event) {
        var value = event.target.value.trim();
        clearTimeout(filterTimer);
        filterTimer = setTimeout(function () { state.workflow = value; reloadList(); }, 250);
      }
    })]);
  }

  function reloadList() {
    loadRuns().then(render, showProblem);
  }

  function runList() {
    if (state.runs.length === 0) {
      var filtered = state.status || state.workflow;
      return h('div', { class: 'panel' }, [h('div', { class: 'empty' }, [
        h('strong', { text: filtered ? 'No runs match these filters' : 'No runs yet' }),
        filtered ? 'Clear the status or workflow filter to see every run.' : 'Runs appear here as soon as your app enqueues one.'
      ])]);
    }
    return h('div', { class: 'panel' }, [
      h('ul', { class: 'rows' }, state.runs.map(function (run) {
        return h('li', null, [h('a', {
          class: 'row',
          href: '#/runs/' + encodeURIComponent(run.id),
          'aria-current': String(run.id === state.selected)
        }, [
          h('span', { class: 'dot c-' + run.status + (HOLDS[run.status] || run.status === 'pending' ? ' hollow' : ''), title: STATUS_LABEL[run.status] }),
          h('span', { class: 'wf', text: run.workflow }),
          h('span', { class: 'when' }, [time(run.createdAt, true)]),
          h('span', { class: 'sub' }, [h('span', { class: 'id', text: shortId(run.id) }), h('span', { class: 'why', text: why(run) })])
        ])]);
      })),
      state.next ? h('button', { class: 'more', onclick: loadMore, text: 'Load older runs' }) : null
    ]);
  }

  function detailPanel() {
    var detail = state.detail;
    var close = h('button', { class: 'close', 'aria-label': 'Close run', onclick: function () { location.hash = '#/'; }, text: '×' });
    if (!detail) {
      return h('section', { class: 'panel detail', 'aria-label': 'Run' }, [
        h('div', { class: 'detail-head' }, [h('h1', { text: 'Run not found' }), h('div', { class: 'actions' }, [close])]),
        h('p', { text: 'It may have been pruned, or the link is wrong.' })
      ]);
    }
    var run = detail.run;
    var steps = detail.steps;
    var actions = [];
    if (state.overview && state.overview.actions) {
      if (!TERMINAL[run.status]) actions.push(h('button', { class: 'btn danger', onclick: function () { act(run, 'cancel'); }, text: 'Cancel run' }));
      if (run.status === 'failed' || run.status === 'cancelled') actions.push(h('button', { class: 'btn', onclick: function () { act(run, 'resume'); }, text: 'Resume run' }));
    }
    actions.push(close);

    return h('section', { class: 'panel detail', 'aria-label': 'Run ' + run.id }, [
      h('div', { class: 'detail-head' }, [
        h('div', null, [h('h1', { text: run.workflow }), h('span', { class: 'runid', text: run.id })]),
        h('span', { class: 'badge c-' + run.status, text: STATUS_LABEL[run.status] }),
        h('div', { class: 'actions' }, actions)
      ]),
      facts(run, steps),
      notice(run, steps),
      h('h2', { text: 'Flow' }),
      lane(run, steps),
      h('h2', { text: 'Steps' }),
      stepTable(steps),
      h('div', { class: 'pair' }, [
        h('div', null, [h('h2', { text: 'Input' }), h('pre', { class: 'json', text: json(run.input) })]),
        run.status === 'completed' ? h('div', null, [h('h2', { text: 'Output' }), h('pre', { class: 'json', text: json(run.output) })]) : null
      ])
    ]);
  }

  function facts(run, steps) {
    var items = [
      ['Enqueued', time(run.createdAt, false)],
      ['Started', run.startedAt ? time(run.startedAt, false) : 'Not yet'],
      ['Finished', run.finishedAt ? time(run.finishedAt, false) : 'Not yet'],
      [run.finishedAt ? 'Took' : 'Elapsed', run.startedAt ? duration(runElapsed(run)) : '—']
    ];
    return h('dl', { class: 'facts' }, items.reduce(function (nodes, item) {
      return nodes.concat([h('div', null, [h('dt', { text: item[0] }), h('dd', null, [item[1]])])]);
    }, []));
  }

  function notice(run, steps) {
    var retrying = steps.filter(function (step) { return step.status === 'retrying'; })[0];
    if (run.status === 'sleeping' && retrying && run.wakeAt) {
      return h('div', { class: 'notice hold', role: 'note' }, [
        h('p', { text: 'Retrying ' + retrying.name + ' ' + ago(run.wakeAt) + ', after ' + retrying.attempts + (retrying.attempts === 1 ? ' failed attempt.' : ' failed attempts.') }),
        retrying.error ? h('div', { class: 'code', text: retrying.error }) : null
      ]);
    }
    if (run.status === 'failed' && run.error) {
      return h('div', { class: 'notice stop', role: 'note' }, [
        h('p', { text: 'Failed at ' + run.error.stepName + ': ' + run.error.message }),
        run.error.code ? h('div', { class: 'code', text: run.error.code }) : null
      ]);
    }
    if (run.status === 'waiting') {
      return h('div', { class: 'notice hold', role: 'note' }, [h('p', {
        text: 'Waiting for the "' + (run.waitingFor || '') + '" event' + (run.wakeAt ? '. Times out ' + absolute(run.wakeAt) + '.' : '. No timeout.')
      })]);
    }
    if (run.status === 'sleeping' && run.wakeAt) {
      return h('div', { class: 'notice hold', role: 'note' }, [h('p', { text: 'Paused until ' + absolute(run.wakeAt) + '.' })]);
    }
    return null;
  }

  // Each step becomes a segment as wide as its share of the run's time, so
  // where a run spent itself — working, holding, or stopped — reads at a glance.
  function lane(run, steps) {
    var origin = run.createdAt;
    var end = run.finishedAt || Date.now();
    var parts = [];
    var cursor = run.startedAt || end;
    if (run.startedAt) parts.push({ kind: 'queued', label: 'Queued', from: origin, to: run.startedAt });
    steps.forEach(function (step) {
      var from = step.startedAt || step.createdAt;
      var to = HOLDS[step.status] ? Math.max(from, Math.min(end, Date.now())) : step.updatedAt;
      var part = { kind: step.status, label: step.name, from: from, to: to, attempts: step.attempts };
      // Branches of a parallel group share a start time: stack them, since
      // they ran side by side rather than one after another.
      var last = parts[parts.length - 1];
      if (last && step.startedAt && last.from === step.startedAt && last.kind !== 'queued') {
        if (last.kind !== 'group') {
          parts[parts.length - 1] = last = { kind: 'group', label: 'Parallel', from: last.from, to: last.to, members: [last] };
        }
        last.members.push(part);
        last.to = Math.max(last.to, to);
      } else {
        parts.push(part);
      }
      cursor = Math.max(cursor, to);
    });
    if (run.status === 'running') parts.push({ kind: 'running', label: 'In progress', from: cursor, to: Date.now(), live: true });

    var total = Math.max(1, end - origin);
    function describe(part) {
      return part.label + ', ' + (STATUS_LABEL[part.kind] || part.kind) + ', ' + duration(Math.max(0, part.to - part.from)) + (part.attempts > 1 ? ', ' + part.attempts + ' attempts' : '');
    }
    var segments = parts.map(function (part) {
      var span = Math.max(0, part.to - part.from);
      if (part.kind === 'group') {
        var group = h('div', { class: 'seg-group', title: part.members.map(describe).join('\n') }, part.members.map(function (member) {
          var bar = h('div', { class: 'seg thin c-' + member.kind + (member.kind === 'failed' ? ' failed' : '') });
          bar.style.width = Math.max(8, 100 * (member.to - member.from) / Math.max(1, span)) + '%';
          return bar;
        }));
        group.style.flexGrow = String(Math.max(span / total, 0.015));
        group.style.flexBasis = '0';
        return group;
      }
      var classes = 'seg c-' + part.kind;
      if (part.kind === 'queued') classes = 'seg queued';
      else if (HOLDS[part.kind]) classes += ' hold';
      else if (part.kind === 'skipped') classes += ' skipped';
      else if (part.kind === 'failed') classes += ' failed';
      if (part.live) classes += ' live-now';
      var el = h('div', { class: classes, title: describe(part) });
      el.style.flexGrow = String(Math.max(span / total, 0.015));
      el.style.flexBasis = '0';
      for (var i = 1; i < (part.attempts || 0); i++) {
        var tick = h('span', { class: 'tick' });
        tick.style.left = (100 * i / part.attempts) + '%';
        el.appendChild(tick);
      }
      return el;
    });

    return h('div', null, [
      h('div', { class: 'lane', role: 'img', 'aria-label': parts.map(function (part) {
        return part.kind === 'group' ? part.members.map(describe).join('; ') : describe(part);
      }).join('; ') }, segments),
      h('div', { class: 'lane-scale' }, [
        h('span', { text: 'Enqueued' }),
        h('span', { text: run.finishedAt ? duration(total) : duration(total) + ' so far' + (run.wakeAt ? ', resumes ' + ago(run.wakeAt) : '') })
      ])
    ]);
  }

  function stepTable(steps) {
    if (steps.length === 0) return h('p', { class: 'empty', text: 'No step has run yet.' });
    return h('table', { class: 'steps' }, [
      h('thead', null, [h('tr', null, [
        h('th', { text: 'Step' }), h('th', { text: 'Status' }), h('th', { class: 'num', text: 'Attempts' }), h('th', { class: 'num', text: 'Took' })
      ])]),
      h('tbody', null, steps.map(function (step) {
        var took = step.startedAt && !HOLDS[step.status] ? duration(step.updatedAt - step.startedAt) : '';
        return h('tr', null, [
          h('td', null, [h('span', { class: 'name', text: step.name }), step.error ? h('span', { class: 'err', text: step.error }) : null]),
          h('td', { class: 'c-' + step.status, text: STATUS_LABEL[step.status] || step.status }),
          h('td', { class: 'num', text: String(step.attempts) }),
          h('td', { class: 'num', text: took })
        ]);
      }))
    ]);
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

  function renderSchedules(schedules) {
    if (schedules.length === 0) {
      app.replaceChildren(h('div', { class: 'panel' }, [h('div', { class: 'empty' }, [
        h('strong', { text: 'No schedules registered' }),
        'Schedules registered with engine.schedule() appear here.'
      ])]));
      return;
    }
    app.replaceChildren(h('div', { class: 'panel' }, [h('table', { class: 'sched' }, [
      h('thead', null, [h('tr', null, [h('th', { text: 'Workflow' }), h('th', { text: 'Repeats' }), h('th', { text: 'Next run' }), h('th', { text: 'Key' })])]),
      h('tbody', null, schedules.map(function (schedule) {
        var repeats = schedule.recurrence.kind === 'cron'
          ? h('code', { text: schedule.recurrence.expression + ' (UTC)' })
          : 'Every ' + duration(schedule.recurrence.intervalMs);
        return h('tr', null, [
          h('td', { text: schedule.workflow }),
          h('td', null, [repeats]),
          h('td', null, [time(schedule.nextRunAt, false), ' (' + ago(schedule.nextRunAt) + ')']),
          h('td', null, [h('code', { text: schedule.key })])
        ]);
      }))
    ])]));
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

  window.addEventListener('hashchange', function () {
    state.detail = null;
    refresh();
  });
  refresh().then(connect);
})();
`

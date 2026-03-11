const copyButtons = document.querySelectorAll('[data-copy]')
const copyBlockButtons = document.querySelectorAll('[data-copy-block]')

const codeKeywords = new Set([
  'const',
  'await',
  'async',
  'return',
  'new',
])

const codeTypes = new Set([
  'createWorkflow',
  'createEngine',
  'SQLiteStorage',
  'z',
  'fetchPage',
  'llm',
  'db',
  'engine',
  'pipeline',
  'entities',
])

enhanceCodeBlocks()
bindCopyButtons()
setupInstallPicker()
setupCrashDemo()
setupHeroOutput()
setupDocsSidebar()

function bindCopyButtons() {
  for (const button of copyButtons) {
    button.addEventListener('click', async () => {
      const text = button.getAttribute('data-copy')
      if (!text) return
      await copyText(button, text)
    })
  }

  for (const button of copyBlockButtons) {
    button.addEventListener('click', async () => {
      const targetId = button.getAttribute('data-copy-block')
      if (!targetId) return

      const target = document.getElementById(targetId)
      if (!target) return

      const text = target.dataset.rawCode ?? target.textContent ?? ''
      await copyText(button, text)
    })
  }
}

function enhanceCodeBlocks() {
  const blocks = document.querySelectorAll('.code-block')

  for (const block of blocks) {
    const code = block.querySelector('code')
    if (!code) continue

    const rawCode = normalizeCode(code.textContent ?? '')
    const showLineNumbers = block.dataset.lineNumbers !== 'false'

    block.dataset.rawCode = rawCode

    if (block.dataset.compact === 'true') {
      block.classList.add('is-compact')
    }

    const fragment = document.createDocumentFragment()
    const lines = rawCode.split('\n')

    for (let index = 0; index < lines.length; index++) {
      const line = lines[index]
      const lineElement = document.createElement('span')
      lineElement.className = 'code-line'

      if (showLineNumbers) {
        const numberElement = document.createElement('span')
        numberElement.className = 'code-line-no'
        numberElement.textContent = String(index + 1)
        lineElement.append(numberElement)
      }

      const contentElement = document.createElement('span')
      contentElement.className = 'code-line-content'
      // highlightTypeScript only processes hardcoded token sets from this file,
      // not user-supplied content, so the resulting HTML is safe to assign.
      contentElement.innerHTML = line.length === 0 ? '&nbsp;' : highlightTypeScript(line)
      lineElement.append(contentElement)
      fragment.append(lineElement)
    }

    code.replaceChildren(fragment)
  }
}

function normalizeCode(text) {
  return text.replace(/^\n+/, '').replace(/\n+$/, '')
}

function highlightTypeScript(line) {
  let index = 0
  let html = ''

  while (index < line.length) {
    const character = line[index]

    if (line.startsWith('//', index)) {
      html += wrapToken(escapeHtml(line.slice(index)), 'token-comment')
      break
    }

    if (character === "'" || character === '"') {
      const value = readString(line, index, character)
      html += wrapToken(escapeHtml(value.text), 'token-string')
      index = value.nextIndex
      continue
    }

    if (isDigit(character)) {
      const value = readNumber(line, index)
      html += wrapToken(escapeHtml(value.text), 'token-number')
      index = value.nextIndex
      continue
    }

    if (isIdentifierStart(character)) {
      const value = readIdentifier(line, index)
      const nextNonWhitespace = nextNonWhitespaceCharacter(line, value.nextIndex)

      if (codeKeywords.has(value.text)) {
        html += wrapToken(escapeHtml(value.text), 'token-keyword')
      } else if (nextNonWhitespace === ':') {
        html += wrapToken(escapeHtml(value.text), 'token-property')
      } else if (nextNonWhitespace === '(') {
        html += wrapToken(escapeHtml(value.text), codeTypes.has(value.text) ? 'token-type' : 'token-function')
      } else if (codeTypes.has(value.text)) {
        html += wrapToken(escapeHtml(value.text), 'token-type')
      } else {
        html += escapeHtml(value.text)
      }

      index = value.nextIndex
      continue
    }

    if (line.startsWith('=>', index)) {
      html += wrapToken('=&gt;', 'token-operator')
      index += 2
      continue
    }

    if ('{}()[].,'.includes(character)) {
      html += wrapToken(escapeHtml(character), 'token-operator')
      index += 1
      continue
    }

    html += escapeHtml(character)
    index += 1
  }

  return html
}

function wrapToken(text, className) {
  return `<span class="${className}">${text}</span>`
}

function escapeHtml(text) {
  return text
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
}

function readString(text, startIndex, quote) {
  let index = startIndex + 1

  while (index < text.length) {
    if (text[index] === '\\') {
      index += 2
      continue
    }

    if (text[index] === quote) {
      index += 1
      break
    }

    index += 1
  }

  return { text: text.slice(startIndex, index), nextIndex: index }
}

function readNumber(text, startIndex) {
  let index = startIndex

  while (index < text.length && /[\d_]/.test(text[index])) {
    index += 1
  }

  return { text: text.slice(startIndex, index), nextIndex: index }
}

function readIdentifier(text, startIndex) {
  let index = startIndex + 1

  while (index < text.length && /[\w$]/.test(text[index])) {
    index += 1
  }

  return { text: text.slice(startIndex, index), nextIndex: index }
}

function nextNonWhitespaceCharacter(text, startIndex) {
  let index = startIndex

  while (index < text.length && /\s/.test(text[index])) {
    index += 1
  }

  return text[index] ?? ''
}

function isDigit(character) {
  return /\d/.test(character)
}

function isIdentifierStart(character) {
  return /[A-Za-z_$]/.test(character)
}

async function copyText(button, text) {
  const original = button.textContent

  try {
    await navigator.clipboard.writeText(text)
    button.textContent = 'Copied'
  } catch {
    button.textContent = 'Copy failed'
  }

  await wait(1400)
  button.textContent = original
}

function setupInstallPicker() {
  const tabs = document.getElementById('install-tabs')
  const cmdEl = document.getElementById('install-cmd')
  const copyBtn = document.getElementById('install-copy')
  if (!tabs || !cmdEl || !copyBtn) return

  const commands = {
    npm: 'npm install reflow-ts better-sqlite3',
    pnpm: 'pnpm add reflow-ts better-sqlite3',
    yarn: 'yarn add reflow-ts better-sqlite3',
    bun: 'bun add reflow-ts',
  }

  for (const tab of tabs.querySelectorAll('.install-tab')) {
    tab.addEventListener('click', () => {
      for (const t of tabs.querySelectorAll('.install-tab')) {
        t.classList.remove('is-active')
      }
      tab.classList.add('is-active')

      const pm = tab.dataset.pm
      const cmd = commands[pm] ?? commands.npm
      cmdEl.textContent = cmd
      copyBtn.setAttribute('data-copy', cmd)
    })
  }
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function setupCrashDemo() {
  const bad = {
    badge: document.getElementById('bad-badge'),
    outcome: document.getElementById('bad-outcome'),
    scrape: document.getElementById('bad-scrape'),
    summarize: document.getElementById('bad-summarize'),
    extract: document.getElementById('bad-extract'),
    store: document.getElementById('bad-store'),
  }

  const good = {
    badge: document.getElementById('good-badge'),
    outcome: document.getElementById('good-outcome'),
    scrape: document.getElementById('good-scrape'),
    summarize: document.getElementById('good-summarize'),
    extract: document.getElementById('good-extract'),
    store: document.getElementById('good-store'),
  }

  if (!bad.badge || !good.badge) return

  function setStep(el, cls, status) {
    el.className = 'crash-step ' + cls
    el.querySelector('.crash-step-status').textContent = status
  }

  function setBadge(el, text, cls) {
    el.textContent = text
    el.className = 'crash-demo-badge ' + cls
  }

  function setOutcome(el, text, cls) {
    el.textContent = text
    el.className = 'crash-outcome ' + cls
  }

  function runDemo() {
    // Phase 1: Both running — scrape done, summarize calling LLM
    setBadge(bad.badge, 'Running', 'is-running')
    setBadge(good.badge, 'Running', 'is-running')
    setStep(bad.scrape, 'is-complete', 'completed')
    setStep(bad.summarize, 'is-running', 'calling LLM...')
    setStep(bad.extract, 'is-pending', 'pending')
    setStep(bad.store, 'is-pending', 'pending')
    setStep(good.scrape, 'is-complete', 'completed')
    setStep(good.summarize, 'is-running', 'calling LLM...')
    setStep(good.extract, 'is-pending', 'pending')
    setStep(good.store, 'is-pending', 'pending')
    setOutcome(bad.outcome, '', '')
    setOutcome(good.outcome, '', '')

    setTimeout(() => {
      // Phase 2: summarize done, extract running
      setStep(bad.summarize, 'is-complete', 'completed 8s')
      setStep(bad.extract, 'is-running', 'calling LLM...')
      setStep(good.summarize, 'is-complete', 'completed 8s')
      setStep(good.extract, 'is-running', 'calling LLM...')
    }, 1800)

    setTimeout(() => {
      // Phase 3: extract done, store running
      setStep(bad.extract, 'is-complete', 'completed 6s')
      setStep(bad.store, 'is-running', 'writing...')
      setStep(good.extract, 'is-complete', 'completed 6s')
      setStep(good.store, 'is-running', 'writing...')
    }, 3200)

    setTimeout(() => {
      // Phase 4: Both crash during store
      setBadge(bad.badge, 'Crashed', 'is-crashed')
      setBadge(good.badge, 'Crashed', 'is-crashed')
      setStep(bad.store, 'is-crashed', 'process killed')
      setStep(good.store, 'is-crashed', 'process killed')
    }, 4400)

    setTimeout(() => {
      // Phase 5: Bad has to re-run everything from scratch
      setBadge(bad.badge, 'Retrying', 'is-crashed')
      setStep(bad.scrape, 'is-running', 're-running...')
      setStep(bad.summarize, 'is-pending', 'must re-run')
      setStep(bad.extract, 'is-pending', 'must re-run')
      setStep(bad.store, 'is-pending', 'pending')
      setOutcome(bad.outcome, 'Re-running 2 LLM calls. $0.20 + 14s wasted.', 'is-bad')

      // Good recovers from checkpoint — skips everything already done
      setBadge(good.badge, 'Recovering', 'is-recovering')
      setStep(good.scrape, 'is-complete', 'skipped (cached)')
      setStep(good.summarize, 'is-complete', 'skipped (cached)')
      setStep(good.extract, 'is-complete', 'skipped (cached)')
      setStep(good.store, 'is-running', 'retrying...')
    }, 6200)

    setTimeout(() => {
      // Phase 6: Good completes
      setBadge(good.badge, 'Complete', 'is-complete')
      setStep(good.store, 'is-complete', 'completed')
      setOutcome(good.outcome, 'Resumed from checkpoint. $0 re-spent.', 'is-good')
    }, 8000)

    // Restart
    setTimeout(runDemo, 12500)
  }

  runDemo()
}

function setupDocsSidebar() {
  const nav = document.querySelector('.docs-nav')
  if (!nav) return

  const links = Array.from(nav.querySelectorAll('a[href^="#"]'))
  const targets = links
    .map((link) => ({
      link,
      target: document.getElementById(link.getAttribute('href').slice(1)),
    }))
    .filter((entry) => entry.target)

  if (targets.length === 0) return

  const observer = new IntersectionObserver(
    (entries) => {
      for (const entry of entries) {
        if (entry.isIntersecting) {
          for (const { link } of targets) link.classList.remove('is-active')
          const match = targets.find((t) => t.target === entry.target)
          if (match) match.link.classList.add('is-active')
        }
      }
    },
    { rootMargin: '-80px 0px -60% 0px', threshold: 0 },
  )

  for (const { target } of targets) observer.observe(target)
}

function setupHeroOutput() {
  const output = document.getElementById('hero-output')
  if (!output) return

  const lines = [
    '<span class="out-ts">12:00:01</span>  <span class="out-dim">engine</span>  started <span class="out-dim">polling=1s</span>',
    '<span class="out-ts">12:00:01</span>  <span class="out-name">pipeline</span>  step <span class="out-ok">scrape</span> <span class="out-dim">done 220ms</span>',
    '<span class="out-ts">12:00:04</span>  <span class="out-name">pipeline</span>  step <span class="out-ok">summarize</span> <span class="out-dim">done 3.2s</span>',
    '<span class="out-ts">12:00:04</span>  <span class="out-name">pipeline</span>  step <span class="out-ok">store</span> <span class="out-dim">done 48ms</span>',
  ]

  let current = 0

  function addLine() {
    if (current >= lines.length) {
      // Restart after a pause
      setTimeout(() => {
        output.innerHTML = ''
        current = 0
        setTimeout(addLine, 600)
      }, 4000)
      return
    }

    const div = document.createElement('div')
    div.innerHTML = lines[current]
    output.append(div)
    current++
    setTimeout(addLine, current === 1 ? 400 : 280 + Math.random() * 200)
  }

  // Start after a short delay so the page loads first
  setTimeout(addLine, 1200)
}

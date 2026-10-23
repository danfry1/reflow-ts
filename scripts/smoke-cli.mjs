/**
 * Runs the built `reflow-ts dashboard` command against a real database file
 * and checks it serves the API, so the published bin is exercised end to end
 * rather than only through its imports. Run after `bun run build`.
 */
import { spawn } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createEngine, createWorkflow } from '../dist/index.mjs'
import { SQLiteStorage } from '../dist/storage/sqlite-node.mjs'

const dir = mkdtempSync(join(tmpdir(), 'reflow-cli-'))
const path = join(dir, 'smoke.db')

const passthrough = { '~standard': { version: 1, vendor: 'smoke', validate: (value) => ({ value }) } }
const workflow = createWorkflow({ name: 'smoke', input: passthrough }).step('only', async () => ({ ok: true }))
const storage = new SQLiteStorage(path)
const engine = createEngine({ storage, workflows: [workflow] })
await storage.initialize()
await engine.enqueue('smoke', {})
await engine.tick()
storage.close()

const cli = spawn(process.execPath, ['dist/cli.mjs', 'dashboard', path, '--port', '0'], { stdio: ['ignore', 'pipe', 'inherit'] })
try {
  const url = await new Promise((resolve, reject) => {
    let output = ''
    cli.stdout.on('data', (chunk) => {
      output += chunk
      const match = /http:\/\/[^\s]+\//.exec(output)
      if (match) resolve(match[0])
    })
    cli.once('exit', (code) => reject(new Error(`CLI exited early with code ${code}`)))
  })

  const overview = await (await fetch(`${url}api/overview`)).json()
  if (overview.counts.completed !== 1 || overview.actions !== false) {
    throw new Error(`unexpected overview: ${JSON.stringify(overview)}`)
  }
  const page = await fetch(url)
  if (page.status !== 200 || !(await page.text()).includes('<main id="app"')) {
    throw new Error(`page did not render (status ${page.status})`)
  }
  console.log('OK: reflow-ts dashboard serves a database read-only')
} finally {
  cli.kill()
  rmSync(dir, { recursive: true, force: true })
}

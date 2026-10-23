/**
 * Runs the built `reflow-ts dashboard` command against a real database file
 * and checks it serves the API, so the published bin is exercised end to end
 * rather than only through its imports. Run after `bun run build`.
 */
import { spawn } from 'node:child_process'
import { createHash } from 'node:crypto'
import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { request } from 'node:http'
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
const digest = () => createHash('sha256').update(readFileSync(path)).digest('hex')
const before = digest()

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
  // A page re-pointing its own domain at this address sends its own Host.
  const rebound = await new Promise((resolve, reject) => {
    const { port } = new URL(url)
    request({ host: '127.0.0.1', port, path: '/api/runs', headers: { host: `attacker.example:${port}` } }, (res) => {
      res.resume()
      resolve(res.statusCode)
    }).on('error', reject).end()
  })
  if (rebound !== 421) {
    throw new Error(`a request for another host got ${rebound}, not 421`)
  }

  if (digest() !== before) {
    throw new Error('serving the dashboard changed the database file')
  }
  console.log('OK: reflow-ts dashboard serves a database read-only, untouched, to its own host only')
} finally {
  cli.kill()
  rmSync(dir, { recursive: true, force: true })
}

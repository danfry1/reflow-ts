/**
 * Rewrites the contributors list at the bottom of README.md.
 *
 * Anyone who has opened an issue or a pull request on the repository counts,
 * listed in the order they first did. The maintainer and bots are left out.
 * The list sits between the `contributors:start` and `contributors:end`
 * markers; everything else in the README is left as it is.
 *
 * Run with `bun run contributors`. Set GITHUB_TOKEN to avoid the
 * unauthenticated rate limit.
 */
import { readFile, writeFile } from 'node:fs/promises'
import { z } from 'zod'

const REPO = 'danfry1/reflow-ts'
const MAINTAINER = 'danfry1'
const README = new URL('../README.md', import.meta.url)
const START = '<!-- contributors:start -->'
const END = '<!-- contributors:end -->'

const Item = z.object({
  number: z.number(),
  created_at: z.string(),
  user: z.object({ login: z.string(), type: z.string() }),
  pull_request: z.unknown().optional(),
})

interface Contributor {
  login: string
  first: string
  issues: number
  pulls: number
}

async function fetchItems(): Promise<z.infer<typeof Item>[]> {
  const items: z.infer<typeof Item>[] = []
  const headers: Record<string, string> = { accept: 'application/vnd.github+json' }
  if (process.env.GITHUB_TOKEN) headers.authorization = `Bearer ${process.env.GITHUB_TOKEN}`
  for (let page = 1; ; page++) {
    const response = await fetch(
      `https://api.github.com/repos/${REPO}/issues?state=all&per_page=100&page=${page}`,
      { headers },
    )
    if (!response.ok) throw new Error(`GitHub responded ${response.status}: ${await response.text()}`)
    const batch = z.array(Item).parse(await response.json())
    items.push(...batch)
    if (batch.length < 100) return items
  }
}

function plural(count: number, word: string): string {
  return `${count} ${word}${count === 1 ? '' : 's'}`
}

function render(contributors: Contributor[]): string {
  if (contributors.length === 0) return 'None yet. Open an issue or a pull request to be the first.'
  const cells = contributors.map((c) => {
    const kinds = [c.issues > 0 ? plural(c.issues, 'issue') : '', c.pulls > 0 ? plural(c.pulls, 'pull request') : '']
      .filter(Boolean)
      .join(', ')
    const filter = `https://github.com/search?q=repo%3A${encodeURIComponent(REPO)}+author%3A${c.login}&type=issues`
    return [
      '    <td align="center" valign="top" width="120">',
      `      <a href="https://github.com/${c.login}"><img src="https://github.com/${c.login}.png?size=128" width="64" height="64" alt="${c.login}" /><br /><sub><b>${c.login}</b></sub></a><br />`,
      `      <sub><a href="${filter}">${kinds}</a></sub>`,
      '    </td>',
    ].join('\n')
  })
  const rows: string[] = []
  for (let i = 0; i < cells.length; i += 6) rows.push(`  <tr>\n${cells.slice(i, i + 6).join('\n')}\n  </tr>`)
  return `<table>\n${rows.join('\n')}\n</table>`
}

const byLogin = new Map<string, Contributor>()
for (const item of await fetchItems()) {
  const { login, type } = item.user
  if (login === MAINTAINER || type === 'Bot') continue
  const entry = byLogin.get(login) ?? { login, first: item.created_at, issues: 0, pulls: 0 }
  if (item.pull_request === undefined) entry.issues++
  else entry.pulls++
  if (item.created_at < entry.first) entry.first = item.created_at
  byLogin.set(login, entry)
}
const contributors = [...byLogin.values()].sort((a, b) => a.first.localeCompare(b.first))

const readme = await readFile(README, 'utf8')
const start = readme.indexOf(START)
const end = readme.indexOf(END)
if (start === -1 || end < start) throw new Error(`README.md needs ${START} and ${END} markers`)
const next = `${readme.slice(0, start + START.length)}\n${render(contributors)}\n${readme.slice(end)}`
await writeFile(README, next)
// eslint-disable-next-line no-console
console.log(`README.md lists ${plural(contributors.length, 'contributor')}`)

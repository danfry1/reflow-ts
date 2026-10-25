/**
 * Writes website/public/llms.txt, the plain-text guide to Reflow for language
 * models (llmstxt.org), from README.md.
 *
 * The README is the reference that is kept current, so the file is built from
 * it rather than maintained beside it: the README's body, minus the logo
 * markup and the contributors list, under the llms.txt title and summary.
 * Runs before every docs build (`bun run build:website`).
 */
import { readFile, writeFile } from 'node:fs/promises'

const README = new URL('../README.md', import.meta.url)
const OUT = new URL('../website/public/llms.txt', import.meta.url)

const readme = await readFile(README, 'utf8')
const title = '<h1 align="center">Reflow</h1>\n'
const start = readme.indexOf(title)
const end = readme.indexOf('\n## Contributors')
if (start === -1 || end < start) throw new Error('README.md must have the Reflow title and a Contributors section')

const body = readme.slice(start + title.length, end).trim()
const [summary = '', ...rest] = body.split('\n\n')

await writeFile(OUT, `# Reflow\n\n> ${summary}\n\nDocs: https://reflow.danfry.dev\n\n${rest.join('\n\n')}\n`)
// eslint-disable-next-line no-console
console.log('website/public/llms.txt written from README.md')

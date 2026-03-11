import { cpSync, existsSync, mkdirSync, rmSync } from 'node:fs'
import { resolve } from 'node:path'

const sourceDir = resolve('docs/site/src')
const outputDir = resolve('docs/site/dist')

if (!existsSync(sourceDir)) {
  throw new Error(`Site source directory not found: ${sourceDir}`)
}

rmSync(outputDir, { recursive: true, force: true })
mkdirSync(outputDir, { recursive: true })
cpSync(sourceDir, outputDir, { recursive: true })

console.log(`Built site: ${outputDir}`)

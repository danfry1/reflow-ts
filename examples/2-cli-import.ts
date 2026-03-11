/**
 * Example 2: CLI Tool — CSV Import with Resume
 *
 * Scenario: You're building a CLI that imports data from a CSV into
 * multiple systems. The CSV has 10,000 rows, and the import involves:
 *   1. Parse and validate the CSV
 *   2. Batch-insert into your database
 *   3. Sync to a third-party CRM
 *   4. Generate a summary report
 *
 * This takes 10+ minutes. The user's laptop might sleep, the WiFi might
 * drop during the CRM sync, or they might Ctrl+C accidentally.
 * Without durability, they'd have to start over from scratch.
 *
 * Run: npx tsx examples/2-cli-import.ts
 */
import { createWorkflow, createEngine } from '../src/index'
import { SQLiteStorage } from '../src/storage/sqlite-node'
import { z } from 'zod'

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms))
}

// =====================================================
// ❌ WITHOUT REFLOW — The fragile script
// =====================================================
//
// async function importCsv(filePath: string) {
//   console.log('Parsing CSV...')
//   const records = await parseCsv(filePath)        // 30 seconds
//
//   console.log('Inserting into database...')
//   await batchInsert(records)                       // 2 minutes
//
//   console.log('Syncing to CRM...')
//   await syncToCrm(records)                         // 5 minutes — 💥 WiFi drops here
//
//   console.log('Generating report...')
//   await generateReport(records)                    // 1 minute
// }
//
// // After the crash:
// // - CSV is parsed ✓ (but the result is lost — it was in memory)
// // - DB has partial data ✓ (but you don't know which records)
// // - CRM has nothing ✗
// // - No report ✗
// //
// // Options:
// //   a) Re-run everything from scratch (wastes 2.5 minutes, might duplicate DB rows)
// //   b) Build manual checkpoint logic with state files
// //
// // Most people do (a) and add dedupe logic. That's fragile and slow.

// =====================================================
// ✅ WITH REFLOW — Resumable import
// =====================================================

const csvImportWorkflow = createWorkflow({
  name: 'csv-import',
  input: z.object({
    filePath: z.string(),
    targetTable: z.string(),
  }),
})
  .step('parse', async ({ input }) => {
    console.log(`  Parsing ${input.filePath}...`)
    await sleep(1000)

    // Simulate parsed records
    const records = Array.from({ length: 500 }, (_, i) => ({
      id: i + 1,
      name: `Record ${i + 1}`,
      email: `user${i + 1}@example.com`,
    }))

    console.log(`  ✓ Parsed ${records.length} records`)
    // The parsed result is persisted. If we crash after this,
    // we won't re-parse the CSV — we'll use the stored result.
    return { records, count: records.length }
  })
  .step('insert-db', async ({ prev, input }) => {
    console.log(`  Inserting ${prev.count} records into ${input.targetTable}...`)
    await sleep(1500)

    console.log(`  ✓ Inserted ${prev.count} records`)
    return { dbCount: prev.count, records: prev.records }
  })
  .step('sync-crm', {
    // CRM APIs are flaky — retry with backoff
    retry: { maxAttempts: 3, backoff: 'exponential', initialDelayMs: 1000 },
    handler: async ({ prev }) => {
      console.log(`  Syncing ${prev.dbCount} records to CRM...`)
      await sleep(2000)

      console.log(`  ✓ CRM sync complete`)
      return { crmCount: prev.dbCount }
    },
  })
  .step('generate-report', async ({ prev, input }) => {
    console.log('  Generating summary report...')
    await sleep(500)

    const report = {
      file: input.filePath,
      table: input.targetTable,
      recordsProcessed: prev.crmCount,
      completedAt: new Date().toISOString(),
    }

    console.log('  ✓ Report generated:')
    console.log(`    ${JSON.stringify(report, null, 2).split('\n').join('\n    ')}`)
    return report
  })

// =====================================================
// Run it like a CLI tool
// =====================================================

async function main() {
  const dbPath = '/tmp/reflow-cli-import.db'
  const storage = new SQLiteStorage(dbPath)
  await storage.initialize()

  const engine = createEngine({ storage, workflows: [csvImportWorkflow] })

  console.log('Starting CSV import...')
  console.log('(If you Ctrl+C mid-import and re-run, it resumes where it left off)\n')

  await engine.enqueue('csv-import', {
    filePath: './data/customers.csv',
    targetTable: 'customers',
  })

  // For a CLI, just tick until done rather than polling
  await engine.tick()

  console.log('\nImport complete!')
  storage.close()
}

main()

import { SQLiteStorage } from './src/storage/sqlite-node'
const s = new SQLiteStorage('/tmp/reflow-bench-probe.db')
await s.initialize()
console.log('better-sqlite3 under bun: OK')
s.close()

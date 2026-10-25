import { describe, expect, it } from 'vitest'
import { RecentReads } from '../recent'

describe('RecentReads', () => {
  it('shares a read within its TTL and reads again after it', async () => {
    const reads = new RecentReads<number>(1_000, 10)
    let calls = 0
    const read = async () => ++calls

    await reads.shared('k', read, 0)
    await reads.shared('k', read, 999)
    expect(calls).toBe(1)

    await reads.shared('k', read, 1_000)
    expect(calls).toBe(2)
  })

  it('stays bounded however many distinct keys are read', async () => {
    const reads = new RecentReads<number>(60_000, 8)

    for (let index = 0; index < 1_000; index++) {
      await reads.shared(`page-${index}`, async () => index, index)
    }

    expect(reads.size).toBe(8)
  })

  it('drops expired reads when a new one is kept', async () => {
    const reads = new RecentReads<number>(100, 50)
    for (let index = 0; index < 20; index++) {
      await reads.shared(`k${index}`, async () => index, 0)
    }

    await reads.shared('fresh', async () => 1, 500)

    expect(reads.size).toBe(1)
  })

  it('does not keep a failed read, so the next request tries again', async () => {
    const reads = new RecentReads<number>(60_000, 10)
    const failure = new Error('disk I/O error')

    await expect(reads.shared('k', async () => { throw failure }, 0)).rejects.toBe(failure)
    await new Promise((resolve) => setTimeout(resolve, 0))

    expect(await reads.shared('k', async () => 7, 1)).toBe(7)
  })
})
